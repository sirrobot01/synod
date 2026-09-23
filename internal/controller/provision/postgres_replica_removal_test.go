package provision

import (
	"slices"
	"testing"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/cluster"
	"github.com/sirrobot01/synod/internal/store/postgres"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/proto"
)

func TestFailedPostgresReplicaRemovalKeepsOriginalCopyGuard(t *testing.T) {
	f := provisioned(t)
	copied := f.act(t, postgres.ScaleClusterAPIOperation)
	if err := f.planner.Plan(t.Context(), copied); err != nil {
		t.Fatal(err)
	}
	original := installedPlanFor(t, f, copied.OperationID).Steps[4].Command
	if _, err := f.pool.Exec(t.Context(), `UPDATE operations SET status='failed',finished_at=clock_timestamp(),claim_active=false WHERE id=$1`, copied.OperationID); err != nil {
		t.Fatal(err)
	}
	healthy, err := f.store.PreviewClusterScaleDown(t.Context(), copied.InstallationID, copied.ClusterID, "integration")
	if err != nil {
		t.Fatal(err)
	}
	preview, err := f.store.PreviewFailedReplicaRemoval(t.Context(), copied.InstallationID, copied.ClusterID, "integration")
	if err != nil || preview.Action != "scale_down_failed" || preview.ResourceID.String() != original.Input.ResourceId || preview.MemberID != healthy.MemberID || !slices.Equal(preview.RetainedVolumes, healthy.RetainedVolumes) {
		t.Fatalf("preview: %+v %v", preview, err)
	}
	claim := f.scaleDown(t, preview)
	if err := f.planner.Plan(t.Context(), claim); err != nil {
		t.Fatal(err)
	}
	plan := installedPlanFor(t, f, claim.OperationID)
	if len(plan.Steps) != 3 {
		t.Fatalf("steps=%d", len(plan.Steps))
	}
	proof := plan.Steps[0].Command.Input.GetVerifyPostgresReplicaFailure()
	if proof == nil || proof.CloneCommandId != original.CommandId || !proto.Equal(proof.Postgres, original.Input.GetClonePostgresReplica().Postgres) || len(plan.Steps[0].Command.Input.Secrets) != 0 || plan.Steps[0].Postcondition != agentv1.Postcondition_POSTCONDITION_POSTGRES_REPLICA_FAILED {
		t.Fatal("removal lost native failure proof")
	}
	if plan.Steps[1].Command.Input.GetStopContainer() == nil || plan.Steps[2].Command.Input.GetRemoveContainer() == nil {
		t.Fatal("unexpected effects")
	}
	for i, step := range plan.Steps {
		if step.Command.Input.ResourceId != original.Input.ResourceId || step.Command.Epoch.NodeId != original.Epoch.NodeId {
			t.Fatal("removal moved the target")
		}
		if i > 0 && !slices.Equal(step.DependsOn, []string{plan.Steps[i-1].Command.StepId}) {
			t.Fatal("removal can bypass native proof")
		}
	}
	// An explicit new review can retry a failed removal at the same desired count.
	if _, err := f.pool.Exec(t.Context(), `UPDATE operations SET status='failed',finished_at=clock_timestamp(),claim_active=false WHERE id=$1`, claim.OperationID); err != nil {
		t.Fatal(err)
	}
	again, err := f.store.PreviewFailedReplicaRemoval(t.Context(), copied.InstallationID, copied.ClusterID, "integration")
	if err != nil {
		t.Fatal(err)
	}
	retry := f.scaleDown(t, again)
	var retryOf uuid.UUID
	if err := f.pool.QueryRow(t.Context(), `SELECT retry_of FROM operations WHERE id=$1`, retry.OperationID).Scan(&retryOf); err != nil || retryOf != claim.OperationID {
		t.Fatalf("retry link: %s %v", retryOf, err)
	}
	if err := f.planner.Plan(t.Context(), retry); err != nil {
		t.Fatal(err)
	}
	retried := installedPlanFor(t, f, retry.OperationID).Steps[0].Command.Input.GetVerifyPostgresReplicaFailure()
	if !proto.Equal(proof, retried) {
		t.Fatal("removal retry changed copy identity")
	}
}

func TestFailedReplicaRemovalRefusesChangedReview(t *testing.T) {
	for _, change := range []string{"resource", "volumes", "member", "actor", "wrong_engine"} {
		t.Run(change, func(t *testing.T) {
			var f *fixture
			if change == "wrong_engine" {
				f = running(t, "aof")
			} else {
				f = provisioned(t)
			}
			copied := f.act(t, postgres.ScaleClusterAPIOperation)
			if err := f.planner.Plan(t.Context(), copied); err != nil {
				t.Fatal(err)
			}
			if _, err := f.pool.Exec(t.Context(), `UPDATE operations SET status='failed',finished_at=clock_timestamp(),claim_active=false WHERE id=$1`, copied.OperationID); err != nil {
				t.Fatal(err)
			}
			preview, err := f.store.PreviewFailedReplicaRemoval(t.Context(), copied.InstallationID, copied.ClusterID, "integration")
			if change == "wrong_engine" {
				if err == nil {
					t.Fatal("Redis accepted failed PostgreSQL removal")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			actor := "integration"
			var query string
			switch change {
			case "resource":
				query = `UPDATE cluster_action_plans SET resource_id=gen_random_uuid() WHERE plan_token=$1`
			case "volumes":
				query = `UPDATE cluster_action_plans SET retained_volumes=ARRAY[gen_random_uuid()] WHERE plan_token=$1`
			case "member":
				query = `UPDATE cluster_members SET removed_at=clock_timestamp() WHERE member_id=(SELECT member_id FROM cluster_action_plans WHERE plan_token=$1)`
			case "actor":
				actor = "another"
			}
			if query != "" {
				if _, err := f.pool.Exec(t.Context(), query, preview.PlanToken); err != nil {
					t.Fatal(err)
				}
			}
			_, err = f.store.AdmitClusterAction(t.Context(), postgres.ClusterActionRequest{
				Identity:  postgres.RequestIdentity{InstallationID: copied.InstallationID, ActorID: actor, APIOperation: postgres.ScaleClusterAPIOperation, IdempotencyKey: uuid.NewString()},
				RequestID: uuid.NewString(), ClusterID: copied.ClusterID, ExpectedRevision: preview.ExpectedRevision, Replicas: new(0),
				Confirmation: &cluster.ActionConfirmation{PlanToken: preview.PlanToken, Confirm: true, DataDisposition: "retain"},
			})
			if err == nil {
				t.Fatal("changed review admitted removal")
			}
		})
	}
}
