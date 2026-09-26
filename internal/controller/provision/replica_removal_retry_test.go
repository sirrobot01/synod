package provision

import (
	"crypto/sha256"
	"testing"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/operation"
	"github.com/sirrobot01/synod/internal/store/postgres"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/proto"
)

func TestRemovalRetryRequiresProvedDeletionAndCompletesMembership(t *testing.T) {
	for _, evidence := range []string{"missing", "failed", "no_effect", "wrong_state", "deleted"} {
		t.Run(evidence, func(t *testing.T) {
			f, _ := scaledUp(t)
			preview, err := f.store.PreviewClusterScaleDown(t.Context(), f.claim.InstallationID, f.claim.ClusterID, "integration")
			if err != nil {
				t.Fatal(err)
			}
			claim := f.scaleDown(t, preview)
			if err := f.planner.Plan(t.Context(), claim); err != nil {
				t.Fatal(err)
			}
			plan := installedPlanFor(t, f, claim.OperationID)
			last := plan.Steps[2].Command
			report := &agentv1.BarrierResult{Epoch: last.Epoch, AdmissionsClosed: true, Drained: true}
			if err := f.store.RecordBarrier(t.Context(), claim, report); err != nil {
				t.Fatal(err)
			}
			encoded, err := proto.Marshal(report)
			if err != nil {
				t.Fatal(err)
			}
			if err := f.store.ReconcileBarrier(t.Context(), claim, f.node, sha256.Sum256(encoded), "No prior effects remain."); err != nil {
				t.Fatal(err)
			}
			if err := f.store.ActivateClaim(t.Context(), claim); err != nil {
				t.Fatal(err)
			}
			if err := f.store.RecordDispatch(t.Context(), claim, last); err != nil {
				t.Fatal(err)
			}
			if evidence != "missing" {
				result := &agentv1.TerminalResult{Epoch: last.Epoch, CommandId: last.CommandId, InputHash: last.InputHash, Sequence: 1, State: "succeeded", Effects: []*agentv1.Effect{{CommandId: last.CommandId, ResourceId: last.Input.ResourceId, NativeOperationId: "docker-command:" + last.CommandId, State: "absent"}}}
				switch evidence {
				case "failed":
					result.State, result.FailureCode = "failed", "runtime_configuration_conflict"
				case "no_effect":
					result.Effects = nil
				case "wrong_state":
					result.Effects[0].State = "present"
				}
				if err := f.store.RecordAgentResult(t.Context(), claim.InstallationID, f.node, result); err != nil {
					t.Fatal(err)
				}
			}
			// Simulate a terminal failure after receipt storage but before metadata completion.
			for attempt := range 2 {
				if _, err := f.pool.Exec(t.Context(), `UPDATE operations SET status='failed',claim_active=false WHERE id=$1`, claim.OperationID); err != nil {
					t.Fatal(err)
				}
				again, err := f.store.PreviewClusterScaleDown(t.Context(), claim.InstallationID, claim.ClusterID, "integration")
				if err != nil {
					t.Fatal(err)
				}
				claim = f.scaleDown(t, again)
				if err := f.planner.Plan(t.Context(), claim); err != nil {
					t.Fatal(err)
				}
				retried := installedPlanFor(t, f, claim.OperationID)
				if evidence != "deleted" {
					if len(retried.Steps) != 3 || retried.Steps[0].Command.Input.GetCheckReadiness().GetRedisReplication().GetExpectedRole() != "replica" {
						t.Fatal("unproved deletion bypassed the native role check")
					}
					break
				}
				if len(retried.Steps) != 1 || retried.Steps[0].Command.Input.GetVerifyContainerAbsent() == nil || retried.Steps[0].Command.Input.ResourceId != last.Input.ResourceId || retried.Steps[0].Command.Epoch.NodeId != last.Epoch.NodeId {
					t.Fatal("proved deletion did not preserve bound absence check")
				}
				if attempt == 0 {
					continue
				}
				step := retried.Steps[0]
				command := step.Command
				report = &agentv1.BarrierResult{Epoch: command.Epoch, AdmissionsClosed: true, Drained: true}
				if err := f.store.RecordBarrier(t.Context(), claim, report); err != nil {
					t.Fatal(err)
				}
				encoded, err = proto.Marshal(report)
				if err != nil {
					t.Fatal(err)
				}
				if err := f.store.ReconcileBarrier(t.Context(), claim, f.node, sha256.Sum256(encoded), "No native effects remain."); err != nil {
					t.Fatal(err)
				}
				if err := f.store.ActivateClaim(t.Context(), claim); err != nil {
					t.Fatal(err)
				}
				if err := f.store.UpdateProgress(t.Context(), claim, postgres.ProgressUpdate{Sequence: 1, Status: operation.Succeeded, Reason: "Unproved absence."}); err == nil {
					t.Fatal("completed without fresh native proof")
				}
				if err := f.store.RecordDispatch(t.Context(), claim, command); err != nil {
					t.Fatal(err)
				}
				result := &agentv1.TerminalResult{Epoch: command.Epoch, CommandId: command.CommandId, InputHash: command.InputHash, Sequence: 1, State: "succeeded", Effects: []*agentv1.Effect{{CommandId: command.CommandId, ResourceId: command.Input.ResourceId, NativeOperationId: "docker-command:" + command.CommandId, State: "absent"}}}
				if err := f.store.RecordAgentResult(t.Context(), claim.InstallationID, f.node, result); err != nil {
					t.Fatal(err)
				}
				encoded, err = proto.Marshal(result)
				if err != nil {
					t.Fatal(err)
				}
				if err := f.store.RecordStepCompletion(t.Context(), claim, uuid.MustParse(command.StepId), result.Epoch, sha256.Sum256(encoded)); err != nil {
					t.Fatal(err)
				}
				if err := f.store.UpdateProgress(t.Context(), claim, postgres.ProgressUpdate{Sequence: 1, Status: operation.Succeeded, Reason: "Native absence is proved."}); err != nil {
					t.Fatal(err)
				}
				members, err := f.store.ReadClusterMembers(t.Context(), claim.InstallationID, claim.ClusterID)
				if err != nil || len(members) != 1 || members[0].Role != "primary" {
					t.Fatalf("membership: %+v %v", members, err)
				}
				var retained int
				if err := f.pool.QueryRow(t.Context(), `SELECT count(*) FROM volume_identities WHERE resource_id=ANY($1) AND retention='retained'`, preview.RetainedVolumes).Scan(&retained); err != nil || retained != len(preview.RetainedVolumes) {
					t.Fatalf("retained volumes: %d %v", retained, err)
				}
				var live int
				if err := f.pool.QueryRow(t.Context(), `SELECT count(*) FROM resource_allocations WHERE cluster_id=$1 AND released_at IS NULL`, claim.ClusterID).Scan(&live); err != nil || live != 1 {
					t.Fatalf("live holds: %d %v", live, err)
				}
			}
		})
	}
}
