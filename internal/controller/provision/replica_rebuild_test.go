package provision

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/cluster"
	"github.com/sirrobot01/synod/internal/operation"
	"github.com/sirrobot01/synod/internal/store/postgres"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/proto"
)

func TestReplicaRebuildSettlesOnlyProvedEffects(t *testing.T) {
	for _, boundary := range []struct {
		proved    int
		cancelled bool
	}{{0, false}, {3, false}, {8, false}, {12, false}, {0, true}, {3, true}, {8, true}} {
		proved := boundary.proved
		t.Run(fmt.Sprintf("proved_%d_cancelled_%v", proved, boundary.cancelled), func(t *testing.T) {
			f := provisioned(t)
			copied := f.act(t, postgres.ScaleClusterAPIOperation)
			if err := f.planner.Plan(t.Context(), copied); err != nil {
				t.Fatal(err)
			}
			original := installedPlanFor(t, f, copied.OperationID)
			if _, err := f.pool.Exec(t.Context(), `UPDATE operations SET status='failed',claim_active=false WHERE id=$1`, copied.OperationID); err != nil {
				t.Fatal(err)
			}
			claim := f.act(t, postgres.RebuildReplicaAPIOperation)
			if err := f.planner.Plan(t.Context(), claim); err != nil {
				t.Fatal(err)
			}
			plan := installedPlanFor(t, f, claim.OperationID)
			if len(plan.Steps) != 12 || plan.Steps[0].Command.Input.GetVerifyPostgresReplicaFailure().CloneCommandId != original.Steps[4].Command.CommandId || plan.Steps[4].Command.Input.ResourceId == original.Steps[1].Command.Input.ResourceId || plan.Steps[5].Command.Input.ResourceId == original.Steps[2].Command.Input.ResourceId {
				t.Fatal("rebuild reused old copy or storage")
			}
			op, err := f.store.GetOperation(t.Context(), claim.InstallationID, claim.OperationID)
			if err != nil {
				t.Fatal(err)
			}
			if err := operation.ValidatePlan(plan, op); err != nil {
				t.Fatal(err)
			}
			if proved == 0 && !boundary.cancelled {
				for name, change := range map[string]func(*agentv1.ExecutionPlan){
					"source_port": func(p *agentv1.ExecutionPlan) { p.Steps[3].Command.Input.GetCheckReadiness().Port++ },
					"native_port": func(p *agentv1.ExecutionPlan) { p.Steps[10].Command.Input.GetCheckReadiness().Port++ },
					"app_port":    func(p *agentv1.ExecutionPlan) { p.Steps[11].Command.Input.GetCheckReadiness().Port++ },
					"initialization": func(p *agentv1.ExecutionPlan) {
						p.Steps[9].Command.Input.GetStartContainer().AllowInitialization = true
					},
					"old_identity":        func(p *agentv1.ExecutionPlan) { p.Steps[1].Command.Input.ResourceId = uuid.NewString() },
					"copy_before_removal": func(p *agentv1.ExecutionPlan) { p.Steps[3].DependsOn = nil },
				} {
					t.Run(name, func(t *testing.T) {
						bad := proto.Clone(plan).(*agentv1.ExecutionPlan)
						change(bad)
						for _, step := range bad.Steps {
							hash, err := operation.CommandHash(step.Command)
							if err != nil {
								t.Fatal(err)
							}
							step.Command.InputHash = hash[:]
						}
						if err := operation.ValidatePlan(bad, op); !errors.Is(err, operation.ErrUnsupportedPlan) {
							t.Fatal("unsafe rebuild plan accepted", err)
						}
					})
				}
			}
			report := &agentv1.BarrierResult{Epoch: plan.Steps[0].Command.Epoch, AdmissionsClosed: true, Drained: true}
			if err := f.store.RecordBarrier(t.Context(), claim, report); err != nil {
				t.Fatal(err)
			}
			encoded, err := proto.Marshal(report)
			if err != nil {
				t.Fatal(err)
			}
			if err := f.store.ReconcileBarrier(t.Context(), claim, f.node, sha256.Sum256(encoded), "No old native work remains."); err != nil {
				t.Fatal(err)
			}
			if err := f.store.ActivateClaim(t.Context(), claim); err != nil {
				t.Fatal(err)
			}
			for _, step := range plan.Steps[:proved] {
				command := step.Command
				if err := f.store.RecordDispatch(t.Context(), claim, command); err != nil {
					t.Fatal(err)
				}
				state, prefix := "present", "docker-command:"
				switch step.Postcondition {
				case agentv1.Postcondition_POSTCONDITION_POSTGRES_REPLICA_FAILED:
					state = "replica_refused"
				case agentv1.Postcondition_POSTCONDITION_CONTAINER_STOPPED:
					state = "exited"
				case agentv1.Postcondition_POSTCONDITION_CONTAINER_ABSENT:
					state = "absent"
				case agentv1.Postcondition_POSTCONDITION_ENGINE_READY:
					state, prefix = "ready", "readiness:"
				case agentv1.Postcondition_POSTCONDITION_CONTAINER_EXISTS:
					state = "created"
				case agentv1.Postcondition_POSTCONDITION_POSTGRES_REPLICA_PREPARED:
					state = "replica_prepared"
				case agentv1.Postcondition_POSTCONDITION_POSTGRES_REPLICA_CLONED:
					state = "replica_cloned"
				case agentv1.Postcondition_POSTCONDITION_CONTAINER_RUNNING:
					state = "running"
				}
				result := &agentv1.TerminalResult{Epoch: command.Epoch, CommandId: command.CommandId, InputHash: command.InputHash, Sequence: 1, State: "succeeded", Effects: []*agentv1.Effect{{CommandId: command.CommandId, ResourceId: command.Input.ResourceId, NativeOperationId: prefix + command.CommandId, State: state}}}
				if err := f.store.RecordAgentResult(t.Context(), claim.InstallationID, f.node, result); err != nil {
					t.Fatal(err)
				}
				encoded, err := proto.Marshal(result)
				if err != nil {
					t.Fatal(err)
				}
				if err := f.store.RecordStepCompletion(t.Context(), claim, uuid.MustParse(command.StepId), result.Epoch, sha256.Sum256(encoded)); err != nil {
					t.Fatal(err)
				}
			}
			status := operation.Failed
			if proved == 12 {
				status = operation.Succeeded
			}
			sequence := int64(1)
			if boundary.cancelled {
				if err := f.store.UpdateProgress(t.Context(), claim, postgres.ProgressUpdate{Sequence: 1, Status: operation.Cancelling, Reason: "Fixture cancellation."}); err != nil {
					t.Fatal(err)
				}
				sequence, status = 2, operation.Cancelled
			}
			if err := f.store.UpdateProgress(t.Context(), claim, postgres.ProgressUpdate{Sequence: sequence, Status: status, Reason: "Fixture terminal boundary."}); err != nil {
				t.Fatal(err)
			}
			members, err := f.store.ReadClusterMembers(t.Context(), claim.InstallationID, claim.ClusterID)
			if err != nil || len(members) != 2 {
				t.Fatalf("members: %+v %v", members, err)
			}
			var remaining postgres.Member
			for _, member := range members {
				if member.Role == "replica" {
					remaining = member
				}
			}
			want := plan.Steps[5].Command.Input.ResourceId
			if proved == 0 {
				want = original.Steps[2].Command.Input.ResourceId
			}
			if remaining.ResourceID.String() != want || (proved == 12 && remaining.State != "active") || (proved > 0 && proved < 12 && remaining.State != "residual") {
				t.Fatalf("residual target: %+v", remaining)
			}
			var volumes, holds int
			if err := f.pool.QueryRow(t.Context(), `SELECT count(*) FROM volume_identities WHERE cluster_id=$1 AND retention='retained'`, claim.ClusterID).Scan(&volumes); err != nil || volumes != 3 {
				t.Fatalf("volumes: %d %v", volumes, err)
			}
			if err := f.pool.QueryRow(t.Context(), `SELECT count(*) FROM resource_allocations WHERE cluster_id=$1 AND released_at IS NULL`, claim.ClusterID).Scan(&holds); err != nil || holds != 2 {
				t.Fatalf("holds: %d %v", holds, err)
			}
			if proved == 3 || proved == 8 {
				if proved == 8 && !boundary.cancelled {
					// A refused second rebuild discards its unused candidate, not the retained copy.
					refused := f.act(t, postgres.RebuildReplicaAPIOperation)
					if err := f.planner.Plan(t.Context(), refused); err != nil {
						t.Fatal(err)
					}
					unused := installedPlanFor(t, f, refused.OperationID)
					barrier := &agentv1.BarrierResult{Epoch: unused.Steps[0].Command.Epoch, AdmissionsClosed: true, Drained: true}
					if err := f.store.RecordBarrier(t.Context(), refused, barrier); err != nil {
						t.Fatal(err)
					}
					encoded, err := proto.Marshal(barrier)
					if err != nil {
						t.Fatal(err)
					}
					if err := f.store.ReconcileBarrier(t.Context(), refused, f.node, sha256.Sum256(encoded), "No native work remains."); err != nil {
						t.Fatal(err)
					}
					if err := f.store.ActivateClaim(t.Context(), refused); err != nil {
						t.Fatal(err)
					}
					if err := f.store.UpdateProgress(t.Context(), refused, postgres.ProgressUpdate{Sequence: 1, Status: operation.Failed, Reason: "The retained copy has no native failure."}); err != nil {
						t.Fatal(err)
					}
				}
				retry := f.act(t, postgres.ScaleClusterAPIOperation)
				if proved == 8 {
					_, originalID, err := f.store.ReadPostgresReplicaClone(t.Context(), retry, remaining.MemberID)
					if err != nil || originalID.String() != plan.Steps[7].Command.CommandId {
						t.Fatalf("refused rebuild replaced copy provenance: %s %v", originalID, err)
					}
				}
				if err := f.planner.Plan(t.Context(), retry); err != nil {
					t.Fatal(err)
				}
				retried := installedPlanFor(t, f, retry.OperationID)
				length := 9
				if proved == 8 {
					length = 6
				}
				if len(retried.Steps) != length || retried.Steps[len(retried.Steps)-1].Command.Input.ResourceId != remaining.ResourceID.String() {
					t.Fatal("retry lost replacement identity")
				}
			}
		})
	}
}

func TestReplicaRebuildRefusesChangedReview(t *testing.T) {
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
			preview, err := f.store.PreviewReplicaRebuild(t.Context(), copied.InstallationID, copied.ClusterID, "integration")
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
				Identity:  postgres.RequestIdentity{InstallationID: copied.InstallationID, ActorID: actor, APIOperation: postgres.RebuildReplicaAPIOperation, IdempotencyKey: uuid.NewString()},
				RequestID: uuid.NewString(), ClusterID: copied.ClusterID, ExpectedRevision: preview.ExpectedRevision,
				Confirmation: &cluster.ActionConfirmation{PlanToken: preview.PlanToken, Confirm: true, DataDisposition: "retain"},
			})
			if err == nil {
				t.Fatal("changed review admitted removal")
			}
		})
	}
}
