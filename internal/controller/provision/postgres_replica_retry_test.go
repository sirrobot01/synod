package provision

import (
	"crypto/sha256"
	"errors"
	"reflect"
	"slices"
	"testing"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/operation"
	"github.com/sirrobot01/synod/internal/store/postgres"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/proto"
)

func TestPostgresReplicaRetryPreservesCopyAndAllocation(t *testing.T) {
	for _, test := range []struct {
		name, state, code            string
		effect, unknownEpoch, verify bool
	}{
		{name: "before_copy"},
		{name: "missing_result", verify: true},
		{name: "capacity_refused", state: "failed", code: "capacity_unavailable"},
		{name: "secret_refused", state: "failed", code: "secret_unavailable"},
		{name: "native_failure", state: "failed", code: "runtime_configuration_conflict", verify: true},
		{name: "effectful_capacity", state: "failed", code: "capacity_unavailable", effect: true, verify: true},
		{name: "cancelled", state: "cancelled", verify: true},
		{name: "succeeded", state: "succeeded", verify: true},
		{name: "another_epoch_unknown", state: "failed", code: "capacity_unavailable", unknownEpoch: true, verify: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := provisioned(t)
			claim := f.act(t, postgres.ScaleClusterAPIOperation)
			if err := f.planner.Plan(t.Context(), claim); err != nil {
				t.Fatal(err)
			}
			original := installedPlanFor(t, f, claim.OperationID)
			if test.name != "before_copy" {
				report := &agentv1.BarrierResult{Epoch: original.Steps[0].Command.Epoch, AdmissionsClosed: true, Drained: true}
				if err := f.store.RecordBarrier(t.Context(), claim, report); err != nil {
					t.Fatal(err)
				}
				encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(report)
				if err != nil {
					t.Fatal(err)
				}
				if err := f.store.ReconcileBarrier(t.Context(), claim, f.node, sha256.Sum256(encoded), "No prior effects remain."); err != nil {
					t.Fatal(err)
				}
				if err := f.store.ActivateClaim(t.Context(), claim); err != nil {
					t.Fatal(err)
				}
				if err := f.store.RecordDispatch(t.Context(), claim, original.Steps[4].Command); err != nil {
					t.Fatal(err)
				}
			}
			if test.state != "" {
				command := original.Steps[4].Command
				result := &agentv1.TerminalResult{Epoch: command.Epoch, CommandId: command.CommandId, InputHash: command.InputHash, Sequence: 1, State: test.state, FailureCode: test.code}
				if test.effect {
					result.Effects = []*agentv1.Effect{{CommandId: command.CommandId, ResourceId: command.Input.ResourceId, NativeOperationId: "docker-command:" + command.CommandId, State: "replica_refused"}}
				}
				if err := f.store.RecordAgentResult(t.Context(), claim.InstallationID, f.node, result); err != nil {
					t.Fatal(err)
				}
				if test.unknownEpoch {
					later := proto.Clone(command).(*agentv1.Command)
					later.Epoch.ExecutionEpoch++
					encoded, err := proto.Marshal(later)
					if err != nil {
						t.Fatal(err)
					}
					if _, err := f.pool.Exec(t.Context(), `INSERT INTO command_dispatches
 (command_id,execution_epoch,installation_id,cluster_id,operation_id,recovery_generation,node_id,step_id,input_hash,command_bytes)
 SELECT command_id,execution_epoch+1,installation_id,cluster_id,operation_id,recovery_generation,node_id,step_id,input_hash,$2
 FROM command_dispatches WHERE command_id=$1`, command.CommandId, encoded); err != nil {
						t.Fatal(err)
					}
				}
			}
			before, err := f.store.ReadPlacementHolds(t.Context(), claim.InstallationID)
			if err != nil {
				t.Fatal(err)
			}
			members, err := f.store.ReadClusterMembers(t.Context(), claim.InstallationID, claim.ClusterID)
			if err != nil {
				t.Fatal(err)
			}
			placement := f.decider.request.OperationID
			f.planner.config.StepTimeout *= 2
			f.planner.config.Budget.DiskBytes *= 2
			f.planner.config.EndpointAddress = "192.0.2.99"
			for attempt := range 3 {
				if _, err := f.pool.Exec(t.Context(), `UPDATE operations SET status='failed',claim_active=false WHERE id=$1`, claim.OperationID); err != nil {
					t.Fatal(err)
				}
				prior := claim.OperationID
				claim = f.act(t, postgres.ScaleClusterAPIOperation)
				if attempt == 1 {
					continue
				}
				if err := f.planner.Plan(t.Context(), claim); err != nil {
					t.Fatal(err)
				}
				retried := installedPlanFor(t, f, claim.OperationID)
				op, err := f.store.GetOperation(t.Context(), claim.InstallationID, claim.OperationID)
				if err != nil || op.RetryOf == nil || *op.RetryOf != prior {
					t.Fatalf("retry link: %+v %v", op, err)
				}
				if err := operation.ValidatePlan(retried, op); err != nil {
					t.Fatal(err)
				}
				indexes := []int{0, 1, 2, 3, 4, 5, 6, 7, 8}
				if test.verify {
					indexes = []int{0, 4, 5, 6, 7, 8}
				}
				if len(retried.Steps) != len(indexes) {
					t.Fatalf("steps=%d", len(retried.Steps))
				}
				for i, step := range retried.Steps {
					old := original.Steps[indexes[i]]
					want := proto.Clone(old.Command.Input).(*agentv1.CommandInput)
					if test.verify && i == 1 {
						want.Secrets = nil
						want.Payload = &agentv1.CommandInput_VerifyPostgresReplicaClone{VerifyPostgresReplicaClone: &agentv1.VerifyPostgresReplicaClone{Postgres: original.Steps[4].Command.Input.GetClonePostgresReplica().Postgres, CloneCommandId: original.Steps[4].Command.CommandId}}
					}
					if !proto.Equal(want, step.Command.Input) || step.Command.CommandId == old.Command.CommandId || step.Command.StepId == old.Command.StepId {
						t.Fatalf("step %d changed inputs or reused identity", i)
					}
					if i > 0 && !slices.Equal(step.DependsOn, []string{retried.Steps[i-1].Command.StepId}) {
						t.Fatal("retry permits concurrent copy effects")
					}
				}
			}
			after, err := f.store.ReadPlacementHolds(t.Context(), claim.InstallationID)
			if err != nil || len(before) != len(after) || f.decider.request.OperationID != placement {
				t.Fatalf("retry changed placement: %v", err)
			}
			for i, hold := range before {
				now := after[i]
				if hold.ReservationID != now.ReservationID || hold.ResourceID != now.ResourceID || hold.NodeID != now.NodeID || hold.OperationID != now.OperationID || hold.Budget != now.Budget || !slices.Equal(hold.Ports, now.Ports) {
					t.Fatal("retry changed reserved resources")
				}
			}
			afterMembers, err := f.store.ReadClusterMembers(t.Context(), claim.InstallationID, claim.ClusterID)
			if err != nil || !reflect.DeepEqual(members, afterMembers) {
				t.Fatalf("retry changed members: %v", err)
			}
			stale := claim
			stale.ID = uuid.New()
			if _, _, err := f.store.ReadScaleRetryPlan(t.Context(), stale); !errors.Is(err, operation.ErrStaleOwner) {
				t.Fatalf("stale retry read: %v", err)
			}
			latest := installedPlanFor(t, f, claim.OperationID)
			current := f.act(t, postgres.StopClusterAPIOperation)
			var target uuid.UUID
			for _, member := range members {
				if member.Role == "replica" {
					target = member.MemberID
				}
			}
			config, cloneID, err := f.store.ReadPostgresReplicaClone(t.Context(), current, target)
			expected := latest.Steps[4].Command.CommandId
			if test.verify {
				expected = latest.Steps[1].Command.Input.GetVerifyPostgresReplicaClone().CloneCommandId
			}
			if err != nil || cloneID.String() != expected || !proto.Equal(config, original.Steps[4].Command.Input.GetClonePostgresReplica().Postgres) {
				t.Fatalf("retained copy provenance: %s %v", cloneID, err)
			}

		})
	}
}

func TestPostgresReplicaRetryAfterPlanningFailure(t *testing.T) {
	f := provisioned(t)
	claim := f.act(t, postgres.ScaleClusterAPIOperation)
	if _, err := f.pool.Exec(t.Context(), `UPDATE operations SET status='failed',claim_active=false WHERE id=$1`, claim.OperationID); err != nil {
		t.Fatal(err)
	}
	claim = f.act(t, postgres.ScaleClusterAPIOperation)
	if err := f.planner.Plan(t.Context(), claim); err != nil {
		t.Fatal(err)
	}
	plan := installedPlanFor(t, f, claim.OperationID)
	if len(plan.Steps) != 9 || plan.Steps[4].Command.Input.GetClonePostgresReplica() == nil {
		t.Fatal("retry did not plan the first copy")
	}
}

func TestPostgresReplicaRetryRejectsChangedTopology(t *testing.T) {
	for _, change := range []string{"active_replica", "missing_replica", "primary_role", "replica_port"} {
		t.Run(change, func(t *testing.T) {
			f := provisioned(t)
			claim := f.act(t, postgres.ScaleClusterAPIOperation)
			if err := f.planner.Plan(t.Context(), claim); err != nil {
				t.Fatal(err)
			}
			if _, err := f.pool.Exec(t.Context(), `UPDATE operations SET status='failed',claim_active=false WHERE id=$1`, claim.OperationID); err != nil {
				t.Fatal(err)
			}
			claim = f.act(t, postgres.ScaleClusterAPIOperation)
			op, err := f.store.GetOperation(t.Context(), claim.InstallationID, claim.OperationID)
			if err != nil {
				t.Fatal(err)
			}
			members, err := f.planner.members(t.Context(), claim)
			if err != nil {
				t.Fatal(err)
			}
			var primary member
			for _, each := range members {
				if each.Role == "primary" {
					primary = each
				}
			}
			for i := range members {
				if members[i].Role != "replica" {
					continue
				}
				switch change {
				case "active_replica":
					members[i].State = "active"
				case "missing_replica":
					members[i].Role = "primary"
				case "replica_port":
					members[i].Port++
				}
			}
			if change == "primary_role" {
				primary.ResourceID = uuid.New()
			}
			if planned, err := f.planner.retryPostgresReplica(t.Context(), claim, op, primary, members); err == nil || planned {
				t.Fatalf("changed topology accepted: %v %v", planned, err)
			}
			current, err := f.store.GetOperation(t.Context(), claim.InstallationID, claim.OperationID)
			if err != nil || current.PlanVersion != 0 {
				t.Fatalf("failed retry installed a plan: %+v %v", current, err)
			}
		})
	}
}
