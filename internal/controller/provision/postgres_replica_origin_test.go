package provision

import (
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/operation"
	"github.com/sirrobot01/synod/internal/store/postgres"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/proto"
)

func TestPostgresReplicaOriginRequiresOwnedUnambiguousHistory(t *testing.T) {
	for _, change := range []string{"unchanged", "stale_claim", "missing_member", "nil_member", "released_allocation", "changed_creation_revision", "primary", "removed", "wrong_engine", "no_plan", "bad_plan", "large_plan", "changed_node", "ambiguous_copy", "missing_copy"} {
		t.Run(change, func(t *testing.T) {
			var f *fixture
			if change == "wrong_engine" {
				f = running(t, "aof")
			} else {
				f = provisioned(t)
			}
			copyClaim := f.act(t, postgres.ScaleClusterAPIOperation)
			if err := f.planner.Plan(t.Context(), copyClaim); err != nil {
				t.Fatal(err)
			}
			plan := installedPlanFor(t, f, copyClaim.OperationID)
			var original *agentv1.Command
			if change != "wrong_engine" {
				original = plan.Steps[4].Command
			}
			members, err := f.store.ReadClusterMembers(t.Context(), copyClaim.InstallationID, copyClaim.ClusterID)
			if err != nil {
				t.Fatal(err)
			}
			var target, primary uuid.UUID
			for _, member := range members {
				if member.Role == "replica" {
					target = member.MemberID
				} else {
					primary = member.MemberID
				}
			}
			current := f.act(t, postgres.StopClusterAPIOperation)
			// The latest cluster operation has a different plan and must not become copy provenance.
			if err := f.planner.Plan(t.Context(), current); err != nil {
				t.Fatal(err)
			}
			switch change {
			case "stale_claim":
				current.ID = uuid.New()
			case "missing_member":
				target = uuid.New()
			case "nil_member":
				target = uuid.Nil
			case "released_allocation":
				if _, err := f.pool.Exec(t.Context(), `UPDATE resource_allocations SET released_at=clock_timestamp() WHERE resource_id=(SELECT resource_id FROM cluster_members WHERE member_id=$1)`, target); err != nil {
					t.Fatal(err)
				}
			case "changed_creation_revision":
				if _, err := f.pool.Exec(t.Context(), `UPDATE cluster_members SET created_revision=created_revision+1 WHERE member_id=$1`, target); err != nil {
					t.Fatal(err)
				}
			case "primary":
				target = primary
			case "removed":
				if _, err := f.pool.Exec(t.Context(), `UPDATE cluster_members SET removed_at=clock_timestamp() WHERE member_id=$1`, target); err != nil {
					t.Fatal(err)
				}
			case "no_plan":
				if _, err := f.pool.Exec(t.Context(), `UPDATE operations SET plan_bytes=NULL,plan_version=0 WHERE id=$1`, copyClaim.OperationID); err != nil {
					t.Fatal(err)
				}
			case "bad_plan", "large_plan", "changed_node", "ambiguous_copy", "missing_copy":
				if change == "changed_node" {
					node := uuid.NewString()
					for i, step := range plan.Steps {
						if i > 0 {
							step.Command.Epoch.NodeId = node
						}
					}
				}
				if change == "ambiguous_copy" {
					duplicate := proto.Clone(plan.Steps[4]).(*agentv1.PlanStep)
					duplicate.Command.CommandId, duplicate.Command.StepId = uuid.NewString(), uuid.NewString()
					hash, err := operation.CommandHash(duplicate.Command)
					if err != nil {
						t.Fatal(err)
					}
					duplicate.Command.InputHash = hash[:]
					plan.Steps = append(plan.Steps, duplicate)
				}
				if change == "missing_copy" {
					plan.Steps = plan.Steps[:4]
				}
				encoded, err := proto.Marshal(plan)
				if err != nil {
					t.Fatal(err)
				}
				if change == "bad_plan" {
					encoded = []byte{0xff}
				}
				if change == "large_plan" {
					encoded = make([]byte, 17<<20)
				}
				if _, err := f.pool.Exec(t.Context(), `UPDATE operations SET plan_bytes=$2 WHERE id=$1`, copyClaim.OperationID, encoded); err != nil {
					t.Fatal(err)
				}
			}
			config, cloneID, err := f.store.ReadPostgresReplicaClone(t.Context(), current, target)
			if change == "unchanged" {
				if err != nil || cloneID.String() != original.CommandId || !proto.Equal(config, original.Input.GetClonePostgresReplica().Postgres) {
					t.Fatalf("wrong provenance: %s %v", cloneID, err)
				}
				return
			}
			if err == nil || config != nil || cloneID != uuid.Nil {
				t.Fatalf("invalid provenance accepted: %s %v", cloneID, err)
			}
			if change == "stale_claim" && !errors.Is(err, operation.ErrStaleOwner) {
				t.Fatalf("stale provenance read: %v", err)
			}
			if change == "large_plan" && !errors.Is(err, postgres.ErrExecutionStateTooLarge) {
				t.Fatalf("unbounded provenance read: %v", err)
			}
		})
	}
}
