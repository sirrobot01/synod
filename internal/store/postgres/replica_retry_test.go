package postgres

import (
	"testing"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/operation"
)

func TestFailedScaleRecordsResidualMembersWithItsTerminalState(t *testing.T) {
	for _, status := range []operation.Status{operation.Failed, operation.Cancelled} {
		t.Run(string(status), func(t *testing.T) {
			host, claim, _ := completionFixture(t)
			if _, err := host.store.pool.Exec(t.Context(), `UPDATE operations SET kind='cluster.scale' WHERE id=$1`, claim.OperationID); err != nil {
				t.Fatal(err)
			}
			for _, state := range []string{"pending", "active"} {
				if _, err := host.store.pool.Exec(t.Context(), `INSERT INTO cluster_members
 (installation_id,cluster_id,member_id,role,resource_id,state,created_operation_id,created_revision)
 VALUES ($1,$2,$3,'replica',$4,$5,$6,1)`, host.id, claim.ClusterID, uuid.New(), uuid.New(), state, claim.OperationID); err != nil {
					t.Fatal(err)
				}
			}
			sequence := int64(1)
			if status == operation.Cancelled {
				if err := host.store.UpdateProgress(t.Context(), claim, ProgressUpdate{Sequence: sequence, Status: operation.Cancelling, Reason: "Stop at a safe boundary."}); err != nil {
					t.Fatal(err)
				}
				sequence++
			}
			update := ProgressUpdate{Sequence: sequence, Status: status, Reason: "The replica add stopped."}
			for range 2 {
				if err := host.store.UpdateProgress(t.Context(), claim, update); err != nil {
					t.Fatal(err)
				}
			}
			members, err := host.store.ReadClusterMembers(t.Context(), host.id, claim.ClusterID)
			if err != nil {
				t.Fatal(err)
			}
			states := make(map[string]int)
			for _, member := range members {
				states[member.Role+":"+member.State]++
			}
			if states["primary:pending"] != 1 || states["replica:active"] != 1 || states["replica:residual"] != 1 {
				t.Fatalf("unexpected terminal membership: %v", states)
			}
		})
	}
}
