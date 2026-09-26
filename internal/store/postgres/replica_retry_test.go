package postgres

import (
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/cluster"
	"github.com/sirrobot01/synod/internal/operation"
)

func TestReplicaRetryAdmissionLinksOnlyTerminalAdds(t *testing.T) {
	for _, engine := range []cluster.Engine{cluster.PostgreSQL, cluster.Redis} {
		for _, state := range []operation.Status{operation.Pending, operation.Running, operation.Waiting, operation.Cancelling, operation.Succeeded, operation.Failed, operation.Cancelled} {
			t.Run(string(engine)+"/"+string(state), func(t *testing.T) {
				s := testStore(t)
				create := testRequest()
				if engine == cluster.Redis {
					create.Spec.Engine, create.Spec.PostgreSQL = cluster.Redis, nil
					create.Spec.Redis = &cluster.RedisSpec{Version: "8.2.7", Topology: "standalone", Persistence: "aof"}
				}
				created, err := s.CreateCluster(t.Context(), create)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := s.pool.Exec(t.Context(), `UPDATE operations SET status='succeeded' WHERE id=$1`, created.OperationID); err != nil {
					t.Fatal(err)
				}
				request := ClusterActionRequest{Identity: create.Identity, RequestID: uuid.NewString(),
					ClusterID: created.ClusterID, ExpectedRevision: 1, Replicas: new(1)}
				request.Identity.APIOperation, request.Identity.IdempotencyKey = ScaleClusterAPIOperation, uuid.NewString()
				first, err := s.AdmitClusterAction(t.Context(), request)
				// Standalone PostgreSQL has no replicas. Only Patroni has members to add.
				if engine == cluster.PostgreSQL {
					requireCode(t, err, UnsupportedSpec)
					return
				}
				if err != nil {
					t.Fatal(err)
				}
				if _, err := s.pool.Exec(t.Context(), `UPDATE operations SET status=$1 WHERE id=$2`, state, first.OperationID); err != nil {
					t.Fatal(err)
				}
				request.ExpectedRevision, request.Identity.IdempotencyKey = 2, uuid.NewString()
				retried, err := s.AdmitClusterAction(t.Context(), request)
				if state != operation.Failed && state != operation.Cancelled {
					want := OperationConflict
					if state == operation.Succeeded {
						want = InvalidRequest
					}
					if admission, ok := errors.AsType[*AdmissionError](err); !ok || admission.Code != want {
						t.Fatalf("state %s: retry error %v, want %s", state, err, want)
					}
					return
				}
				if err != nil || retried.DesiredRevision != 3 || retried.OperationID == first.OperationID {
					t.Fatalf("retry %+v: %v", retried, err)
				}
				op, err := s.GetOperation(t.Context(), create.Identity.InstallationID, retried.OperationID)
				if err != nil || op.RetryOf == nil || *op.RetryOf != first.OperationID {
					t.Fatalf("retry link %+v: %v", op, err)
				}
				replayed, err := s.AdmitClusterAction(t.Context(), request)
				if err != nil || !replayed.Replayed || replayed.OperationID != retried.OperationID {
					t.Fatalf("replay %+v: %v", replayed, err)
				}
				request.Identity.IdempotencyKey = uuid.NewString()
				if _, err := s.AdmitClusterAction(t.Context(), request); err == nil {
					t.Fatal("a retry with a stale revision was accepted")
				}
			})
		}
	}
}

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
