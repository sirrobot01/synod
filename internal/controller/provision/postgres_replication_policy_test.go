package provision

import (
	"strings"
	"testing"

	"github.com/sirrobot01/synod/internal/store/postgres"
)

func TestPostgresPlansPreserveReplicationPolicy(t *testing.T) {
	for _, policy := range []string{"", "asynchronous", "synchronous"} {
		t.Run(policy, func(t *testing.T) {
			spec := postgresSpec()
			spec.PostgreSQL.ReplicationPolicy = policy
			f := newFixture(t, spec)
			if _, err := f.planner.provision(t.Context(), f.claim); err != nil {
				t.Fatal(err)
			}
			primary := installedPlanFor(t, f, f.claim.OperationID)
			want := policy == "synchronous"
			if primary.Steps[1].Command.Input.GetEnsureContainer().Postgres.SynchronousReplication != want {
				t.Fatal("primary lost policy")
			}
			recovery := f.planFor(t, postgres.RecoverSetupAPIOperation)
			if recovery.Steps[0].Command.Input.GetRecoverPostgresSetup().Postgres.SynchronousReplication != want {
				t.Fatal("setup recovery lost policy")
			}
			claim := f.act(t, postgres.ScaleClusterAPIOperation)
			if err := f.planner.Plan(t.Context(), claim); err != nil {
				t.Fatal(err)
			}
			replica := installedPlanFor(t, f, claim.OperationID)
			if replica.Steps[2].Command.Input.GetEnsureContainer().Postgres.SynchronousReplication != want || replica.Steps[4].Command.Input.GetClonePostgresReplica().Postgres.SynchronousReplication != want {
				t.Fatal("replica lost policy")
			}
			if _, err := f.pool.Exec(t.Context(), `UPDATE operations SET status='failed',finished_at=clock_timestamp(),claim_active=false WHERE id=$1`, claim.OperationID); err != nil {
				t.Fatal(err)
			}
			retry := f.act(t, postgres.ScaleClusterAPIOperation)
			if err := f.planner.Plan(t.Context(), retry); err != nil {
				t.Fatal(err)
			}
			retried := installedPlanFor(t, f, retry.OperationID)
			if retried.Steps[4].Command.Input.GetClonePostgresReplica().Postgres.SynchronousReplication != want {
				t.Fatal("retry lost immutable policy")
			}
			if _, err := f.pool.Exec(t.Context(), `UPDATE operations SET status='failed',finished_at=clock_timestamp(),claim_active=false WHERE id=$1`, retry.OperationID); err != nil {
				t.Fatal(err)
			}
			for _, preview := range []bool{false, true} {
				method := f.store.PreviewClusterScaleDown
				if preview {
					method = f.store.PreviewFailedReplicaRemoval
				}
				plan, err := method(t.Context(), claim.InstallationID, claim.ClusterID, "integration")
				if err != nil || strings.Contains(plan.Downtime, "Commits wait") != want {
					t.Fatalf("removal availability: %q %v", plan.Downtime, err)
				}
			}
		})
	}
}
