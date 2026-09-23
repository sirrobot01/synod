package provision

import (
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/cluster"
	"github.com/sirrobot01/synod/internal/operation"
	"github.com/sirrobot01/synod/internal/store/postgres"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
)

func TestPostgresStartOrdersIdentityAndApplicationChecks(t *testing.T) {
	f := provisioned(t)
	running, err := f.planner.members(t.Context(), f.claim)
	if err != nil {
		t.Fatal(err)
	}
	primary := running[0]
	// Replica admission remains closed. Supply a second member at the planner boundary.
	replica := member{MemberID: uuid.New(), ResourceID: uuid.New(), NodeID: uuid.New(), Role: "replica", Port: primary.Port + 1}
	found, err := f.store.GetCluster(t.Context(), f.claim.InstallationID, f.claim.ClusterID)
	if err != nil {
		t.Fatal(err)
	}
	claim := f.act(t, postgres.StartClusterAPIOperation)
	op, err := f.store.GetOperation(t.Context(), claim.InstallationID, claim.OperationID)
	if err != nil {
		t.Fatal(err)
	}
	steps, err := f.planner.startSteps(t.Context(), claim, op, found, []member{replica, primary})
	if err != nil {
		t.Fatal(err)
	}
	if len(steps) != 6 {
		t.Fatalf("steps=%d, want three per member", len(steps))
	}
	if err := operation.ValidatePlan(&agentv1.ExecutionPlan{Version: 1, Steps: steps}, op); err != nil {
		t.Fatal(err)
	}
	for i, each := range []member{primary, replica} {
		start, native, app := steps[3*i], steps[3*i+1], steps[3*i+2]
		if start.Command.Input.GetStartContainer() == nil || start.Command.Input.GetStartContainer().AllowInitialization {
			t.Fatal("start can replace data")
		}
		for _, step := range []*agentv1.PlanStep{start, native, app} {
			if step.Command.Input.ResourceId != each.ResourceID.String() || step.Command.Epoch.NodeId != each.NodeID.String() {
				t.Fatal("step lost member identity or placement")
			}
		}
		check := native.Command.Input.GetCheckReadiness()
		if check == nil || check.PostgresRole != each.Role || check.PostgresReplication == nil || check.Port != uint32(each.Port) || check.Role != "synod_admin" || check.Database != "postgres" || len(native.Command.Input.Secrets) != 1 || native.Command.Input.Secrets[0].Purpose != postgres.SuperuserCredential {
			t.Fatal("native proof lacks the member role or admin reference")
		}
		if each.Role == "replica" {
			policy := check.PostgresReplication
			if policy.Source == nil || policy.Source.ResourceId != primary.ResourceID.String() || policy.Source.Address != "192.0.2.10" || policy.Source.Port != uint32(primary.Port) || policy.MaxReplayLagBytes != 16<<20 || policy.MaxMessageAge.AsDuration() != 30*time.Second {
				t.Fatal("replica proof lost source identity or bounded policy")
			}
		} else if check.PostgresReplication.Source != nil {
			t.Fatal("primary has an upstream")
		}
		check = app.Command.Input.GetCheckReadiness()
		if check == nil || check.Role != "synod_app" || check.Database != "app" || check.PostgresRole != each.Role || check.PostgresReplication != nil || len(app.Command.Input.Secrets) != 1 || app.Command.Input.Secrets[0].Purpose != postgres.ApplicationCredential {
			t.Fatal("application access was replaced by admin access")
		}
	}
	for i, step := range steps {
		if i > 0 && (len(step.DependsOn) != 1 || step.DependsOn[0] != steps[i-1].Command.StepId) {
			t.Fatal("member effects can race or precede the required proof")
		}
	}
	for name, members := range map[string][]member{
		"no primary": {replica}, "two primaries": {primary, primary}, "unknown role": {primary, {MemberID: uuid.New(), Role: "leader"}},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := f.planner.startSteps(t.Context(), claim, op, found, slices.Clone(members)); err == nil {
				t.Fatal("invalid topology produced a plan")
			}
		})
	}
	if _, err := f.pool.Exec(t.Context(), `UPDATE node_placement_policies SET policy_bytes=convert_to((convert_from(policy_bytes,'UTF8')::jsonb - 'client_address')::text,'UTF8') WHERE installation_id=$1 AND node_id=$2`, claim.InstallationID, primary.NodeID); err != nil {
		t.Fatal(err)
	}
	if _, err := f.planner.startSteps(t.Context(), claim, op, found, []member{primary, replica}); !errors.Is(err, ErrUnplannable) {
		t.Fatalf("missing peer produced err=%v", err)
	}
}

func TestPostgresRemovalUsesReviewedMemberAndNativeProof(t *testing.T) {
	// Reuse the recorded member/preview fixture. Call the PostgreSQL planner boundary
	// directly because public PostgreSQL scale admission is still unavailable.
	f, replica := scaledUp(t)
	preview, err := f.store.PreviewClusterScaleDown(t.Context(), f.claim.InstallationID, f.claim.ClusterID, "integration")
	if err != nil {
		t.Fatal(err)
	}
	claim := f.scaleDown(t, preview)
	op, err := f.store.GetOperation(t.Context(), claim.InstallationID, claim.OperationID)
	if err != nil {
		t.Fatal(err)
	}
	members, err := f.planner.members(t.Context(), claim)
	if err != nil {
		t.Fatal(err)
	}
	found := cluster.Cluster{Spec: postgresSpec()}
	if err := f.planner.removeReplica(t.Context(), claim, op, found, members); err != nil {
		t.Fatal(err)
	}
	plan := installedPlanFor(t, f, claim.OperationID)
	if len(plan.Steps) != 3 {
		t.Fatalf("steps=%d", len(plan.Steps))
	}
	native := plan.Steps[0].Command.Input
	check := native.GetCheckReadiness()
	if native.ResourceId != replica.ResourceID.String() || len(native.Secrets) != 1 || native.Secrets[0].Purpose != postgres.SuperuserCredential || check == nil || check.Engine != "postgres" || check.PostgresRole != "replica" || check.Role != "synod_admin" || check.Database != "postgres" || check.RedisReplication != nil || check.PostgresReplication == nil {
		t.Fatal("removal lacks PostgreSQL replica proof")
	}
	for _, each := range members {
		if each.Role == "primary" && (check.PostgresReplication.Source.ResourceId != each.ResourceID.String() || check.PostgresReplication.Source.Port != uint32(each.Port)) {
			t.Fatal("removal checks another primary")
		}
	}
	if check.PostgresReplication.Source.Address != "192.0.2.10" || check.PostgresReplication.MaxReplayLagBytes != 16<<20 || check.PostgresReplication.MaxMessageAge.AsDuration() != 30*time.Second {
		t.Fatal("removal omitted source or bounded replication proof")
	}
	if plan.Steps[1].Command.Input.GetStopContainer() == nil || plan.Steps[2].Command.Input.GetRemoveContainer() == nil {
		t.Fatal("removal has unrelated effects")
	}
	for i, step := range plan.Steps {
		if step.Command.Input.ResourceId != replica.ResourceID.String() {
			t.Fatal("removal touches another member")
		}
		if i > 0 && (len(step.DependsOn) != 1 || step.DependsOn[0] != plan.Steps[i-1].Command.StepId) {
			t.Fatal("removal can run before its proof")
		}
	}
	for _, each := range members {
		if each.Role == "replica" {
			if err := f.planner.removeReplica(t.Context(), claim, op, found, []member{each}); !errors.Is(err, ErrNoWorkload) {
				t.Fatalf("missing primary: %v", err)
			}
		}
	}
}
