package provision

import (
	"crypto/sha256"
	"errors"
	"reflect"
	"slices"
	"testing"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/cluster"
	"github.com/sirrobot01/synod/internal/operation"
	"github.com/sirrobot01/synod/internal/store/postgres"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/proto"
)

func replicatedSpec(persistence string) cluster.CreateSpec {
	return cluster.CreateSpec{SchemaVersion: 1, Name: "cache", Engine: cluster.Redis,
		Redis: &cluster.RedisSpec{Version: "8.2.7", Topology: "standalone", Persistence: persistence}}
}

// running provisions the cluster and settles its create, so a scale has a primary.
func running(t *testing.T, persistence string) *fixture {
	t.Helper()
	f := newFixture(t, replicatedSpec(persistence))
	if _, err := f.planner.provision(t.Context(), f.claim); err != nil {
		t.Fatal(err)
	}
	return f
}

// A scale adds one member. The primary keeps its identity and its reservation, and the
// new member is planned from nothing: its own volume, container, start, and readiness.
func TestScaleAddsOneReplicaBesideThePrimary(t *testing.T) {
	f := running(t, "aof")
	before, err := f.store.ReadClusterMembers(t.Context(), f.claim.InstallationID, f.claim.ClusterID)
	if err != nil {
		t.Fatal(err)
	}
	claim := f.act(t, postgres.ScaleClusterAPIOperation)
	if err := f.planner.Plan(t.Context(), claim); err != nil {
		t.Fatal(err)
	}
	after, err := f.store.ReadClusterMembers(t.Context(), f.claim.InstallationID, f.claim.ClusterID)
	if err != nil {
		t.Fatal(err)
	}
	if len(after) != 2 {
		t.Fatalf("members=%d, want the primary and one replica", len(after))
	}
	var replica postgres.Member
	for _, each := range after {
		if each.Role == "replica" {
			replica = each
		}
	}
	if replica.MemberID == before[0].MemberID || replica.Role != "replica" {
		t.Fatalf("the replica did not receive its own identity: %+v", replica)
	}
	if after[0].MemberID != before[0].MemberID || after[0].Role != "primary" {
		t.Fatalf("the primary lost its identity: %+v", after[0])
	}
	plan := installedPlanFor(t, f, claim.OperationID)
	if len(plan.Steps) != 4 {
		t.Fatalf("steps=%d, want volume, container, start, and readiness", len(plan.Steps))
	}
	// Every step belongs to the new member. Touching the primary would risk its data.
	for _, step := range plan.Steps {
		if step.Command.Input.ResourceId == before[0].ResourceID.String() {
			t.Fatalf("the add planned work on the primary: %+v", step.Command.Input)
		}
	}
	container := plan.Steps[1].Command.Input.GetEnsureContainer()
	if container.Redis == nil || container.Redis.Upstream == nil {
		t.Fatalf("the replica was not given an upstream: %+v", container)
	}
	if container.Redis.Upstream.Address != "192.0.2.10" {
		t.Fatalf("upstream address=%q, want the primary node's canonical address", container.Redis.Upstream.Address)
	}
	check := plan.Steps[3].Command.Input.GetCheckReadiness()
	if check.RedisReplication == nil || check.RedisReplication.ExpectedRole != "replica" {
		t.Fatalf("readiness does not prove the replica role: %+v", check)
	}
	if check.RedisReplication.Upstream.Address != container.Redis.Upstream.Address ||
		check.RedisReplication.Upstream.Port != container.Redis.Upstream.Port {
		t.Fatalf("readiness checks a different upstream than the container declares: %+v", check.RedisReplication)
	}
	if plan.Steps[2].Command.Input.GetStartContainer().AllowInitialization != true {
		t.Fatal("a new replica must be allowed to initialize its empty volume")
	}
}

// A replica copies the primary's dataset. Without persistence a restart empties both,
// so replication would hand back an empty cluster.
func TestScaleRefusesWithoutPersistence(t *testing.T) {
	f := running(t, "none")
	claim := f.act(t, postgres.ScaleClusterAPIOperation)
	if err := f.planner.Plan(t.Context(), claim); !errors.Is(err, ErrUnplannable) {
		t.Fatalf("err=%v, want ErrUnplannable", err)
	}
}

// The replica dials the primary's recorded canonical address. Without one there is no
// address another node can use, so the plan refuses rather than guessing.
func TestScaleRefusesWithoutACanonicalPeerAddress(t *testing.T) {
	f := running(t, "rdb")
	if _, err := f.pool.Exec(t.Context(), `UPDATE node_placement_policies
 SET policy_bytes=convert_to((convert_from(policy_bytes,'UTF8')::jsonb - 'client_address')::text,'UTF8')
 WHERE installation_id=$1 AND node_id=$2`, f.claim.InstallationID, f.node); err != nil {
		t.Fatal(err)
	}
	claim := f.act(t, postgres.ScaleClusterAPIOperation)
	if err := f.planner.Plan(t.Context(), claim); !errors.Is(err, ErrUnplannable) {
		t.Fatalf("err=%v, want ErrUnplannable", err)
	}
}

// scaledUp adds one replica and settles the scale, so a removal has a member to take.
func scaledUp(t *testing.T) (*fixture, postgres.Member) {
	t.Helper()
	f := running(t, "aof")
	claim := f.act(t, postgres.ScaleClusterAPIOperation)
	if err := f.planner.Plan(t.Context(), claim); err != nil {
		t.Fatal(err)
	}
	// The preview refuses while work is in flight, so settle the scale first.
	if _, err := f.pool.Exec(t.Context(), `UPDATE operations
 SET status='succeeded',finished_at=clock_timestamp(),claim_active=false
 WHERE installation_id=$1 AND status NOT IN ('succeeded','failed','cancelled')`, f.claim.InstallationID); err != nil {
		t.Fatal(err)
	}
	members, err := f.store.ReadClusterMembers(t.Context(), f.claim.InstallationID, f.claim.ClusterID)
	if err != nil {
		t.Fatal(err)
	}
	for _, each := range members {
		if each.Role == "replica" {
			return f, each
		}
	}
	t.Fatal("the scale did not record a replica")
	return nil, postgres.Member{}
}

// A removal stops and removes only the member the reviewed plan named. The primary and
// the replica's volume are left alone.
func TestScaleDownRemovesOnlyThePlannedReplica(t *testing.T) {
	f, replica := scaledUp(t)
	// An older unconfirmed preview must not choose the removal target.
	other, err := f.store.PreviewClusterScaleDown(t.Context(), f.claim.InstallationID, f.claim.ClusterID, "integration")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.pool.Exec(t.Context(), `UPDATE cluster_action_plans SET member_id=$1 WHERE plan_token=$2`, uuid.New(), other.PlanToken); err != nil {
		t.Fatal(err)
	}
	plan, err := f.store.PreviewClusterScaleDown(t.Context(), f.claim.InstallationID, f.claim.ClusterID, "integration")
	if err != nil {
		t.Fatal(err)
	}
	if plan.MemberID != replica.MemberID {
		t.Fatalf("the plan named %s, want the replica %s", plan.MemberID, replica.MemberID)
	}
	if plan.Action != "scale_down" || plan.DataDisposition != "retain" || plan.Downtime == "" {
		t.Fatalf("plan %+v", plan)
	}
	claim := f.scaleDown(t, plan)
	var confirmed uuid.UUID
	if err := f.pool.QueryRow(t.Context(), `SELECT action_plan_token FROM operations WHERE id=$1`, claim.OperationID).Scan(&confirmed); err != nil || confirmed != plan.PlanToken {
		t.Fatalf("removal lost confirmed plan: %s %v", confirmed, err)
	}
	if _, err := f.pool.Exec(t.Context(), `UPDATE cluster_action_plans SET created_at=clock_timestamp()-interval '10 minutes',expires_at=clock_timestamp()-interval '1 minute' WHERE plan_token=$1`, plan.PlanToken); err != nil {
		t.Fatal(err)
	}

	if err := f.planner.Plan(t.Context(), claim); err != nil {
		t.Fatal(err)
	}
	installed := installedPlanFor(t, f, claim.OperationID)
	if len(installed.Steps) != 3 {
		t.Fatalf("steps=%d, want a role check, stop, and remove", len(installed.Steps))
	}
	for _, step := range installed.Steps {
		if step.Command.Input.ResourceId != replica.ResourceID.String() {
			t.Fatalf("the removal acted on %s, want the planned replica", step.Command.Input.ResourceId)
		}
	}
	if installed.Steps[2].Postcondition != agentv1.Postcondition_POSTCONDITION_CONTAINER_ABSENT {
		t.Fatalf("postcondition=%v, want the container absent", installed.Steps[2].Postcondition)
	}
	check := installed.Steps[0].Command.Input
	readiness := check.GetCheckReadiness()
	if readiness == nil || readiness.RedisReplication.GetExpectedRole() != "replica" || readiness.RedisReplication.Upstream.GetAddress() != "192.0.2.10" || readiness.Role != "synod_admin" || len(check.Secrets) != 1 || check.Secrets[0].Purpose != postgres.SuperuserCredential {
		t.Fatal("removal has no authenticated native replica proof")
	}
	for i := 1; i < len(installed.Steps); i++ {
		if len(installed.Steps[i].DependsOn) != 1 || installed.Steps[i].DependsOn[0] != installed.Steps[i-1].Command.StepId {
			t.Fatal("removal can run before its native role proof")
		}
	}
	// Nothing removes storage. A purge is a separate decision.
	for _, step := range installed.Steps {
		if step.Command.Input.GetRemoveContainer() == nil && step.Command.Input.GetStopContainer() == nil && step.Command.Input.GetCheckReadiness() == nil {
			t.Fatalf("a removal planned an unexpected effect: %+v", step.Command.Input)
		}
	}
}

// A cluster keeps its primary. A removal that has no replica to take is not work.
func TestScaleDownRefusesWithoutAReplica(t *testing.T) {
	f := running(t, "aof")
	if _, err := f.store.PreviewClusterScaleDown(t.Context(), f.claim.InstallationID, f.claim.ClusterID, "integration"); err == nil {
		t.Fatal("a removal was planned for a cluster that runs only its primary")
	}
}

func TestReplicaRetryPreservesItsPlanAndPlacement(t *testing.T) {
	for _, dispatchedStart := range []bool{false, true} {
		t.Run(map[bool]string{false: "before_start", true: "after_start_dispatch"}[dispatchedStart], func(t *testing.T) {
			f := running(t, "aof")
			claim := f.act(t, postgres.ScaleClusterAPIOperation)
			if err := f.planner.Plan(t.Context(), claim); err != nil {
				t.Fatal(err)
			}
			original := installedPlanFor(t, f, claim.OperationID)
			if dispatchedStart {
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
				if err := f.store.RecordDispatch(t.Context(), claim, original.Steps[2].Command); err != nil {
					t.Fatal(err)
				}
			}
			before, err := f.store.ReadPlacementHolds(t.Context(), f.claim.InstallationID)
			if err != nil {
				t.Fatal(err)
			}
			members, err := f.store.ReadClusterMembers(t.Context(), f.claim.InstallationID, f.claim.ClusterID)
			if err != nil {
				t.Fatal(err)
			}
			// A retry must use the stored settings even if controller defaults change.
			f.planner.config.StepTimeout *= 2
			f.planner.config.Budget.MemoryBytes *= 2
			for attempt := range 3 {
				if _, err := f.pool.Exec(t.Context(), `UPDATE operations SET status='failed',claim_active=false WHERE id=$1`, claim.OperationID); err != nil {
					t.Fatal(err)
				}
				prior := claim.OperationID
				claim = f.act(t, postgres.ScaleClusterAPIOperation)
				// Also recover through an attempt that failed before it installed a plan.
				if attempt == 1 {
					continue
				}
				if err := f.planner.Plan(t.Context(), claim); err != nil {
					t.Fatal(err)
				}
				retried := installedPlanFor(t, f, claim.OperationID)
				op, err := f.store.GetOperation(t.Context(), claim.InstallationID, claim.OperationID)
				if err != nil || op.RetryOf == nil || *op.RetryOf != prior {
					t.Fatalf("retry link %+v: %v", op, err)
				}
				if err := operation.ValidatePlan(retried, op); err != nil {
					t.Fatalf("invalid retry plan: %v", err)
				}
				for i, step := range retried.Steps {
					old := original.Steps[i]
					want := proto.Clone(old.Command.Input).(*agentv1.CommandInput)
					if start := want.GetStartContainer(); start != nil && dispatchedStart {
						start.AllowInitialization = false
					}
					if !proto.Equal(want, step.Command.Input) || step.Command.CommandId == old.Command.CommandId || step.Command.StepId == old.Command.StepId {
						t.Fatalf("step %d lost its inputs or reused a command identity", i)
					}
				}
			}
			after, err := f.store.ReadPlacementHolds(t.Context(), f.claim.InstallationID)
			if err != nil || len(before) != len(after) {
				t.Fatalf("retry changed placement: before %+v after %+v: %v", before, after, err)
			}
			for i, hold := range before {
				now := after[i]
				if hold.ReservationID != now.ReservationID || hold.ResourceID != now.ResourceID || hold.NodeID != now.NodeID || hold.Budget != now.Budget || !slices.Equal(hold.Ports, now.Ports) {
					t.Fatalf("retry changed reserved resources: before %+v after %+v", hold, now)
				}
			}
			var bound int
			if err := f.pool.QueryRow(t.Context(), `SELECT count(*) FROM operation_allocation_bindings WHERE operation_id=$1`, claim.OperationID).Scan(&bound); err != nil || bound != 1 {
				t.Fatalf("retry holds %d allocation bindings: %v", bound, err)
			}
			afterMembers, err := f.store.ReadClusterMembers(t.Context(), f.claim.InstallationID, f.claim.ClusterID)
			if err != nil || !reflect.DeepEqual(members, afterMembers) {
				t.Fatalf("retry changed member identities: %+v: %v", afterMembers, err)
			}
			stale := claim
			stale.ID = uuid.New()
			if _, _, err := f.store.ReadScaleRetryPlan(t.Context(), stale); !errors.Is(err, operation.ErrStaleOwner) {
				t.Fatalf("stale claim read retry plan: %v", err)
			}
		})
	}
}

func TestReplicaRetryAfterPlanningFailure(t *testing.T) {
	f := running(t, "aof")
	claim := f.act(t, postgres.ScaleClusterAPIOperation)
	if _, err := f.pool.Exec(t.Context(), `UPDATE operations SET status='failed',claim_active=false WHERE id=$1`, claim.OperationID); err != nil {
		t.Fatal(err)
	}
	claim = f.act(t, postgres.ScaleClusterAPIOperation)
	if err := f.planner.Plan(t.Context(), claim); err != nil {
		t.Fatal(err)
	}
	members, err := f.store.ReadClusterMembers(t.Context(), claim.InstallationID, claim.ClusterID)
	if err != nil || len(members) != 2 {
		t.Fatalf("retry did not place exactly one replica: %+v: %v", members, err)
	}
}
