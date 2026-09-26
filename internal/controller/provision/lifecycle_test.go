package provision

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/cluster"
	"github.com/sirrobot01/synod/internal/operation"
	"github.com/sirrobot01/synod/internal/store/postgres"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/proto"
)

// act finishes the current operation, admits one lifecycle action, and claims it.
func (f *fixture) act(t *testing.T, apiOperation string) operation.Claim {
	t.Helper()
	if _, err := f.pool.Exec(t.Context(), `UPDATE operations
 SET status='succeeded',finished_at=clock_timestamp(),claim_active=false
 WHERE installation_id=$1 AND status NOT IN ('succeeded','failed','cancelled')`, f.claim.InstallationID); err != nil {
		t.Fatal(err)
	}
	found, err := f.store.GetCluster(t.Context(), f.claim.InstallationID, f.claim.ClusterID)
	if err != nil {
		t.Fatal(err)
	}
	request := postgres.ClusterActionRequest{
		Identity: postgres.RequestIdentity{
			InstallationID: f.claim.InstallationID, ActorID: "integration",
			APIOperation: apiOperation, IdempotencyKey: uuid.NewString(),
		},
		RequestID: uuid.NewString(), ClusterID: f.claim.ClusterID, ExpectedRevision: found.DesiredRevision,
	}
	// A Redis scale asks for its one replica, and a retry asks again. A Patroni scale adds a member.
	if apiOperation == postgres.ScaleClusterAPIOperation {
		request.Replicas = new(1)
		if found.Spec.PostgreSQL != nil && found.Spec.PostgreSQL.Topology == "patroni" {
			members, err := f.store.ReadClusterMembers(t.Context(), f.claim.InstallationID, f.claim.ClusterID)
			if err != nil {
				t.Fatal(err)
			}
			request.Replicas = new(len(members))
		}
	}
	if apiOperation == postgres.DeleteClusterAPIOperation {
		plan, err := f.store.PreviewClusterDelete(t.Context(), f.claim.InstallationID, f.claim.ClusterID, "integration")
		if err != nil {
			t.Fatal(err)
		}
		request.Confirmation = &cluster.ActionConfirmation{PlanToken: plan.PlanToken, Confirm: true, DataDisposition: "retain"}
	}
	if apiOperation == postgres.RecoverSetupAPIOperation {
		plan, err := f.store.PreviewSetupRecovery(t.Context(), f.claim.InstallationID, f.claim.ClusterID, "integration")
		if err != nil {
			t.Fatal(err)
		}
		request.Confirmation = &cluster.ActionConfirmation{PlanToken: plan.PlanToken, Confirm: true, DataDisposition: "retain"}
	}
	admission, err := f.store.AdmitClusterAction(t.Context(), request)
	if err != nil {
		t.Fatalf("admit %s: %v: %v", apiOperation, err, errors.Unwrap(err))
	}
	claim, err := f.store.AcquireClaim(t.Context(), postgres.ClaimRequest{
		ClaimID: uuid.New(), InstallationID: f.claim.InstallationID, OperationID: admission.OperationID,
		RecoveryGeneration: f.claim.RecoveryGeneration, OwnerID: "owner-" + apiOperation, TTL: 2 * time.Minute,
		LeaseID: 1, LeaseRevision: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	return claim
}

// planFor plans one lifecycle action and reads the installed plan back.
func (f *fixture) planFor(t *testing.T, apiOperation string) *agentv1.ExecutionPlan {
	t.Helper()
	claim := f.act(t, apiOperation)
	if err := f.planner.Plan(t.Context(), claim); err != nil {
		t.Fatalf("plan %s: %v", apiOperation, err)
	}
	f.claim = claim
	return installedPlanFor(t, f, claim.OperationID)
}

// provisioned puts one cluster on the fixture node so a lifecycle action has a
// workload to act on.
func provisioned(t *testing.T) *fixture {
	t.Helper()
	f := newFixture(t, postgresSpec())
	if _, err := f.planner.provision(t.Context(), f.claim); err != nil {
		t.Fatal(err)
	}
	return f
}

// A stop stops the engine and touches nothing else. Specification 33 requires stop to
// preserve data, so a plan that removes anything is the wrong plan.
func TestStopPlanOnlyStops(t *testing.T) {
	f := provisioned(t)
	plan := f.planFor(t, postgres.StopClusterAPIOperation)
	if len(plan.Steps) != 1 {
		t.Fatalf("steps=%d, want one stop", len(plan.Steps))
	}
	step := plan.Steps[0]
	if step.Postcondition != agentv1.Postcondition_POSTCONDITION_CONTAINER_STOPPED {
		t.Fatalf("postcondition=%v, want the container stopped", step.Postcondition)
	}
	if step.Command.Input.GetStopContainer().GracePeriod.AsDuration() <= 0 {
		t.Fatal("the engine was given no time to shut down cleanly")
	}
	// A stop that has begun is worse to abandon halfway than to finish.
	if step.CancellationBoundary != agentv1.CancellationBoundary_CANCELLATION_BOUNDARY_SAFE_STOP {
		t.Fatalf("boundary=%v, want a safe stop", step.CancellationBoundary)
	}
}

// A start starts the engine and proves it answers before the operation succeeds.
func TestStartPlanProvesTheEngineAnswers(t *testing.T) {
	f := provisioned(t)
	plan := f.planFor(t, postgres.StartClusterAPIOperation)
	if len(plan.Steps) != 2 {
		t.Fatalf("steps=%d, want start and application readiness", len(plan.Steps))
	}
	start, ready := plan.Steps[0], plan.Steps[1]
	if ready.Command.Input.GetCheckReadiness().PostgresRole != "primary" {
		t.Fatal("start omitted the native primary check")
	}
	if start.Command.Input.GetStartContainer().AllowInitialization {
		t.Fatal("a lifecycle start permits empty storage")
	}
	if start.Postcondition != agentv1.Postcondition_POSTCONDITION_CONTAINER_RUNNING ||
		ready.Postcondition != agentv1.Postcondition_POSTCONDITION_ENGINE_READY {
		t.Fatal("a start does not end by checking the engine answers")
	}
	if len(ready.DependsOn) != 1 || ready.DependsOn[0] != start.Command.StepId {
		t.Fatalf("application readiness depends on %v, want start", ready.DependsOn)
	}
	// It connects with the credential this installation recorded, by reference.
	if len(ready.Command.Input.Secrets) != 1 || ready.Command.Input.Secrets[0].Purpose != postgres.ApplicationCredential {
		t.Fatalf("secrets=%+v, want the application reference", ready.Command.Input.Secrets)
	}
}

// A delete stops the engine and removes its container, and leaves the volume alone.
func TestDeletePlanRetainsTheVolume(t *testing.T) {
	f := provisioned(t)
	plan := f.planFor(t, postgres.DeleteClusterAPIOperation)
	if len(plan.Steps) != 2 {
		t.Fatalf("steps=%d, want stop and remove", len(plan.Steps))
	}
	stop, remove := plan.Steps[0], plan.Steps[1]
	if stop.Postcondition != agentv1.Postcondition_POSTCONDITION_CONTAINER_STOPPED ||
		remove.Postcondition != agentv1.Postcondition_POSTCONDITION_CONTAINER_ABSENT {
		t.Fatal("a delete does not stop and then remove")
	}
	// The plan validator requires this: a container removed without being stopped is
	// torn out from under its clients.
	if len(remove.DependsOn) != 1 || remove.DependsOn[0] != stop.Command.StepId {
		t.Fatalf("remove depends on %v, want the stop step", remove.DependsOn)
	}
	// Nothing in the plan touches storage.
	for _, step := range plan.Steps {
		if step.Command.Input.GetEnsureVolume() != nil {
			t.Fatal("a delete plan touches the volume")
		}
	}
	identity, err := f.store.ReadVolumeIdentity(t.Context(), f.claim.InstallationID, volumeOf(t, f))
	if err != nil {
		t.Fatal(err)
	}
	if identity.Retention != postgres.Retained {
		t.Fatalf("retention=%q, want a delete to retain", identity.Retention)
	}
}

// A lifecycle action on a cluster that holds nothing is a mistake, not an empty plan.
func TestLifecyclePlanRefusesAnAbsentWorkload(t *testing.T) {
	f := newFixture(t, postgresSpec())
	claim := f.act(t, postgres.StopClusterAPIOperation)
	if err := f.planner.Plan(t.Context(), claim); !errors.Is(err, ErrNoWorkload) {
		t.Fatalf("err=%v, want ErrNoWorkload", err)
	}
}

// volumeOf answers the volume this installation declared for the cluster.
func volumeOf(t *testing.T, f *fixture) uuid.UUID {
	t.Helper()
	identities, err := f.store.ReadNodeVolumeIdentities(t.Context(), f.claim.InstallationID, f.node)
	if err != nil {
		t.Fatal(err)
	}
	if len(identities) != 1 {
		t.Fatalf("identities=%d, want the one volume this cluster declared", len(identities))
	}
	return identities[0].ResourceID
}

// installedPlanFor reads back the plan the store holds for one operation.
func installedPlanFor(t *testing.T, f *fixture, operationID uuid.UUID) *agentv1.ExecutionPlan {
	t.Helper()
	var encoded []byte
	if err := f.pool.QueryRow(t.Context(), `SELECT plan_bytes FROM operations
 WHERE installation_id=$1 AND id=$2`, f.claim.InstallationID, operationID).Scan(&encoded); err != nil {
		t.Fatal(err)
	}
	var plan agentv1.ExecutionPlan
	if err := proto.Unmarshal(encoded, &plan); err != nil {
		t.Fatal(err)
	}
	return &plan
}

// The plan comes from the recorded membership, so a provisioned cluster names exactly
// one member and names it the primary.
func TestLifecyclePlansFromTheRecordedMembership(t *testing.T) {
	f := provisioned(t)
	members, err := f.store.ReadClusterMembers(t.Context(), f.claim.InstallationID, f.claim.ClusterID)
	if err != nil {
		t.Fatal(err)
	}
	if len(members) != 1 || members[0].Role != "primary" {
		t.Fatalf("members %+v, want one primary", members)
	}
	plan := f.planFor(t, postgres.StopClusterAPIOperation)
	if len(plan.Steps) != 1 {
		t.Fatalf("steps=%d, want one stop for one member", len(plan.Steps))
	}
	if plan.Steps[0].Command.Input.ResourceId != members[0].ResourceID.String() {
		t.Fatalf("the plan acted on %s, want the recorded member %s",
			plan.Steps[0].Command.Input.ResourceId, members[0].ResourceID)
	}
}

// A member whose reservation is gone is a broken cluster. Planning around it would hide
// that, so the planner refuses and names the member.
func TestLifecyclePlanRefusesAMemberWithNoReservation(t *testing.T) {
	f := provisioned(t)
	members, err := f.store.ReadClusterMembers(t.Context(), f.claim.InstallationID, f.claim.ClusterID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.pool.Exec(t.Context(), `UPDATE resource_allocations SET released_at=clock_timestamp()
 WHERE installation_id=$1 AND cluster_id=$2`, f.claim.InstallationID, f.claim.ClusterID); err != nil {
		t.Fatal(err)
	}
	claim := f.act(t, postgres.StopClusterAPIOperation)
	err = f.planner.Plan(t.Context(), claim)
	if !errors.Is(err, ErrNoWorkload) {
		t.Fatalf("err=%v, want ErrNoWorkload", err)
	}
	if !strings.Contains(err.Error(), members[0].MemberID.String()) {
		t.Fatalf("the refusal did not name the member: %v", err)
	}
}

// scaleDown admits a confirmed removal against a reviewed plan.
func (f *fixture) scaleDown(t *testing.T, plan cluster.ActionPlan) operation.Claim {
	t.Helper()
	if _, err := f.pool.Exec(t.Context(), `UPDATE operations
 SET status='succeeded',finished_at=clock_timestamp(),claim_active=false
 WHERE installation_id=$1 AND status NOT IN ('succeeded','failed','cancelled')`, f.claim.InstallationID); err != nil {
		t.Fatal(err)
	}
	zero := 0
	admission, err := f.store.AdmitClusterAction(t.Context(), postgres.ClusterActionRequest{
		Identity: postgres.RequestIdentity{
			InstallationID: f.claim.InstallationID, ActorID: "integration",
			APIOperation: postgres.ScaleClusterAPIOperation, IdempotencyKey: uuid.NewString(),
		},
		RequestID: uuid.NewString(), ClusterID: f.claim.ClusterID, ExpectedRevision: plan.ExpectedRevision,
		Replicas:     &zero,
		Confirmation: &cluster.ActionConfirmation{PlanToken: plan.PlanToken, Confirm: true, DataDisposition: "retain"},
	})
	if err != nil {
		t.Fatal(err)
	}
	claim, err := f.store.AcquireClaim(t.Context(), postgres.ClaimRequest{
		ClaimID: uuid.New(), InstallationID: f.claim.InstallationID, OperationID: admission.OperationID,
		RecoveryGeneration: f.claim.RecoveryGeneration, OwnerID: "scale-down", TTL: 2 * time.Minute,
		LeaseID: 1, LeaseRevision: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	return claim
}

func TestRedisStartChecksEveryNativeRole(t *testing.T) {
	f, _ := scaledUp(t)
	members, err := f.planner.members(t.Context(), f.claim)
	if err != nil {
		t.Fatal(err)
	}
	plan := f.planFor(t, postgres.StartClusterAPIOperation)
	if len(plan.Steps) != 2*len(members) {
		t.Fatal("start omitted a member")
	}
	admin, err := f.store.ReadClusterCredential(t.Context(), f.claim.InstallationID, f.claim.ClusterID, postgres.SuperuserCredential)
	if err != nil {
		t.Fatal(err)
	}
	for i, member := range members {
		start, ready := plan.Steps[2*i], plan.Steps[2*i+1]
		input := ready.Command.Input
		check := input.GetCheckReadiness()
		if start.Command.Input.GetStartContainer().AllowInitialization {
			t.Fatal("start permits replacement data")
		}
		if i > 0 && (len(start.DependsOn) != 1 || start.DependsOn[0] != plan.Steps[2*i-1].Command.StepId) {
			t.Fatal("member start does not wait for the prior native proof")
		}
		if input.ResourceId != member.ResourceID.String() || check.Port != uint32(member.Port) || check.RedisReplication.GetExpectedRole() != member.Role {
			t.Fatalf("wrong member proof: %v", input)
		}
		if len(input.Secrets) != 1 || input.Secrets[0].CredentialId != admin.CredentialID.String() || check.Role != admin.RoleName {
			t.Fatal("role proof does not use the stored admin credential")
		}
		if len(ready.DependsOn) != 1 || ready.DependsOn[0] != start.Command.StepId {
			t.Fatal("role proof does not follow start")
		}
		if member.Role == "replica" {
			upstream := check.RedisReplication.Upstream
			if upstream == nil || upstream.Address != "192.0.2.10" {
				t.Fatal("replica proof lost its canonical peer")
			}
			for _, primary := range members {
				if primary.Role == "primary" && upstream.Port != uint32(primary.Port) {
					t.Fatal("replica proof uses the wrong primary port")
				}
			}
		} else if check.RedisReplication.Upstream != nil {
			t.Fatal("primary proof declares an upstream")
		}
	}
}

func TestMemberLifecycleOrdersAgentAdmissions(t *testing.T) {
	for _, action := range []string{postgres.StopClusterAPIOperation, postgres.DeleteClusterAPIOperation} {
		t.Run(action, func(t *testing.T) {
			f, _ := scaledUp(t)
			plan := f.planFor(t, action)
			for i, step := range plan.Steps {
				if i > 0 && (len(step.DependsOn) != 1 || step.DependsOn[0] != plan.Steps[i-1].Command.StepId) {
					t.Fatal("member lifecycle commands compete for the cluster admission slot")
				}
			}
		})
	}
}
