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

// admitSwitchover admits a switchover to member with key against the current revision, or
// against expected when it is positive.
func admitSwitchover(t *testing.T, f *patroniFixture, member uuid.UUID, key string, expected int64) (postgres.Admission, error) {
	t.Helper()
	if expected == 0 {
		found, err := f.store.GetCluster(t.Context(), f.claim.InstallationID, f.claim.ClusterID)
		if err != nil {
			t.Fatal(err)
		}
		expected = found.DesiredRevision
	}
	return f.store.AdmitClusterAction(t.Context(), postgres.ClusterActionRequest{
		Identity:  postgres.RequestIdentity{InstallationID: f.claim.InstallationID, ActorID: "integration", APIOperation: postgres.SwitchoverAPIOperation, IdempotencyKey: key},
		RequestID: uuid.NewString(), ClusterID: f.claim.ClusterID, ExpectedRevision: expected, MemberID: member,
	})
}

// A switchover checks every member, asks the named member to take over, then proves that
// member writes, the others replicate, and every router routes. Completion needs one writer
// among the first checks and every receipt.
func TestPatroniSwitchover(t *testing.T) {
	f := createdPatroni(t)
	members, err := f.store.ReadClusterMembers(t.Context(), f.claim.InstallationID, f.claim.ClusterID)
	if err != nil || len(members) != 3 {
		t.Fatal(members, err)
	}
	target := members[1]
	for name, member := range map[string]uuid.UUID{"no member": uuid.Nil, "unknown member": uuid.New()} {
		_, err := admitSwitchover(t, f, member, uuid.NewString(), 0)
		if refused, ok := errors.AsType[*postgres.AdmissionError](err); !ok || refused.Code != postgres.InvalidRequest {
			t.Fatalf("%s: %v", name, err)
		}
	}
	key := uuid.NewString()
	admission, err := admitSwitchover(t, f, target.MemberID, key, 0)
	if err != nil {
		t.Fatal(err)
	}
	if replay, err := admitSwitchover(t, f, target.MemberID, key, admission.DesiredRevision-1); err != nil || !replay.Replayed || replay.OperationID != admission.OperationID {
		t.Fatal("a repeated switchover was not replayed", replay, err)
	}
	if _, err := admitSwitchover(t, f, members[0].MemberID, key, admission.DesiredRevision-1); err == nil {
		t.Fatal("a repeated key named another member")
	}
	claim, err := f.store.AcquireClaim(t.Context(), postgres.ClaimRequest{
		ClaimID: uuid.New(), InstallationID: f.claim.InstallationID, OperationID: admission.OperationID,
		RecoveryGeneration: f.claim.RecoveryGeneration, OwnerID: "owner-switchover", TTL: 2 * time.Minute,
		CoordinationKey: "/lifecycle/switchover", CoordinationLease: 1, CoordinationRevision: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := f.planner.Plan(t.Context(), claim); err != nil {
		t.Fatal(err)
	}
	f.claim = claim
	plan := installedPlanFor(t, f.fixture, claim.OperationID)
	// Seven member steps, then a writer and a reader check on each router.
	if len(plan.Steps) != 13 {
		t.Fatalf("switchover plan has %d steps, want 13", len(plan.Steps))
	}
	switched := plan.Steps[3].Command
	stored, err := f.store.ReadMemberContainer(t.Context(), claim.InstallationID, claim.ClusterID, target.ResourceID)
	if err != nil {
		t.Fatal(err)
	}
	// The switchover runs on the node where the first checks found the member.
	node := ""
	for _, step := range plan.Steps[:3] {
		if step.Command.Input.ResourceId == target.ResourceID.String() {
			node = step.Command.Epoch.NodeId
		}
	}
	p := switched.Input.GetSwitchoverPatroni()
	if p == nil || switched.Input.ResourceId != target.ResourceID.String() || node == "" || switched.Epoch.NodeId != node ||
		p.Address != stored.Postgres.Patroni.Address || p.RestPort != stored.Postgres.Patroni.RestPort {
		t.Fatalf("switchover step does not act on the named member: %+v", switched.Input)
	}
	var purposes []string
	for _, reference := range switched.Input.Secrets {
		if !slices.ContainsFunc(stored.Postgres.Patroni.Credentials, func(held *agentv1.SecretReference) bool {
			return held.CredentialId == reference.CredentialId && held.Version == reference.Version && held.Purpose == reference.Purpose
		}) {
			t.Fatalf("switchover secret %s is not the member's", reference.Purpose)
		}
		purposes = append(purposes, reference.Purpose)
	}
	slices.Sort(purposes)
	if !slices.Equal(purposes, []string{"patroni_member_tls", "patroni_rest"}) {
		t.Fatalf("switchover secrets: %v", purposes)
	}
	if check := plan.Steps[4].Command; check.Input.ResourceId != target.ResourceID.String() || check.Input.GetCheckReadiness().GetPostgresRole() != "primary" {
		t.Fatal("the named member does not prove it writes after the switchover")
	}
	// Two writers among the first checks do not prove a switchover.
	roles := map[string]string{plan.Steps[0].Command.StepId: "ready_primary", plan.Steps[1].Command.StepId: "ready_primary", plan.Steps[2].Command.StepId: "ready_replica"}
	recordPatroniReceiptsWith(t, f, plan.Steps, roles)
	if err := f.store.UpdateProgress(t.Context(), claim, postgres.ProgressUpdate{Sequence: 1, Status: operation.Succeeded, Reason: "Fixture terminal boundary."}); !errors.Is(err, operation.ErrPostconditionUnproved) {
		t.Fatalf("a switchover with two writers completed: %v", err)
	}
}

// A switchover completes with one writer among the first checks and moves the observed revision.
func TestPatroniSwitchoverCompletes(t *testing.T) {
	f := createdPatroni(t)
	members, err := f.store.ReadClusterMembers(t.Context(), f.claim.InstallationID, f.claim.ClusterID)
	if err != nil {
		t.Fatal(err)
	}
	admission, err := admitSwitchover(t, f, members[2].MemberID, uuid.NewString(), 0)
	if err != nil {
		t.Fatal(err)
	}
	claim, err := f.store.AcquireClaim(t.Context(), postgres.ClaimRequest{
		ClaimID: uuid.New(), InstallationID: f.claim.InstallationID, OperationID: admission.OperationID,
		RecoveryGeneration: f.claim.RecoveryGeneration, OwnerID: "owner-switchover", TTL: 2 * time.Minute,
		CoordinationKey: "/lifecycle/switchover", CoordinationLease: 1, CoordinationRevision: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := f.planner.Plan(t.Context(), claim); err != nil {
		t.Fatal(err)
	}
	f.claim = claim
	plan := installedPlanFor(t, f.fixture, claim.OperationID)
	recordPatroniReceiptsWith(t, f, plan.Steps, map[string]string{plan.Steps[0].Command.StepId: "ready_primary"})
	if err := f.store.UpdateProgress(t.Context(), claim, postgres.ProgressUpdate{Sequence: 1, Status: operation.Succeeded, Reason: "Fixture terminal boundary."}); err != nil {
		t.Fatal(err)
	}
	found, err := f.store.GetCluster(t.Context(), claim.InstallationID, claim.ClusterID)
	if err != nil || found.ObservedRevision != admission.DesiredRevision || found.ObservedLifecycle != cluster.ObservedRunning {
		t.Fatalf("cluster after switchover: %+v %v", found, err)
	}
	// A stopped cluster has no leader to move.
	f.fixture.planFor(t, postgres.StopClusterAPIOperation)
	if _, err := admitSwitchover(t, f, members[0].MemberID, uuid.NewString(), 0); err == nil {
		t.Fatal("a stopped cluster admitted a switchover")
	}
}
