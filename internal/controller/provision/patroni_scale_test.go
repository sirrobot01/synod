package provision

import (
	"errors"
	"net"
	"slices"
	"strconv"
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

// addHost approves one more node on its own host domain for a Patroni fixture.
func addHost(t *testing.T, f *patroniFixture, domain, address string) uuid.UUID {
	t.Helper()
	node := uuid.New()
	if _, err := f.pool.Exec(t.Context(), `INSERT INTO nodes (id,installation_id,public_key_hash,enrolled_at)
 VALUES ($1,$2,$3,clock_timestamp())`, node, f.claim.InstallationID, make([]byte, 32)); err != nil {
		t.Fatal(err)
	}
	readyNodeWith(t, f.store, f.claim.InstallationID, node, onHost(domain, address, nodeArchitecture))
	f.nodes[node] = address
	return node
}

// patroniPlanFor admits one action and plans it with the Patroni planner.
func patroniPlanFor(t *testing.T, f *patroniFixture, apiOperation string) *agentv1.ExecutionPlan {
	t.Helper()
	claim := f.fixture.act(t, apiOperation)
	if err := f.planner.Plan(t.Context(), claim); err != nil {
		t.Fatalf("plan %s: %v", apiOperation, err)
	}
	f.fixture.claim = claim
	return installedPlanFor(t, f.fixture, claim.OperationID)
}

// grownPatroni plans a fourth member for a created cluster on a new host.
func grownPatroni(t *testing.T) (*patroniFixture, uuid.UUID, map[string]*agentv1.EnsureContainer, *agentv1.ExecutionPlan) {
	t.Helper()
	f := createdPatroni(t)
	node := addHost(t, f, "host-d", "192.0.2.13")
	before := memberContainers(t, f)
	return f, node, before, patroniPlanFor(t, f, postgres.ScaleClusterAPIOperation)
}

// completeScale records every receipt with one final writer and completes the operation.
func completeScale(t *testing.T, f *patroniFixture, plan *agentv1.ExecutionPlan) {
	t.Helper()
	prefix, members := operation.PatroniScalePrefix(plan), operation.LeadingChecks(plan.Steps)+2
	roles := map[string]string{plan.Steps[prefix-members].Command.StepId: "ready_primary"}
	recordPatroniReceiptsWith(t, f, plan.Steps, roles)
	if err := f.store.UpdateProgress(t.Context(), f.claim, postgres.ProgressUpdate{Sequence: 1, Status: operation.Succeeded, Reason: "Fixture terminal boundary."}); err != nil {
		t.Fatal(err)
	}
}

// An added member joins on a new host. Each current member is first recreated so that it admits
// the new address and presents a leaf that names it. The new member clones from the leader, and
// every route is replaced so that each router reaches four members. etcd stays as it was.
func TestPatroniScaleAddsAMember(t *testing.T) {
	f, node, before, plan := grownPatroni(t)
	etcd, err := f.store.ReadEtcdMembers(t.Context(), f.claim.InstallationID, f.claim.ClusterID)
	if err != nil || len(etcd) != 3 {
		t.Fatalf("etcd members: %+v %v", etcd, err)
	}
	// Three renewal blocks of six, the new member's four steps, and four final checks. Then each
	// of four members replaces its pooler and router and checks two routes.
	if prefix := operation.PatroniScalePrefix(plan); prefix != 26 || len(plan.Steps) != 66 {
		t.Fatalf("scale plan has %d steps with prefix %d, want 26 member steps and 40 route steps", len(plan.Steps), prefix)
	}
	for block := range 3 {
		recreate := plan.Steps[6*block+3].Command.Input.GetRecreatePatroniMember()
		member := plan.Steps[6*block+3].Command.Input.ResourceId
		if recreate == nil || !proto.Equal(recreate.Previous, before[member]) {
			t.Fatalf("block %d does not recreate a stored member", block)
		}
		previous, next := recreate.Previous.Postgres.Patroni, recreate.Next.Postgres.Patroni
		if !slices.Equal(next.PeerNetworks, append(slices.Clone(previous.PeerNetworks), "192.0.2.13/32")) || !slices.Equal(next.DcsEndpoints, previous.DcsEndpoints) {
			t.Fatalf("block %d next peers %v", block, next.PeerNetworks)
		}
		if memberVersion, dcsVersion := versions(recreate.Next); memberVersion != 2 || dcsVersion != 1 {
			t.Fatalf("block %d renews to member version %d and DCS version %d", block, memberVersion, dcsVersion)
		}
	}
	volume, container, start := plan.Steps[18], plan.Steps[19], plan.Steps[20]
	added := container.Command.Input.GetEnsureContainer()
	patroni := added.GetPostgres().GetPatroni()
	if volume.Command.Input.GetEnsureVolume() == nil || container.Command.Epoch.NodeId != node.String() || patroni.Address != "192.0.2.13" ||
		!patroni.AllowInitialization || len(patroni.PeerNetworks) != 4 || !start.Command.Input.GetStartPatroniMember().AllowInitialization ||
		!slices.Equal(patroni.DcsEndpoints, before[plan.Steps[3].Command.Input.ResourceId].Postgres.Patroni.DcsEndpoints) {
		t.Fatalf("new member: %+v", added)
	}
	if memberVersion, _ := versions(added); memberVersion != 1 {
		t.Fatalf("new member leaf version %d, want its own first version", memberVersion)
	}
	for _, step := range plan.Steps[26:] {
		if router := step.Command.Input.GetEnsureContainer().GetRoute().GetRouter(); router != nil && len(router.Backends) != 4 {
			t.Fatalf("router has %d backends, want 4", len(router.Backends))
		}
	}
	completeScale(t, f, plan)
	members, err := f.store.ReadClusterMembers(t.Context(), f.claim.InstallationID, f.claim.ClusterID)
	if err != nil || len(members) != 4 {
		t.Fatalf("members after scale: %+v %v", members, err)
	}
	for _, member := range members {
		if member.State != "active" || member.Role != "patroni" {
			t.Fatalf("member after scale: %+v", member)
		}
	}
	after := memberContainers(t, f)
	for block := range 3 {
		recreate := plan.Steps[6*block+3].Command.Input.GetRecreatePatroniMember()
		if !proto.Equal(after[plan.Steps[6*block+3].Command.Input.ResourceId], recreate.Next) {
			t.Fatalf("block %d member does not run its recreated configuration", block)
		}
	}
	if !proto.Equal(after[container.Command.Input.ResourceId], added) {
		t.Fatal("the new member does not run its created configuration")
	}
	if found, err := f.store.GetCluster(t.Context(), f.claim.InstallationID, f.claim.ClusterID); err != nil || found.ObservedLifecycle != cluster.ObservedRunning || found.ObservedRevision != found.DesiredRevision || found.DesiredReplicas != 3 {
		t.Fatalf("cluster after scale: %+v %v", found, err)
	}
	if after, err := f.store.ReadEtcdMembers(t.Context(), f.claim.InstallationID, f.claim.ClusterID); err != nil || !slices.Equal(after, etcd) {
		t.Fatalf("etcd members after scale: %+v %v", after, err)
	}

	// A four-member cluster renews every member one at a time, and completes with one writer.
	renewal := renewPatroni(t, f)
	if prefix := operation.RenewalPrefix(renewal); prefix != 32 {
		t.Fatalf("four-member renewal prefix %d, want 32", prefix)
	}
	recordPatroniReceiptsWith(t, f, renewal.Steps, finalRoles(renewal, 1))
	if err := f.store.UpdateProgress(t.Context(), f.claim, postgres.ProgressUpdate{Sequence: 1, Status: operation.Succeeded, Reason: "Fixture terminal boundary."}); err != nil {
		t.Fatal(err)
	}
	// It also stops and starts all four members.
	stop := patroniPlanFor(t, f, postgres.StopClusterAPIOperation)
	recordPatroniReceipts(t, f, stop.Steps)
	if err := f.store.UpdateProgress(t.Context(), f.claim, postgres.ProgressUpdate{Sequence: 1, Status: operation.Succeeded, Reason: "Fixture terminal boundary."}); err != nil {
		t.Fatal(err)
	}
	start4 := patroniPlanFor(t, f, postgres.StartClusterAPIOperation)
	starts := 0
	for _, step := range start4.Steps {
		if step.Command.Input.GetStartPatroniMember() != nil {
			starts++
		}
	}
	if starts != 4 {
		t.Fatalf("start plan starts %d members, want 4", starts)
	}
}

// A scale adds exactly one member to a running cluster whose members are all active. Removal is
// not supported yet, and a cluster without a free host cannot grow.
func TestPatroniScaleAdmission(t *testing.T) {
	f := createdPatroni(t)
	found, err := f.store.GetCluster(t.Context(), f.claim.InstallationID, f.claim.ClusterID)
	if err != nil || found.DesiredReplicas != 2 {
		t.Fatalf("a new Patroni cluster records %d replicas, want 2: %v", found.DesiredReplicas, err)
	}
	for replicas, want := range map[int]postgres.ErrorCode{1: postgres.UnsupportedSpec, 2: postgres.UnsupportedSpec, 4: postgres.UnsupportedSpec, 5: postgres.InvalidRequest} {
		_, err := f.store.AdmitClusterAction(t.Context(), postgres.ClusterActionRequest{
			Identity:  postgres.RequestIdentity{InstallationID: f.claim.InstallationID, ActorID: "integration", APIOperation: postgres.ScaleClusterAPIOperation, IdempotencyKey: uuid.NewString()},
			RequestID: uuid.NewString(), ClusterID: f.claim.ClusterID, ExpectedRevision: found.DesiredRevision, Replicas: new(replicas),
		})
		if refused, ok := errors.AsType[*postgres.AdmissionError](err); !ok || refused.Code != want {
			t.Errorf("scale to %d replicas: %v, want %s", replicas, err, want)
		}
	}
	// Three hosts hold three members, so a fourth has nowhere to go.
	claim := f.fixture.act(t, postgres.ScaleClusterAPIOperation)
	if err := f.planner.Plan(t.Context(), claim); err == nil {
		t.Fatal("a fourth member was planned on a host that a member already uses")
	}
}

// A failed scale keeps the recreate that completed and leaves the new member residual, so a
// member rebuild can resume or replace it.
func TestPatroniScaleFailureLeavesResidual(t *testing.T) {
	f, _, before, plan := grownPatroni(t)
	recordPatroniReceiptsWith(t, f, plan.Steps[:6], nil)
	if err := f.store.UpdateProgress(t.Context(), f.claim, postgres.ProgressUpdate{Sequence: 1, Status: operation.Failed, Reason: "Fixture terminal boundary.", FailureCode: "fixture_failure"}); err != nil {
		t.Fatal(err)
	}
	recreated := plan.Steps[3].Command.Input.ResourceId
	after := memberContainers(t, f)
	for member, container := range before {
		want := container
		if member == recreated {
			want = plan.Steps[3].Command.Input.GetRecreatePatroniMember().Next
		}
		if !proto.Equal(after[member], want) {
			t.Fatalf("member %s configuration after a failed scale", member)
		}
	}
	members, err := f.store.ReadClusterMembers(t.Context(), f.claim.InstallationID, f.claim.ClusterID)
	if err != nil || len(members) != 4 {
		t.Fatalf("members after a failed scale: %+v %v", members, err)
	}
	for _, member := range members {
		if (member.ResourceID.String() == plan.Steps[19].Command.Input.ResourceId) != (member.State == "residual") {
			t.Fatalf("member after a failed scale: %+v", member)
		}
	}
}

// A four-member cluster rebuilds a member with three peer checks and switches its leader
// with four checks before and after.
func TestPatroniFourMembersRebuildAndSwitchOver(t *testing.T) {
	f, _, _, plan := grownPatroni(t)
	completeScale(t, f, plan)
	members, err := f.store.ReadClusterMembers(t.Context(), f.claim.InstallationID, f.claim.ClusterID)
	if err != nil || len(members) != 4 {
		t.Fatalf("members: %+v %v", members, err)
	}
	rebuild := rebuildPatroniMember(t, f, members[0])
	if prefix := operation.MemberRebuildPrefix(rebuild); prefix != 11 || operation.LeadingChecks(rebuild.Steps) != 3 {
		t.Fatalf("four-member rebuild prefix %d, want three peer checks and eight more steps", prefix)
	}
	roles := map[string]string{rebuild.Steps[0].Command.StepId: "ready_primary"}
	recordPatroniReceiptsWith(t, f, rebuild.Steps, roles)
	if err := f.store.UpdateProgress(t.Context(), f.claim, postgres.ProgressUpdate{Sequence: 1, Status: operation.Succeeded, Reason: "Fixture terminal boundary."}); err != nil {
		t.Fatal(err)
	}
	if members, err = f.store.ReadClusterMembers(t.Context(), f.claim.InstallationID, f.claim.ClusterID); err != nil || len(members) != 4 {
		t.Fatalf("members after rebuild: %+v %v", members, err)
	}
	admission, err := admitSwitchover(t, f, members[1].MemberID, uuid.NewString(), 0)
	if err != nil {
		t.Fatal(err)
	}
	claim, err := f.store.AcquireClaim(t.Context(), postgres.ClaimRequest{
		ClaimID: uuid.New(), InstallationID: f.claim.InstallationID, OperationID: admission.OperationID,
		RecoveryGeneration: f.claim.RecoveryGeneration, OwnerID: "owner-switchover", TTL: 2 * time.Minute,
		LeaseID: 1, LeaseRevision: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := f.planner.Plan(t.Context(), claim); err != nil {
		t.Fatal(err)
	}
	f.claim = claim
	switchover := installedPlanFor(t, f.fixture, claim.OperationID)
	if prefix := operation.SwitchoverPrefix(switchover); prefix != 9 {
		t.Fatalf("four-member switchover prefix %d, want 9", prefix)
	}
	roles = map[string]string{switchover.Steps[0].Command.StepId: "ready_primary"}
	recordPatroniReceiptsWith(t, f, switchover.Steps, roles)
	if err := f.store.UpdateProgress(t.Context(), f.claim, postgres.ProgressUpdate{Sequence: 1, Status: operation.Succeeded, Reason: "Fixture terminal boundary."}); err != nil {
		t.Fatal(err)
	}
}

// admitRemoval previews the removal of member and admits the confirmed scale down.
func admitRemoval(t *testing.T, f *patroniFixture, member uuid.UUID) operation.Claim {
	t.Helper()
	preview, err := f.store.PreviewMemberRemoval(t.Context(), f.claim.InstallationID, f.claim.ClusterID, member, "integration")
	if err != nil {
		t.Fatal(err)
	}
	members, err := f.store.ReadClusterMembers(t.Context(), f.claim.InstallationID, f.claim.ClusterID)
	if err != nil {
		t.Fatal(err)
	}
	admission, err := f.store.AdmitClusterAction(t.Context(), postgres.ClusterActionRequest{
		Identity:  postgres.RequestIdentity{InstallationID: f.claim.InstallationID, ActorID: "integration", APIOperation: postgres.ScaleClusterAPIOperation, IdempotencyKey: uuid.NewString()},
		RequestID: uuid.NewString(), ClusterID: f.claim.ClusterID, ExpectedRevision: preview.ExpectedRevision, Replicas: new(len(members) - 2),
		Confirmation: &cluster.ActionConfirmation{PlanToken: preview.PlanToken, Confirm: true, DataDisposition: "retain"},
	})
	if err != nil {
		t.Fatal(err)
	}
	claim, err := f.store.AcquireClaim(t.Context(), postgres.ClaimRequest{
		ClaimID: uuid.New(), InstallationID: f.claim.InstallationID, OperationID: admission.OperationID,
		RecoveryGeneration: f.claim.RecoveryGeneration, OwnerID: "owner-removal", TTL: 2 * time.Minute,
		LeaseID: 1, LeaseRevision: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	return claim
}

// The operator names the member to remove. The removal proves the member is a replica, removes
// its container and routes, keeps its volume, and completes with one writer among the kept
// members. A member on a node without etcd leaves etcd alone.
func TestPatroniScaleRemovesANamedMember(t *testing.T) {
	f, node, _, grow := grownPatroni(t)
	completeScale(t, f, grow)
	etcd, err := f.store.ReadEtcdMembers(t.Context(), f.claim.InstallationID, f.claim.ClusterID)
	if err != nil || len(etcd) != 3 {
		t.Fatalf("etcd members: %+v %v", etcd, err)
	}
	members, err := f.store.ReadClusterMembers(t.Context(), f.claim.InstallationID, f.claim.ClusterID)
	if err != nil || len(members) != 4 {
		t.Fatalf("members: %+v %v", members, err)
	}
	target := members[slices.IndexFunc(members, func(member postgres.Member) bool { return member.NodeID == node })]
	// A removal without a reviewed plan is refused.
	found, err := f.store.GetCluster(t.Context(), f.claim.InstallationID, f.claim.ClusterID)
	if err != nil {
		t.Fatal(err)
	}
	_, err = f.store.AdmitClusterAction(t.Context(), postgres.ClusterActionRequest{
		Identity:  postgres.RequestIdentity{InstallationID: f.claim.InstallationID, ActorID: "integration", APIOperation: postgres.ScaleClusterAPIOperation, IdempotencyKey: uuid.NewString()},
		RequestID: uuid.NewString(), ClusterID: f.claim.ClusterID, ExpectedRevision: found.DesiredRevision, Replicas: new(2),
	})
	if refused, ok := errors.AsType[*postgres.AdmissionError](err); !ok || refused.Code != postgres.InvalidRequest {
		t.Fatalf("unreviewed removal: %v", err)
	}

	claim := admitRemoval(t, f, target.MemberID)
	if err := f.planner.Plan(t.Context(), claim); err != nil {
		t.Fatal(err)
	}
	f.fixture.claim = claim
	plan := installedPlanFor(t, f.fixture, claim.OperationID)
	// Three checks, the replica proof, stop, and removal, three final checks. Then two route
	// removals of two steps, three router replaces of four steps, and six route checks.
	if prefix := operation.PatroniRemovalPrefix(plan); prefix != 9 || len(plan.Steps) != 31 {
		t.Fatalf("removal plan has %d steps with prefix %d, want 9 and 31", len(plan.Steps), prefix)
	}
	if check := plan.Steps[3].Command; check.Input.ResourceId != target.ResourceID.String() || check.Input.GetCheckReadiness().GetPostgresRole() != "replica" ||
		plan.Steps[5].Command.Input.GetRemoveContainer() == nil {
		t.Fatal("the removal does not prove the named member is a replica before it removes it")
	}
	for _, step := range plan.Steps[9:] {
		if router := step.Command.Input.GetEnsureContainer().GetRoute().GetRouter(); router != nil && len(router.Backends) != 3 {
			t.Fatalf("router has %d backends, want 3", len(router.Backends))
		}
	}
	roles := map[string]string{plan.Steps[6].Command.StepId: "ready_primary"}
	recordPatroniReceiptsWith(t, f, plan.Steps, roles)
	if err := f.store.UpdateProgress(t.Context(), f.claim, postgres.ProgressUpdate{Sequence: 1, Status: operation.Succeeded, Reason: "Fixture terminal boundary."}); err != nil {
		t.Fatal(err)
	}
	after, err := f.store.ReadClusterMembers(t.Context(), f.claim.InstallationID, f.claim.ClusterID)
	if err != nil || len(after) != 3 || slices.ContainsFunc(after, func(member postgres.Member) bool { return member.MemberID == target.MemberID }) {
		t.Fatalf("members after removal: %+v %v", after, err)
	}
	var released bool
	var retention string
	if err := f.pool.QueryRow(t.Context(), `SELECT
 (SELECT released_at IS NOT NULL FROM resource_allocations WHERE installation_id=$1 AND resource_id=$2),
 (SELECT retention FROM volume_identities WHERE installation_id=$1 AND resource_id=$3)`,
		f.claim.InstallationID, target.ResourceID, grow.Steps[18].Command.Input.ResourceId).Scan(&released, &retention); err != nil || !released || retention != "retained" {
		t.Fatalf("removed member released=%v volume=%q %v", released, retention, err)
	}
	if kept, err := f.store.ReadEtcdMembers(t.Context(), f.claim.InstallationID, f.claim.ClusterID); err != nil || !slices.Equal(kept, etcd) {
		t.Fatalf("etcd members after removal: %+v %v", kept, err)
	}
	// Three members remain, and a removal below three is refused.
	_, err = f.store.PreviewMemberRemoval(t.Context(), f.claim.InstallationID, f.claim.ClusterID, after[0].MemberID, "integration")
	if refused, ok := errors.AsType[*postgres.AdmissionError](err); !ok || refused.Code != postgres.OperationConflict {
		t.Fatalf("removal from three members: %v", err)
	}
	renewal := renewPatroni(t, f)
	recordPatroniReceiptsWith(t, f, renewal.Steps, finalRoles(renewal, 1))
	if err := f.store.UpdateProgress(t.Context(), f.claim, postgres.ProgressUpdate{Sequence: 1, Status: operation.Succeeded, Reason: "Fixture terminal boundary."}); err != nil {
		t.Fatal(err)
	}
}

// A member on an etcd host leaves after its etcd service moves to the kept member's node that
// holds none. The new service joins as a learner through an established service, is promoted,
// and the old service leaves the cluster before its container is removed.
func TestPatroniScaleMovesEtcdOffARemovedMember(t *testing.T) {
	f, spare, _, grow := grownPatroni(t)
	completeScale(t, f, grow)
	etcd, err := f.store.ReadEtcdMembers(t.Context(), f.claim.InstallationID, f.claim.ClusterID)
	if err != nil || len(etcd) != 3 {
		t.Fatalf("etcd members: %+v %v", etcd, err)
	}
	members, err := f.store.ReadClusterMembers(t.Context(), f.claim.InstallationID, f.claim.ClusterID)
	if err != nil {
		t.Fatal(err)
	}
	target := members[slices.IndexFunc(members, func(member postgres.Member) bool { return member.NodeID == etcd[0].NodeID })]
	previous, err := f.store.ReadEtcdContainer(t.Context(), f.claim.InstallationID, f.claim.ClusterID, etcd[0].ResourceID)
	if err != nil {
		t.Fatal(err)
	}
	claim := admitRemoval(t, f, target.MemberID)
	if err := f.planner.Plan(t.Context(), claim); err != nil {
		t.Fatal(err)
	}
	f.fixture.claim = claim
	plan := installedPlanFor(t, f.fixture, claim.OperationID)
	// Three checks, the replica proof, eight move steps, stop, removal, and three final checks.
	if prefix := operation.PatroniRemovalPrefix(plan); prefix != 17 || operation.EtcdMoveSource(plan) != etcd[0].ResourceID.String() {
		t.Fatalf("removal with an etcd move has prefix %d and moves %q", prefix, operation.EtcdMoveSource(plan))
	}
	move := plan.Steps[4:12]
	container := move[1].Command.Input.GetEnsureContainer().GetEtcd()
	added, promoted, removed := move[2].Command.Input.GetEtcdMembership(), move[4].Command.Input.GetEtcdMembership(), move[5].Command.Input.GetEtcdMembership()
	if move[1].Command.Epoch.NodeId != spare.String() || container.InitialClusterState != "existing" || len(strings.Split(container.InitialCluster, ",")) != 4 ||
		container.Address != "192.0.2.13" || added.GetAction() != "add_learner" || promoted.GetName() != container.Name || removed.GetName() != previous.Etcd.Name {
		t.Fatalf("etcd move: %+v", container)
	}
	for _, step := range []*agentv1.PlanStep{move[2], move[4], move[5]} {
		if node := step.Command.Epoch.NodeId; node == spare.String() || node == etcd[0].NodeID.String() {
			t.Fatal("a membership change runs on the old or the new service's node")
		}
	}
	op, err := f.store.GetOperation(t.Context(), claim.InstallationID, claim.OperationID)
	if err != nil {
		t.Fatal(err)
	}
	for name, tamper := range map[string]func(*agentv1.ExecutionPlan){
		"membership on the new node": func(p *agentv1.ExecutionPlan) { p.Steps[6].Command.Epoch.NodeId = spare.String() },
		"a new cluster": func(p *agentv1.ExecutionPlan) {
			p.Steps[5].Command.Input.GetEnsureContainer().Etcd.InitialClusterState = "new"
		},
		"removal of the new service": func(p *agentv1.ExecutionPlan) { p.Steps[9].Command.Input.GetEtcdMembership().Name = container.Name },
	} {
		changed := proto.Clone(plan).(*agentv1.ExecutionPlan)
		tamper(changed)
		// Each command keeps a valid hash, so only the move shape can refuse it.
		for _, step := range changed.Steps {
			hash, err := operation.CommandHash(step.Command)
			if err != nil {
				t.Fatal(err)
			}
			step.Command.InputHash = hash[:]
		}
		if err := operation.ValidatePlan(changed, op); !errors.Is(err, operation.ErrUnsupportedPlan) {
			t.Errorf("%s: plan accepted", name)
		}
	}
	roles := map[string]string{plan.Steps[14].Command.StepId: "ready_primary"}
	recordPatroniReceiptsWith(t, f, plan.Steps, roles)
	if err := f.store.UpdateProgress(t.Context(), f.claim, postgres.ProgressUpdate{Sequence: 1, Status: operation.Succeeded, Reason: "Fixture terminal boundary."}); err != nil {
		t.Fatal(err)
	}
	after, err := f.store.ReadEtcdMembers(t.Context(), f.claim.InstallationID, f.claim.ClusterID)
	if err != nil || len(after) != 3 || slices.Contains(after, etcd[0]) || after[2].NodeID != spare || after[2].ResourceID.String() != move[1].Command.Input.ResourceId {
		t.Fatalf("etcd members after the move: %+v %v", after, err)
	}
	if kept, err := f.store.ReadClusterMembers(t.Context(), f.claim.InstallationID, f.claim.ClusterID); err != nil || len(kept) != 3 {
		t.Fatalf("members after removal: %+v %v", kept, err)
	}
	// The next member added uses the live etcd endpoints, including the moved service.
	addHost(t, f, "host-e", "192.0.2.14")
	next := patroniPlanFor(t, f, postgres.ScaleClusterAPIOperation)
	endpoints := next.Steps[19].Command.Input.GetEnsureContainer().GetPostgres().GetPatroni().GetDcsEndpoints()
	if !slices.Contains(endpoints, net.JoinHostPort(container.Address, strconv.Itoa(int(container.ClientPort)))) ||
		slices.Contains(endpoints, net.JoinHostPort(previous.Etcd.Address, strconv.Itoa(int(previous.Etcd.ClientPort)))) || len(endpoints) != 3 {
		t.Fatalf("new member DCS endpoints %v", endpoints)
	}
}

// A removal that failed during its etcd move resumes it with the service it already placed.
func TestPatroniScaleResumesAnEtcdMove(t *testing.T) {
	f, _, _, grow := grownPatroni(t)
	completeScale(t, f, grow)
	etcd, err := f.store.ReadEtcdMembers(t.Context(), f.claim.InstallationID, f.claim.ClusterID)
	if err != nil {
		t.Fatal(err)
	}
	members, err := f.store.ReadClusterMembers(t.Context(), f.claim.InstallationID, f.claim.ClusterID)
	if err != nil {
		t.Fatal(err)
	}
	target := members[slices.IndexFunc(members, func(member postgres.Member) bool { return member.NodeID == etcd[0].NodeID })]
	claim := admitRemoval(t, f, target.MemberID)
	if err := f.planner.Plan(t.Context(), claim); err != nil {
		t.Fatal(err)
	}
	f.fixture.claim = claim
	first := installedPlanFor(t, f.fixture, claim.OperationID)
	recordPatroniReceiptsWith(t, f, first.Steps[:7], nil)
	if err := f.store.UpdateProgress(t.Context(), f.claim, postgres.ProgressUpdate{Sequence: 1, Status: operation.Failed, Reason: "Fixture terminal boundary.", FailureCode: "fixture_failure"}); err != nil {
		t.Fatal(err)
	}
	if live, err := f.store.ReadEtcdMembers(t.Context(), f.claim.InstallationID, f.claim.ClusterID); err != nil || len(live) != 4 {
		t.Fatalf("etcd members after a failed move: %+v %v", live, err)
	}
	claim = admitRemoval(t, f, target.MemberID)
	if err := f.planner.Plan(t.Context(), claim); err != nil {
		t.Fatal(err)
	}
	f.fixture.claim = claim
	second := installedPlanFor(t, f.fixture, claim.OperationID)
	if second.Steps[5].Command.Input.ResourceId != first.Steps[5].Command.Input.ResourceId ||
		!proto.Equal(second.Steps[5].Command.Input.GetEnsureContainer(), first.Steps[5].Command.Input.GetEnsureContainer()) {
		t.Fatal("the resumed move does not reuse the placed etcd service")
	}
	// The resume places nothing: the cluster has recorded four etcd services in all.
	var recorded int
	if err := f.pool.QueryRow(t.Context(), `SELECT count(*) FROM etcd_members WHERE installation_id=$1 AND cluster_id=$2`, f.claim.InstallationID, f.claim.ClusterID).Scan(&recorded); err != nil || recorded != 4 {
		t.Fatalf("recorded etcd services=%d %v, want 4", recorded, err)
	}
	roles := map[string]string{second.Steps[14].Command.StepId: "ready_primary"}
	recordPatroniReceiptsWith(t, f, second.Steps, roles)
	if err := f.store.UpdateProgress(t.Context(), f.claim, postgres.ProgressUpdate{Sequence: 1, Status: operation.Succeeded, Reason: "Fixture terminal boundary."}); err != nil {
		t.Fatal(err)
	}
	if live, err := f.store.ReadEtcdMembers(t.Context(), f.claim.InstallationID, f.claim.ClusterID); err != nil || len(live) != 3 || slices.Contains(live, etcd[0]) {
		t.Fatalf("etcd members after the resumed move: %+v %v", live, err)
	}
}
