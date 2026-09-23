package postgres

import (
	"testing"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/operation"
	"github.com/sirrobot01/synod/internal/placement"
)

// A committed placement names the cluster's first member, and names it the primary.
func TestPlacementNamesTheClusterPrimary(t *testing.T) {
	host := placementFixture(t)
	claim := host.claimCluster(t, "db")
	request, decision, plan := host.place(t, claim, smallBudget, nil)
	if _, err := host.store.InstallPlacementPlan(t.Context(), claim, request, decision, plan, nil); err != nil {
		t.Fatal(err)
	}
	members, err := host.store.ReadClusterMembers(t.Context(), host.id, claim.ClusterID)
	if err != nil {
		t.Fatal(err)
	}
	if len(members) != 1 {
		t.Fatalf("members %d, want one", len(members))
	}
	member := members[0]
	if member.Role != "primary" || member.State != "pending" {
		t.Fatalf("member %+v, want a pending primary", member)
	}
	if member.MemberID == uuid.Nil || member.ResourceID != request.Resources[0].ID {
		t.Fatalf("member identity %+v, want the placed instance resource", member)
	}
	if member.CreatedByOperation != claim.OperationID || member.CreatedRevision != request.DesiredRevision {
		t.Fatalf("member provenance %+v", member)
	}
}

// The primary is one member for each cluster. The database enforces it, so no writer
// has to remember the rule.
func TestClusterRefusesASecondLivePrimary(t *testing.T) {
	host := placementFixture(t)
	claim := host.claimCluster(t, "db")
	request, decision, plan := host.place(t, claim, smallBudget, nil)
	if _, err := host.store.InstallPlacementPlan(t.Context(), claim, request, decision, plan, nil); err != nil {
		t.Fatal(err)
	}
	_, err := host.store.pool.Exec(t.Context(), `INSERT INTO cluster_members
 (installation_id,cluster_id,member_id,role,resource_id,state,created_operation_id,created_revision)
 VALUES ($1,$2,$3,'primary',$4,'pending',$5,1)`,
		host.id, claim.ClusterID, uuid.New(), uuid.New(), claim.OperationID)
	if err == nil {
		t.Fatal("a second live primary was accepted")
	}
	// A replica alongside the primary is the supported shape, so the index must not
	// refuse that too.
	if _, err := host.store.pool.Exec(t.Context(), `INSERT INTO cluster_members
 (installation_id,cluster_id,member_id,role,resource_id,state,created_operation_id,created_revision)
 VALUES ($1,$2,$3,'replica',$4,'pending',$5,1)`,
		host.id, claim.ClusterID, uuid.New(), uuid.New(), claim.OperationID); err != nil {
		t.Fatal(err)
	}
}

// A removed member leaves the live list, and its name stays taken. A later add must
// receive a new identity, because a node can still hold the old member's data.
func TestReadClusterMembersOmitsARemovedMember(t *testing.T) {
	host := placementFixture(t)
	claim := host.claimCluster(t, "db")
	request, decision, plan := host.place(t, claim, smallBudget, nil)
	if _, err := host.store.InstallPlacementPlan(t.Context(), claim, request, decision, plan, nil); err != nil {
		t.Fatal(err)
	}
	members, err := host.store.ReadClusterMembers(t.Context(), host.id, claim.ClusterID)
	if err != nil {
		t.Fatal(err)
	}
	removed := members[0].MemberID
	if _, err := host.store.pool.Exec(t.Context(), `UPDATE cluster_members SET removed_at=clock_timestamp() WHERE member_id=$1`, removed); err != nil {
		t.Fatal(err)
	}
	live, err := host.store.ReadClusterMembers(t.Context(), host.id, claim.ClusterID)
	if err != nil {
		t.Fatal(err)
	}
	if len(live) != 0 {
		t.Fatalf("members %d, want none after removal", len(live))
	}
	if total := host.count(t, `SELECT count(*) FROM cluster_members WHERE member_id=$1`, removed); total != 1 {
		t.Fatal("the removed member lost its row, so its name could be reused")
	}
	// The primary index counts live members only, so the cluster can take a new one.
	if _, err := host.store.pool.Exec(t.Context(), `INSERT INTO cluster_members
 (installation_id,cluster_id,member_id,role,resource_id,state,created_operation_id,created_revision)
 VALUES ($1,$2,$3,'primary',$4,'pending',$5,1)`,
		host.id, claim.ClusterID, uuid.New(), uuid.New(), claim.OperationID); err != nil {
		t.Fatal(err)
	}
}

// A later operation places the same resource again to change its reservation. That is
// the same member, so its identity must not change and the cluster must not gain a
// second primary.
func TestReplacingAResourceKeepsItsMemberIdentity(t *testing.T) {
	host := placementFixture(t)
	first := host.claimCluster(t, "db")
	request, decision, plan := host.place(t, first, smallBudget, nil)
	if _, err := host.store.InstallPlacementPlan(t.Context(), first, request, decision, plan, nil); err != nil {
		t.Fatal(err)
	}
	before, err := host.store.ReadClusterMembers(t.Context(), host.id, first.ClusterID)
	if err != nil {
		t.Fatal(err)
	}
	second := host.reclaim(t, first, "succeeded")
	grown := placementRequest(host.id, second.ClusterID, second.OperationID, host.policy, nil)
	grown.Resources[0].ID = request.Resources[0].ID
	grown.Resources[0].ReservationID = request.Resources[0].ReservationID
	grown.Resources[0].OwnedResourceIDs = request.Resources[0].OwnedResourceIDs
	grown.Resources[0].Budgets.Workload = &halfBudget
	snapshot, err := host.store.ReadPlacementSnapshot(t.Context(), host.id)
	if err != nil {
		t.Fatal(err)
	}
	grownDecision, err := placement.Plan(grown, snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := host.store.InstallPlacementPlan(t.Context(), second, grown, grownDecision, planFor(t, second, host.node, grown.Resources[0].ID, 1), nil); err != nil {
		t.Fatal(err)
	}
	after, err := host.store.ReadClusterMembers(t.Context(), host.id, first.ClusterID)
	if err != nil {
		t.Fatal(err)
	}
	if len(after) != 1 {
		t.Fatalf("members %d, want the one member the cluster already had", len(after))
	}
	if after[0].MemberID != before[0].MemberID {
		t.Fatalf("member identity changed from %s to %s", before[0].MemberID, after[0].MemberID)
	}
	if after[0].CreatedByOperation != first.OperationID {
		t.Fatalf("member provenance moved to %s, want the operation that named it", after[0].CreatedByOperation)
	}
}

// A member is pending until a checked receipt proves its postcondition. Successful
// completion is what makes it active, so a member that never ran is never active.
func TestSuccessfulCompletionActivatesTheProvedMember(t *testing.T) {
	host, claim, plan := completionFixture(t)
	members, err := host.store.ReadClusterMembers(t.Context(), host.id, claim.ClusterID)
	if err != nil {
		t.Fatal(err)
	}
	if len(members) != 1 || members[0].State != "pending" {
		t.Fatalf("members %+v, want one pending member before completion", members)
	}
	volume := declared(host, claim.ClusterID)
	if err := host.store.RecordVolumeIdentity(t.Context(), volume); err != nil {
		t.Fatal(err)
	}
	completeSteps(t, host.store, claim, plan)
	if err := host.store.UpdateProgress(t.Context(), claim, ProgressUpdate{
		Sequence: 1, Status: operation.Succeeded, Reason: "Every native postcondition is proved.",
	}); err != nil {
		t.Fatal(err)
	}
	after, err := host.store.ReadClusterMembers(t.Context(), host.id, claim.ClusterID)
	if err != nil {
		t.Fatal(err)
	}
	if len(after) != 1 || after[0].State != "active" {
		t.Fatalf("members %+v, want the proved member active", after)
	}
	if after[0].MemberID != members[0].MemberID {
		t.Fatal("activation replaced the member identity")
	}
}
