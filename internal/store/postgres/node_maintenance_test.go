package postgres

import (
	"testing"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/placement"
)

// probe issues and answers one capacity challenge, so the planner has evidence that
// matches the node's current policy revision.
func (h *placementHost) probe(t *testing.T) {
	t.Helper()
	var session uuid.UUID
	if err := h.store.pool.QueryRow(t.Context(), `SELECT session_id FROM agent_presence WHERE node_id=$1`, h.node).Scan(&session); err != nil {
		t.Fatal(err)
	}
	issued, err := h.store.BeginCapacityProbe(t.Context(), h.id, h.node, session)
	if err != nil {
		t.Fatal(err)
	}
	policy := h.policy
	policy.Revision = issued.PolicyRevision
	if err := h.store.RecordNodeCapacity(t.Context(), h.id, h.node, testReport(issued, policy)); err != nil {
		t.Fatal(err)
	}
}

// placeable reports whether the planner would put new work on the fixture node.
func (h *placementHost) placeable(t *testing.T) bool {
	t.Helper()
	snapshot, err := h.store.ReadPlacementSnapshot(t.Context(), h.id)
	if err != nil {
		t.Fatal(err)
	}
	_, err = placement.Plan(placementRequest(h.id, uuid.New(), uuid.New(), h.policy, nil), snapshot)
	return err == nil
}

// Cordon stops new placement and leaves everything already placed exactly where it is.
func TestCordonStopsNewPlacementAndMovesNothing(t *testing.T) {
	host := placementFixture(t)
	claim := host.claimApp(t, "db")
	port := placement.Port{Address: "127.0.0.1", Transport: "tcp", Number: 5432}
	request, decision, plan := host.place(t, claim, smallBudget, []placement.Port{port})
	if _, err := host.store.InstallPlacementPlan(t.Context(), claim, request, decision, plan); err != nil {
		t.Fatal(err)
	}
	if !host.placeable(t) {
		t.Fatal("the fixture node was not placeable before the cordon")
	}
	revision, err := host.store.SetNodeCordon(t.Context(), host.id, host.node, true)
	if err != nil {
		t.Fatal(err)
	}
	if revision <= host.policy.Revision {
		t.Fatalf("revision=%d, want the policy advanced past %d", revision, host.policy.Revision)
	}
	if host.placeable(t) {
		t.Fatal("a cordoned node still took new placement")
	}
	// Nothing already placed moved. The reservation, its port, and its allocation
	// are untouched by a cordon.
	if live := host.count(t, `SELECT count(*) FROM resource_allocations
 WHERE installation_id=$1 AND node_id=$2 AND released_at IS NULL`, host.id, host.node); live != 1 {
		t.Fatalf("live allocations=%d, want the existing placement kept", live)
	}
	if ports := host.count(t, `SELECT count(*) FROM allocation_ports
 WHERE installation_id=$1 AND released_at IS NULL`, host.id); ports != 1 {
		t.Fatalf("live ports=%d, want the reserved port kept", ports)
	}
}

// Uncordon alone does not make a node placeable again: the policy change invalidates
// the capacity evidence, so the node must be measured before it takes work.
func TestUncordonNeedsFreshEvidence(t *testing.T) {
	host := placementFixture(t)
	if _, err := host.store.SetNodeCordon(t.Context(), host.id, host.node, true); err != nil {
		t.Fatal(err)
	}
	if _, err := host.store.SetNodeCordon(t.Context(), host.id, host.node, false); err != nil {
		t.Fatal(err)
	}
	if host.placeable(t) {
		t.Fatal("an uncordoned node took work on evidence taken under its old policy")
	}
	host.probe(t)
	if !host.placeable(t) {
		t.Fatal("a measured, uncordoned node was still refused")
	}
}

// A repeated cordon writes nothing. A revision bump throws away the node's capacity
// evidence, so a repeat that changes nothing must not cost the node its measurement.
func TestCordonIsIdempotent(t *testing.T) {
	host := placementFixture(t)
	first, err := host.store.SetNodeCordon(t.Context(), host.id, host.node, true)
	if err != nil {
		t.Fatal(err)
	}
	for range 3 {
		again, err := host.store.SetNodeCordon(t.Context(), host.id, host.node, true)
		if err != nil || again != first {
			t.Fatalf("revision=%d err=%v, want %d unchanged", again, err, first)
		}
	}
	// A node with no policy carries no work, so there is nothing to cordon.
	if _, err := host.store.SetNodeCordon(t.Context(), host.id, host.enroll(t), true); err == nil {
		t.Fatal("a node with no placement policy was cordoned")
	}
}
