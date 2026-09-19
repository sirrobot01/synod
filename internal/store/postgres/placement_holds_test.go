package postgres

import (
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/operation"
	"github.com/sirrobot01/synod/internal/placement"
)

// held returns the one hold this installation carries, failing if there is not
// exactly one. A read that answers a different count is not a usable answer.
func held(t *testing.T, host *placementHost) Hold {
	t.Helper()
	holds, err := host.store.ReadPlacementHolds(t.Context(), host.id)
	if err != nil {
		t.Fatal(err)
	}
	if len(holds) != 1 {
		t.Fatalf("holds=%d, want one", len(holds))
	}
	return holds[0]
}

// A reservation must name its node and resource, so an operator can see where the
// capacity went and which work took it.
func TestReadPlacementHoldsNamesTheNodeAndResource(t *testing.T) {
	host := placementFixture(t)
	claim := host.claimCluster(t, "db")
	port := placement.Port{Address: "127.0.0.1", Transport: "tcp", Number: 5432}
	request, decision, plan := host.place(t, claim, smallBudget, []placement.Port{port})
	if _, err := host.store.InstallPlacementPlan(t.Context(), claim, request, decision, plan); err != nil {
		t.Fatal(err)
	}
	hold := held(t, host)
	if hold.ReservationID != request.Resources[0].ReservationID {
		t.Fatalf("reservation=%s, want the one that was installed", hold.ReservationID)
	}
	if hold.NodeID != host.node || hold.ResourceID != request.Resources[0].ID || hold.ClusterID != claim.ClusterID {
		t.Fatalf("hold=%+v, want it bound to the node and resource it reserved", hold)
	}
	if hold.OperationID != claim.OperationID || hold.Kind == "" {
		t.Fatalf("hold=%+v, want the operation that created it", hold)
	}
	// The capacity and the ports are what a later placement cannot use.
	if hold.Budget != smallBudget {
		t.Fatalf("budget=%+v, want the reserved amount", hold.Budget)
	}
	if len(hold.Ports) != 1 || hold.Ports[0] != port {
		t.Fatalf("ports=%+v, want the reserved host port", hold.Ports)
	}
}

// A working operation is not blocked. A held reservation only needs a decision
// when its holder has stopped or is waiting on one.
func TestReadPlacementHoldsMarksABlockedHolder(t *testing.T) {
	host := placementFixture(t)
	claim := host.claimCluster(t, "db")
	request, decision, plan := host.place(t, claim, smallBudget, nil)
	if _, err := host.store.InstallPlacementPlan(t.Context(), claim, request, decision, plan); err != nil {
		t.Fatal(err)
	}
	if hold := held(t, host); hold.Blocked || hold.Status != operation.Pending {
		t.Fatalf("hold=%+v, want a working holder that is not blocked", hold)
	}
	for _, status := range []operation.Status{operation.Waiting, operation.Cancelling, operation.Failed, operation.Cancelled} {
		if _, err := host.store.pool.Exec(t.Context(), `UPDATE operations SET status=$1 WHERE id=$2`, string(status), claim.OperationID); err != nil {
			t.Fatal(err)
		}
		hold := held(t, host)
		if !hold.Blocked || hold.Status != status {
			t.Fatalf("status=%s hold=%+v, want a blocked holder", status, hold)
		}
	}
	// Running work is progressing, so it is not holding capacity for a decision.
	if _, err := host.store.pool.Exec(t.Context(), `UPDATE operations SET status='running' WHERE id=$1`, claim.OperationID); err != nil {
		t.Fatal(err)
	}
	if hold := held(t, host); hold.Blocked {
		t.Fatalf("hold=%+v, want running work read as working", hold)
	}
}

// The bound operations are exactly what release consults, so a refused release and
// this read must give an operator the same reason.
func TestReadPlacementHoldsNamesWhatBlocksARelease(t *testing.T) {
	host := placementFixture(t)
	claim := host.claimCluster(t, "db")
	request, decision, plan := host.place(t, claim, smallBudget, nil)
	if _, err := host.store.InstallPlacementPlan(t.Context(), claim, request, decision, plan); err != nil {
		t.Fatal(err)
	}
	reservation := request.Resources[0].ReservationID
	hold := held(t, host)
	if len(hold.Bound) != 1 || hold.Bound[0].OperationID != claim.OperationID {
		t.Fatalf("bound=%+v, want the operation that still stands in the way", hold.Bound)
	}
	// That same operation is why the release is refused.
	if err := host.store.ReleaseUnusedPlacement(t.Context(), host.id, claim.OperationID, reservation); !errors.Is(err, ErrPlacementHeld) {
		t.Fatalf("an unfinished operation released its reservation: %v", err)
	}
	// An operation that stopped without success no longer blocks, and the read
	// stops naming it.
	if _, err := host.store.pool.Exec(t.Context(), `UPDATE operations SET status='failed',finished_at=clock_timestamp(),claim_active=false WHERE id=$1`, claim.OperationID); err != nil {
		t.Fatal(err)
	}
	if hold := held(t, host); len(hold.Bound) != 0 {
		t.Fatalf("bound=%+v, want nothing standing in the way", hold.Bound)
	}
}

// A released reservation is not held, so it must leave the answer.
func TestReadPlacementHoldsDropsAReleasedReservation(t *testing.T) {
	host := placementFixture(t)
	claim := host.claimCluster(t, "db")
	request, decision, plan := host.place(t, claim, smallBudget, []placement.Port{{Address: "127.0.0.1", Transport: "tcp", Number: 5432}})
	if _, err := host.store.InstallPlacementPlan(t.Context(), claim, request, decision, plan); err != nil {
		t.Fatal(err)
	}
	if _, err := host.store.pool.Exec(t.Context(), `UPDATE operations SET status='failed',finished_at=clock_timestamp(),claim_active=false WHERE id=$1`, claim.OperationID); err != nil {
		t.Fatal(err)
	}
	if err := host.store.ReleaseUnusedPlacement(t.Context(), host.id, claim.OperationID, request.Resources[0].ReservationID); err != nil {
		t.Fatal(err)
	}
	holds, err := host.store.ReadPlacementHolds(t.Context(), host.id)
	if err != nil {
		t.Fatal(err)
	}
	if len(holds) != 0 {
		t.Fatalf("holds=%+v, want nothing held after a release", holds)
	}
}

// The read answers for one installation only, and refuses a request that names none.
func TestReadPlacementHoldsIsScopedToItsInstallation(t *testing.T) {
	host := placementFixture(t)
	claim := host.claimCluster(t, "db")
	request, decision, plan := host.place(t, claim, smallBudget, nil)
	if _, err := host.store.InstallPlacementPlan(t.Context(), claim, request, decision, plan); err != nil {
		t.Fatal(err)
	}
	other, err := host.store.ReadPlacementHolds(t.Context(), uuid.New())
	if err != nil || len(other) != 0 {
		t.Fatalf("holds=%+v err=%v, want another installation to see nothing", other, err)
	}
	if _, err := host.store.ReadPlacementHolds(t.Context(), uuid.Nil); err == nil {
		t.Fatal("a request naming no installation was accepted")
	}
}

// The read never changes what it reports on. A reservation that failed to place
// must not cause anything to move.
func TestReadPlacementHoldsChangesNothing(t *testing.T) {
	host := placementFixture(t)
	claim := host.claimCluster(t, "db")
	request, decision, plan := host.place(t, claim, smallBudget, nil)
	if _, err := host.store.InstallPlacementPlan(t.Context(), claim, request, decision, plan); err != nil {
		t.Fatal(err)
	}
	before := host.count(t, `SELECT count(*) FROM resource_allocations WHERE released_at IS NULL`)
	for range 3 {
		if _, err := host.store.ReadPlacementHolds(t.Context(), host.id); err != nil {
			t.Fatal(err)
		}
	}
	if after := host.count(t, `SELECT count(*) FROM resource_allocations WHERE released_at IS NULL`); after != before {
		t.Fatalf("allocations before=%d after=%d, want a read that changes nothing", before, after)
	}
	if moved := host.count(t, `SELECT count(*) FROM resource_allocations WHERE node_id<>$1`, host.node); moved != 0 {
		t.Fatal("a reservation moved to another node")
	}
}

// A reservation that cannot be placed must leave existing data where it is.
// Specification 33 states that a failed reservation never justifies relocating it.
func TestAFailedReservationRelocatesNothing(t *testing.T) {
	host := placementFixture(t)
	first := host.claimCluster(t, "db")
	request, decision, plan := host.place(t, first, smallBudget, []placement.Port{{Address: "127.0.0.1", Transport: "tcp", Number: 5432}})
	if _, err := host.store.InstallPlacementPlan(t.Context(), first, request, decision, plan); err != nil {
		t.Fatal(err)
	}
	before := held(t, host)

	// A second cluster asks for more than the node can still offer. Planning it
	// must fail rather than move the reservation already standing.
	second := host.claimCluster(t, "other")
	huge := placement.Vector{CPUMillis: 1 << 40, MemoryBytes: 1 << 50, DiskBytes: 1 << 50}
	next := placementRequest(host.id, second.ClusterID, second.OperationID, host.policy, nil)
	next.Resources[0].Budgets.Workload = &huge
	snapshot, err := host.store.ReadPlacementSnapshot(t.Context(), host.id)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := placement.Plan(next, snapshot); err == nil {
		t.Fatal("a request beyond the node's capacity was placed")
	}

	after := held(t, host)
	if after.ReservationID != before.ReservationID || after.NodeID != before.NodeID || after.ResourceID != before.ResourceID {
		t.Fatalf("before=%+v after=%+v, want the standing reservation untouched", before, after)
	}
	if after.Budget != before.Budget || len(after.Ports) != len(before.Ports) {
		t.Fatalf("the failed request changed what the standing reservation holds")
	}
	if total := host.count(t, `SELECT count(*) FROM resource_allocations`); total != 1 {
		t.Fatalf("allocations=%d, want the one that was installed", total)
	}
}
