package postgres

import (
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/placement"
)

// enroll adds a second node to the same installation. A cluster spans more than one
// node, and a delete is only finished when every one of them has answered.
func (h *placementHost) enroll(t *testing.T) uuid.UUID {
	t.Helper()
	node := uuid.New()
	if _, err := h.store.pool.Exec(t.Context(), `INSERT INTO nodes (id,installation_id,public_key_hash,enrolled_at)
 VALUES ($1,$2,$3,clock_timestamp())`, node, h.id, make([]byte, 32)); err != nil {
		t.Fatal(err)
	}
	return node
}

// A delete is not finished when the controller says so. It is finished when every
// node that held part of the cluster has proved its runtime objects are gone, so an
// offline agent holds the tombstone open for as long as it stays offline.
func TestTombstoneStaysOpenWhileANodeIsOffline(t *testing.T) {
	host := placementFixture(t)
	claim := host.claimCluster(t, "db")
	offline := host.enroll(t)
	request := TombstoneRequest{
		InstallationID: host.id, ClusterID: claim.ClusterID, DeletedByOperation: claim.OperationID,
		RetainedVolumes: 2, BackupDisposition: BackupsRetainedUnderPolicy,
		Nodes: []uuid.UUID{host.node, offline},
	}
	if err := host.store.CreateClusterTombstone(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	if err := host.store.AcknowledgeTombstone(t.Context(), host.id, claim.ClusterID, host.node); err != nil {
		t.Fatal(err)
	}
	// One node answered. The other never came back, so the delete is still open.
	found, err := host.store.ReadClusterTombstone(t.Context(), host.id, claim.ClusterID)
	if err != nil {
		t.Fatal(err)
	}
	if found.Cleared {
		t.Fatal("the tombstone cleared while a node had not proved cleanup")
	}
	if len(found.Pending) != 1 || found.Pending[0] != offline {
		t.Fatalf("pending=%v, want only the offline node", found.Pending)
	}
	if found.RetainedVolumes != 2 || found.BackupDisposition != BackupsRetainedUnderPolicy {
		t.Fatalf("tombstone=%+v, want what the delete retained kept", found)
	}
	// The node comes back and answers. Only now is the delete finished.
	if err := host.store.AcknowledgeTombstone(t.Context(), host.id, claim.ClusterID, offline); err != nil {
		t.Fatal(err)
	}
	found, err = host.store.ReadClusterTombstone(t.Context(), host.id, claim.ClusterID)
	if err != nil {
		t.Fatal(err)
	}
	if !found.Cleared || len(found.Pending) != 0 {
		t.Fatalf("tombstone=%+v, want it cleared once every node answered", found)
	}
}

// A delete and a purge are both retried. A repeat must be ordinary; a repeat that
// disagrees about what the delete retained is a delete nobody can account for.
func TestTombstoneRepeatsAreOrdinaryAndDisagreementIsNot(t *testing.T) {
	host := placementFixture(t)
	claim := host.claimCluster(t, "db")
	request := TombstoneRequest{
		InstallationID: host.id, ClusterID: claim.ClusterID, DeletedByOperation: claim.OperationID,
		RetainedVolumes: 1, BackupDisposition: BackupsNoPolicy, Nodes: []uuid.UUID{host.node},
	}
	if err := host.store.CreateClusterTombstone(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	if err := host.store.CreateClusterTombstone(t.Context(), request); err != nil {
		t.Fatalf("a repeated delete was refused: %v", err)
	}
	for name, change := range map[string]func(*TombstoneRequest){
		"another count":       func(r *TombstoneRequest) { r.RetainedVolumes = 0 },
		"another disposition": func(r *TombstoneRequest) { r.BackupDisposition = BackupsRetainedUnderPolicy },
		"another operation":   func(r *TombstoneRequest) { r.DeletedByOperation = uuid.New() },
	} {
		t.Run(name, func(t *testing.T) {
			changed := request
			change(&changed)
			if err := host.store.CreateClusterTombstone(t.Context(), changed); !errors.Is(err, ErrTombstoneConflict) {
				t.Fatalf("err=%v, want ErrTombstoneConflict", err)
			}
		})
	}
	// A repeated acknowledgement is an ordinary retry, and it clears the tombstone
	// once rather than reopening or double-counting it.
	for range 3 {
		if err := host.store.AcknowledgeTombstone(t.Context(), host.id, claim.ClusterID, host.node); err != nil {
			t.Fatalf("a repeated acknowledgement was refused: %v", err)
		}
	}
	if pending := host.count(t, `SELECT count(*) FROM cluster_tombstone_nodes
 WHERE cluster_id=$1 AND acknowledged_at IS NULL`, claim.ClusterID); pending != 0 {
		t.Fatalf("pending=%d, want none", pending)
	}
	// A node that was never part of the cluster cannot answer for it.
	if err := host.store.AcknowledgeTombstone(t.Context(), host.id, claim.ClusterID, host.enroll(t)); err == nil {
		t.Fatal("a node that never held the cluster acknowledged its cleanup")
	}
}

// A delete removes nothing by itself. Specification 33 retains data by default, so
// the volumes the cluster held stay declared and stay retained, and storage that
// belongs to anything else is untouched.
func TestDeleteLeavesRetainedAndUnrelatedStorageAlone(t *testing.T) {
	host := placementFixture(t)
	claim := host.claimCluster(t, "db")
	deleted := declared(host, claim.ClusterID)
	// Another cluster, on another node, that this delete has nothing to do with.
	other := host.claimCluster(t, "cache")
	elsewhere := declared(host, other.ClusterID)
	elsewhere.NodeID = host.enroll(t)
	for _, identity := range []VolumeIdentity{deleted, elsewhere} {
		if err := host.store.RecordVolumeIdentity(t.Context(), identity); err != nil {
			t.Fatal(err)
		}
	}
	request := TombstoneRequest{
		InstallationID: host.id, ClusterID: claim.ClusterID, DeletedByOperation: claim.OperationID,
		RetainedVolumes: 1, BackupDisposition: BackupsRetainedUnderPolicy, Nodes: []uuid.UUID{host.node},
	}
	if err := host.store.CreateClusterTombstone(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	if err := host.store.AcknowledgeTombstone(t.Context(), host.id, claim.ClusterID, host.node); err != nil {
		t.Fatal(err)
	}
	// The deleted cluster's storage is still declared and still retained. An operator
	// has to be able to find it after the cluster is gone.
	stored, err := host.store.ReadVolumeIdentity(t.Context(), host.id, deleted.ResourceID)
	if err != nil {
		t.Fatalf("the deleted cluster's storage was lost with it: %v", err)
	}
	if stored.Retention != Retained {
		t.Fatalf("retention=%q, want a delete to retain", stored.Retention)
	}
	// Nothing that belongs to anything else moved.
	if stored, err := host.store.ReadVolumeIdentity(t.Context(), host.id, elsewhere.ResourceID); err != nil || stored != elsewhere {
		t.Fatalf("stored=%+v err=%v, want another cluster's storage unchanged", stored, err)
	}
	if total := host.count(t, `SELECT count(*) FROM volume_identities WHERE installation_id=$1`, host.id); total != 2 {
		t.Fatalf("identities=%d, want both kept", total)
	}
	// Asking to purge one volume moves that volume's intent and nothing else.
	if err := host.store.SetVolumeRetention(t.Context(), host.id, deleted.ResourceID, PurgeRequested); err != nil {
		t.Fatal(err)
	}
	if stored, err := host.store.ReadVolumeIdentity(t.Context(), host.id, elsewhere.ResourceID); err != nil || stored.Retention != Retained {
		t.Fatalf("stored=%+v err=%v, want a purge request to reach one volume only", stored, err)
	}
}

func TestTombstoneRefusesAnUnsoundRecord(t *testing.T) {
	host := placementFixture(t)
	claim := host.claimCluster(t, "db")
	sound := TombstoneRequest{
		InstallationID: host.id, ClusterID: claim.ClusterID, DeletedByOperation: claim.OperationID,
		RetainedVolumes: 1, BackupDisposition: BackupsRetainedUnderPolicy, Nodes: []uuid.UUID{host.node},
	}
	for name, spoil := range map[string]func(*TombstoneRequest){
		// A delete that names no node claims cleanup nobody proved.
		"no nodes":               func(r *TombstoneRequest) { r.Nodes = nil },
		"a nil node":             func(r *TombstoneRequest) { r.Nodes = []uuid.UUID{uuid.Nil} },
		"a repeated node":        func(r *TombstoneRequest) { r.Nodes = []uuid.UUID{r.Nodes[0], r.Nodes[0]} },
		"no operation":           func(r *TombstoneRequest) { r.DeletedByOperation = uuid.Nil },
		"no cluster":             func(r *TombstoneRequest) { r.ClusterID = uuid.Nil },
		"negative retention":     func(r *TombstoneRequest) { r.RetainedVolumes = -1 },
		"unstated backups":       func(r *TombstoneRequest) { r.BackupDisposition = "" },
		"an unknown disposition": func(r *TombstoneRequest) { r.BackupDisposition = BackupDisposition("deleted") },
	} {
		t.Run(name, func(t *testing.T) {
			bad := sound
			spoil(&bad)
			if err := host.store.CreateClusterTombstone(t.Context(), bad); err == nil {
				t.Fatalf("an unsound delete record was accepted for %s", name)
			}
		})
	}
	// Nothing unsound was written.
	if total := host.count(t, `SELECT count(*) FROM cluster_tombstones WHERE installation_id=$1`, host.id); total != 0 {
		t.Fatalf("tombstones=%d, want none", total)
	}
}

// A released port tells the planner a host resource is free. An offline node may
// still be holding it, so the reservations stay until every node has answered.
func TestReservationsAreHeldUntilCleanupIsProved(t *testing.T) {
	host := placementFixture(t)
	claim := host.claimCluster(t, "db")
	port := placement.Port{Address: "127.0.0.1", Transport: "tcp", Number: 5432}
	request, decision, plan := host.place(t, claim, smallBudget, []placement.Port{port})
	if _, err := host.store.InstallPlacementPlan(t.Context(), claim, request, decision, plan); err != nil {
		t.Fatal(err)
	}
	retained := declared(host, claim.ClusterID)
	if err := host.store.RecordVolumeIdentity(t.Context(), retained); err != nil {
		t.Fatal(err)
	}
	offline := host.enroll(t)
	if err := host.store.CreateClusterTombstone(t.Context(), TombstoneRequest{
		InstallationID: host.id, ClusterID: claim.ClusterID, DeletedByOperation: claim.OperationID,
		RetainedVolumes: 1, BackupDisposition: BackupsRetainedUnderPolicy,
		Nodes: []uuid.UUID{host.node, offline},
	}); err != nil {
		t.Fatal(err)
	}
	live := `SELECT count(*) FROM allocation_ports WHERE installation_id=$1 AND released_at IS NULL`
	if err := host.store.ReleaseTombstonedPlacement(t.Context(), host.id, claim.ClusterID); !errors.Is(err, ErrTombstoneHeld) {
		t.Fatalf("err=%v, want the reservations held", err)
	}
	if err := host.store.AcknowledgeTombstone(t.Context(), host.id, claim.ClusterID, host.node); err != nil {
		t.Fatal(err)
	}
	// One node still owes an answer, so the port is still not free.
	if err := host.store.ReleaseTombstonedPlacement(t.Context(), host.id, claim.ClusterID); !errors.Is(err, ErrTombstoneHeld) {
		t.Fatalf("err=%v, want one offline node to hold the reservations", err)
	}
	if ports := host.count(t, live, host.id); ports != 1 {
		t.Fatalf("live ports=%d, want the port still reserved", ports)
	}
	if err := host.store.AcknowledgeTombstone(t.Context(), host.id, claim.ClusterID, offline); err != nil {
		t.Fatal(err)
	}
	if err := host.store.ReleaseTombstonedPlacement(t.Context(), host.id, claim.ClusterID); err != nil {
		t.Fatal(err)
	}
	if err := host.store.ReleaseTombstonedPlacement(t.Context(), host.id, claim.ClusterID); err != nil {
		t.Fatalf("a repeated release was refused: %v", err)
	}
	if ports := host.count(t, live, host.id); ports != 0 {
		t.Fatalf("live ports=%d, want the port free once cleanup was proved", ports)
	}
	if allocations := host.count(t, `SELECT count(*) FROM resource_allocations
 WHERE installation_id=$1 AND released_at IS NULL`, host.id); allocations != 0 {
		t.Fatalf("live allocations=%d, want the capacity free", allocations)
	}
	// Freeing the reservation frees capacity, not storage. The data the delete
	// retained is still declared and still retained.
	if stored, err := host.store.ReadVolumeIdentity(t.Context(), host.id, retained.ResourceID); err != nil || stored != retained {
		t.Fatalf("stored=%+v err=%v, want the release to touch reservations only", stored, err)
	}
}
