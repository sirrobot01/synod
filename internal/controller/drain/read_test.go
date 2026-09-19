package drain

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/cluster"
	"github.com/sirrobot01/synod/internal/operation"
	"github.com/sirrobot01/synod/internal/placement"
	"github.com/sirrobot01/synod/internal/store/postgres"
)

// The real store must satisfy the boundary this package reads through.
var _ Store = (*postgres.Store)(nil)

// fakeStore answers the four reads this package makes. The reads themselves are the
// store's own contract and are covered there; what this stands in for is the
// assembly, which is what Read owns.
type fakeStore struct {
	snapshot   placement.Snapshot
	holds      []postgres.Hold
	identities []postgres.VolumeIdentity
	clusters   map[uuid.UUID]cluster.Cluster
	asked      []uuid.UUID
}

func (f *fakeStore) ReadPlacementSnapshot(context.Context, uuid.UUID) (placement.Snapshot, error) {
	return f.snapshot, nil
}

func (f *fakeStore) ReadPlacementHolds(context.Context, uuid.UUID) ([]postgres.Hold, error) {
	return f.holds, nil
}

func (f *fakeStore) ReadNodeVolumeIdentities(_ context.Context, _, nodeID uuid.UUID) ([]postgres.VolumeIdentity, error) {
	var found []postgres.VolumeIdentity
	for _, identity := range f.identities {
		if identity.NodeID == nodeID {
			found = append(found, identity)
		}
	}
	return found, nil
}

func (f *fakeStore) GetCluster(_ context.Context, _, clusterID uuid.UUID) (cluster.Cluster, error) {
	f.asked = append(f.asked, clusterID)
	found, ok := f.clusters[clusterID]
	if !ok {
		return cluster.Cluster{}, errors.New("no such cluster")
	}
	return found, nil
}

func node(id uuid.UUID, cordoned bool) placement.Node {
	return placement.Node{ID: id, Policy: &placement.NodePolicy{NodeID: id, Revision: 1, Cordoned: cordoned}}
}

// Read must answer for one node only, and must find every cluster that holds it,
// whether it holds the node through a reservation or only through its storage.
func TestReadAnswersForOneNode(t *testing.T) {
	installation, here, elsewhere := uuid.New(), uuid.New(), uuid.New()
	live, stored := uuid.New(), uuid.New()
	spec := cluster.CreateSpec{SchemaVersion: 1, Name: "db", Engine: cluster.PostgreSQL,
		PostgreSQL: &cluster.PostgreSQLSpec{Version: "17.11", Topology: "standalone"}}
	store := &fakeStore{
		snapshot: placement.Snapshot{InstallationID: installation, Nodes: []placement.Node{node(here, true), node(elsewhere, false)}},
		holds: []postgres.Hold{
			{ClusterID: live, NodeID: here, ResourceID: uuid.New(), Status: operation.Succeeded},
			{ClusterID: live, NodeID: here, ResourceID: uuid.New(), Status: operation.Succeeded},
			// Another node's work must not appear in this node's drain.
			{ClusterID: uuid.New(), NodeID: elsewhere, ResourceID: uuid.New(), Status: operation.Succeeded},
		},
		identities: []postgres.VolumeIdentity{
			// Storage for a cluster with nothing running here. It is exactly what
			// stops this node being declared free.
			{ClusterID: stored, NodeID: here, ResourceID: uuid.New(), Retention: postgres.Retained, CapacityRequestBytes: 1 << 30},
			{ClusterID: uuid.New(), NodeID: elsewhere, ResourceID: uuid.New(), Retention: postgres.Retained},
		},
		clusters: map[uuid.UUID]cluster.Cluster{
			live:   {ID: live, Spec: spec, DesiredLifecycle: cluster.DesiredRunning},
			stored: {ID: stored, Spec: spec, DesiredLifecycle: cluster.DesiredDeleted},
		},
	}
	state, err := Read(t.Context(), store, installation, here)
	if err != nil {
		t.Fatal(err)
	}
	if !state.Cordoned || state.NodeID != here {
		t.Fatalf("state=%+v, want the cordoned node under drain", state)
	}
	if len(state.Workloads) != 2 {
		t.Fatalf("workloads=%d, want the running cluster and the one that only holds storage", len(state.Workloads))
	}
	for _, workload := range state.Workloads {
		switch workload.ClusterID {
		case live:
			if len(workload.Resources) != 2 || len(workload.Volumes) != 0 || workload.InFlight {
				t.Fatalf("workload=%+v, want two reservations and no storage", workload)
			}
			if workload.DesiredLifecycle != cluster.DesiredRunning || workload.Spec.Name != "db" {
				t.Fatalf("workload=%+v, want the cluster's own spec", workload)
			}
		case stored:
			if len(workload.Resources) != 0 || len(workload.Volumes) != 1 {
				t.Fatalf("workload=%+v, want storage with nothing running", workload)
			}
		default:
			t.Fatalf("workload=%+v, want only this node's clusters", workload)
		}
	}
	// One read per cluster, not one per hold.
	if len(store.asked) != 2 {
		t.Fatalf("cluster reads=%d, want one per cluster", len(store.asked))
	}
}

// A cluster being changed right now must be named, because acting on it races the
// operation that is already running.
func TestReadReportsAnOperationInFlight(t *testing.T) {
	installation, here, live := uuid.New(), uuid.New(), uuid.New()
	spec := cluster.CreateSpec{SchemaVersion: 1, Name: "db", Engine: cluster.PostgreSQL,
		PostgreSQL: &cluster.PostgreSQLSpec{Version: "17.11", Topology: "standalone"}}
	for _, status := range []operation.Status{operation.Pending, operation.Running, operation.Waiting, operation.Cancelling} {
		t.Run(string(status), func(t *testing.T) {
			store := &fakeStore{
				snapshot: placement.Snapshot{InstallationID: installation, Nodes: []placement.Node{node(here, true)}},
				holds:    []postgres.Hold{{ClusterID: live, NodeID: here, ResourceID: uuid.New(), Status: status}},
				clusters: map[uuid.UUID]cluster.Cluster{live: {ID: live, Spec: spec, DesiredLifecycle: cluster.DesiredRunning}},
			}
			state, err := Read(t.Context(), store, installation, here)
			if err != nil || len(state.Workloads) != 1 || !state.Workloads[0].InFlight {
				t.Fatalf("state=%+v err=%v, want the running operation reported", state, err)
			}
			found := Decide(state).Clusters[0]
			if !slices.Contains(found.Unhandled, ReasonOperationInFlight) {
				t.Fatalf("unhandled=%v, want %s", found.Unhandled, ReasonOperationInFlight)
			}
		})
	}
}

// A drain of a node this installation does not have is a mistake, not an empty report.
func TestReadRefusesAnUnknownNode(t *testing.T) {
	installation, here := uuid.New(), uuid.New()
	store := &fakeStore{snapshot: placement.Snapshot{InstallationID: installation, Nodes: []placement.Node{node(here, true)}}}
	if _, err := Read(t.Context(), store, installation, uuid.New()); !errors.Is(err, ErrNodeUnknown) {
		t.Fatalf("err=%v, want ErrNodeUnknown", err)
	}
	if _, err := Read(t.Context(), nil, installation, here); !errors.Is(err, ErrNodeUnknown) {
		t.Fatalf("err=%v, want a missing store refused", err)
	}
}
