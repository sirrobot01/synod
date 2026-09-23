package postgres

import (
	"encoding/json"
	"errors"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/placement"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// readyNode gives one node a policy, a challenge, and synthetic capacity evidence.
func readyNode(t *testing.T, s *Store, installation, node, session uuid.UUID) placement.NodePolicy {
	t.Helper()
	policy := testPolicy(installation, node)
	revision, err := s.PutNodePlacementPolicy(t.Context(), policy, 0)
	if err != nil {
		t.Fatal(err)
	}
	policy.Revision = revision
	probe, err := s.BeginCapacityProbe(t.Context(), installation, node, session)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.RecordNodeCapacity(t.Context(), installation, node, testReport(probe, policy)); err != nil {
		t.Fatal(err)
	}
	return policy
}

// placementRequest asks for one small PostgreSQL instance that the fixture node can hold.
func placementRequest(installation, clusterID, operationID uuid.UUID, policy placement.NodePolicy, ports []placement.Port) placement.Request {
	resource := uuid.New()
	return placement.Request{
		Version: 1, InstallationID: installation, ClusterID: clusterID, OperationID: operationID,
		DesiredRevision: 1, Profile: "compact",
		Resources: []placement.Resource{{
			ID: resource, ReservationID: uuid.New(), OwnedResourceIDs: []uuid.UUID{resource},
			Kind: "instance", Role: "primary", Engine: "postgres", Version: "17.11",
			Architecture: policy.Architecture, RuntimeVersion: policy.RuntimeVersion, StorageID: policy.StorageID,
			Labels: map[string]string{"zone": "a"},
			Budgets: placement.Budgets{
				Workload:   &placement.Vector{CPUMillis: 1000, MemoryBytes: 1 << 30, DiskBytes: 1 << 32},
				Supporting: &placement.Vector{},
				Backup:     &placement.Vector{},
			},
			Ports: ports,
		}},
	}
}

// The snapshot must carry everything the planner needs to place a resource.
func TestPlacementSnapshotFeedsThePlanner(t *testing.T) {
	s, installation, node, session := capacityFixture(t)
	policy := readyNode(t, s, installation, node, session)
	snapshot, err := s.ReadPlacementSnapshot(t.Context(), installation)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.InstallationID != installation || snapshot.ObservedAt.IsZero() || len(snapshot.Nodes) != 1 || len(snapshot.Allocations) != 0 {
		t.Fatalf("snapshot %+v", snapshot)
	}
	observed := snapshot.Nodes[0]
	if observed.ID != node || observed.Revoked || observed.SessionID != session || observed.RecoveryRequired {
		t.Fatalf("node %+v", observed)
	}
	if observed.Policy == nil || observed.Probe == nil || observed.Report == nil {
		t.Fatalf("node evidence %+v", observed)
	}
	if observed.BootID != observed.Probe.BootID || observed.JournalGeneration != observed.Probe.JournalGeneration {
		t.Fatal("the snapshot lost the session journal identity")
	}
	if observed.LastSeenAt.IsZero() || observed.LastSeenAt.After(snapshot.ObservedAt) {
		t.Fatalf("presence %v observed %v", observed.LastSeenAt, snapshot.ObservedAt)
	}
	decision, err := placement.Plan(placementRequest(installation, uuid.New(), uuid.New(), policy, nil), snapshot)
	if err != nil {
		t.Fatalf("the planner refused a ready node: %v", err)
	}
	if len(decision.Assignments) != 1 || decision.Assignments[0].NodeID != node {
		t.Fatalf("decision %+v", decision)
	}
	if decision.Assignments[0].PolicyRevision != policy.Revision || decision.Assignments[0].ProbeRevision != observed.Probe.Revision {
		t.Fatalf("the decision lost its input revisions: %+v", decision.Assignments[0])
	}
}

// Each excluded condition must reach the planner through the snapshot.
func TestPlacementSnapshotReportsExcludingConditions(t *testing.T) {
	for name, exclude := range map[string]func(*testing.T, *Store, uuid.UUID, uuid.UUID, uuid.UUID){
		"revoked": func(t *testing.T, s *Store, installation, node, session uuid.UUID) {
			if _, err := s.pool.Exec(t.Context(), `UPDATE nodes SET revoked_at=clock_timestamp() WHERE id=$1`, node); err != nil {
				t.Fatal(err)
			}
		},
		"recovering": func(t *testing.T, s *Store, installation, node, session uuid.UUID) {
			if _, err := s.pool.Exec(t.Context(), `UPDATE agent_presence SET recovery_required=true WHERE node_id=$1`, node); err != nil {
				t.Fatal(err)
			}
		},
		"stale presence": func(t *testing.T, s *Store, installation, node, session uuid.UUID) {
			if _, err := s.pool.Exec(t.Context(), `UPDATE agent_presence SET last_seen_at=clock_timestamp()-interval '1 hour' WHERE node_id=$1`, node); err != nil {
				t.Fatal(err)
			}
		},
		"no capacity report": func(t *testing.T, s *Store, installation, node, session uuid.UUID) {
			if _, err := s.pool.Exec(t.Context(), `UPDATE node_capacity SET report_bytes=NULL WHERE node_id=$1`, node); err != nil {
				t.Fatal(err)
			}
		},
		"no policy": func(t *testing.T, s *Store, installation, node, session uuid.UUID) {
			if _, err := s.pool.Exec(t.Context(), `DELETE FROM node_placement_policies WHERE node_id=$1`, node); err != nil {
				t.Fatal(err)
			}
		},
	} {
		t.Run(name, func(t *testing.T) {
			s, installation, node, session := capacityFixture(t)
			policy := readyNode(t, s, installation, node, session)
			exclude(t, s, installation, node, session)
			snapshot, err := s.ReadPlacementSnapshot(t.Context(), installation)
			if err != nil {
				t.Fatal(err)
			}
			if len(snapshot.Nodes) != 1 {
				t.Fatalf("nodes %d, want the excluded node still reported", len(snapshot.Nodes))
			}
			if _, err := placement.Plan(placementRequest(installation, uuid.New(), uuid.New(), policy, nil), snapshot); !errors.Is(err, placement.ErrUnsatisfied) {
				t.Fatalf("an excluded node was used: %v", err)
			}
		})
	}
}

// A cordoned or maintenance node stays out of new placement without losing its record.
func TestPlacementSnapshotReportsOperatorState(t *testing.T) {
	s, installation, node, session := capacityFixture(t)
	policy := readyNode(t, s, installation, node, session)
	policy.Cordoned = true
	revision, err := s.PutNodePlacementPolicy(t.Context(), policy, policy.Revision)
	if err != nil {
		t.Fatal(err)
	}
	policy.Revision = revision
	snapshot, err := s.ReadPlacementSnapshot(t.Context(), installation)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Nodes[0].Policy == nil || !snapshot.Nodes[0].Policy.Cordoned {
		t.Fatal("the snapshot lost the cordon")
	}
	if _, err := placement.Plan(placementRequest(installation, uuid.New(), uuid.New(), policy, nil), snapshot); !errors.Is(err, placement.ErrUnsatisfied) {
		t.Fatalf("a cordoned node was used: %v", err)
	}
	// A new policy revision leaves the old challenge behind, so evidence is stale too.
	if snapshot.Nodes[0].Probe.PolicyRevision == revision {
		t.Fatal("a policy change kept its old probe revision")
	}
}

// A held allocation must reach the planner, and a released one must not hold capacity.
func TestPlacementSnapshotReportsAllocations(t *testing.T) {
	s, installation, node, session := capacityFixture(t)
	policy := readyNode(t, s, installation, node, session)
	request := testRequest()
	request.Identity.InstallationID = installation
	admission, err := s.CreateCluster(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	clusterID, operationID := admission.ClusterID, admission.OperationID
	held := placement.Allocation{
		ID: uuid.New(), InstallationID: installation, NodeID: node, ClusterID: clusterID, ResourceID: uuid.New(),
		CreatedByOperation: operationID, CreatedRevision: 1,
		Budget: placement.Vector{CPUMillis: 7000, MemoryBytes: 1 << 33, DiskBytes: 1 << 39},
		Ports:  []placement.Port{{Address: "127.0.0.1", Transport: "tcp", Number: 5432}},
	}
	held.OwnedResourceIDs = []uuid.UUID{held.ResourceID}
	s.seedAllocation(t, held, operationID)
	snapshot, err := s.ReadPlacementSnapshot(t.Context(), installation)
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Allocations) != 1 || snapshot.Allocations[0].Released || snapshot.Allocations[0].ID != held.ID {
		t.Fatalf("allocations %+v", snapshot.Allocations)
	}
	if len(snapshot.Allocations[0].Ports) != 1 || snapshot.Allocations[0].Ports[0] != held.Ports[0] {
		t.Fatalf("ports %+v", snapshot.Allocations[0].Ports)
	}
	// The held budget consumes the node, so a new request cannot fit.
	if _, err := placement.Plan(placementRequest(installation, clusterID, uuid.New(), policy, nil), snapshot); !errors.Is(err, placement.ErrUnsatisfied) {
		t.Fatalf("a held allocation did not hold capacity: %v", err)
	}
	// Its port is taken as well.
	if _, err := placement.Plan(placementRequest(installation, clusterID, uuid.New(), policy, held.Ports), snapshot); !errors.Is(err, placement.ErrUnsatisfied) {
		t.Fatalf("a held port was offered again: %v", err)
	}
	// A tombstone holds neither capacity nor a port, so it leaves the planning snapshot.
	// Counting one would eventually exhaust the planning bound with no way back.
	if _, err := s.pool.Exec(t.Context(), `UPDATE resource_allocations SET released_at=clock_timestamp() WHERE id=$1`, held.ID); err != nil {
		t.Fatal(err)
	}
	snapshot, err = s.ReadPlacementSnapshot(t.Context(), installation)
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Allocations) != 0 {
		t.Fatalf("a released allocation stayed in the planning snapshot: %+v", snapshot.Allocations)
	}
	if _, err := placement.Plan(placementRequest(installation, clusterID, uuid.New(), policy, held.Ports), snapshot); err != nil {
		t.Fatalf("a released allocation still held capacity: %v", err)
	}
	// The record itself survives. Release is a tombstone, never a delete.
	var rows int
	if err := s.pool.QueryRow(t.Context(), `SELECT count(*) FROM resource_allocations WHERE id=$1`, held.ID).Scan(&rows); err != nil || rows != 1 {
		t.Fatalf("the release deleted the record: %d %v", rows, err)
	}
}

func TestPlacementSnapshotRejectsAnEmptyInstallation(t *testing.T) {
	s := testStore(t)
	if _, err := s.ReadPlacementSnapshot(t.Context(), uuid.Nil); err == nil {
		t.Fatal("accepted a nil installation")
	}
	snapshot, err := s.ReadPlacementSnapshot(t.Context(), uuid.New())
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Nodes) != 0 || len(snapshot.Allocations) != 0 || snapshot.ObservedAt.IsZero() {
		t.Fatalf("snapshot %+v", snapshot)
	}
}

// seedAllocation writes one allocation and its mirror rows directly.
// Stage three replaces this with InstallPlacementPlan.
func (s *Store) seedAllocation(t *testing.T, allocation placement.Allocation, operationID uuid.UUID) {
	t.Helper()
	encoded, err := json.Marshal(allocation)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.pool.Exec(t.Context(), `INSERT INTO resource_allocations (id,installation_id,node_id,cluster_id,resource_id,created_operation_id,created_revision,allocation_bytes)
 VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`,
		allocation.ID, allocation.InstallationID, allocation.NodeID, allocation.ClusterID, allocation.ResourceID,
		operationID, allocation.CreatedRevision, encoded); err != nil {
		t.Fatal(err)
	}
	for _, resource := range allocation.OwnedResourceIDs {
		if _, err := s.pool.Exec(t.Context(), `INSERT INTO allocation_resources (installation_id,cluster_id,node_id,allocation_id,resource_id) VALUES ($1,$2,$3,$4,$5)`,
			allocation.InstallationID, allocation.ClusterID, allocation.NodeID, allocation.ID, resource); err != nil {
			t.Fatal(err)
		}
	}
	for _, port := range allocation.Ports {
		if _, err := s.pool.Exec(t.Context(), `INSERT INTO allocation_ports (installation_id,cluster_id,node_id,allocation_id,address,transport,port) VALUES ($1,$2,$3,$4,$5,$6,$7)`,
			allocation.InstallationID, allocation.ClusterID, allocation.NodeID, allocation.ID, port.Address, port.Transport, port.Number); err != nil {
			t.Fatal(err)
		}
	}
}

// The observation time must never precede a record that the same snapshot returns.
// The planner rejects any node it sees in the future, so a heartbeat that commits
// during the read would otherwise drop a healthy node at random.
func TestPlacementSnapshotNeverObservesTheFuture(t *testing.T) {
	s, installation, node, session := capacityFixture(t)
	readyNode(t, s, installation, node, session)
	stop := make(chan struct{})
	var beating sync.WaitGroup
	beating.Go(func() {
		for sequence := uint64(1); ; sequence++ {
			select {
			case <-stop:
				return
			default:
			}
			if err := s.RecordAgentHeartbeat(t.Context(), installation, node, session, &agentv1.Heartbeat{Sequence: sequence, ObservedAt: timestamppb.Now()}); err != nil {
				return
			}
		}
	})
	defer func() { close(stop); beating.Wait() }()
	for range 50 {
		snapshot, err := s.ReadPlacementSnapshot(t.Context(), installation)
		if err != nil {
			t.Fatal(err)
		}
		for _, observed := range snapshot.Nodes {
			if observed.LastSeenAt.After(snapshot.ObservedAt) {
				t.Fatalf("presence %v is after the observation time %v", observed.LastSeenAt, snapshot.ObservedAt)
			}
		}
	}
}
