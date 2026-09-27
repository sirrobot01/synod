package postgres

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/operation"
	"github.com/sirrobot01/synod/internal/placement"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
)

// capacityFixture enrolls one node and connects one agent session.
func capacityFixture(t *testing.T) (*Store, uuid.UUID, uuid.UUID, uuid.UUID) {
	t.Helper()
	s := testStore(t)
	installation, node, session := uuid.New(), uuid.New(), uuid.New()
	if _, err := s.pool.Exec(t.Context(), `INSERT INTO nodes (id,installation_id,public_key_hash,enrolled_at) VALUES ($1,$2,$3,clock_timestamp())`, node, installation, make([]byte, 32)); err != nil {
		t.Fatal(err)
	}
	hello := &agentv1.Hello{InstallationId: installation.String(), NodeId: node.String(), BootId: uuid.NewString(), JournalGeneration: uuid.NewString(), Protocol: &agentv1.VersionRange{Minimum: 1, Maximum: 1}, ActivePayloadVersions: []uint32{1}}
	if err := s.RecordAgentSession(t.Context(), installation, node, session, hello); err != nil {
		t.Fatal(err)
	}
	return s, installation, node, session
}

func testPolicy(installation, node uuid.UUID) placement.NodePolicy {
	return placement.NodePolicy{
		InstallationID: installation, NodeID: node, Architecture: "amd64", RuntimeVersion: "27.1.1", StorageID: "pool-a",
		Engines:  []placement.EngineVersion{{Engine: "postgres", Version: "17.11"}},
		Labels:   map[string]string{"zone": "a"},
		Headroom: placement.Vector{CPUMillis: 500, MemoryBytes: 1 << 28, DiskBytes: 1 << 30},
	}
}

// testReport answers one challenge with synthetic host capacity.
// It is test input. It does not qualify actual host capacity collection.
func testReport(probe placement.CapacityProbe, policy placement.NodePolicy) placement.CapacityReport {
	return placement.CapacityReport{
		ProbeID: probe.ID, ProbeRevision: probe.Revision, InstallationID: probe.InstallationID, NodeID: probe.NodeID,
		SessionID: probe.SessionID, BootID: probe.BootID, JournalGeneration: probe.JournalGeneration,
		Architecture: policy.Architecture, RuntimeVersion: policy.RuntimeVersion, StorageID: policy.StorageID,
		Capacity: placement.Vector{CPUMillis: 8000, MemoryBytes: 1 << 34, DiskBytes: 1 << 40},
		Used:     placement.Vector{CPUMillis: 500, MemoryBytes: 1 << 30, DiskBytes: 1 << 35},
	}
}

func TestPolicyRevisionAdvancesAndRejectsARace(t *testing.T) {
	s, installation, node, _ := capacityFixture(t)
	policy := testPolicy(installation, node)
	first, err := s.PutNodePlacementPolicy(t.Context(), policy, 0)
	if err != nil || first != 1 {
		t.Fatalf("revision %d: %v", first, err)
	}
	if _, err := s.PutNodePlacementPolicy(t.Context(), policy, 0); !errors.Is(err, ErrPolicyRevision) {
		t.Fatalf("a stale expected revision was accepted: %v", err)
	}
	policy.Labels = map[string]string{"zone": "b"}
	second, err := s.PutNodePlacementPolicy(t.Context(), policy, first)
	if err != nil || second != 2 {
		t.Fatalf("revision %d: %v", second, err)
	}
	var stored int64
	if err := s.pool.QueryRow(t.Context(), `SELECT revision FROM node_placement_policies WHERE node_id=$1`, node).Scan(&stored); err != nil || stored != 2 {
		t.Fatalf("stored revision %d: %v", stored, err)
	}
	// A revoked node keeps no policy authority.
	if _, err := s.pool.Exec(t.Context(), `UPDATE nodes SET revoked_at=clock_timestamp() WHERE id=$1`, node); err != nil {
		t.Fatal(err)
	}
	if _, err := s.PutNodePlacementPolicy(t.Context(), policy, second); !errors.Is(err, ErrStaleSession) {
		t.Fatalf("a revoked node accepted policy: %v", err)
	}
}

func TestPolicyRejectsInvalidInput(t *testing.T) {
	s, installation, node, _ := capacityFixture(t)
	for name, change := range map[string]func(*placement.NodePolicy){
		"no node":           func(p *placement.NodePolicy) { p.NodeID = uuid.Nil },
		"other node":        func(p *placement.NodePolicy) { p.NodeID = uuid.New() },
		"unknown engine":    func(p *placement.NodePolicy) { p.Engines[0].Engine = "sqlite" },
		"unknown machine":   func(p *placement.NodePolicy) { p.Architecture = "riscv" },
		"no headroom":       func(p *placement.NodePolicy) { p.Headroom.DiskBytes = 0 },
		"negative headroom": func(p *placement.NodePolicy) { p.Headroom.CPUMillis = -1 },
	} {
		policy := testPolicy(installation, node)
		policy.Engines = []placement.EngineVersion{{Engine: "postgres", Version: "17.11"}}
		change(&policy)
		if _, err := s.PutNodePlacementPolicy(t.Context(), policy, 0); err == nil {
			t.Fatalf("%s: accepted", name)
		}
	}
}

func TestCapacityProbeBindsTheCurrentSession(t *testing.T) {
	s, installation, node, session := capacityFixture(t)
	initial, err := s.BeginCapacityProbe(t.Context(), installation, node, session)
	if err != nil || initial.PolicyRevision != 0 || initial.Revision != 1 {
		t.Fatalf("initial facts probe: %+v %v", initial, err)
	}
	policy := testPolicy(installation, node)
	revision, err := s.PutNodePlacementPolicy(t.Context(), policy, 0)
	if err != nil {
		t.Fatal(err)
	}
	probe, err := s.BeginCapacityProbe(t.Context(), installation, node, session)
	if err != nil {
		t.Fatal(err)
	}
	if probe.ID == uuid.Nil || probe.Revision != 2 || probe.SessionID != session || probe.PolicyRevision != revision {
		t.Fatalf("probe %+v", probe)
	}
	if !probe.ExpiresAt.Equal(probe.IssuedAt.Add(placement.ProbeWindow)) || !probe.ValidUntil.Equal(probe.IssuedAt.Add(placement.CapacityAge)) {
		t.Fatalf("probe window %+v", probe)
	}
	// A second challenge advances the revision and replaces the first.
	next, err := s.BeginCapacityProbe(t.Context(), installation, node, session)
	if err != nil || next.Revision != 3 || next.ID == probe.ID {
		t.Fatalf("second probe %+v: %v", next, err)
	}
	if err := s.RecordNodeCapacity(t.Context(), installation, node, testReport(probe, policy)); !errors.Is(err, ErrProbeExpired) {
		t.Fatalf("a replaced challenge published capacity: %v", err)
	}
	// A replaced session cannot answer the challenge it did not receive.
	replacement := uuid.New()
	presence, err := s.GetAgentPresence(t.Context(), installation, node)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.RecordAgentSession(t.Context(), installation, node, replacement, presence.Hello); err != nil {
		t.Fatal(err)
	}
	if err := s.RecordNodeCapacity(t.Context(), installation, node, testReport(next, policy)); !errors.Is(err, ErrStaleSession) {
		t.Fatalf("a replaced session published capacity: %v", err)
	}
}

func TestCapacityReportReplayAndConflict(t *testing.T) {
	s, installation, node, session := capacityFixture(t)
	policy := testPolicy(installation, node)
	if _, err := s.PutNodePlacementPolicy(t.Context(), policy, 0); err != nil {
		t.Fatal(err)
	}
	probe, err := s.BeginCapacityProbe(t.Context(), installation, node, session)
	if err != nil {
		t.Fatal(err)
	}
	report := testReport(probe, policy)
	if err := s.RecordNodeCapacity(t.Context(), installation, node, report); err != nil {
		t.Fatal(err)
	}
	// An exact replay is idempotent.
	if err := s.RecordNodeCapacity(t.Context(), installation, node, report); err != nil {
		t.Fatalf("an exact replay failed: %v", err)
	}
	changed := report
	changed.Used = placement.Vector{CPUMillis: 600, MemoryBytes: 1 << 30, DiskBytes: 1 << 35}
	if err := s.RecordNodeCapacity(t.Context(), installation, node, changed); !errors.Is(err, operation.ErrCommandConflict) {
		t.Fatalf("a changed answer for one challenge: %v", err)
	}
	var stored []byte
	if err := s.pool.QueryRow(t.Context(), `SELECT report_bytes FROM node_capacity WHERE node_id=$1`, node).Scan(&stored); err != nil || stored == nil {
		t.Fatalf("stored report: %v", err)
	}
}

func TestCapacityReportRejectsWeakEvidence(t *testing.T) {
	s, installation, node, session := capacityFixture(t)
	policy := testPolicy(installation, node)
	if _, err := s.PutNodePlacementPolicy(t.Context(), policy, 0); err != nil {
		t.Fatal(err)
	}
	probe, err := s.BeginCapacityProbe(t.Context(), installation, node, session)
	if err != nil {
		t.Fatal(err)
	}
	for name, change := range map[string]func(*placement.CapacityReport){
		"other challenge":   func(r *placement.CapacityReport) { r.ProbeID = uuid.New() },
		"other revision":    func(r *placement.CapacityReport) { r.ProbeRevision = probe.Revision + 1 },
		"other session":     func(r *placement.CapacityReport) { r.SessionID = uuid.New() },
		"other boot":        func(r *placement.CapacityReport) { r.BootID = uuid.New() },
		"other journal":     func(r *placement.CapacityReport) { r.JournalGeneration = uuid.New() },
		"no capacity":       func(r *placement.CapacityReport) { r.Capacity.DiskBytes = 0 },
		"negative use":      func(r *placement.CapacityReport) { r.Used.CPUMillis = -1 },
		"unknown machine":   func(r *placement.CapacityReport) { r.Architecture = "riscv" },
		"repeated item":     func(r *placement.CapacityReport) { r.Reservations = repeatedUsage() },
		"item beyond total": func(r *placement.CapacityReport) { r.Reservations = oversizedUsage() },
	} {
		report := testReport(probe, policy)
		change(&report)
		if err := s.RecordNodeCapacity(t.Context(), installation, node, report); err == nil {
			t.Fatalf("%s: accepted", name)
		}
	}
	// Another node cannot publish this node's capacity.
	if err := s.RecordNodeCapacity(t.Context(), installation, uuid.New(), testReport(probe, policy)); !errors.Is(err, operation.ErrStaleOwner) {
		t.Fatalf("another node published capacity: %v", err)
	}
	var stored []byte
	if err := s.pool.QueryRow(t.Context(), `SELECT report_bytes FROM node_capacity WHERE node_id=$1`, node).Scan(&stored); err != nil || stored != nil {
		t.Fatalf("weak evidence reached the store: %v", err)
	}
}

func repeatedUsage() []placement.Usage {
	id, resource := uuid.New(), uuid.New()
	return []placement.Usage{{ReservationID: id, ResourceID: resource}, {ReservationID: id, ResourceID: uuid.New()}}
}

func oversizedUsage() []placement.Usage {
	return []placement.Usage{{ReservationID: uuid.New(), ResourceID: uuid.New(), Used: placement.Vector{CPUMillis: 9000}}}
}

// An expired challenge cannot publish, even from the session that received it.
func TestCapacityReportRejectsAnExpiredChallenge(t *testing.T) {
	s, installation, node, session := capacityFixture(t)
	policy := testPolicy(installation, node)
	if _, err := s.PutNodePlacementPolicy(t.Context(), policy, 0); err != nil {
		t.Fatal(err)
	}
	probe, err := s.BeginCapacityProbe(t.Context(), installation, node, session)
	if err != nil {
		t.Fatal(err)
	}
	aged := probe
	aged.IssuedAt = probe.IssuedAt.Add(-2 * placement.ProbeWindow)
	aged.ExpiresAt = aged.IssuedAt.Add(placement.ProbeWindow)
	aged.ValidUntil = aged.IssuedAt.Add(placement.CapacityAge)
	if err := s.storeProbe(t, node, aged); err != nil {
		t.Fatal(err)
	}
	if err := s.RecordNodeCapacity(t.Context(), installation, node, testReport(aged, policy)); !errors.Is(err, ErrProbeExpired) {
		t.Fatalf("an expired challenge published capacity: %v", err)
	}
	if elapsed := time.Since(aged.ExpiresAt); elapsed <= 0 {
		t.Fatalf("the fixture challenge has not expired: %v", elapsed)
	}
}

// storeProbe rewrites the stored challenge so a test can age it.
func (s *Store) storeProbe(t *testing.T, nodeID uuid.UUID, probe placement.CapacityProbe) error {
	t.Helper()
	encoded, err := json.Marshal(probe)
	if err != nil {
		return err
	}
	_, err = s.pool.Exec(t.Context(), `UPDATE node_capacity SET probe_bytes=$1 WHERE node_id=$2`, encoded, nodeID)
	return err
}

func TestGenericOnlyNodePolicyNeedsNoDatabaseEngine(t *testing.T) {
	s, installation, node, _ := capacityFixture(t)
	policy := testPolicy(installation, node)
	policy.Engines = nil
	if _, err := s.PutNodePlacementPolicy(t.Context(), policy, 0); err != nil {
		t.Fatalf("generic-only policy: %v", err)
	}
}

func TestCapacityFactsStayBoundToTheCurrentSessionAndApproval(t *testing.T) {
	s, installation, node, session := capacityFixture(t)
	policy := testPolicy(installation, node)
	probe, err := s.BeginCapacityProbe(t.Context(), installation, node, session)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.RecordNodeCapacity(t.Context(), installation, node, testReport(probe, policy)); err != nil {
		t.Fatal(err)
	}
	facts, err := s.ReadNodeCapacityReport(t.Context(), installation, node)
	if err != nil || facts == nil {
		t.Fatalf("initial facts: %+v %v", facts, err)
	}
	aged := probe
	aged.IssuedAt = aged.IssuedAt.Add(-placement.CapacityAge)
	aged.ExpiresAt = aged.ExpiresAt.Add(-placement.CapacityAge)
	aged.ValidUntil = aged.ValidUntil.Add(-placement.CapacityAge)
	if err := s.storeProbe(t, node, aged); err != nil {
		t.Fatal(err)
	}
	facts, err = s.ReadNodeCapacityReport(t.Context(), installation, node)
	if err != nil || facts != nil {
		t.Fatalf("expired facts survived: %+v %v", facts, err)
	}
	if err := s.storeProbe(t, node, probe); err != nil {
		t.Fatal(err)
	}
	snapshot, err := s.ReadPlacementSnapshot(t.Context(), installation)
	if err != nil {
		t.Fatal(err)
	}
	request := placementRequest(installation, uuid.New(), uuid.New(), policy, nil)
	if _, err := placement.Plan(request, snapshot); !errors.Is(err, placement.ErrUnsatisfied) {
		t.Fatalf("unapproved placement: %v", err)
	}
	if _, err := s.PutNodePlacementPolicy(t.Context(), policy, 0); err != nil {
		t.Fatal(err)
	}
	snapshot, err = s.ReadPlacementSnapshot(t.Context(), installation)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := placement.Plan(request, snapshot); !errors.Is(err, placement.ErrUnsatisfied) {
		t.Fatalf("old policy evidence placed work: %v", err)
	}
	probe, err = s.BeginCapacityProbe(t.Context(), installation, node, session)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.RecordNodeCapacity(t.Context(), installation, node, testReport(probe, policy)); err != nil {
		t.Fatal(err)
	}
	snapshot, err = s.ReadPlacementSnapshot(t.Context(), installation)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := placement.Plan(request, snapshot); err != nil {
		t.Fatalf("approved fresh placement: %v", err)
	}
	presence, err := s.GetAgentPresence(t.Context(), installation, node)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.RecordAgentSession(t.Context(), installation, node, uuid.New(), presence.Hello); err != nil {
		t.Fatal(err)
	}
	facts, err = s.ReadNodeCapacityReport(t.Context(), installation, node)
	if err != nil || facts != nil {
		t.Fatalf("old session facts survived: %+v %v", facts, err)
	}
}

func TestCapacityProbeBindsAppPathsToItsReport(t *testing.T) {
	s, installation, node, session := capacityFixture(t)
	policy := testPolicy(installation, node)
	if _, err := s.PutNodePlacementPolicy(t.Context(), policy, 0); err != nil {
		t.Fatal(err)
	}
	id := uuid.New()
	if _, err := s.pool.Exec(t.Context(), `INSERT INTO apps(id,installation_id,name,tier,spec_version,spec,desired_revision,desired_lifecycle,observed_revision,observed_lifecycle,created_at,updated_at) VALUES($1,$2,'paths',2,1,'{"name":"paths","tier":2,"schema_version":1,"service":{"mounts":[{"type":"bind","source":"/media"},{"type":"replicated","source":"config"}]}}',1,'running',0,'provisioning',clock_timestamp(),clock_timestamp())`, id, installation); err != nil {
		t.Fatal(err)
	}
	probe, err := s.BeginCapacityProbe(t.Context(), installation, node, session)
	if err != nil || len(probe.Paths) != 1 || probe.Paths[0] != "/media" {
		t.Fatalf("probe: %+v %v", probe, err)
	}
	report := testReport(probe, policy)
	report.ExistingPaths = []string{"/not-requested"}
	if err := s.RecordNodeCapacity(t.Context(), installation, node, report); err == nil {
		t.Fatal("unsolicited path was accepted")
	}
	report.ExistingPaths = []string{"/media"}
	if err := s.RecordNodeCapacity(t.Context(), installation, node, report); err != nil {
		t.Fatal(err)
	}
	snapshot, err := s.ReadPlacementSnapshot(t.Context(), installation)
	if err != nil || len(snapshot.Nodes[0].Report.ExistingPaths) != 1 {
		t.Fatalf("path evidence lost: %+v %v", snapshot, err)
	}
}
