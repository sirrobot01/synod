package agentstream

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sirrobot01/synod/internal/controlstream"
	"github.com/sirrobot01/synod/internal/operation"
	"github.com/sirrobot01/synod/internal/placement"
	"github.com/sirrobot01/synod/internal/store/postgres"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
)

// bound returns a peer and a report that agree, so a test can break one of them.
func bound(t *testing.T) (controlstream.Peer, uuid.UUID, *agentv1.CapacityReport) {
	t.Helper()
	sessionID := uuid.New()
	peer := controlstream.Peer{
		InstallationID: uuid.New(), NodeID: uuid.New(), SessionID: sessionID.String(),
		BootID: uuid.NewString(), JournalGeneration: uuid.NewString(),
	}
	report := &agentv1.CapacityReport{
		ProbeId: uuid.NewString(), ProbeRevision: 2,
		SessionId: peer.SessionID, BootId: peer.BootID, JournalGeneration: peer.JournalGeneration,
		Architecture: "amd64", RuntimeVersion: "26.1", StorageId: "pool-a",
		Capacity: &agentv1.ResourceVector{CpuMillicores: 16000, MemoryBytes: 64 << 30, DiskBytes: 900 << 30},
		Used:     &agentv1.ResourceVector{CpuMillicores: 2000, MemoryBytes: 8 << 30, DiskBytes: 100 << 30},
		Reservations: []*agentv1.ReservationUsage{{
			ReservationId: uuid.NewString(), ResourceId: uuid.NewString(),
			Used: &agentv1.ResourceVector{CpuMillicores: 500, MemoryBytes: 2 << 30, DiskBytes: 40 << 30},
		}},
	}
	return peer, sessionID, report
}

// The authenticated peer supplies the identity. The frame never does.
func TestCapacityReportTakesItsIdentityFromThePeer(t *testing.T) {
	peer, sessionID, report := bound(t)
	found, err := capacityReport(peer, sessionID, report)
	if err != nil {
		t.Fatal(err)
	}
	if found.InstallationID != peer.InstallationID || found.NodeID != peer.NodeID || found.SessionID != sessionID {
		t.Fatal("the report did not take its identity from the certificate that proved one")
	}
	if found.ProbeRevision != 2 || found.StorageID != "pool-a" || found.Architecture != "amd64" {
		t.Fatalf("report=%+v", found)
	}
	if len(found.Reservations) != 1 || found.Reservations[0].Used.DiskBytes != 40<<30 {
		t.Fatalf("reservations=%+v", found.Reservations)
	}
	// The Store applies its own rules, and this report must survive them.
	if err := found.Validate(); err != nil {
		t.Fatalf("a report the receiver accepted fails the Store: %v", err)
	}
}

// A report measured against other state is a replay, not evidence.
func TestCapacityReportRefusesStateThePeerDoesNotProve(t *testing.T) {
	for name, change := range map[string]func(*agentv1.CapacityReport){
		"another session": func(r *agentv1.CapacityReport) { r.SessionId = uuid.NewString() },
		// A changed session invalidates capacity even when the boot matches, and
		// a changed boot means the host restarted since the challenge was issued.
		"another boot":    func(r *agentv1.CapacityReport) { r.BootId = uuid.NewString() },
		"another journal": func(r *agentv1.CapacityReport) { r.JournalGeneration = uuid.NewString() },
	} {
		t.Run(name, func(t *testing.T) {
			peer, sessionID, report := bound(t)
			change(report)
			if _, err := capacityReport(peer, sessionID, report); !errors.Is(err, operation.ErrStaleOwner) {
				t.Fatalf("err=%v, want ErrStaleOwner", err)
			}
		})
	}
}

// A malformed frame is refused before any transaction sees it.
func TestCapacityReportRefusesAMalformedFrame(t *testing.T) {
	peer, sessionID, report := bound(t)
	report.Architecture = "riscv64"
	if _, err := capacityReport(peer, sessionID, report); !errors.Is(err, controlstream.ErrMessage) {
		t.Fatalf("err=%v, want ErrMessage", err)
	}
}

// A receiver with no store cannot commit, and must say so rather than drop it.
func TestReceiverRefusesCapacityWithoutAStore(t *testing.T) {
	peer, _, report := bound(t)
	message := &agentv1.AgentMessage{Message: &agentv1.AgentMessage_CapacityReport{CapacityReport: report}}
	if err := (Receiver{}).Handle(t.Context(), peer, message); !errors.Is(err, ErrUnsupportedMessage) {
		t.Fatalf("err=%v, want ErrUnsupportedMessage", err)
	}
}

// testStore creates one isolated database for a real end-to-end commit.
// The pool comes back with it: the Store exposes no read for stored capacity, so
// this asserts against the row the way the Store's own tests do.
func testStore(t *testing.T) (*postgres.Store, *pgxpool.Pool) {
	t.Helper()
	dsn := os.Getenv("SYNOD_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("run scripts/test-postgres.sh")
	}
	admin, err := pgxpool.New(t.Context(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	name := "capacity_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if _, err := admin.Exec(t.Context(), "CREATE DATABASE "+pgx.Identifier{name}.Sanitize()); err != nil {
		admin.Close()
		t.Fatal(err)
	}
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	config.ConnConfig.Database = name
	pool, err := pgxpool.NewWithConfig(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		pool.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if _, err := admin.Exec(ctx, "DROP DATABASE "+pgx.Identifier{name}.Sanitize()+" WITH (FORCE)"); err != nil {
			t.Error(err)
		}
		admin.Close()
	})
	store := postgres.New(pool)
	if err := store.Migrate(t.Context()); err != nil {
		t.Fatal(err)
	}
	return store, pool
}

// challenged enrols one node, approves a policy, and issues a real challenge.
func challenged(t *testing.T) (*postgres.Store, *pgxpool.Pool, controlstream.Peer, uuid.UUID, placement.CapacityProbe) {
	t.Helper()
	store, pool := testStore(t)
	installation, node, sessionID := uuid.New(), uuid.New(), uuid.New()
	// Enrolment needs a CA and a CSR, which this test is not about. The node row
	// is inserted directly, as the Store's own capacity fixture does.
	if _, err := pool.Exec(t.Context(), `INSERT INTO nodes (id,installation_id,public_key_hash,enrolled_at) VALUES ($1,$2,$3,clock_timestamp())`, node, installation, make([]byte, 32)); err != nil {
		t.Fatal(err)
	}
	boot, generation := uuid.NewString(), uuid.NewString()
	hello := &agentv1.Hello{
		InstallationId: installation.String(), NodeId: node.String(), BootId: boot, JournalGeneration: generation,
		Protocol: &agentv1.VersionRange{Minimum: 1, Maximum: 1}, ActivePayloadVersions: []uint32{1},
	}
	if err := store.RecordAgentSession(t.Context(), installation, node, sessionID, hello); err != nil {
		t.Fatal(err)
	}
	policy := placement.NodePolicy{
		InstallationID: installation, NodeID: node, Architecture: "amd64", RuntimeVersion: "26.1", StorageID: "pool-a",
		Engines: []placement.EngineVersion{{Engine: "postgres", Version: "17.11"}},
		Labels:  map[string]string{"zone": "a"},
		// Operator-declared headroom is reserved before any new work, so a policy
		// without it is refused.
		Headroom: placement.Vector{CPUMillis: 500, MemoryBytes: 1 << 28, DiskBytes: 1 << 30},
	}
	if _, err := store.PutNodePlacementPolicy(t.Context(), policy, 0); err != nil {
		t.Fatal(err)
	}
	probe, err := store.BeginCapacityProbe(t.Context(), installation, node, sessionID)
	if err != nil {
		t.Fatal(err)
	}
	peer := controlstream.Peer{InstallationID: installation, NodeID: node, SessionID: sessionID.String(), BootID: boot, JournalGeneration: generation}
	return store, pool, peer, sessionID, probe
}

// wireReport answers one real challenge with a measured-shaped frame.
func wireReport(peer controlstream.Peer, probe placement.CapacityProbe) *agentv1.CapacityReport {
	return &agentv1.CapacityReport{
		ProbeId: probe.ID.String(), ProbeRevision: probe.Revision,
		SessionId: peer.SessionID, BootId: peer.BootID, JournalGeneration: peer.JournalGeneration,
		Architecture: "amd64", RuntimeVersion: "26.1", StorageId: "pool-a",
		Capacity: &agentv1.ResourceVector{CpuMillicores: 16000, MemoryBytes: 64 << 30, DiskBytes: 900 << 30},
		Used:     &agentv1.ResourceVector{CpuMillicores: 2000, MemoryBytes: 8 << 30, DiskBytes: 100 << 30},
	}
}

// A measured report must cross the receiver into durable placement evidence.
func TestCapacityReportReachesTheStore(t *testing.T) {
	store, pool, peer, _, probe := challenged(t)
	message := &agentv1.AgentMessage{Message: &agentv1.AgentMessage_CapacityReport{CapacityReport: wireReport(peer, probe)}}
	if err := (Receiver{Store: store}).Handle(t.Context(), peer, message); err != nil {
		t.Fatal(err)
	}
	var stored []byte
	if err := pool.QueryRow(t.Context(), `SELECT report_bytes FROM node_capacity WHERE node_id=$1`, peer.NodeID).Scan(&stored); err != nil {
		t.Fatal(err)
	}
	var found placement.CapacityReport
	if err := json.Unmarshal(stored, &found); err != nil {
		t.Fatal(err)
	}
	if found.ProbeID != probe.ID || found.NodeID != peer.NodeID {
		t.Fatalf("report=%+v, want the answered challenge", found)
	}
	// Placement now holds measured evidence rather than synthetic test input.
	if found.Capacity.DiskBytes != 900<<30 || found.Architecture != "amd64" {
		t.Fatalf("stored report=%+v", found)
	}
}

// A lost reply leaves the challenge unanswered. A later answer cannot publish.
func TestCapacityReportRefusedAfterALostReply(t *testing.T) {
	store, _, peer, sessionID, probe := challenged(t)
	// The answer never arrived, so the controller issued another challenge.
	next, err := store.BeginCapacityProbe(t.Context(), peer.InstallationID, peer.NodeID, sessionID)
	if err != nil || next.ID == probe.ID {
		t.Fatalf("second probe %+v: %v", next, err)
	}
	message := &agentv1.AgentMessage{Message: &agentv1.AgentMessage_CapacityReport{CapacityReport: wireReport(peer, probe)}}
	if err := (Receiver{Store: store}).Handle(t.Context(), peer, message); err == nil {
		t.Fatal("a replaced challenge published capacity")
	}
}
