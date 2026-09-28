package capacity

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sirrobot01/synod/internal/controller/agentstream"
	"github.com/sirrobot01/synod/internal/controlstream"
	"github.com/sirrobot01/synod/internal/placement"
	"github.com/sirrobot01/synod/internal/store/postgres"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
)

// realStore creates one isolated database so the driver runs against the real
// evidence boundary rather than a stand-in.
func realStore(t *testing.T) (*postgres.Store, *pgxpool.Pool) {
	t.Helper()
	dsn := os.Getenv("SYNOD_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("run scripts/test-postgres.sh")
	}
	admin, err := pgxpool.New(t.Context(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	name := "probe_" + strings.ReplaceAll(uuid.NewString(), "-", "")
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
		// The bound guards against a hang, and is not an assertion about speed:
		// on a loaded host many suites drop their databases at once, and a teardown
		// that is merely slow must not fail a test that passed.
		ctx, cancel := context.WithTimeout(context.Background(), dropTimeout)
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
	// The pool comes back too: enrolment needs a CA and a CSR, which this test is
	// not about, so the node row is inserted directly.
	return store, pool
}

// installed enrols one node with an approved policy and a live session.
func installed(t *testing.T, store *postgres.Store, pool *pgxpool.Pool) (uuid.UUID, controlstream.Peer) {
	t.Helper()
	installation, node, sessionID := uuid.New(), uuid.New(), uuid.New()
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
		Engines:  []placement.EngineVersion{{Engine: "postgres", Version: "17.11"}},
		Labels:   map[string]string{"zone": "a"},
		Headroom: placement.Vector{CPUMillis: 500, MemoryBytes: 1 << 28, DiskBytes: 1 << 30},
	}
	if _, err := store.PutNodePlacementPolicy(t.Context(), policy, 0); err != nil {
		t.Fatal(err)
	}
	return installation, controlstream.Peer{
		InstallationID: installation, NodeID: node, SessionID: sessionID.String(),
		BootID: boot, JournalGeneration: generation,
	}
}

// answer measures nothing; it commits a report for one issued challenge the way
// the receiver would, so the driver sees evidence arrive exactly as in production.
func answer(t *testing.T, store *postgres.Store, peer controlstream.Peer, probe placement.CapacityProbe) {
	t.Helper()
	report := &agentv1.CapacityReport{
		ProbeId: probe.ID.String(), ProbeRevision: probe.Revision,
		SessionId: peer.SessionID, BootId: peer.BootID, JournalGeneration: peer.JournalGeneration,
		Architecture: "amd64", RuntimeVersion: "26.1", StorageId: "pool-a",
		Capacity: &agentv1.ResourceVector{CpuMillicores: 16000, MemoryBytes: 64 << 30, DiskBytes: 900 << 30},
		Used:     &agentv1.ResourceVector{CpuMillicores: 2000, MemoryBytes: 8 << 30, DiskBytes: 100 << 30},
	}
	message := &agentv1.AgentMessage{Message: &agentv1.AgentMessage_CapacityReport{CapacityReport: report}}
	if err := (agentstream.Receiver{Store: store}).Handle(t.Context(), peer, message); err != nil {
		t.Fatal(err)
	}
}

// The driver must issue a real challenge and then read the answer back as evidence.
func TestRefreshIssuesARealProbeAndReadsTheAnswer(t *testing.T) {
	store, pool := realStore(t)
	installation, peer := installed(t, store, pool)
	transport := &fakeTransport{sessions: map[uuid.UUID]controlstream.Peer{peer.NodeID: peer}}
	d, err := New(Config{Store: store, Transport: transport, Wait: 2 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := store.ReadPlacementSnapshot(t.Context(), installation)
	if err != nil {
		t.Fatal(err)
	}
	challenged, err := d.Refresh(t.Context(), installation, snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if len(challenged) != 1 || challenged[0] != peer.NodeID {
		t.Fatalf("challenged=%v, want the enrolled node", challenged)
	}
	// The challenge that went out must be the one the store issued.
	request := transport.sent[0]
	probeID, err := uuid.Parse(request.ProbeId)
	if err != nil {
		t.Fatal(err)
	}
	answer(t, store, peer, placement.CapacityProbe{ID: probeID, Revision: request.ProbeRevision})

	after, err := store.ReadPlacementSnapshot(t.Context(), installation)
	if err != nil {
		t.Fatal(err)
	}
	if len(after.Nodes) != 1 || after.Nodes[0].Report == nil {
		t.Fatal("the answered challenge produced no durable evidence")
	}
	if !usable(&after.Nodes[0], time.Now()) {
		t.Fatal("the committed report is not evidence the planner would accept")
	}
	// A second refresh leaves the fresh answer alone.
	again, err := d.Refresh(t.Context(), installation, after)
	if err != nil || len(again) != 0 {
		t.Fatalf("challenged=%v err=%v, want the fresh evidence left alone", again, err)
	}
}

// A node that never answers leaves the decision without evidence.
func TestDecideRefusesWhenTheRealNodeStaysSilent(t *testing.T) {
	store, pool := realStore(t)
	installation, peer := installed(t, store, pool)
	transport := &fakeTransport{sessions: map[uuid.UUID]controlstream.Peer{peer.NodeID: peer}}
	d, err := New(Config{Store: store, Transport: transport, Wait: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	request := placement.Request{Version: 1, InstallationID: installation, AppID: uuid.New(), OperationID: uuid.New(), DesiredRevision: 1}
	if _, _, err := d.Decide(t.Context(), request); !errors.Is(err, ErrNoEvidence) {
		t.Fatalf("err=%v, want ErrNoEvidence rather than a guess", err)
	}
	if len(transport.sent) == 0 {
		t.Fatal("the decision never challenged the node")
	}
}

// dropTimeout bounds one test database teardown. It exists to stop a hang, not to
// assert how fast PostgreSQL drops a database under load.
const dropTimeout = 60 * time.Second
