package provision

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sirrobot01/synod/internal/agent/replication"
	"github.com/sirrobot01/synod/internal/controlstream"
	"github.com/sirrobot01/synod/internal/operation"
	"github.com/sirrobot01/synod/internal/placement"
	"github.com/sirrobot01/synod/internal/secret"
	"github.com/sirrobot01/synod/internal/store/postgres"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
)

// The node shape this fixture declares.
const (
	nodeArchitecture = "amd64"
	nodeRuntime      = "27.1.1"
	nodeStorage      = "pool-a"
)

// readyNodeWith approves one node after customize changes its policy, then answers its capacity probe.
func readyNodeWith(t *testing.T, store *postgres.Store, installation, node uuid.UUID, customize func(*placement.NodePolicy)) {
	t.Helper()
	session := uuid.New()
	hello := &agentv1.Hello{
		InstallationId: installation.String(), NodeId: node.String(),
		BootId: uuid.NewString(), JournalGeneration: uuid.NewString(),
		Capabilities: []string{"runtime.docker", controlstream.RouterCapability, replication.Capability},
		Protocol:     &agentv1.VersionRange{Minimum: 1, Maximum: 1}, ActivePayloadVersions: []uint32{1},
	}
	if err := store.RecordAgentSession(t.Context(), installation, node, session, hello); err != nil {
		t.Fatal(err)
	}
	policy := placement.NodePolicy{
		InstallationID: installation, NodeID: node, Architecture: nodeArchitecture,
		RuntimeVersion: nodeRuntime, StorageID: nodeStorage,
		// An approved node records the address clients and peers reach it on. It must
		// be reachable from another host, so the policy refuses a loopback address.
		ClientAddress: "192.0.2.10",
		Engines:       []placement.EngineVersion{{Engine: "postgres", Version: "17.11"}, {Engine: "redis", Version: "8.2.7"}},
		Headroom:      placement.Vector{CPUMillis: 500, MemoryBytes: 1 << 28, DiskBytes: 1 << 30},
	}
	if customize != nil {
		customize(&policy)
	}
	revision, err := store.PutNodePlacementPolicy(t.Context(), policy, 0)
	if err != nil {
		t.Fatal(err)
	}
	policy.Revision = revision
	probe, err := store.BeginCapacityProbe(t.Context(), installation, node, session)
	if err != nil {
		t.Fatal(err)
	}
	report := placement.CapacityReport{
		ProbeID: probe.ID, ProbeRevision: probe.Revision, InstallationID: installation, NodeID: node,
		SessionID: probe.SessionID, BootID: probe.BootID, JournalGeneration: probe.JournalGeneration,
		// The agent reports the architecture it runs on, which is the one its policy declares.
		Architecture: policy.Architecture, RuntimeVersion: nodeRuntime, StorageID: nodeStorage,
		Capacity: placement.Vector{CPUMillis: 8000, MemoryBytes: 1 << 34, DiskBytes: 1 << 40},
		Used:     placement.Vector{CPUMillis: 500, MemoryBytes: 1 << 30, DiskBytes: 1 << 35},
	}
	if err := store.RecordNodeCapacity(t.Context(), installation, node, report); err != nil {
		t.Fatal(err)
	}
}

// realStore creates one isolated database, so the planner runs against the real
// admission and placement boundary rather than a stand-in.
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
	name := "provision_" + strings.ReplaceAll(uuid.NewString(), "-", "")
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
	return store, pool
}

// localDecider plans on the real snapshot without refreshing capacity first. It skips
// exactly one thing the real driver does — the probe challenge, which needs a live
// agent — so the decision it returns is a real one that the store will accept.
type localDecider struct {
	store         *postgres.Store
	request       placement.Request
	decision      placement.Decision
	afterDecision func()
	err           error
}

func (d *localDecider) Decide(ctx context.Context, request placement.Request) (placement.Decision, placement.Snapshot, error) {
	d.request = request
	if d.err != nil {
		return placement.Decision{}, placement.Snapshot{}, d.err
	}
	snapshot, err := d.store.ReadPlacementSnapshot(ctx, request.InstallationID)
	if err != nil {
		return placement.Decision{}, snapshot, err
	}
	decision, err := placement.Plan(request, snapshot)
	d.decision = decision
	if err == nil && d.afterDecision != nil {
		d.afterDecision()
	}
	return decision, snapshot, err
}

// fixture enrols one node and returns a planner bound to its installation.
type fixture struct {
	store   *postgres.Store
	pool    *pgxpool.Pool
	ring    *secret.Keyring
	planner *Planner
	decider *localDecider
	claim   operation.Claim
	node    uuid.UUID
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	store, pool := realStore(t)
	installation, node := uuid.New(), uuid.New()
	if _, err := pool.Exec(t.Context(), `INSERT INTO nodes (id,installation_id,public_key_hash,enrolled_at)
 VALUES ($1,$2,$3,clock_timestamp())`, node, installation, make([]byte, 32)); err != nil {
		t.Fatal(err)
	}
	readyNodeWith(t, store, installation, node, nil)
	generation := uuid.New()
	if err := store.InitializeGeneration(t.Context(), installation, generation); err != nil {
		t.Fatal(err)
	}
	active := uuid.New()
	ring, err := secret.NewKeyring(active, map[uuid.UUID][]byte{active: make([]byte, 32)})
	if err != nil {
		t.Fatal(err)
	}
	decider := &localDecider{store: store}
	planner, err := New(Config{
		Store: store, Decider: decider, Keyring: ring,
		Budget:        placement.Vector{CPUMillis: 1000, MemoryBytes: 1 << 30, DiskBytes: 1 << 32},
		EndpointPorts: placement.PortRange{First: endpointPort, Last: endpointPort + 11},
	})
	if err != nil {
		t.Fatal(err)
	}
	claim := operation.Claim{InstallationID: installation, RecoveryGeneration: generation}
	return &fixture{store: store, pool: pool, ring: ring, planner: planner, decider: decider, claim: claim, node: node}
}

// dropTimeout bounds one test database teardown. It exists to stop a hang, not to
// assert how fast PostgreSQL drops a database under load.
const dropTimeout = 60 * time.Second

// endpointPort is the first host port the fixture planner places copies on.
const endpointPort = 5433
