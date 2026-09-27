package provision

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
	"github.com/sirrobot01/synod/internal/cluster"
	"github.com/sirrobot01/synod/internal/controlstream"
	"github.com/sirrobot01/synod/internal/engine"
	postgresengine "github.com/sirrobot01/synod/internal/engine/postgres"
	redisengine "github.com/sirrobot01/synod/internal/engine/redis"
	"github.com/sirrobot01/synod/internal/operation"
	"github.com/sirrobot01/synod/internal/placement"
	"github.com/sirrobot01/synod/internal/secret"
	"github.com/sirrobot01/synod/internal/store/postgres"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/proto"
)

const testDigest = "postgres@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

// The node shape this fixture declares. Placement matches a node's operator-declared
// policy exactly, so the planner's profile has to name the same values.
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
		Capabilities: []string{"runtime.docker", controlstream.RouterCapability},
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
		Engines:       []placement.EngineVersion{{Engine: "postgres", Version: "17.11"}, {Engine: "postgres", Version: "18.6"}, {Engine: "redis", Version: "8.2.7"}},
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
	store    *postgres.Store
	request  placement.Request
	decision placement.Decision
	err      error
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
	return decision, snapshot, err
}

// fixture admits one cluster, enrols one node, and returns a planner bound to both.
type fixture struct {
	store   *postgres.Store
	pool    *pgxpool.Pool
	ring    *secret.Keyring
	planner *Planner
	decider *localDecider
	claim   operation.Claim
	node    uuid.UUID
}

func newFixture(t *testing.T, spec cluster.CreateSpec) *fixture {
	t.Helper()
	return newFixtureWith(t, spec, nil)
}

// newFixtureWith lets customize change the first node's policy before approval.
func newFixtureWith(t *testing.T, spec cluster.CreateSpec, customize func(*placement.NodePolicy)) *fixture {
	t.Helper()
	store, pool := realStore(t)
	installation, node := uuid.New(), uuid.New()
	if _, err := pool.Exec(t.Context(), `INSERT INTO nodes (id,installation_id,public_key_hash,enrolled_at)
 VALUES ($1,$2,$3,clock_timestamp())`, node, installation, make([]byte, 32)); err != nil {
		t.Fatal(err)
	}
	readyNodeWith(t, store, installation, node, customize)
	admission, err := store.CreateCluster(t.Context(), postgres.CreateClusterRequest{
		Identity: postgres.RequestIdentity{
			InstallationID: installation, ActorID: "actor-1",
			APIOperation: postgres.CreateClusterAPIOperation, IdempotencyKey: uuid.NewString(),
		},
		RequestID: "request-1", Spec: spec,
	})
	if err != nil {
		t.Fatal(err)
	}
	generation := uuid.New()
	if err := store.InitializeGeneration(t.Context(), installation, generation); err != nil {
		t.Fatal(err)
	}
	claim, err := store.AcquireClaim(t.Context(), postgres.ClaimRequest{
		ClaimID: uuid.New(), InstallationID: installation, OperationID: admission.OperationID,
		RecoveryGeneration: generation, OwnerID: "owner-1", TTL: 2 * time.Minute,
		LeaseID: 1, LeaseRevision: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	active := uuid.New()
	ring, err := secret.NewKeyring(active, map[uuid.UUID][]byte{active: make([]byte, 32)})
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := engine.NewCatalog(postgresengine.New(), redisengine.New())
	if err != nil {
		t.Fatal(err)
	}
	decider := &localDecider{store: store}
	planner, err := New(Config{
		Store: store, Decider: decider, Catalog: catalog, Keyring: ring,
		Profile:       Profile{Architectures: []string{nodeArchitecture}, RuntimeVersion: nodeRuntime, StorageID: nodeStorage},
		Images:        []Image{{Engine: cluster.PostgreSQL, Version: "17.11", Architecture: "amd64", Digest: testDigest}, {Engine: cluster.Redis, Version: "8.2.7", Architecture: "amd64", Digest: "redis@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"}},
		Budget:        placement.Vector{CPUMillis: 1000, MemoryBytes: 1 << 30, DiskBytes: 1 << 32},
		EndpointPorts: placement.PortRange{First: endpointPort, Last: endpointPort + 11},
	})
	if err != nil {
		t.Fatal(err)
	}
	return &fixture{store: store, pool: pool, ring: ring, planner: planner, decider: decider, claim: claim, node: node}
}

func postgresSpec() cluster.CreateSpec {
	return cluster.CreateSpec{SchemaVersion: 1, Name: "db", Engine: cluster.PostgreSQL,
		PostgreSQL: &cluster.PostgreSQLSpec{Version: "17.11", Topology: "standalone"}}
}

// The plan and its reservations are installed together, and the plan is the one a
// worker will accept.
func TestProvisionInstallsAPlanAndItsReservations(t *testing.T) {
	f := newFixture(t, postgresSpec())
	record, err := f.planner.provision(t.Context(), f.claim)
	if err != nil {
		t.Fatal(err)
	}
	if record.CommittedAt.IsZero() || len(record.Allocations) != 1 {
		t.Fatalf("record=%+v, want one committed reservation", record)
	}
	// The reservation exists and covers both the instance and its storage.
	var owned int
	if err := f.pool.QueryRow(t.Context(), `SELECT count(*) FROM resource_allocations
 WHERE installation_id=$1 AND released_at IS NULL`, f.claim.InstallationID).Scan(&owned); err != nil {
		t.Fatal(err)
	}
	if owned != 1 {
		t.Fatalf("allocations=%d, want one reservation for the instance", owned)
	}
	// The request asked for what the profile declares, because placement matches a
	// node's operator-declared policy and a cluster spec says none of it.
	resource := f.decider.request.Resources[0]
	if resource.Architecture != nodeArchitecture || resource.RuntimeVersion != nodeRuntime || resource.StorageID != nodeStorage {
		t.Fatalf("resource=%+v, want the configured profile", resource)
	}
	if resource.Engine != "postgres" || resource.Version != "17.11" || resource.Kind != "instance" {
		t.Fatalf("resource=%+v, want the cluster's own engine", resource)
	}
	if len(resource.OwnedResourceIDs) != 2 {
		t.Fatalf("owned=%v, want the instance and its volume", resource.OwnedResourceIDs)
	}
}

// The steps are ordered by declared dependency, not by position, and every identity
// is generated once and carried through.
func TestProvisionOrdersItsStepsByDependency(t *testing.T) {
	f := newFixture(t, postgresSpec())
	if err := f.planner.Plan(t.Context(), f.claim); err != nil {
		t.Fatal(err)
	}
	plan := installedPlan(t, f)
	if len(plan.Steps) != 4 {
		t.Fatalf("steps=%d, want volume, container, start, readiness", len(plan.Steps))
	}
	volume, container, start, ready := plan.Steps[0], plan.Steps[1], plan.Steps[2], plan.Steps[3]
	if ready.Postcondition != agentv1.Postcondition_POSTCONDITION_ENGINE_READY {
		t.Fatal("the plan does not end by checking the engine answers")
	}
	if len(ready.DependsOn) != 1 || ready.DependsOn[0] != start.Command.StepId {
		t.Fatalf("readiness depends on %v, want the start step", ready.DependsOn)
	}
	// It connects to the endpoint the cluster publishes, not to the container.
	check := ready.Command.Input.GetCheckReadiness()
	if check.Port != endpointPort || check.Database != "app" || check.Engine != "postgres" {
		t.Fatalf("readiness=%+v, want the published endpoint", check)
	}
	if volume.Postcondition != agentv1.Postcondition_POSTCONDITION_VOLUME_EXISTS ||
		container.Postcondition != agentv1.Postcondition_POSTCONDITION_CONTAINER_EXISTS ||
		start.Postcondition != agentv1.Postcondition_POSTCONDITION_CONTAINER_RUNNING {
		t.Fatal("the plan does not create, then start")
	}
	if len(container.DependsOn) != 1 || container.DependsOn[0] != volume.Command.StepId {
		t.Fatalf("container depends on %v, want the volume step", container.DependsOn)
	}
	if len(start.DependsOn) != 1 || start.DependsOn[0] != container.Command.StepId {
		t.Fatalf("start depends on %v, want the container step", start.DependsOn)
	}
	// The container mounts the volume the plan created, not some other one.
	ensure := container.Command.Input.GetEnsureContainer()
	if ensure.VolumeId != volume.Command.Input.ResourceId {
		t.Fatalf("container mounts %s, want the volume this plan created", ensure.VolumeId)
	}
	if ensure.ImageDigest != testDigest {
		t.Fatalf("digest=%s, want the pinned image", ensure.ImageDigest)
	}
}

// The plan names a credential and never carries one.
func TestProvisionPlanNamesACredentialAndCarriesNone(t *testing.T) {
	f := newFixture(t, postgresSpec())
	if err := f.planner.Plan(t.Context(), f.claim); err != nil {
		t.Fatal(err)
	}
	stored, err := f.store.ReadClusterCredential(t.Context(), f.claim.InstallationID, f.claim.ClusterID, postgres.SuperuserCredential)
	if err != nil {
		t.Fatal(err)
	}
	binding := secret.Binding{
		InstallationID: f.claim.InstallationID, OwnerType: "cluster", OwnerID: f.claim.ClusterID,
		CredentialID: stored.CredentialID, Version: stored.Version,
	}
	password, err := f.ring.Open(binding, stored.Envelope)
	if err != nil {
		t.Fatal(err)
	}
	plan := installedPlan(t, f)
	encoded, err := proto.Marshal(plan)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), string(password)) {
		t.Fatal("the installed plan carries the credential")
	}
	container := plan.Steps[1].Command
	if len(container.Input.Secrets) != 2 {
		t.Fatalf("secrets=%v, want two references", container.Input.Secrets)
	}
	reference := container.Input.Secrets[0]
	if reference.CredentialId != stored.CredentialID.String() || reference.Version != stored.Version {
		t.Fatalf("reference=%+v, want the recorded credential", reference)
	}
	// The variable name travels so the agent knows where to put the value.
	env := container.Input.GetEnsureContainer().SecretEnv
	if len(env) != 2 || env[0].Name != "POSTGRES_PASSWORD" || env[0].Purpose != postgres.SuperuserCredential || env[1].Name != "SYNOD_APP_VERIFIER" || env[1].Purpose != postgres.ApplicationCredential {
		t.Fatalf("env=%+v, want the engine's password variable", env)
	}
}

// The intent is recorded before anything can dispatch, and the databases it names are
// owned by roles it creates.
func TestProvisionRecordsItsIntentFirst(t *testing.T) {
	f := newFixture(t, postgresSpec())
	if err := f.planner.Plan(t.Context(), f.claim); err != nil {
		t.Fatal(err)
	}
	databases, err := f.store.ReadClusterDatabases(t.Context(), f.claim.InstallationID, f.claim.ClusterID)
	if err != nil {
		t.Fatal(err)
	}
	if len(databases) != 1 || databases[0].Name != "app" {
		t.Fatalf("databases=%+v, want the application database", databases)
	}
	application, err := f.store.ReadClusterCredential(t.Context(), f.claim.InstallationID, f.claim.ClusterID, postgres.ApplicationCredential)
	if err != nil {
		t.Fatal(err)
	}
	if databases[0].OwnerRole != application.RoleName || application.RoleName != "synod_app" {
		t.Fatalf("owner=%s, want the application role", databases[0].OwnerRole)
	}
	plan := installedPlan(t, f)
	ensure := plan.Steps[1].Command.Input.GetEnsureContainer()
	if len(ensure.PlainEnv) != 0 || ensure.Postgres.ApplicationCredentialId != application.CredentialID.String() || ensure.Postgres.ApplicationCredentialVersion != application.Version {
		t.Fatal("container configuration does not bind the recorded application intent")
	}
	ready := plan.Steps[3].Command.Input
	if ready.GetCheckReadiness().PostgresRole != "primary" || ready.GetCheckReadiness().Role != "synod_app" || ready.Secrets[0].CredentialId != application.CredentialID.String() {
		t.Fatal("readiness does not verify the application credential")
	}
}

// An engine version with no pinned image is refused. Specification 36 forbids a moving
// tag, so an unpinned combination has no digest to resolve at run time.
func TestProvisionRefusesAnUnpinnedImage(t *testing.T) {
	spec := postgresSpec()
	spec.PostgreSQL.Version = "18.6"
	f := newFixture(t, spec)
	if err := f.planner.Plan(t.Context(), f.claim); !errors.Is(err, ErrNoImage) {
		t.Fatalf("err=%v, want ErrNoImage", err)
	}
	// Nothing was recorded for a cluster that cannot be provisioned.
	var credentials int
	if err := f.pool.QueryRow(t.Context(), `SELECT count(*) FROM cluster_credentials
 WHERE installation_id=$1`, f.claim.InstallationID).Scan(&credentials); err != nil {
		t.Fatal(err)
	}
	if credentials != 0 {
		t.Fatalf("credentials=%d, want none for a refused provision", credentials)
	}
}

// Placement that cannot be satisfied leaves no plan and no reservation.
func TestProvisionLeavesNothingWhenPlacementFails(t *testing.T) {
	f := newFixture(t, postgresSpec())
	f.decider.err = placement.ErrUnsatisfied
	if err := f.planner.Plan(t.Context(), f.claim); !errors.Is(err, placement.ErrUnsatisfied) {
		t.Fatalf("err=%v, want ErrUnsatisfied", err)
	}
	var allocations int
	if err := f.pool.QueryRow(t.Context(), `SELECT count(*) FROM resource_allocations
 WHERE installation_id=$1`, f.claim.InstallationID).Scan(&allocations); err != nil {
		t.Fatal(err)
	}
	if allocations != 0 {
		t.Fatalf("allocations=%d, want none", allocations)
	}
}

func TestPostgresProvisionReusesAllCredentialsAfterPlacementFailure(t *testing.T) {
	f := newFixture(t, postgresSpec())
	f.decider.err = placement.ErrUnsatisfied
	if err := f.planner.Plan(t.Context(), f.claim); !errors.Is(err, placement.ErrUnsatisfied) {
		t.Fatal(err)
	}
	var credentialCount int
	if err := f.pool.QueryRow(t.Context(), `SELECT count(*) FROM cluster_credentials WHERE installation_id=$1 AND cluster_id=$2`, f.claim.InstallationID, f.claim.ClusterID).Scan(&credentialCount); err != nil {
		t.Fatal(err)
	}
	// Standalone PostgreSQL has no replicas, so it records no replication credential.
	if credentialCount != 2 {
		t.Fatalf("PostgreSQL stored credentials=%d, want exactly two", credentialCount)
	}
	var recorded []postgres.Credential
	var plaintext [][]byte
	for _, purpose := range []string{postgres.SuperuserCredential, postgres.ApplicationCredential} {
		credential, err := f.store.ReadClusterCredential(t.Context(), f.claim.InstallationID, f.claim.ClusterID, purpose)
		if err != nil {
			t.Fatal(err)
		}
		password, err := f.ring.Open(secret.Binding{
			InstallationID: f.claim.InstallationID, OwnerType: "cluster", OwnerID: f.claim.ClusterID,
			CredentialID: credential.CredentialID, Version: credential.Version,
		}, credential.Envelope)
		if err != nil {
			t.Fatal(err)
		}
		recorded, plaintext = append(recorded, credential), append(plaintext, password)
	}
	if recorded[0].CredentialID == recorded[1].CredentialID || string(plaintext[0]) == string(plaintext[1]) {
		t.Fatal("administrative and application credentials are not separate")
	}
	f.decider.err = nil
	if err := f.planner.Plan(t.Context(), f.claim); err != nil {
		t.Fatal(err)
	}
	plan := installedPlan(t, f)
	encoded, err := proto.Marshal(plan)
	if err != nil {
		t.Fatal(err)
	}
	for i, reference := range plan.Steps[1].Command.Input.Secrets {
		if reference.CredentialId != recorded[i].CredentialID.String() || reference.Version != recorded[i].Version {
			t.Fatal("retry replaced a persisted credential")
		}
		if strings.Contains(string(encoded), string(plaintext[i])) {
			t.Fatal("plan contains a plaintext credential")
		}
	}
}

// installedPlan reads back the plan the store holds for the claim.
func installedPlan(t *testing.T, f *fixture) *agentv1.ExecutionPlan {
	t.Helper()
	var encoded []byte
	if err := f.pool.QueryRow(t.Context(), `SELECT plan_bytes FROM operations
 WHERE installation_id=$1 AND id=$2`, f.claim.InstallationID, f.claim.OperationID).Scan(&encoded); err != nil {
		t.Fatal(err)
	}
	var plan agentv1.ExecutionPlan
	if err := proto.Unmarshal(encoded, &plan); err != nil {
		t.Fatal(err)
	}
	return &plan
}

// dropTimeout bounds one test database teardown. It exists to stop a hang, not to
// assert how fast PostgreSQL drops a database under load.
const dropTimeout = 60 * time.Second

// The plan reserves a host port because this profile uses host networking, so the
// engine must be told to bind that same port. A reservation the engine ignores names
// a port nothing listens on, and the engine collides with whatever already holds its
// own default.
const endpointPort = 5433

func TestProvisionBindsTheEngineToTheReservedPort(t *testing.T) {
	f := newFixture(t, postgresSpec())
	if _, err := f.planner.provision(t.Context(), f.claim); err != nil {
		t.Fatal(err)
	}
	plan := installedPlan(t, f)
	// The request names a range and leaves the number to placement, which knows what
	// every node already holds.
	asked := f.decider.request.Resources[0].Ports
	if len(asked) != 1 || asked[0].Number != 0 || f.decider.request.PortRange.First != endpointPort {
		t.Fatalf("request ports=%+v range=%+v, want one port chosen from the range", asked, f.decider.request.PortRange)
	}
	reserved := f.decider.decision.Assignments[0].Ports
	if len(reserved) != 1 || reserved[0].Number != endpointPort {
		t.Fatalf("ports=%+v, want the first free port of the range reserved", reserved)
	}
	configuration := plan.Steps[1].Command.Input.GetEnsureContainer().Postgres
	if configuration.Port != endpointPort {
		t.Fatalf("port=%d, want reserved port %d", configuration.Port, endpointPort)
	}
}
