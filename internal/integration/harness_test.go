// Package integration_test proves the paths that only exist when the controller and an
// agent are both real.
//
// Everything here runs against a real PostgreSQL, a real etcd, and a real Docker
// daemon. Nothing in it stands in for a component that ships.
package integration_test

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/client"
	"github.com/sirrobot01/synod/internal/agent/capacity"
	"github.com/sirrobot01/synod/internal/agent/executor"
	"github.com/sirrobot01/synod/internal/agent/journal"
	"github.com/sirrobot01/synod/internal/agent/transport"
	"github.com/sirrobot01/synod/internal/cluster"
	controllercapacity "github.com/sirrobot01/synod/internal/controller/capacity"
	"github.com/sirrobot01/synod/internal/controller/provision"
	"github.com/sirrobot01/synod/internal/controller/runner"
	"github.com/sirrobot01/synod/internal/controlstream"
	"github.com/sirrobot01/synod/internal/coordinator"
	"github.com/sirrobot01/synod/internal/engine"
	postgresengine "github.com/sirrobot01/synod/internal/engine/postgres"
	redisengine "github.com/sirrobot01/synod/internal/engine/redis"
	"github.com/sirrobot01/synod/internal/pki"
	"github.com/sirrobot01/synod/internal/placement"
	"github.com/sirrobot01/synod/internal/runtime/docker"
	"github.com/sirrobot01/synod/internal/secret"
	"github.com/sirrobot01/synod/internal/store/postgres"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

// The PostgreSQL 17.11 images this fixture pins, by architecture. Each digest was run
// and reported that version on that architecture; specification 36 forbids a moving
// tag, and an unverified digest is the same problem with more ceremony.
var pinnedPostgres = map[string]string{
	"arm64": "postgres@sha256:051f7b7b3abdd564d5d1bd1e8c4b9c1b6e77087d1dd22020ede611c096a272e0",
	"amd64": "postgres@sha256:f4c66b820c6f974249089d3d16d86a3698eae11e8746eb6644b2271031e91232",
}

// The Redis index is multi-architecture, so one digest serves both hosts.
const pinnedRedis = "redis@sha256:d30960f73a599496d8b2802c97758bab6b1cd421fd06337f837779c47a57e1f3"

// nodeArchitecture is what the agent will measure and report, so it is read rather
// than asserted: a policy naming another one makes the node unplaceable and says
// nothing about why.
var nodeArchitecture = runtime.GOARCH

const (
	nodeRuntime = "27.1.1"
	nodeStorage = "pool-a"
)

// engineImage answers the digest pinned for the architecture this host runs.
func engineImage(t *testing.T) string {
	t.Helper()
	digest, ok := pinnedPostgres[nodeArchitecture]
	if !ok {
		t.Skipf("no PostgreSQL image is pinned for %s", nodeArchitecture)
	}
	return digest
}

type harness struct {
	store          *postgres.Store
	pool           *pgxpool.Pool
	ring           *secret.Keyring
	api            *client.Client
	driver         *controllercapacity.Driver
	installationID uuid.UUID
	nodeID         uuid.UUID
	endpointPort   uint16
}

// start brings up a controller and an agent that talk to each other over real mTLS.
func start(t *testing.T, stepTimeout ...time.Duration) *harness {
	t.Helper()
	return startWithAgentOptions(t, agentTestOptions{}, stepTimeout...)
}

type agentTestOptions struct {
	postgresProber engine.Prober
	beforeCommand  func(context.Context, *executor.Executor, *agentv1.Command) error
}

func startWithAgentOptions(t *testing.T, options agentTestOptions, stepTimeout ...time.Duration) *harness {
	t.Helper()
	timeout := 2 * time.Minute
	if len(stepTimeout) > 0 {
		timeout = stepTimeout[0]
	}
	dockerHost := os.Getenv("SYNOD_TEST_DOCKER_HOST")
	if dockerHost == "" {
		t.Skip("set SYNOD_TEST_DOCKER_HOST for the isolated Docker fixture")
	}
	etcdEndpoint := os.Getenv("SYNOD_TEST_ETCD_ENDPOINT")
	if etcdEndpoint == "" {
		t.Skip("run scripts/test-etcd.sh")
	}
	requireSharedNetwork(t, dockerHost)
	store, pool := isolatedStore(t)
	installation, node := uuid.New(), uuid.New()
	h := &harness{store: store, pool: pool, installationID: installation, nodeID: node, endpointPort: enginePort}

	// The node row, its approval, and the installation's recovery generation. Enrolment
	// is TASK-012's and already proved, so the row is inserted rather than negotiated.
	if _, err := pool.Exec(t.Context(), `INSERT INTO nodes (id,installation_id,public_key_hash,enrolled_at)
 VALUES ($1,$2,$3,clock_timestamp())`, node, installation, make([]byte, 32)); err != nil {
		t.Fatal(err)
	}
	// The agent's journal must carry the same recovery generation the controller's
	// claims do, because a command names it and the journal refuses one that does not
	// match.
	generation := uuid.New()
	if err := store.InitializeGeneration(t.Context(), installation, generation); err != nil {
		t.Fatal(err)
	}
	if _, err := store.PutNodePlacementPolicy(t.Context(), placement.NodePolicy{
		InstallationID: installation, NodeID: node, Architecture: nodeArchitecture,
		RuntimeVersion: nodeRuntime, StorageID: nodeStorage,
		Engines:  []placement.EngineVersion{{Engine: "postgres", Version: "17.11"}, {Engine: "redis", Version: "8.2.7"}},
		Headroom: placement.Vector{CPUMillis: 500, MemoryBytes: 1 << 28, DiskBytes: 1 << 30},
		// A replica dials its primary through this address, so it must be one another
		// host could use. The policy refuses a loopback address for that reason.
		ClientAddress: hostAddress(t),
	}, 0); err != nil {
		t.Fatal(err)
	}

	roots, pairs := identities(t, installation, node)
	active := uuid.New()
	ring, err := secret.NewKeyring(active, map[uuid.UUID][]byte{active: make([]byte, 32)})
	if err != nil {
		t.Fatal(err)
	}
	h.ring = ring

	// The controller's agent transport. The runner is built after it and the handler
	// reads the pointer, exactly as the daemon wires it.
	var operations *runner.Runner
	server, err := controlstream.New(controlstream.Config{
		InstallationID: installation, Capabilities: []string{"runtime.docker"},
		TLS:               &tls.Config{MinVersion: tls.VersionTLS12, ClientCAs: roots, Certificates: []tls.Certificate{pairs[pki.Controller]}},
		HeartbeatInterval: 200 * time.Millisecond, StaleAfter: 10 * time.Second,
		Authorize: func(_ context.Context, i, n uuid.UUID, leaf *x509.Certificate) error {
			if i != installation || n != node || !leaf.Equal(pairs[pki.Agent].Leaf) {
				return errors.New("unexpected TLS identity")
			}
			return nil
		},
		Handle: func(ctx context.Context, peer controlstream.Peer, message *agentv1.AgentMessage) error {
			if operations == nil {
				return nil
			}
			err := operations.Handle(ctx, peer, message)
			if ack := message.GetAdmissionAck(); ack != nil {
				t.Logf("admission %s %s %s", ack.CommandId, ack.State, ack.ErrorCode)
			}
			if result := message.GetTerminalResult(); result != nil {
				// The error matters as much as the receipt: a result the controller
				// refuses is never acknowledged, so the agent replays it forever.
				t.Logf("terminal %s %s %s -> %v", result.CommandId, result.State, result.FailureCode, err)
			}
			return err
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	served := make(chan error, 1)
	go func() { served <- server.Serve(listener) }()
	t.Cleanup(func() { server.Stop(); <-served })

	catalog, err := engine.NewCatalog(postgresengine.New(), redisengine.New())
	if err != nil {
		t.Fatal(err)
	}
	driver, err := controllercapacity.New(controllercapacity.Config{Store: store, Transport: server, Wait: 10 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	h.driver = driver
	planner, err := provision.New(provision.Config{
		Store: store, Decider: driver, Catalog: catalog, Keyring: ring,
		Profile: provision.Profile{Architectures: []string{nodeArchitecture}, RuntimeVersion: nodeRuntime, StorageID: nodeStorage},
		Images: []provision.Image{
			{Engine: cluster.PostgreSQL, Version: "17.11", Architecture: nodeArchitecture, Digest: engineImage(t)},
			{Engine: cluster.Redis, Version: "8.2.7", Architecture: nodeArchitecture, Digest: pinnedRedis},
		},
		Budget: placement.Vector{CPUMillis: 1000, MemoryBytes: 1 << 30, DiskBytes: 4 << 30},
		// Placement chooses one port from this span and the plan reserves it, so a
		// second cluster on the same node lands on a different one.
		EndpointPorts: placement.PortRange{First: h.endpointPort, Last: h.endpointPort + 9},
		StepTimeout:   timeout,
	})
	if err != nil {
		t.Fatal(err)
	}
	etcd, err := clientv3.New(clientv3.Config{
		Endpoints: []string{etcdEndpoint}, DialTimeout: 5 * time.Second,
		Context: t.Context(), Logger: zap.NewNop(),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { etcd.Close() })
	elector, err := coordinator.New(etcd, "/synod-integration/"+uuid.NewString())
	if err != nil {
		t.Fatal(err)
	}
	operations, err = runner.New(runner.Config{
		Store: store, Transport: server, Coordinator: elector, InstallationID: installation,
		OwnerID: "integration-" + uuid.NewString(), Logger: slog.New(slog.NewTextHandler(testWriter{t}, &slog.HandlerOptions{Level: slog.LevelDebug})),
		Keyring: ring, Planner: planner, ScanInterval: time.Second, IdleBackoff: time.Second,
		// A short claim so an attempt that fails before evidence exists is retried
		// promptly rather than after a production-length lease.
		ClaimTTL: 5 * time.Second, LeaseSeconds: 3,
	})
	if err != nil {
		t.Fatal(err)
	}

	h.api = startAgent(t, dockerHost, installation, node, generation, listener.Addr().String(), roots, pairs, options)
	// Registered here rather than in each test, so a test that fails during setup
	// still removes what it created. The engine binds a fixed host port, so one
	// leaked container stops every later run.
	t.Cleanup(func() { h.removeNativeObjects(t) })

	runnerCtx, stopRunner := context.WithCancel(t.Context())
	running := make(chan error, 1)
	go func() { running <- operations.Run(runnerCtx) }()
	t.Cleanup(func() {
		stopRunner()
		select {
		case err := <-running:
			if err != nil && !errors.Is(err, context.Canceled) {
				t.Log("runner stopped:", err)
			}
		case <-time.After(30 * time.Second):
			t.Error("the runner did not stop")
		}
	})
	return h
}

// startAgent runs a real executor over a real Docker daemon and returns its API client
// so a test can clean up what the agent created.
func startAgent(t *testing.T, dockerHost string, installation, node, generation uuid.UUID, endpoint string, roots *x509.CertPool, pairs map[pki.Role]tls.Certificate, options agentTestOptions) *client.Client {
	t.Helper()
	identity := journal.Identity{
		InstallationID: installation.String(), NodeID: node.String(),
		RecoveryGeneration: generation.String(),
	}
	j, err := journal.Open(filepath.Join(t.TempDir(), "journal"), journal.Options{Identity: identity, AllowCreate: true, EmptyInventory: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { j.Close() })
	api, err := client.New(client.WithHost(dockerHost))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { api.Close() })
	native, err := docker.New(api, docker.Options{InstallationID: installation, NodeID: node})
	if err != nil {
		t.Fatal(err)
	}
	if options.postgresProber == nil {
		options.postgresProber = postgresengine.New()
	}
	boot := uuid.NewString()
	e, err := executor.New(j, native, executor.Options{
		Workers: 2, QueueSize: 8, PollInterval: 200 * time.Millisecond,
		Host: measurable(t), BootID: boot, StorageID: nodeStorage, RuntimeVersion: nodeRuntime,
		Probers: map[string]engine.Prober{"postgres": options.postgresProber, "redis": redisengine.New()},
	})
	if err != nil {
		t.Fatal(err)
	}
	stream, err := transport.New(transport.Config{
		Endpoints: []string{endpoint},
		TLS:       &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots, Certificates: []tls.Certificate{pairs[pki.Agent]}},
		Hello: &agentv1.Hello{
			InstallationId: installation.String(), NodeId: node.String(), BootId: boot,
			// The journal's own generation, not the installation's recovery generation.
			// A capacity answer echoes what the journal holds, and a Hello that
			// claimed a different one would make every probe look expired.
			JournalGeneration: j.Identity().JournalGeneration, Protocol: &agentv1.VersionRange{Minimum: 1, Maximum: 1},
			ActivePayloadVersions: []uint32{1}, Capabilities: []string{"runtime.docker"},
		},
		BackoffMin: 20 * time.Millisecond, BackoffMax: 100 * time.Millisecond,
		Handle: func(ctx context.Context, m *agentv1.ControllerMessage) (*agentv1.AgentMessage, error) {
			if command := m.GetCommand(); command != nil && options.beforeCommand != nil {
				if err := options.beforeCommand(ctx, e, command); err != nil {
					return nil, err
				}
			}
			reply, err := e.Handle(ctx, m)
			if err != nil {
				t.Logf("agent handling %T failed: %v", m.Message, err)
			}
			return reply, err
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	executorDone, clientDone := make(chan error, 1), make(chan error, 1)
	go func() { executorDone <- e.Run(ctx, stream) }()
	go func() { clientDone <- stream.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		for _, done := range []chan error{executorDone, clientDone} {
			select {
			case err := <-done:
				if err != nil && !errors.Is(err, context.Canceled) {
					t.Log("agent stopped:", err)
				}
			case <-time.After(30 * time.Second):
				t.Error("the agent did not stop")
			}
		}
	})
	return api
}

// identities issues one controller and one agent certificate from a fresh CA.
func identities(t *testing.T, installation, node uuid.UUID) (*x509.CertPool, map[pki.Role]tls.Certificate) {
	t.Helper()
	root, key, err := pki.CreateCA(installation, time.Now(), 24*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	ca, err := pki.LoadCA(installation, root, key)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AppendCertsFromPEM(root)
	pairs := make(map[pki.Role]tls.Certificate)
	for _, role := range []pki.Role{pki.Agent, pki.Controller} {
		_, private, err := ed25519.GenerateKey(rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		csr, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{}, private)
		if err != nil {
			t.Fatal(err)
		}
		subject, names := node, []string(nil)
		if role == pki.Controller {
			subject, names = uuid.New(), []string{"127.0.0.1"}
		}
		leaf, err := ca.Issue(csr, pki.Identity{InstallationID: installation, SubjectID: subject, Role: role}, names, time.Now(), time.Hour)
		if err != nil {
			t.Fatal(err)
		}
		der, err := x509.MarshalPKCS8PrivateKey(private)
		if err != nil {
			t.Fatal(err)
		}
		pair, err := tls.X509KeyPair(leaf, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}))
		if err != nil {
			t.Fatal(err)
		}
		pairs[role] = pair
	}
	return roots, pairs
}

// measurable writes a proc tree the capacity probe can read. macOS has no /proc, and
// what this fixture measures is the challenge path rather than the host.
func measurable(t *testing.T) capacity.Host {
	t.Helper()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "meminfo"), []byte("MemTotal: 16000000 kB\nMemAvailable: 12000000 kB\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	stat := filepath.Join(dir, "stat")
	if err := os.WriteFile(stat, []byte("cpu  100 0 100 700 100 0 0 0\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	// The counters must advance between two samples. CPU use is a rate, so writing
	// the same values repeatedly leaves the two reads identical and the measurement
	// with nothing to divide by.
	done := make(chan struct{})
	written := make(chan struct{})
	go func() {
		defer close(written)
		for tick := 1; ; tick++ {
			select {
			case <-done:
				return
			case <-time.After(10 * time.Millisecond):
			}
			busy, idle := 100+tick*20, 700+tick*80
			_ = os.WriteFile(stat, fmt.Appendf(nil, "cpu  %d 0 %d %d 100 0 0 0\n", busy, busy, idle), 0o600)
		}
	}()
	t.Cleanup(func() { close(done); <-written })
	return capacity.Host{ProcRoot: dir, StoragePath: t.TempDir(), Sample: 40 * time.Millisecond}
}

// enginePort is where PostgreSQL listens. Under host networking the engine binds its
// own default port, so this is the engine's choice and not Synod's. It is why this
// profile holds one engine per node, and why the plan reserves the port.
const enginePort = 5432

// isolatedStore creates one database per test so nothing shares state.
func isolatedStore(t *testing.T) (*postgres.Store, *pgxpool.Pool) {
	t.Helper()
	dsn := os.Getenv("SYNOD_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("run scripts/test-postgres.sh")
	}
	admin, err := pgxpool.New(t.Context(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	name := "integration_" + strings.ReplaceAll(uuid.NewString(), "-", "")
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

// testWriter sends controller logs to the test output, so a run that fails says why.
type testWriter struct{ t *testing.T }

func (w testWriter) Write(p []byte) (int, error) {
	w.t.Log(strings.TrimSpace(string(p)))
	return len(p), nil
}

// awaitAgent waits until the controller has recorded the agent's session.
//
// A cluster admitted before any node is connected is legitimate and the runner retries
// until one is, but a test that does not wait is measuring the retry loop rather than
// the path it means to.
func (h *harness) awaitAgent(t *testing.T, limit time.Duration) {
	t.Helper()
	deadline := time.Now().Add(limit)
	for time.Now().Before(deadline) {
		snapshot, err := h.store.ReadPlacementSnapshot(t.Context(), h.installationID)
		if err != nil {
			t.Fatal(err)
		}
		if len(snapshot.Nodes) == 1 && snapshot.Nodes[0].SessionID != uuid.Nil && snapshot.Nodes[0].Policy != nil {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatal("the agent did not connect")
}

// requireSharedNetwork skips when the test process cannot reach a host-networked
// container.
//
// This profile runs the engine on the host's own network, so the agent reaches it at
// 127.0.0.1 and so does a client. That holds when the agent runs on the Docker host,
// which is the topology Synod ships. It does not hold on a Docker Desktop VM, where
// the daemon has its own network namespace.
//
// The port is checked before the container starts as well as after. A port something
// else on this machine already holds would otherwise answer the dial and report a
// shared namespace that is not there.
func requireSharedNetwork(t *testing.T, dockerHost string) {
	t.Helper()
	if dialable(reachabilityPort) {
		t.Skipf("something already listens on 127.0.0.1:%d, so reachability cannot be told apart from it", reachabilityPort)
	}
	api, err := client.New(client.WithHost(dockerHost))
	if err != nil {
		t.Fatal(err)
	}
	defer api.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 120*time.Second)
	defer cancel()
	// The engine image is already pinned and pulled, and it listens on a port of its
	// own choosing, so it needs no tools the image may not carry.
	created, err := api.ContainerCreate(ctx, client.ContainerCreateOptions{
		Name: "integration-reachability-" + uuid.NewString(),
		Config: &container.Config{
			Image: reachabilityImage,
			Cmd:   []string{"redis-server", "--port", strconv.Itoa(reachabilityPort), "--bind", "0.0.0.0"},
		},
		HostConfig: &container.HostConfig{NetworkMode: "host"},
	})
	if err != nil {
		t.Skipf("cannot create the reachability probe: %v", err)
	}
	defer api.ContainerRemove(context.WithoutCancel(ctx), created.ID, client.ContainerRemoveOptions{Force: true})
	if _, err := api.ContainerStart(ctx, created.ID, client.ContainerStartOptions{}); err != nil {
		t.Skipf("cannot start the reachability probe: %v", err)
	}
	for range 40 {
		if dialable(reachabilityPort) {
			return
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Skip("this Docker daemon does not share a network namespace with the test process, " +
		"so a host-networked engine is unreachable here. Synod runs its agent on the Docker host, " +
		"where it is reachable; this end-to-end proof needs such a host.")
}

// dialable reports a TCP listener answering on loopback.
func dialable(port int) bool {
	conn, err := net.DialTimeout("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(port)), 250*time.Millisecond)
	if err != nil {
		return false
	}
	conn.Close()
	return true
}

// reachabilityImage is the engine image the fixtures already pull, so this adds no
// new download.
const reachabilityImage = "redis@sha256:d30960f73a599496d8b2802c97758bab6b1cd421fd06337f837779c47a57e1f3"

// reachabilityPort is only used by the check above.
const reachabilityPort = 47631

// dropTimeout bounds one test database teardown. It exists to stop a hang, not to
// assert how fast PostgreSQL drops a database under load.
const dropTimeout = 60 * time.Second

// hostAddress answers an address of this host that another host could dial.
//
// A replica reaches its primary through the node's recorded client address, and the
// placement policy refuses a loopback one because a peer cannot use it. Under a shared
// network namespace this is the same interface the engine binds.
func hostAddress(t *testing.T) string {
	t.Helper()
	interfaces, err := net.Interfaces()
	if err != nil {
		t.Fatal(err)
	}
	for _, found := range interfaces {
		if found.Flags&net.FlagUp == 0 || found.Flags&net.FlagLoopback != 0 {
			continue
		}
		addresses, err := found.Addrs()
		if err != nil {
			continue
		}
		for _, address := range addresses {
			network, ok := address.(*net.IPNet)
			if !ok {
				continue
			}
			if v4 := network.IP.To4(); v4 != nil && v4.IsGlobalUnicast() {
				return v4.String()
			}
		}
	}
	t.Skip("this host has no routable IPv4 address, so a replica has no peer address to dial")
	return ""
}
