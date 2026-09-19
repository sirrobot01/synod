package runner

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"io"
	"log/slog"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sirrobot01/synod/internal/cluster"
	"github.com/sirrobot01/synod/internal/controller/worker"
	"github.com/sirrobot01/synod/internal/controlstream"
	"github.com/sirrobot01/synod/internal/coordinator"
	"github.com/sirrobot01/synod/internal/operation"
	"github.com/sirrobot01/synod/internal/pki"
	"github.com/sirrobot01/synod/internal/store/postgres"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

// installation runs one controller against real PostgreSQL, real etcd, and real mutual TLS.
type installation struct {
	store         *postgres.Store
	pool          *pgxpool.Pool
	runner        *Runner
	id            uuid.UUID
	node          uuid.UUID
	address       string
	roots         *x509.CertPool
	agentTLS      tls.Certificate
	controllerTLS tls.Certificate
}

func testPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("SYNOD_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("run scripts/test-postgres.sh")
	}
	admin, err := pgxpool.New(t.Context(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	name := "runner_" + strings.ReplaceAll(uuid.NewString(), "-", "")
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
	return pool
}

func testCoordinator(t *testing.T) *coordinator.Coordinator {
	t.Helper()
	endpoint := os.Getenv("SYNOD_TEST_ETCD_ENDPOINT")
	if endpoint == "" {
		t.Skip("run scripts/test-etcd.sh")
	}
	client, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: 2 * time.Second, Logger: zap.NewNop()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { client.Close() })
	elector, err := coordinator.New(client, "/synod-test/"+uuid.NewString())
	if err != nil {
		t.Fatal(err)
	}
	return elector
}

// newInstallation builds a controller that serves one enrolled agent and runs workers.
func newInstallation(t *testing.T) *installation {
	t.Helper()
	pool := testPool(t)
	store := postgres.New(pool)
	if err := store.Migrate(t.Context()); err != nil {
		t.Fatal(err)
	}
	elector := testCoordinator(t)
	inst := &installation{store: store, pool: pool, id: uuid.New(), node: uuid.New()}
	inst.issue(t)
	var routed *Runner
	server, err := controlstream.New(controlstream.Config{
		InstallationID: inst.id,
		TLS:            &tls.Config{MinVersion: tls.VersionTLS12, ClientCAs: inst.roots, Certificates: []tls.Certificate{inst.controllerTLS}},
		Authorize:      store.CheckAgentCertificate,
		Handle: func(ctx context.Context, peer controlstream.Peer, message *agentv1.AgentMessage) error {
			return routed.Handle(ctx, peer, message)
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	t.Cleanup(func() { server.Stop(); <-done })
	inst.address = listener.Addr().String()
	inst.runner, err = New(Config{
		Store: store, Transport: server, Coordinator: elector, InstallationID: inst.id,
		OwnerID: "controller/1", Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		ClaimTTL: 30 * time.Second, LeaseSeconds: 3, ScanInterval: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	routed = inst.runner
	return inst
}

// issue enrolls the agent and issues the controller certificate from one installation CA.
func (i *installation) issue(t *testing.T) {
	t.Helper()
	caCert, caKey, err := pki.CreateCA(i.id, time.Now(), 7*24*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	ca, err := pki.LoadCA(i.id, caCert, caKey)
	if err != nil {
		t.Fatal(err)
	}
	i.roots = x509.NewCertPool()
	if !i.roots.AppendCertsFromPEM(caCert) {
		t.Fatal("invalid fixture CA")
	}
	for _, role := range []pki.Role{pki.Agent, pki.Controller} {
		_, private, err := ed25519.GenerateKey(rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		csr, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{}, private)
		if err != nil {
			t.Fatal(err)
		}
		var leaf []byte
		if role == pki.Agent {
			verifier := sha256.Sum256([]byte(uuid.NewString()))
			if _, err := i.store.RegisterEnrollmentToken(t.Context(), postgres.TokenRegistration{InstallationID: i.id, NodeID: i.node, ActorID: "test-admin", RequestID: uuid.NewString(), IdempotencyKey: uuid.NewString(), TokenVerifier: verifier, TTLSeconds: 600}); err != nil {
				t.Fatal(err)
			}
			enrolled, err := i.store.EnrollAgent(t.Context(), postgres.EnrollmentRequest{InstallationID: i.id, NodeID: i.node, RequestID: uuid.New(), TokenVerifier: verifier, CSRDER: csr}, ca)
			if err != nil {
				t.Fatal(err)
			}
			leaf = []byte(enrolled.CertificatePEM)
		} else if leaf, err = ca.Issue(csr, pki.Identity{InstallationID: i.id, Role: role, SubjectID: uuid.New()}, []string{"127.0.0.1"}, time.Now(), time.Hour); err != nil {
			t.Fatal(err)
		}
		key, err := x509.MarshalPKCS8PrivateKey(private)
		if err != nil {
			t.Fatal(err)
		}
		pair, err := tls.X509KeyPair(leaf, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: key}))
		if err != nil {
			t.Fatal(err)
		}
		if role == pki.Agent {
			i.agentTLS = pair
			continue
		}
		i.controllerTLS = pair
	}
}

// connect runs one scripted agent that answers over the real authenticated stream.
func (i *installation) connect(t *testing.T, script func(*agentv1.ControllerMessage) []*agentv1.AgentMessage) {
	t.Helper()
	connection, err := grpc.NewClient(i.address, grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS12, RootCAs: i.roots, Certificates: []tls.Certificate{i.agentTLS}})))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { connection.Close() })
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	stream, err := agentv1.NewAgentServiceClient(connection).Connect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	hello := &agentv1.Hello{InstallationId: i.id.String(), NodeId: i.node.String(), BootId: uuid.NewString(), JournalGeneration: uuid.NewString(), Protocol: &agentv1.VersionRange{Minimum: 1, Maximum: 1}, ActivePayloadVersions: []uint32{1}}
	if err := stream.Send(&agentv1.AgentMessage{Message: &agentv1.AgentMessage_Hello{Hello: hello}}); err != nil {
		t.Fatal(err)
	}
	if message, err := stream.Recv(); err != nil || message.GetSession() == nil {
		t.Fatalf("session failed: %v", err)
	}
	go func() {
		for {
			message, err := stream.Recv()
			if err != nil {
				return
			}
			for _, answer := range script(message) {
				if stream.Send(answer) != nil {
					return
				}
			}
		}
	}()
}

// cancelWatch reports whether the controller ever asked the agent to cancel a command.
type cancelWatch struct {
	mu        sync.Mutex
	cancelled bool
}

func (w *cancelWatch) script(message *agentv1.ControllerMessage) []*agentv1.AgentMessage {
	if message.GetCancel() != nil {
		w.mu.Lock()
		w.cancelled = true
		w.mu.Unlock()
	}
	return drainOnly(message)
}

func (w *cancelWatch) seen() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.cancelled
}

// drainOnly closes the epoch and then never answers a command.
func drainOnly(message *agentv1.ControllerMessage) []*agentv1.AgentMessage {
	if prepare := message.GetPrepareEpoch(); prepare != nil {
		report := &agentv1.BarrierResult{Epoch: prepare.Epoch, AdmissionsClosed: true, Drained: true}
		return []*agentv1.AgentMessage{{Message: &agentv1.AgentMessage_BarrierResult{BarrierResult: report}}}
	}
	return nil
}

// answer drains an epoch and reports every command as a proved success.
func answer(message *agentv1.ControllerMessage) []*agentv1.AgentMessage {
	if prepare := message.GetPrepareEpoch(); prepare != nil {
		report := &agentv1.BarrierResult{Epoch: prepare.Epoch, AdmissionsClosed: true, Drained: true}
		return []*agentv1.AgentMessage{{Message: &agentv1.AgentMessage_BarrierResult{BarrierResult: report}}}
	}
	command := message.GetCommand()
	if command == nil {
		return nil
	}
	ack := &agentv1.AdmissionAck{Epoch: command.Epoch, CommandId: command.CommandId, State: "queued", InputHash: command.InputHash}
	receipt := &agentv1.TerminalResult{
		Epoch: command.Epoch, CommandId: command.CommandId, InputHash: command.InputHash, Sequence: 1, State: "succeeded",
		Effects: []*agentv1.Effect{{CommandId: command.CommandId, ResourceId: command.Input.ResourceId, NativeOperationId: "docker-command:" + command.CommandId, State: "present"}},
	}
	return []*agentv1.AgentMessage{
		{Message: &agentv1.AgentMessage_AdmissionAck{AdmissionAck: ack}},
		{Message: &agentv1.AgentMessage_TerminalResult{TerminalResult: receipt}},
	}
}

// admit creates one cluster, which writes the outbox row that the runner scans.
func (i *installation) admit(t *testing.T, name string) postgres.Admission {
	t.Helper()
	admission, err := i.store.CreateCluster(t.Context(), postgres.CreateClusterRequest{
		Identity:  postgres.RequestIdentity{InstallationID: i.id, ActorID: "actor-1", APIOperation: postgres.CreateClusterAPIOperation, IdempotencyKey: uuid.NewString()},
		RequestID: "request-1",
		Spec:      cluster.CreateSpec{SchemaVersion: 1, Name: name, Engine: cluster.PostgreSQL, PostgreSQL: &cluster.PostgreSQLSpec{Version: "17.11", Topology: "standalone"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	return admission
}

// install writes a one-step plan under a short-lived claim, then releases it for the runner.
// Plan production belongs to a later task, so this test supplies the plan directly.
func (i *installation) install(t *testing.T, admission postgres.Admission) *agentv1.ExecutionPlan {
	t.Helper()
	generation, err := i.store.RecoveryGeneration(t.Context(), i.id)
	if err != nil {
		t.Fatal(err)
	}
	if generation == uuid.Nil {
		generation = uuid.New()
		if err := i.store.InitializeGeneration(t.Context(), i.id, generation); err != nil {
			t.Fatal(err)
		}
	}
	claim, err := i.store.AcquireClaim(t.Context(), postgres.ClaimRequest{
		ClaimID: uuid.New(), InstallationID: i.id, OperationID: admission.OperationID, RecoveryGeneration: generation,
		OwnerID: "installer", TTL: time.Second, CoordinationKey: "/installer", CoordinationLease: 1, CoordinationRevision: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	resource := uuid.New()
	command := &agentv1.Command{
		Epoch:       &agentv1.Epoch{InstallationId: i.id.String(), NodeId: i.node.String(), ClusterId: claim.ClusterID.String(), RecoveryGeneration: generation.String(), ExecutionEpoch: claim.ExecutionEpoch},
		OperationId: admission.OperationID.String(), StepId: uuid.NewString(), CommandId: uuid.NewString(), DesiredRevision: 1, PlanVersion: 1,
		Input: &agentv1.CommandInput{ResourceId: resource.String(), PayloadVersion: 1, Timeout: durationpb.New(time.Minute), Payload: &agentv1.CommandInput_EnsureVolume{EnsureVolume: &agentv1.EnsureVolume{VolumeId: resource.String()}}},
	}
	hash, err := operation.CommandHash(command)
	if err != nil {
		t.Fatal(err)
	}
	command.InputHash = hash[:]
	plan := &agentv1.ExecutionPlan{Version: 1, Steps: []*agentv1.PlanStep{{
		Command: command, MaxAttempts: 3, CancellationBoundary: agentv1.CancellationBoundary_CANCELLATION_BOUNDARY_BEFORE_EXECUTION,
		Postcondition: agentv1.Postcondition_POSTCONDITION_VOLUME_EXISTS,
	}}}
	if err := i.store.InstallPlan(t.Context(), claim, plan); err != nil {
		t.Fatal(err)
	}
	// Expire the installer claim so the runner can take the operation.
	if _, err := i.pool.Exec(t.Context(), `UPDATE operations SET claim_expires_at=clock_timestamp()-interval '1 second' WHERE id=$1`, admission.OperationID); err != nil {
		t.Fatal(err)
	}
	return plan
}

func (i *installation) count(t *testing.T, query string, args ...any) int {
	t.Helper()
	var total int
	if err := i.pool.QueryRow(t.Context(), query, args...).Scan(&total); err != nil {
		t.Fatal(err)
	}
	return total
}

// held reads the claim that currently owns one operation and its durable deadline.
func (i *installation) held(t *testing.T, operationID uuid.UUID) (uuid.UUID, time.Time) {
	t.Helper()
	var claimID uuid.UUID
	var deadline time.Time
	if err := i.pool.QueryRow(t.Context(), `SELECT claim_id,claim_expires_at FROM operations WHERE id=$1`, operationID).Scan(&claimID, &deadline); err != nil {
		t.Fatal(err)
	}
	return claimID, deadline
}

func (i *installation) waitFor(t *testing.T, done func() bool, message string) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for !done() {
		if time.Now().After(deadline) {
			t.Fatal(message)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// This boundary test runs one admitted operation to success through the real controller path:
// the outbox scan, an etcd election, a durable claim, mutual TLS dispatch, and a receipt.
func TestRunnerDrivesAnAdmittedOperation(t *testing.T) {
	inst := newInstallation(t)
	inst.connect(t, answer)
	admission := inst.admit(t, "db")
	inst.install(t, admission)
	ctx, cancel := context.WithCancel(t.Context())
	stopped := make(chan error, 1)
	go func() { stopped <- inst.runner.Run(ctx) }()
	inst.waitFor(t, func() bool {
		return inst.count(t, `SELECT count(*) FROM operations WHERE id=$1 AND status='succeeded'`, admission.OperationID) == 1
	}, "the runner did not finish the operation")
	if inst.count(t, `SELECT count(*) FROM command_dispatches WHERE operation_id=$1`, admission.OperationID) != 1 {
		t.Fatal("the plan step was not dispatched")
	}
	if inst.count(t, `SELECT count(*) FROM operation_step_completions WHERE operation_id=$1`, admission.OperationID) != 1 {
		t.Fatal("the receipt was not retained")
	}
	if inst.count(t, `SELECT count(*) FROM claim_barriers WHERE operation_id=$1 AND reconciled_at IS NOT NULL`, admission.OperationID) != 1 {
		t.Fatal("the node was not reconciled")
	}
	if inst.count(t, `SELECT count(*) FROM execution_claims WHERE operation_id=$1 AND owner_id='controller/1'`, admission.OperationID) != 1 {
		t.Fatal("the runner did not claim the operation")
	}
	cancel()
	select {
	case err := <-stopped:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("the runner did not stop")
	}
	// Every worker released its cluster before the runner returned.
	inst.runner.mu.Lock()
	remaining := len(inst.runner.workers)
	inst.runner.mu.Unlock()
	if remaining != 0 {
		t.Fatalf("workers still registered after shutdown: %d", remaining)
	}
}

// Every scan sees the same undelivered outbox row, so one cluster must keep one worker.
func TestRunnerRunsOneWorkerForOneCluster(t *testing.T) {
	inst := newInstallation(t)
	inst.connect(t, drainOnly)
	admission := inst.admit(t, "db")
	inst.install(t, admission)
	ctx, cancel := context.WithCancel(t.Context())
	stopped := make(chan error, 1)
	go func() { stopped <- inst.runner.Run(ctx) }()
	inst.waitFor(t, func() bool {
		return inst.count(t, `SELECT count(*) FROM execution_claims WHERE operation_id=$1 AND owner_id='controller/1'`, admission.OperationID) == 1
	}, "the runner did not claim the operation")
	// The worker waits for a receipt that never arrives, across several scans.
	time.Sleep(3 * inst.runner.config.ScanInterval)
	if running := len(inst.runner.slots); running != 1 {
		t.Fatalf("running workers=%d, want one for the cluster", running)
	}
	if total := inst.count(t, `SELECT count(*) FROM execution_claims WHERE operation_id=$1 AND owner_id='controller/1'`, admission.OperationID); total != 1 {
		t.Fatalf("runner claims=%d, want one", total)
	}
	if total := inst.count(t, `SELECT count(*) FROM command_dispatches WHERE operation_id=$1`, admission.OperationID); total != 1 {
		t.Fatalf("dispatches=%d, want one", total)
	}
	cancel()
	<-stopped
}

// A claim must stay alive while its worker waits for evidence.
func TestRunnerRenewsTheClaimWhileAWorkerRuns(t *testing.T) {
	inst := newInstallation(t)
	inst.runner.config.ClaimTTL = 3 * time.Second
	inst.connect(t, drainOnly)
	admission := inst.admit(t, "db")
	inst.install(t, admission)
	ctx, cancel := context.WithCancel(t.Context())
	stopped := make(chan error, 1)
	go func() { stopped <- inst.runner.Run(ctx) }()
	inst.waitFor(t, func() bool {
		return inst.count(t, `SELECT count(*) FROM execution_claims WHERE operation_id=$1 AND owner_id='controller/1'`, admission.OperationID) == 1
	}, "the runner did not claim the operation")
	held, first := inst.held(t, admission.OperationID)
	inst.waitFor(t, func() bool {
		_, deadline := inst.held(t, admission.OperationID)
		return deadline.After(first)
	}, "the runner did not renew the claim")
	// The same claim must move its deadline. A new claim would be a takeover, not a renewal.
	if current, _ := inst.held(t, admission.OperationID); current != held {
		t.Fatalf("claim changed from %s to %s, so the owner reacquired instead of renewing", held, current)
	}
	if inst.count(t, `SELECT count(*) FROM operations WHERE id=$1 AND claim_expires_at>clock_timestamp()`, admission.OperationID) != 1 {
		t.Fatal("the renewed claim already expired")
	}
	if total := inst.count(t, `SELECT count(*) FROM execution_claims WHERE operation_id=$1 AND owner_id='controller/1'`, admission.OperationID); total != 1 {
		t.Fatalf("runner claims=%d, want one across the whole run", total)
	}
	cancel()
	<-stopped
}

// A claim that this owner can no longer renew must stop its worker.
func TestRunnerStopsTheWorkerWhenRenewalFails(t *testing.T) {
	inst := newInstallation(t)
	inst.runner.config.ClaimTTL = 3 * time.Second
	inst.connect(t, drainOnly)
	admission := inst.admit(t, "db")
	inst.install(t, admission)
	ctx, cancel := context.WithCancel(t.Context())
	stopped := make(chan error, 1)
	go func() { stopped <- inst.runner.Run(ctx) }()
	inst.waitFor(t, func() bool { return inst.worker(admission.ClusterID) != nil }, "the runner did not start a worker")
	first := inst.worker(admission.ClusterID)
	epoch := inst.epoch(t, admission.OperationID)

	// Expire the claim behind the owner. RenewClaim never revives an expired claim.
	if _, err := inst.pool.Exec(t.Context(), `UPDATE operations SET claim_expires_at=clock_timestamp()-interval '1 second' WHERE id=$1`, admission.OperationID); err != nil {
		t.Fatal(err)
	}

	// The slot count is not the thing to watch. This runner rescans every second
	// and an expired claim is takeable, so it reclaims the operation and fills the
	// slot again; the count is zero only between the unwind and the next scan.
	// The worker for the cluster being replaced is durable: it only changes after
	// the first one was stopped and a new claim was taken.
	inst.waitFor(t, func() bool {
		found := inst.worker(admission.ClusterID)
		return found != nil && found != first
	}, "a worker outlived its claim")

	// A new claim means a new execution epoch, which is what fences the old owner.
	if next := inst.epoch(t, admission.OperationID); next <= epoch {
		t.Fatalf("execution epoch %d did not advance past %d", next, epoch)
	}
	cancel()
	<-stopped
}

// worker reads the worker the runner currently holds for one cluster.
func (i *installation) worker(clusterID uuid.UUID) *worker.Worker {
	i.runner.mu.Lock()
	defer i.runner.mu.Unlock()
	return i.runner.workers[clusterID]
}

// epoch reads the durable execution epoch of one operation.
func (i *installation) epoch(t *testing.T, operationID uuid.UUID) int64 {
	t.Helper()
	var found int64
	if err := i.pool.QueryRow(t.Context(), `SELECT execution_epoch FROM operations WHERE id=$1`, operationID).Scan(&found); err != nil {
		t.Fatal(err)
	}
	return found
}

// Shutdown stops management dispatch. It must never ask an agent to cancel native work.
func TestRunnerShutdownLeavesNativeWorkAlone(t *testing.T) {
	inst := newInstallation(t)
	watch := &cancelWatch{}
	inst.connect(t, watch.script)
	admission := inst.admit(t, "db")
	inst.install(t, admission)
	ctx, cancel := context.WithCancel(t.Context())
	stopped := make(chan error, 1)
	go func() { stopped <- inst.runner.Run(ctx) }()
	inst.waitFor(t, func() bool {
		return inst.count(t, `SELECT count(*) FROM command_dispatches WHERE operation_id=$1`, admission.OperationID) == 1
	}, "the runner did not dispatch the step")
	cancel()
	select {
	case err := <-stopped:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("the runner did not stop")
	}
	if watch.seen() {
		t.Fatal("shutdown cancelled a command that the agent already holds")
	}
	// The dispatch stays durable for the next owner, and the operation stays unfinished.
	if inst.count(t, `SELECT count(*) FROM command_dispatches WHERE operation_id=$1`, admission.OperationID) != 1 {
		t.Fatal("shutdown removed a committed dispatch")
	}
	if inst.count(t, `SELECT count(*) FROM operations WHERE id=$1 AND finished_at IS NULL`, admission.OperationID) != 1 {
		t.Fatal("shutdown finished an operation with work in flight")
	}
}

// A scan must page through every pending row. One page would hide later operations.
func TestRunnerScansBeyondOnePage(t *testing.T) {
	inst := newInstallation(t)
	inst.runner.config.OutboxPage = 1
	inst.connect(t, answer)
	var operations []uuid.UUID
	for _, name := range []string{"first", "second", "third"} {
		admission := inst.admit(t, name)
		inst.install(t, admission)
		operations = append(operations, admission.OperationID)
	}
	ctx, cancel := context.WithCancel(t.Context())
	stopped := make(chan error, 1)
	go func() { stopped <- inst.runner.Run(ctx) }()
	for index, operationID := range operations {
		inst.waitFor(t, func() bool {
			return inst.count(t, `SELECT count(*) FROM operations WHERE id=$1 AND status='succeeded'`, operationID) == 1
		}, "operation "+strconv.Itoa(index)+" never ran, so the scan stopped at its page")
	}
	cancel()
	<-stopped
}

// Controllers race to store the first generation. Every loser must accept the winner's.
func TestRunnerToleratesAConcurrentGeneration(t *testing.T) {
	inst := newInstallation(t)
	winner := uuid.New()
	if err := inst.store.InitializeGeneration(t.Context(), inst.id, winner); err != nil {
		t.Fatal(err)
	}
	// A controller that proposes its own generation now loses. That is not a failure.
	if err := inst.runner.ensureGeneration(t.Context()); err != nil {
		t.Fatalf("the loser of a generation race stopped: %v", err)
	}
	generation, err := inst.store.RecoveryGeneration(t.Context(), inst.id)
	if err != nil || generation != winner {
		t.Fatalf("generation %s, want %s: %v", generation, winner, err)
	}
	// Concurrent first boots must all continue too.
	fresh := newInstallation(t)
	start := make(chan struct{})
	errs := make(chan error, 8)
	var racing sync.WaitGroup
	for range 8 {
		racing.Go(func() {
			<-start
			errs <- fresh.runner.ensureGeneration(t.Context())
		})
	}
	close(start)
	racing.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("a concurrent first boot stopped: %v", err)
		}
	}
	if generation, err := fresh.store.RecoveryGeneration(t.Context(), fresh.id); err != nil || generation == uuid.Nil {
		t.Fatalf("generation %s: %v", generation, err)
	}
}

// A worker that waits for evidence must keep running, not surrender and retake its claim.
func TestRunnerKeepsTheClaimAcrossAWorkerRetry(t *testing.T) {
	inst := newInstallation(t)
	inst.runner.config.EvidenceWait = time.Second
	inst.runner.config.IdleBackoff = time.Second
	inst.connect(t, drainOnly)
	admission := inst.admit(t, "db")
	inst.install(t, admission)
	ctx, cancel := context.WithCancel(t.Context())
	stopped := make(chan error, 1)
	go func() { stopped <- inst.runner.Run(ctx) }()
	inst.waitFor(t, func() bool {
		return inst.count(t, `SELECT count(*) FROM execution_claims WHERE operation_id=$1 AND owner_id='controller/1'`, admission.OperationID) == 1
	}, "the runner did not claim the operation")
	held, _ := inst.held(t, admission.OperationID)
	// Several worker waits elapse. The worker must hold its slot through all of them,
	// because releasing it would idle the operation until the claim expired.
	deadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) {
		if running := len(inst.runner.slots); running != 1 {
			t.Fatalf("running workers=%d, so the worker surrendered its claim while waiting", running)
		}
		time.Sleep(50 * time.Millisecond)
	}
	if current, _ := inst.held(t, admission.OperationID); current != held {
		t.Fatalf("claim changed from %s to %s while waiting for evidence", held, current)
	}
	if total := inst.count(t, `SELECT count(*) FROM execution_claims WHERE operation_id=$1`, admission.OperationID); total != 2 {
		t.Fatalf("claims=%d, want the installer claim and one runner claim", total)
	}
	cancel()
	<-stopped
}

// A wedged election must release its worker slot instead of holding it forever.
func TestRunnerReleasesASlotWhenNoOwnerIsElected(t *testing.T) {
	inst := newInstallation(t)
	dead, err := clientv3.New(clientv3.Config{Endpoints: []string{"127.0.0.1:1"}, DialTimeout: time.Second, Logger: zap.NewNop()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { dead.Close() })
	elector, err := coordinator.New(dead, "/synod-test/"+uuid.NewString())
	if err != nil {
		t.Fatal(err)
	}
	inst.runner.config.Coordinator = elector
	inst.runner.config.CampaignTimeout = time.Second
	admission := inst.admit(t, "db")
	inst.install(t, admission)
	ctx, cancel := context.WithCancel(t.Context())
	stopped := make(chan error, 1)
	go func() { stopped <- inst.runner.Run(ctx) }()
	inst.waitFor(t, func() bool { return len(inst.runner.slots) == 1 }, "the runner did not try to elect an owner")
	inst.waitFor(t, func() bool { return len(inst.runner.slots) == 0 }, "a wedged election kept its worker slot")
	if inst.count(t, `SELECT count(*) FROM execution_claims WHERE owner_id='controller/1'`) != 0 {
		t.Fatal("an unelected owner claimed the operation")
	}
	cancel()
	<-stopped
}

// An operation with no executable plan stays pending and holds no claim.
func TestRunnerSkipsAnOperationWithoutAPlan(t *testing.T) {
	inst := newInstallation(t)
	inst.connect(t, answer)
	admission := inst.admit(t, "db")
	ctx, cancel := context.WithCancel(t.Context())
	stopped := make(chan error, 1)
	go func() { stopped <- inst.runner.Run(ctx) }()
	time.Sleep(2 * time.Second)
	if inst.count(t, `SELECT count(*) FROM operations WHERE id=$1 AND status='pending'`, admission.OperationID) != 1 {
		t.Fatal("the runner changed an operation with no plan")
	}
	if inst.count(t, `SELECT count(*) FROM execution_claims`) != 0 {
		t.Fatal("the runner claimed an operation with no plan")
	}
	cancel()
	<-stopped
}

// Claim evidence with no owner must not close the agent stream.
// The transport turns any handler error into a disconnect, and a late barrier is ordinary.
func TestRunnerDropsEvidenceWithNoOwner(t *testing.T) {
	inst := newInstallation(t)
	peer := controlstream.Peer{InstallationID: inst.id, NodeID: inst.node, SessionID: uuid.NewString()}
	epoch := &agentv1.Epoch{InstallationId: inst.id.String(), NodeId: inst.node.String(), ClusterId: uuid.NewString(), RecoveryGeneration: uuid.NewString(), ExecutionEpoch: 1}
	for name, message := range map[string]*agentv1.AgentMessage{
		"barrier":   {Message: &agentv1.AgentMessage_BarrierResult{BarrierResult: &agentv1.BarrierResult{Epoch: epoch, AdmissionsClosed: true, Drained: true}}},
		"admission": {Message: &agentv1.AgentMessage_AdmissionAck{AdmissionAck: &agentv1.AdmissionAck{Epoch: epoch, CommandId: uuid.NewString(), State: "queued", InputHash: make([]byte, 32)}}},
	} {
		if err := inst.runner.Handle(t.Context(), peer, message); err != nil {
			t.Fatalf("%s closed the agent stream: %v", name, err)
		}
	}
	// Dropping is not committing. No claim evidence may reach the store.
	if inst.count(t, `SELECT count(*) FROM command_admissions`) != 0 {
		t.Fatal("evidence with no owner reached the store")
	}
	// A reserved cluster has no worker yet and must not receive evidence either.
	clusterID := uuid.MustParse(epoch.ClusterId)
	if !inst.runner.reserve(clusterID) {
		t.Fatal("the cluster was already reserved")
	}
	barrier := &agentv1.AgentMessage{Message: &agentv1.AgentMessage_BarrierResult{BarrierResult: &agentv1.BarrierResult{Epoch: epoch, AdmissionsClosed: true, Drained: true}}}
	if err := inst.runner.Handle(t.Context(), peer, barrier); err != nil {
		t.Fatalf("a reserved cluster closed the agent stream: %v", err)
	}
	inst.runner.release(clusterID)
}

// A receipt outlives its worker. The receiver keeps it durable with no claim context.
func TestRunnerCommitsALateReceipt(t *testing.T) {
	inst := newInstallation(t)
	inst.connect(t, answer)
	admission := inst.admit(t, "db")
	plan := inst.install(t, admission)
	command := plan.Steps[0].Command
	ctx, cancel := context.WithCancel(t.Context())
	stopped := make(chan error, 1)
	go func() { stopped <- inst.runner.Run(ctx) }()
	inst.waitFor(t, func() bool {
		return inst.count(t, `SELECT count(*) FROM operations WHERE id=$1 AND status='succeeded'`, admission.OperationID) == 1
	}, "the runner did not finish the operation")
	cancel()
	<-stopped
	// No worker owns the cluster now. The same receipt must still commit.
	peer := controlstream.Peer{InstallationID: inst.id, NodeID: inst.node, SessionID: uuid.NewString()}
	epoch := proto.Clone(command.Epoch).(*agentv1.Epoch)
	epoch.ExecutionEpoch = 2
	epoch.RecoveryGeneration = mustGeneration(t, inst)
	receipt := &agentv1.TerminalResult{
		Epoch: epoch, CommandId: command.CommandId, InputHash: command.InputHash, Sequence: 1, State: "succeeded",
		Effects: []*agentv1.Effect{{CommandId: command.CommandId, ResourceId: command.Input.ResourceId, NativeOperationId: "docker-command:" + command.CommandId, State: "present"}},
	}
	message := &agentv1.AgentMessage{Message: &agentv1.AgentMessage_TerminalResult{TerminalResult: receipt}}
	if err := inst.runner.Handle(t.Context(), peer, message); err != nil {
		t.Fatalf("a late receipt was refused: %v", err)
	}
	if inst.count(t, `SELECT count(*) FROM command_results WHERE command_id=$1`, command.CommandId) != 1 {
		t.Fatal("the late receipt did not commit")
	}
}

func mustGeneration(t *testing.T, inst *installation) string {
	t.Helper()
	generation, err := inst.store.RecoveryGeneration(t.Context(), inst.id)
	if err != nil {
		t.Fatal(err)
	}
	return generation.String()
}

func TestNewRunnerChecksInputs(t *testing.T) {
	base := Config{
		Store: &postgres.Store{}, Transport: &controlstream.Server{}, Coordinator: &coordinator.Coordinator{},
		InstallationID: uuid.New(), OwnerID: "controller/1", Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	if _, err := New(base); err != nil {
		t.Fatal(err)
	}
	for name, change := range map[string]func(*Config){
		"no store":        func(c *Config) { c.Store = nil },
		"no transport":    func(c *Config) { c.Transport = nil },
		"no coordinator":  func(c *Config) { c.Coordinator = nil },
		"no installation": func(c *Config) { c.InstallationID = uuid.Nil },
		"no owner":        func(c *Config) { c.OwnerID = "" },
		"no logger":       func(c *Config) { c.Logger = nil },
		"short claim":     func(c *Config) { c.ClaimTTL = time.Millisecond },
		"short lease":     func(c *Config) { c.LeaseSeconds = 1 },
		"no workers":      func(c *Config) { c.MaxWorkers = -1 },
		"fast scan":       func(c *Config) { c.ScanInterval = time.Millisecond },
	} {
		config := base
		change(&config)
		if _, err := New(config); err == nil {
			t.Fatalf("%s: accepted", name)
		}
	}
}
