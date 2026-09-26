package worker

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"net"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sirrobot01/synod/internal/cluster"
	"github.com/sirrobot01/synod/internal/controller/agentstream"
	"github.com/sirrobot01/synod/internal/controlstream"
	"github.com/sirrobot01/synod/internal/coordinator"
	"github.com/sirrobot01/synod/internal/operation"
	"github.com/sirrobot01/synod/internal/pki"
	"github.com/sirrobot01/synod/internal/placement"
	"github.com/sirrobot01/synod/internal/store/postgres"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

// staticAuthority stands in only where a test exercises no authority behaviour.
// Every test that depends on elected ownership uses a real *coordinator.Leadership.
type staticAuthority struct{}

func (staticAuthority) Verify(context.Context) error { return nil }

// testLeadership elects this test as the owner of one cluster.
func testLeadership(t *testing.T, pool *pgxpool.Pool, installation, clusterID uuid.UUID) *coordinator.Leadership {
	t.Helper()
	elector, err := coordinator.New(pool)
	if err != nil {
		t.Fatal(err)
	}
	leader, err := elector.Campaign(t.Context(), installation, clusterID, "owner-1", 3)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { leader.Close(context.Background()) })
	return leader
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
	name := "worker_" + strings.ReplaceAll(uuid.NewString(), "-", "")
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
	return pool
}

// harness runs the worker against real PostgreSQL and a real mutual TLS control stream.
type harness struct {
	store     *postgres.Store
	pool      *pgxpool.Pool
	worker    *Worker
	transport *controlstream.Server
	leader    *coordinator.Leadership
	claim     operation.Claim
	plan      *agentv1.ExecutionPlan
	node      uuid.UUID
	received  chan *agentv1.ControllerMessage
	address   string
	roots     *x509.CertPool
	ca        *pki.CA
	// agents holds each enrolled node's identity.
	agents     map[uuid.UUID]tls.Certificate
	controller tls.Certificate
}

// newHarness reserves one instance and connects one enrolled agent.
// Its plan checks the volume, starts the instance, and proves readiness in order.
func newHarness(t *testing.T, reply func(*agentv1.ControllerMessage) []*agentv1.AgentMessage, editPlan ...func(*agentv1.ExecutionPlan)) *harness {
	t.Helper()
	pool := testPool(t)
	store := postgres.New(pool)
	if err := store.Migrate(t.Context()); err != nil {
		t.Fatal(err)
	}
	installation, node := uuid.New(), uuid.New()
	admission, err := store.CreateCluster(t.Context(), postgres.CreateClusterRequest{
		Identity:  postgres.RequestIdentity{InstallationID: installation, ActorID: "actor-1", APIOperation: postgres.CreateClusterAPIOperation, IdempotencyKey: uuid.NewString()},
		RequestID: "request-1",
		Spec:      cluster.CreateSpec{SchemaVersion: 1, Name: "db", Engine: cluster.PostgreSQL, PostgreSQL: &cluster.PostgreSQLSpec{Version: "17.11", Topology: "standalone"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	generation := uuid.New()
	if err := store.InitializeGeneration(t.Context(), installation, generation); err != nil {
		t.Fatal(err)
	}
	leader := testLeadership(t, pool, installation, admission.ClusterID)
	claim, err := store.AcquireClaim(t.Context(), postgres.ClaimRequest{
		ClaimID: uuid.New(), InstallationID: installation, OperationID: admission.OperationID, RecoveryGeneration: generation,
		OwnerID: leader.OwnerID, TTL: 2 * time.Minute, LeaseID: leader.LeaseID, LeaseRevision: leader.Revision,
	})
	if err != nil {
		t.Fatal(err)
	}
	volume := harnessStep(t, claim, node, agentv1.Postcondition_POSTCONDITION_VOLUME_EXISTS)
	start := harnessStep(t, claim, node, agentv1.Postcondition_POSTCONDITION_CONTAINER_RUNNING, volume.Command.StepId)
	ready := harnessStep(t, claim, node, agentv1.Postcondition_POSTCONDITION_ENGINE_READY, start.Command.StepId)
	ready.Command.Input.ResourceId = start.Command.Input.ResourceId
	rehash(t, ready.Command)
	plan := &agentv1.ExecutionPlan{Version: 1, Steps: []*agentv1.PlanStep{volume, start, ready}}
	for _, edit := range editPlan {
		edit(plan)
	}
	if err := store.InstallPlan(t.Context(), claim, plan); err != nil {
		t.Fatal(err)
	}
	op, err := store.GetOperation(t.Context(), installation, admission.OperationID)
	if err != nil {
		t.Fatal(err)
	}
	h := &harness{store: store, pool: pool, leader: leader, claim: claim, plan: plan, node: node, received: make(chan *agentv1.ControllerMessage, 32)}
	h.transport = h.serve(t, installation, node)

	allocation := placement.Allocation{
		ID: uuid.New(), InstallationID: installation, NodeID: node, ClusterID: claim.ClusterID,
		ResourceID: uuid.MustParse(start.Command.Input.ResourceId), CreatedByOperation: admission.OperationID, CreatedRevision: 1,
		OwnedResourceIDs: []uuid.UUID{uuid.MustParse(start.Command.Input.ResourceId), uuid.MustParse(volume.Command.Input.ResourceId)},
		Budget:           placement.Vector{CPUMillis: 250, MemoryBytes: 64 << 20, DiskBytes: 1 << 30},
		Ports:            []placement.Port{{Address: "0.0.0.0", Transport: "tcp", Number: 5432}},
	}
	allocationBytes, err := json.Marshal(allocation)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(t.Context(), `INSERT INTO resource_allocations(id,installation_id,node_id,cluster_id,resource_id,created_operation_id,created_revision,allocation_bytes) VALUES($1,$2,$3,$4,$5,$6,$7,$8)`, allocation.ID, installation, node, claim.ClusterID, allocation.ResourceID, admission.OperationID, allocation.CreatedRevision, allocationBytes); err != nil {
		t.Fatal(err)
	}
	for _, resource := range allocation.OwnedResourceIDs {
		if _, err := pool.Exec(t.Context(), `INSERT INTO allocation_resources(installation_id,cluster_id,node_id,allocation_id,resource_id) VALUES($1,$2,$3,$4,$5)`, installation, claim.ClusterID, node, allocation.ID, resource); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := pool.Exec(t.Context(), `INSERT INTO allocation_ports(installation_id,cluster_id,node_id,allocation_id,address,transport,port,name) VALUES($1,$2,$3,$4,$5,$6,$7,'')`, installation, claim.ClusterID, node, allocation.ID, allocation.Ports[0].Address, allocation.Ports[0].Transport, allocation.Ports[0].Number); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(t.Context(), `INSERT INTO operation_allocation_bindings(installation_id,cluster_id,operation_id,allocation_id) VALUES($1,$2,$3,$4)`, installation, claim.ClusterID, admission.OperationID, allocation.ID); err != nil {
		t.Fatal(err)
	}
	h.worker, err = New(Config{Store: store, Transport: h.transport, Authority: h.leader, Claim: claim, Operation: op, EvidenceWait: 2 * time.Second, RetryDelay: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	h.connect(t, installation, node, reply)
	return h
}

func harnessStep(t *testing.T, claim operation.Claim, node uuid.UUID, postcondition agentv1.Postcondition, dependsOn ...string) *agentv1.PlanStep {
	t.Helper()
	step := planStep(t, operation.Operation{ID: claim.OperationID, InstallationID: claim.InstallationID, ClusterID: claim.ClusterID, DesiredRevision: 1}, node.String(), uuid.NewString(), postcondition, dependsOn...)
	step.Command.Epoch.RecoveryGeneration = claim.RecoveryGeneration.String()
	step.Command.Epoch.ExecutionEpoch = claim.ExecutionEpoch
	rehash(t, step.Command)
	return step
}

// serve starts a real control stream server and routes claim evidence to the worker.
func (h *harness) serve(t *testing.T, installation, node uuid.UUID) *controlstream.Server {
	t.Helper()
	h.issueIdentities(t, installation, node)
	receiver := agentstream.Receiver{Store: h.store}
	server, err := controlstream.New(controlstream.Config{
		InstallationID: installation,
		TLS:            &tls.Config{MinVersion: tls.VersionTLS12, ClientCAs: h.roots, Certificates: []tls.Certificate{h.controller}},
		Authorize:      h.store.CheckAgentCertificate,
		Handle: func(ctx context.Context, peer controlstream.Peer, message *agentv1.AgentMessage) error {
			switch message.Message.(type) {
			case *agentv1.AgentMessage_BarrierResult, *agentv1.AgentMessage_AdmissionAck, *agentv1.AgentMessage_TerminalResult, *agentv1.AgentMessage_CancellationResult:
				// The runner routes these four to the owning worker.
				return h.worker.Handle(ctx, peer, message)
			default:
				return receiver.Handle(ctx, peer, message)
			}
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
	h.address = listener.Addr().String()
	return server
}

// issueIdentities enrolls the agent and issues the controller certificate from one CA.
func (h *harness) issueIdentities(t *testing.T, installation, node uuid.UUID) {
	t.Helper()
	caCert, caKey, err := pki.CreateCA(installation, time.Now(), 7*24*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if h.ca, err = pki.LoadCA(installation, caCert, caKey); err != nil {
		t.Fatal(err)
	}
	h.roots = x509.NewCertPool()
	if !h.roots.AppendCertsFromPEM(caCert) {
		t.Fatal("invalid fixture CA")
	}
	private, csr := fixtureKey(t)
	leaf, err := h.ca.Issue(csr, pki.Identity{InstallationID: installation, Role: pki.Controller, SubjectID: uuid.New()}, []string{"127.0.0.1"}, time.Now(), time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	h.controller = fixturePair(t, leaf, private)
	h.enroll(t, installation, node)
}

// enroll registers a token for node, enrolls it, and keeps its agent identity.
func (h *harness) enroll(t *testing.T, installation, node uuid.UUID) {
	t.Helper()
	private, csr := fixtureKey(t)
	verifier := sha256.Sum256([]byte(uuid.NewString()))
	if _, err := h.store.RegisterEnrollmentToken(t.Context(), postgres.TokenRegistration{InstallationID: installation, NodeID: node, ActorID: "test-admin", RequestID: uuid.NewString(), IdempotencyKey: uuid.NewString(), TokenVerifier: verifier, TTLSeconds: 600}); err != nil {
		t.Fatal(err)
	}
	enrolled, err := h.store.EnrollAgent(t.Context(), postgres.EnrollmentRequest{InstallationID: installation, NodeID: node, RequestID: uuid.New(), TokenVerifier: verifier, CSRDER: csr}, h.ca)
	if err != nil {
		t.Fatal(err)
	}
	if h.agents == nil {
		h.agents = map[uuid.UUID]tls.Certificate{}
	}
	h.agents[node] = fixturePair(t, []byte(enrolled.CertificatePEM), private)
}

// fixtureKey creates one Ed25519 key and its certificate request.
func fixtureKey(t *testing.T) (ed25519.PrivateKey, []byte) {
	t.Helper()
	_, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	csr, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{}, private)
	if err != nil {
		t.Fatal(err)
	}
	return private, csr
}

// fixturePair joins a leaf certificate and its key.
func fixturePair(t *testing.T, leaf []byte, private ed25519.PrivateKey) tls.Certificate {
	t.Helper()
	key, err := x509.MarshalPKCS8PrivateKey(private)
	if err != nil {
		t.Fatal(err)
	}
	pair, err := tls.X509KeyPair(leaf, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: key}))
	if err != nil {
		t.Fatal(err)
	}
	return pair
}

// connect runs one scripted agent that answers controller messages over the real stream.
func (h *harness) connect(t *testing.T, installation, node uuid.UUID, reply func(*agentv1.ControllerMessage) []*agentv1.AgentMessage) {
	t.Helper()
	connection, err := grpc.NewClient(h.address, grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS12, RootCAs: h.roots, Certificates: []tls.Certificate{h.agents[node]}})))
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
	hello := &agentv1.Hello{InstallationId: installation.String(), NodeId: node.String(), BootId: uuid.NewString(), JournalGeneration: uuid.NewString(), Protocol: &agentv1.VersionRange{Minimum: 1, Maximum: 1}, ActivePayloadVersions: []uint32{1}}
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
			select {
			case h.received <- message:
			default:
			}
			for _, answer := range reply(message) {
				if stream.Send(answer) != nil {
					return
				}
			}
		}
	}()
}

// agentScript decides how the scripted agent answers each controller message.
type agentScript struct {
	// drained reports a barrier with no residual effect.
	drained bool
	// admit acknowledges a command before any receipt.
	admit bool
	// reject is the agent error code that refuses a command. It excludes admit.
	reject string
	// receipt is "", "succeeded", "unproved", "failed", or "unknown".
	receipt string
	// cancel is the answer to a CancelCommand: "" loses the reply, "cancelled"
	// stops a queued command, and "not_cancellable" reports work already begun.
	cancel string
	// cancelEffect leaves a native effect active on the cancellation receipt.
	cancelEffect bool
}

// answer plays one scripted reply over the real stream.
func (s agentScript) answer(message *agentv1.ControllerMessage) []*agentv1.AgentMessage {
	if prepare := message.GetPrepareEpoch(); prepare != nil {
		report := &agentv1.BarrierResult{Epoch: prepare.Epoch, AdmissionsClosed: true, Drained: s.drained}
		if !s.drained {
			report.Effects = []*agentv1.Effect{{CommandId: uuid.NewString(), ResourceId: uuid.NewString(), NativeOperationId: "restore-1", Active: true, State: "running"}}
		}
		return []*agentv1.AgentMessage{{Message: &agentv1.AgentMessage_BarrierResult{BarrierResult: report}}}
	}
	if request := message.GetCancel(); request != nil {
		if s.cancel == "" {
			// A lost reply. The controller must not assume anything from silence.
			return nil
		}
		answers := []*agentv1.AgentMessage{{Message: &agentv1.AgentMessage_CancellationResult{
			CancellationResult: &agentv1.CancellationResult{Epoch: request.Epoch, CommandId: request.CommandId, State: s.cancel},
		}}}
		if receipt := s.cancelReceipt(request); s.cancel == "cancelled" && receipt != nil {
			// A stopped command still reports its own durable receipt. The answer
			// above is the acknowledgement; this is the proof.
			answers = append(answers, &agentv1.AgentMessage{Message: &agentv1.AgentMessage_TerminalResult{TerminalResult: receipt}})
		}
		return answers
	}
	command := message.GetCommand()
	if command == nil {
		return nil
	}
	var answers []*agentv1.AgentMessage
	if s.reject != "" {
		ack := &agentv1.AdmissionAck{DeliveryAttemptId: message.DeliveryAttemptId, Epoch: command.Epoch, CommandId: command.CommandId, State: "rejected", ErrorCode: s.reject, InputHash: command.InputHash}
		return []*agentv1.AgentMessage{{Message: &agentv1.AgentMessage_AdmissionAck{AdmissionAck: ack}}}
	}
	recordDispatch(command)
	if s.admit {
		ack := &agentv1.AdmissionAck{DeliveryAttemptId: message.DeliveryAttemptId, Epoch: command.Epoch, CommandId: command.CommandId, State: "queued", InputHash: command.InputHash}
		answers = append(answers, &agentv1.AgentMessage{Message: &agentv1.AgentMessage_AdmissionAck{AdmissionAck: ack}})
	}
	if receipt := s.terminal(command); receipt != nil {
		answers = append(answers, &agentv1.AgentMessage{Message: &agentv1.AgentMessage_TerminalResult{TerminalResult: receipt}})
	}
	return answers
}

// cancelReceipt builds the durable proof that one command stopped.
// The harness holds the dispatched commands, so the receipt reuses their identity.
func (s agentScript) cancelReceipt(request *agentv1.CancelCommand) *agentv1.TerminalResult {
	found, ok := dispatched.Load(request.CommandId)
	if !ok {
		return nil
	}
	command := found.(*agentv1.Command)
	effect := &agentv1.Effect{CommandId: request.CommandId, ResourceId: command.Input.ResourceId, NativeOperationId: "docker-command:" + request.CommandId, State: "absent", Active: s.cancelEffect}
	return &agentv1.TerminalResult{
		Epoch: request.Epoch, CommandId: request.CommandId, InputHash: command.InputHash,
		Sequence: 1, State: "cancelled", Effects: []*agentv1.Effect{effect},
	}
}

// dispatched records every command the controller sent, so a cancellation answer
// can reuse the exact input hash and resource that the dispatch carried. The
// scripted agent runs on the stream goroutine, so the map carries its own lock.
var dispatched sync.Map

func recordDispatch(command *agentv1.Command) { dispatched.Store(command.CommandId, command) }

// terminal builds the receipt that this script reports for one command.
func (s agentScript) terminal(command *agentv1.Command) *agentv1.TerminalResult {
	if s.receipt == "" {
		return nil
	}
	effect := &agentv1.Effect{CommandId: command.CommandId, ResourceId: command.Input.ResourceId, NativeOperationId: "docker-command:" + command.CommandId, State: nativeState(command)}
	receipt := &agentv1.TerminalResult{Epoch: command.Epoch, CommandId: command.CommandId, InputHash: command.InputHash, Sequence: 1, State: "succeeded"}
	switch s.receipt {
	case "unproved":
		effect.State = "absent"
	case "failed":
		receipt.State, receipt.FailureCode, effect.State = "failed", "container_start_failed", "exited"
	case "unknown":
		receipt.State, receipt.FailureCode = "failed", "outcome_unknown"
		effect.OutcomeUnknown = true
	}
	receipt.Effects = []*agentv1.Effect{effect}
	return receipt
}

// nativeState is the Docker state that a finished command leaves behind.
func nativeState(command *agentv1.Command) string {
	switch command.Input.Payload.(type) {
	case *agentv1.CommandInput_EnsureVolume, *agentv1.CommandInput_EnsureNetwork:
		return "present"
	case *agentv1.CommandInput_StartContainer:
		return "running"
	case *agentv1.CommandInput_RemoveContainer:
		return "absent"
	case *agentv1.CommandInput_CheckReadiness:
		return "ready"
	default:
		return "created"
	}
}

// receive commits one agent receipt for a step without the scripted agent.
func (h *harness) receive(t *testing.T, command *agentv1.Command, state, failure string, unknown bool) {
	t.Helper()
	effect := &agentv1.Effect{CommandId: command.CommandId, ResourceId: command.Input.ResourceId, NativeOperationId: "docker-command:" + command.CommandId, State: "present", OutcomeUnknown: unknown}
	receipt := &agentv1.TerminalResult{
		Epoch:     &agentv1.Epoch{InstallationId: h.claim.InstallationID.String(), NodeId: h.node.String(), ClusterId: h.claim.ClusterID.String(), RecoveryGeneration: h.claim.RecoveryGeneration.String(), ExecutionEpoch: h.claim.ExecutionEpoch},
		CommandId: command.CommandId, InputHash: command.InputHash, Sequence: 1, State: state, FailureCode: failure, Effects: []*agentv1.Effect{effect},
	}
	if err := h.store.RecordAgentResult(t.Context(), h.claim.InstallationID, h.node, receipt); err != nil {
		t.Fatal(err)
	}
}

// commands drains the recorded controller messages and returns the command IDs in order.
func (h *harness) commands() []string {
	var ids []string
	for {
		select {
		case message := <-h.received:
			if command := message.GetCommand(); command != nil {
				ids = append(ids, command.CommandId)
			}
		default:
			return ids
		}
	}
}

// awaitAdmission waits for one durable admission acknowledgement.
//
// The scripted agent answers over a real stream, so its ack commits after the
// dispatch returns. A cancellation is sent only to the session that admitted the
// command, so a test that requests one before the ack lands is racing its own
// fixture rather than testing the worker.
func (h *harness) awaitAdmission(t *testing.T, commandID string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if h.count(t, `SELECT count(*) FROM command_admissions WHERE command_id=$1 AND accepted_session_id IS NOT NULL`, commandID) == 1 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("no durable admission for command %s", commandID)
}

// awaitReceipt waits for one durable agent receipt. The stream is asynchronous,
// so a test that depends on committed evidence must wait for the commit itself.
func (h *harness) awaitReceipt(t *testing.T, commandID string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if h.count(t, `SELECT count(*) FROM command_results WHERE command_id=$1`, commandID) == 1 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("no durable receipt for command %s", commandID)
}

// cancelled reports whether the controller asked an agent to stop one command.
func (h *harness) cancelled(commandID string) bool { return h.cancelCount(commandID) > 0 }

// cancelCount counts the cancel requests the controller sent for one command.
// The harness records every controller message, so a resend would be visible.
func (h *harness) cancelCount(commandID string) int {
	total := 0
	for {
		select {
		case message := <-h.received:
			if request := message.GetCancel(); request != nil && request.CommandId == commandID {
				total++
			}
		default:
			return total
		}
	}
}

func (h *harness) count(t *testing.T, query string, args ...any) int {
	t.Helper()
	var total int
	if err := h.pool.QueryRow(t.Context(), query, args...).Scan(&total); err != nil {
		t.Fatal(err)
	}
	return total
}

// step drives one worker pass without waiting for evidence that this test does not script.
func (h *harness) step(t *testing.T) (postgres.ExecutionState, int) {
	t.Helper()
	state, completed, err := h.worker.resume(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if err := h.worker.checkpoint(t.Context(), state, completed); err != nil {
		t.Fatal(err)
	}
	pass, err := h.worker.dispatch(t.Context(), state, completed)
	if err != nil {
		t.Fatal(err)
	}
	return state, pass.committed
}

func TestWorkerPreparesAndDispatchesReadyStepsOnly(t *testing.T) {
	h := newHarness(t, agentScript{drained: true, admit: true}.answer)
	if err := h.worker.Prepare(t.Context()); err != nil {
		t.Fatal(err)
	}
	if h.count(t, `SELECT count(*) FROM claim_barriers WHERE claim_id=$1 AND reconciled_at IS NOT NULL`, h.claim.ID) != 1 {
		t.Fatal("the node was not reconciled")
	}
	if h.count(t, `SELECT count(*) FROM operations WHERE id=$1 AND claim_active AND status='running'`, h.claim.OperationID) != 1 {
		t.Fatal("the claim did not activate")
	}
	if _, sent := h.step(t); sent != 1 {
		t.Fatalf("sent=%d, want the first step alone", sent)
	}
	if h.count(t, `SELECT count(*) FROM claim_recovery_checkpoints WHERE claim_id=$1`, h.claim.ID) != 1 {
		t.Fatal("the recovery frontier was not stored")
	}
	// The second step depends on the first, which no receipt has proved.
	if total := h.count(t, `SELECT count(*) FROM command_dispatches WHERE operation_id=$1`, h.claim.OperationID); total != 1 {
		t.Fatalf("dispatches=%d, want the first step alone", total)
	}
	if h.count(t, `SELECT count(*) FROM command_dispatches WHERE command_id=$1`, h.plan.Steps[0].Command.CommandId) != 1 {
		t.Fatal("the dispatched command is not the first step")
	}
	if h.count(t, `SELECT count(*) FROM command_delivery_attempts WHERE claim_id=$1`, h.claim.ID) != 1 {
		t.Fatal("the delivery budget was not committed")
	}
	waitFor(t, func() bool {
		return h.count(t, `SELECT count(*) FROM command_admissions WHERE accepted_bytes IS NOT NULL`) == 1
	}, "the agent acknowledgement did not commit")

	// A second pass must not repeat a dispatch that the agent already acknowledged.
	if _, sent := h.step(t); sent != 0 {
		t.Fatalf("sent=%d, want no repeat for an acknowledged command", sent)
	}
	if total := h.count(t, `SELECT count(*) FROM command_delivery_attempts WHERE claim_id=$1`, h.claim.ID); total != 1 {
		t.Fatalf("attempts=%d, want no repeat for an acknowledged command", total)
	}
}

// This boundary test runs the whole plan over real PostgreSQL and real mutual TLS.
func TestWorkerRunsPlanToSuccess(t *testing.T) {
	h := newHarness(t, agentScript{drained: true, admit: true, receipt: "succeeded"}.answer)
	if err := h.worker.Run(t.Context()); err != nil {
		t.Fatal(err)
	}
	if h.count(t, `SELECT count(*) FROM operations WHERE id=$1 AND status='succeeded' AND NOT claim_active AND finished_at IS NOT NULL AND failure_code IS NULL`, h.claim.OperationID) != 1 {
		t.Fatal("the operation did not finish")
	}
	if total := h.count(t, `SELECT count(*) FROM operation_step_completions WHERE operation_id=$1`, h.claim.OperationID); total != len(h.plan.Steps) {
		t.Fatalf("completions=%d, want one for each step", total)
	}
	if total := h.count(t, `SELECT count(*) FROM command_dispatches WHERE operation_id=$1`, h.claim.OperationID); total != len(h.plan.Steps) {
		t.Fatalf("dispatches=%d, want one for each step", total)
	}
	if h.count(t, `SELECT count(*) FROM operation_progress WHERE operation_id=$1 AND state='succeeded'`, h.claim.OperationID) != 1 {
		t.Fatal("the terminal status has no progress record")
	}
	if h.count(t, `SELECT count(*) FROM clusters WHERE id=$1 AND observed_revision=1 AND observed_lifecycle='running'`, h.claim.ClusterID) != 1 {
		t.Fatal("success did not commit the proved cluster lifecycle")
	}
	if h.count(t, `SELECT count(*) FROM resource_allocations WHERE cluster_id=$1 AND released_at IS NULL`, h.claim.ClusterID) != 1 {
		t.Fatal("create success released the live reservation")
	}
	// A dependent step must not reach the agent before its dependency is proved.
	if sent := h.commands(); !slices.Equal(sent, []string{h.plan.Steps[0].Command.CommandId, h.plan.Steps[1].Command.CommandId, h.plan.Steps[2].Command.CommandId}) {
		t.Fatalf("command order %v", sent)
	}
	// A finished operation is immutable. A repeated run must change nothing.
	if err := h.worker.Run(t.Context()); err != nil {
		t.Fatal(err)
	}
	if total := h.count(t, `SELECT count(*) FROM operation_progress WHERE operation_id=$1`, h.claim.OperationID); total != len(h.plan.Steps)+1 {
		t.Fatalf("progress rows=%d, want one running record per step and one terminal record", total)
	}
}

func TestWorkerFailsOnUnprovedPostcondition(t *testing.T) {
	h := newHarness(t, agentScript{drained: true, admit: true, receipt: "unproved"}.answer)
	if err := h.worker.Run(t.Context()); err != nil {
		t.Fatal(err)
	}
	if h.count(t, `SELECT count(*) FROM operations WHERE id=$1 AND status='failed' AND failure_code='operation_failed' AND NOT claim_active`, h.claim.OperationID) != 1 {
		t.Fatal("a receipt that proved nothing did not fail the operation")
	}
	if h.count(t, `SELECT count(*) FROM operation_step_completions WHERE operation_id=$1`, h.claim.OperationID) != 0 {
		t.Fatal("an unproved postcondition was retained as a completion")
	}
	if h.count(t, `SELECT count(*) FROM operation_progress WHERE operation_id=$1 AND state='failed' AND reason LIKE 'postcondition_unproved:%'`, h.claim.OperationID) != 1 {
		t.Fatal("the failure reason does not name the unproved postcondition")
	}
}

func TestWorkerFailsOnFailedReceipt(t *testing.T) {
	h := newHarness(t, agentScript{drained: true, admit: true, receipt: "failed"}.answer)
	if err := h.worker.Run(t.Context()); err != nil {
		t.Fatal(err)
	}
	if h.count(t, `SELECT count(*) FROM operations WHERE id=$1 AND status='failed' AND finished_at IS NOT NULL AND failure_code='container_start_failed'`, h.claim.OperationID) != 1 {
		t.Fatal("a failed receipt did not fail the operation")
	}
	if h.count(t, `SELECT count(*) FROM command_dispatches WHERE command_id=$1`, h.plan.Steps[1].Command.CommandId) != 0 {
		t.Fatal("the worker dispatched past a failed dependency")
	}
	// The recorded reason must carry the failure code that the agent reported.
	if h.count(t, `SELECT count(*) FROM operation_progress WHERE operation_id=$1 AND state='failed' AND reason LIKE 'step_failed:%container_start_failed%'`, h.claim.OperationID) != 1 {
		t.Fatal("the failure reason lost the agent failure code")
	}
}

// An unresolved native effect outranks a failure. The operation waits for an operator.
func TestWorkerWaitsOnUnresolvedEffect(t *testing.T) {
	h := newHarness(t, agentScript{drained: true, admit: true, receipt: "unknown"}.answer)
	if err := h.worker.Run(t.Context()); err != nil {
		t.Fatal(err)
	}
	if h.count(t, `SELECT count(*) FROM operations WHERE id=$1 AND status='waiting' AND failure_code='outcome_unknown' AND claim_active AND finished_at IS NULL`, h.claim.OperationID) != 1 {
		t.Fatal("an unresolved effect did not leave the operation waiting")
	}
	if h.count(t, `SELECT count(*) FROM operation_step_completions WHERE operation_id=$1`, h.claim.OperationID) != 0 {
		t.Fatal("an unresolved effect was retained as a completion")
	}
	// A repeated run must not add a second waiting record for the same evidence.
	if err := h.worker.Run(t.Context()); err != nil {
		t.Fatal(err)
	}
	if total := h.count(t, `SELECT count(*) FROM operation_progress WHERE operation_id=$1`, h.claim.OperationID); total != 1 {
		t.Fatalf("progress rows=%d, want one waiting record", total)
	}
}

func TestWorkerCancelsBeforeDispatch(t *testing.T) {
	h := newHarness(t, agentScript{drained: true, admit: true, receipt: "succeeded"}.answer)
	if _, err := h.pool.Exec(t.Context(), `UPDATE operations SET cancellation_requested_at=clock_timestamp() WHERE id=$1`, h.claim.OperationID); err != nil {
		t.Fatal(err)
	}
	if err := h.worker.Run(t.Context()); err != nil {
		t.Fatal(err)
	}
	if h.count(t, `SELECT count(*) FROM operations WHERE id=$1 AND status='cancelled' AND NOT claim_active AND finished_at IS NOT NULL`, h.claim.OperationID) != 1 {
		t.Fatal("the request did not cancel the operation")
	}
	if h.count(t, `SELECT count(*) FROM command_dispatches`) != 0 {
		t.Fatal("cancellation did not stop forward dispatch")
	}
	if h.count(t, `SELECT count(*) FROM operation_progress WHERE operation_id=$1 AND state='cancelling'`, h.claim.OperationID) != 1 {
		t.Fatal("the worker cancelled without recording the transition")
	}
}

// Cancellation cannot finish work that an agent already holds.
func TestWorkerCancelsWithCommandInFlight(t *testing.T) {
	h := newHarness(t, agentScript{drained: true, admit: true}.answer)
	if err := h.worker.Prepare(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, sent := h.step(t); sent != 1 {
		t.Fatalf("sent=%d, want the first step", sent)
	}
	if _, err := h.pool.Exec(t.Context(), `UPDATE operations SET cancellation_requested_at=clock_timestamp() WHERE id=$1`, h.claim.OperationID); err != nil {
		t.Fatal(err)
	}
	if err := h.worker.Run(t.Context()); !errors.Is(err, ErrEvidencePending) {
		t.Fatalf("the worker cancelled over a command in flight: %v", err)
	}
	if h.count(t, `SELECT count(*) FROM operations WHERE id=$1 AND status='cancelling' AND finished_at IS NULL`, h.claim.OperationID) != 1 {
		t.Fatal("the worker did not record the cancellation transition")
	}
	if h.count(t, `SELECT count(*) FROM operation_progress WHERE operation_id=$1 AND state='cancelled'`, h.claim.OperationID) != 0 {
		t.Fatal("the worker finished cancellation over a command in flight")
	}
	// A repeated run must not record the transition twice.
	if err := h.worker.Run(t.Context()); !errors.Is(err, ErrEvidencePending) {
		t.Fatalf("second run: %v", err)
	}
	if total := h.count(t, `SELECT count(*) FROM operation_progress WHERE operation_id=$1`, h.claim.OperationID); total != 1 {
		t.Fatalf("progress rows=%d, want one cancelling record", total)
	}
}

// An operation that is cancelling cannot move to waiting, so it must not try.
func TestWorkerKeepsCancellingOnUnresolvedEffect(t *testing.T) {
	h := newHarness(t, agentScript{drained: true, admit: true}.answer)
	if err := h.worker.Prepare(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, sent := h.step(t); sent != 1 {
		t.Fatalf("committed=%d, want the first step", sent)
	}
	if _, err := h.pool.Exec(t.Context(), `UPDATE operations SET cancellation_requested_at=clock_timestamp() WHERE id=$1`, h.claim.OperationID); err != nil {
		t.Fatal(err)
	}
	if err := h.worker.Run(t.Context()); !errors.Is(err, ErrEvidencePending) {
		t.Fatalf("first cancellation pass: %v", err)
	}
	// The in-flight command now answers with an effect that nobody resolved.
	h.receive(t, h.plan.Steps[0].Command, "failed", "outcome_unknown", true)
	for range 2 {
		err := h.worker.Run(t.Context())
		if !errors.Is(err, ErrEvidencePending) || !strings.Contains(err.Error(), "effect_unresolved") {
			t.Fatalf("cancelling with an unresolved effect: %v", err)
		}
	}
	if h.count(t, `SELECT count(*) FROM operations WHERE id=$1 AND status='cancelling' AND finished_at IS NULL`, h.claim.OperationID) != 1 {
		t.Fatal("an unresolved effect moved a cancelling operation")
	}
	if total := h.count(t, `SELECT count(*) FROM operation_progress WHERE operation_id=$1`, h.claim.OperationID); total != 1 {
		t.Fatalf("progress rows=%d, want one cancelling record", total)
	}
	if h.count(t, `SELECT count(*) FROM operation_step_completions WHERE operation_id=$1`, h.claim.OperationID) != 0 {
		t.Fatal("an unresolved effect was retained as a completion")
	}
}

// Retaining a receipt must not return a cancelling operation to running.
func TestWorkerCancelsAfterAProvedReceipt(t *testing.T) {
	h := newHarness(t, agentScript{drained: true, admit: true}.answer)
	if err := h.worker.Prepare(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, sent := h.step(t); sent != 1 {
		t.Fatalf("committed=%d, want the first step", sent)
	}
	if _, err := h.pool.Exec(t.Context(), `UPDATE operations SET cancellation_requested_at=clock_timestamp() WHERE id=$1`, h.claim.OperationID); err != nil {
		t.Fatal(err)
	}
	if err := h.worker.Run(t.Context()); !errors.Is(err, ErrEvidencePending) {
		t.Fatalf("first cancellation pass: %v", err)
	}
	h.receive(t, h.plan.Steps[0].Command, "succeeded", "", false)
	if err := h.worker.Run(t.Context()); err != nil {
		t.Fatal(err)
	}
	if h.count(t, `SELECT count(*) FROM operations WHERE id=$1 AND status='cancelled' AND finished_at IS NOT NULL`, h.claim.OperationID) != 1 {
		t.Fatal("cancellation did not finish after the last command answered")
	}
	if h.count(t, `SELECT count(*) FROM operation_step_completions WHERE operation_id=$1`, h.claim.OperationID) != 1 {
		t.Fatal("the proved receipt was not retained")
	}
	if h.count(t, `SELECT count(*) FROM operation_progress WHERE operation_id=$1 AND state='cancelling' AND reason LIKE 'steps_proved:%'`, h.claim.OperationID) != 1 {
		t.Fatal("retaining a receipt did not record progress under cancellation")
	}
	if h.count(t, `SELECT count(*) FROM operation_progress WHERE operation_id=$1 AND state='running'`, h.claim.OperationID) != 0 {
		t.Fatal("a cancelling operation returned to running")
	}
}

// busyTransport reports a full send queue. It keeps the real session lookup.
type busyTransport struct{ Transport }

func (busyTransport) Send(uuid.UUID, string, *agentv1.ControllerMessage) error {
	return controlstream.ErrBusy
}

// A full transport queue is backpressure, not missing evidence.
func TestWorkerDefersOnTransportBackpressure(t *testing.T) {
	h := newHarness(t, agentScript{drained: true, admit: true}.answer)
	if err := h.worker.Prepare(t.Context()); err != nil {
		t.Fatal(err)
	}
	state, completed, err := h.worker.resume(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if err := h.worker.checkpoint(t.Context(), state, completed); err != nil {
		t.Fatal(err)
	}
	busy, err := New(Config{
		Store: h.store, Transport: busyTransport{h.transport}, Authority: h.leader,
		Claim: h.claim, Operation: h.worker.config.Operation, EvidenceWait: 2 * time.Second, RetryDelay: 10 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	pass, err := busy.dispatch(t.Context(), state, completed)
	if err != nil {
		t.Fatalf("backpressure ended the pass: %v", err)
	}
	if pass.committed != 1 {
		t.Fatalf("committed=%d, want the committed dispatch counted", pass.committed)
	}
	if pass.retryAt.IsZero() {
		t.Fatal("backpressure did not reserve a retry")
	}
	// The dispatch is durable even though the command did not leave the controller.
	if h.count(t, `SELECT count(*) FROM command_dispatches WHERE command_id=$1`, h.plan.Steps[0].Command.CommandId) != 1 {
		t.Fatal("the worker sent before it committed the dispatch")
	}
}

// A reserved retry must bound the wait. An evidence deadline must not swallow it.
func TestAwaitWakesForAReservedRetry(t *testing.T) {
	claim := operation.Claim{ID: uuid.New(), InstallationID: uuid.New(), ClusterID: uuid.New(), OperationID: uuid.New(), RecoveryGeneration: uuid.New(), OwnerID: "owner-1", ExecutionEpoch: 1}
	w, err := New(Config{
		Store: &postgres.Store{}, Transport: &controlstream.Server{}, Authority: staticAuthority{}, Claim: claim,
		Operation:    operation.Operation{ID: claim.OperationID, InstallationID: claim.InstallationID, ClusterID: claim.ClusterID},
		EvidenceWait: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	if err := w.await(t.Context(), w.subscribe(), time.Now().Add(20*time.Millisecond)); err != nil {
		t.Fatalf("a reserved retry did not wake the loop: %v", err)
	}
	if time.Since(start) > 10*time.Second {
		t.Fatal("the retry wait used the evidence deadline")
	}
	if err := w.await(t.Context(), w.subscribe(), time.Now().Add(-time.Second)); err != nil {
		t.Fatalf("a due retry did not wake the loop: %v", err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := w.await(ctx, w.subscribe(), time.Time{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("a cancelled context: %v", err)
	}
}

// A missing reply is not an outcome. The worker stops and leaves the operation Running.
func TestWorkerStopsWithoutTerminalReceipt(t *testing.T) {
	h := newHarness(t, agentScript{drained: true, admit: true}.answer)
	if err := h.worker.Run(t.Context()); !errors.Is(err, ErrEvidencePending) {
		t.Fatalf("the worker concluded without a receipt: %v", err)
	}
	if h.count(t, `SELECT count(*) FROM operations WHERE id=$1 AND status='running' AND finished_at IS NULL AND failure_code IS NULL`, h.claim.OperationID) != 1 {
		t.Fatal("a missing reply changed the operation status")
	}
	if h.count(t, `SELECT count(*) FROM operation_step_completions WHERE operation_id=$1`, h.claim.OperationID) != 0 {
		t.Fatal("a missing reply was retained as a completion")
	}
}

func TestWorkerFailsWhenDeliveryAttemptsRunOut(t *testing.T) {
	h := newHarness(t, agentScript{drained: true, reject: "unsupported_payload"}.answer)
	if err := h.worker.Prepare(t.Context()); err != nil {
		t.Fatal(err)
	}
	state, completed, err := h.worker.resume(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if err := h.worker.checkpoint(t.Context(), state, completed); err != nil {
		t.Fatal(err)
	}
	budget := int(h.plan.Steps[0].MaxAttempts)
	for attempt := range budget {
		if _, err := h.worker.dispatch(t.Context(), state, completed); err != nil {
			t.Fatal(err)
		}
		// Reload so the next pass sees the attempt that the last one reserved.
		if state, completed, err = h.worker.resume(t.Context()); err != nil {
			t.Fatal(err)
		}
		if attempt == 0 {
			// Hold the reserved retry far out, so the deferral is deterministic.
			if _, err := h.pool.Exec(t.Context(), `UPDATE command_delivery_attempts SET next_attempt_at=clock_timestamp()+interval '1 hour'`); err != nil {
				t.Fatal(err)
			}
			if state, completed, err = h.worker.resume(t.Context()); err != nil {
				t.Fatal(err)
			}
			// A pass inside the reserved delay defers instead of spending an attempt.
			deferred, err := h.worker.dispatch(t.Context(), state, completed)
			if err != nil {
				t.Fatal(err)
			}
			// The wake time must come from the stored attempt, not from a fresh delay.
			if !deferred.retryAt.After(time.Now().Add(30 * time.Minute)) {
				t.Fatalf("retryAt=%v, want the reserved attempt time", deferred.retryAt)
			}
			if total := h.count(t, `SELECT count(*) FROM command_delivery_attempts`); total != 1 {
				t.Fatalf("attempts=%d, want the deferred pass to spend none", total)
			}
		}
		if _, err := h.pool.Exec(t.Context(), `UPDATE command_delivery_attempts SET next_attempt_at=clock_timestamp()-interval '1 second'`); err != nil {
			t.Fatal(err)
		}
	}
	if total := h.count(t, `SELECT count(*) FROM command_delivery_attempts`); total != budget {
		t.Fatalf("attempts=%d, want the whole budget", total)
	}
	waitFor(t, func() bool {
		return h.count(t, `SELECT count(*) FROM command_admissions WHERE rejection_bytes IS NOT NULL`) == 1
	}, "the agent refusal did not commit")
	if state, completed, err = h.worker.resume(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := h.worker.dispatch(t.Context(), state, completed); !errors.Is(err, postgres.ErrAttemptsExhausted) {
		t.Fatalf("the worker sent past its budget: %v", err)
	}
	if err := h.worker.Run(t.Context()); err != nil {
		t.Fatal(err)
	}
	if h.count(t, `SELECT count(*) FROM operations WHERE id=$1 AND status='failed' AND finished_at IS NOT NULL`, h.claim.OperationID) != 1 {
		t.Fatal("an exhausted delivery budget did not fail the operation")
	}
	if h.count(t, `SELECT count(*) FROM operation_progress WHERE operation_id=$1 AND state='failed' AND reason LIKE 'attempts_exhausted:%delivery budget%'`, h.claim.OperationID) != 1 {
		t.Fatal("the failure reason does not name the delivery budget")
	}
	// The reason must keep the agent error code that explains the refusal.
	if h.count(t, `SELECT count(*) FROM operation_progress WHERE operation_id=$1 AND reason LIKE '%unsupported_payload%'`, h.claim.OperationID) != 1 {
		t.Fatal("the failure reason discarded the agent refusal code")
	}
}

// A revoked cluster lease ends the run. Operation claim ownership alone is not enough.
func TestWorkerStopsWithoutElectedAuthority(t *testing.T) {
	h := newHarness(t, agentScript{drained: true, admit: true}.answer)
	if err := h.leader.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := h.worker.Run(t.Context()); !errors.Is(err, coordinator.ErrNotLeader) {
		t.Fatalf("the worker ran without elected authority: %v", err)
	}
	if h.count(t, `SELECT count(*) FROM command_dispatches`) != 0 || h.count(t, `SELECT count(*) FROM operations WHERE claim_active`) != 0 {
		t.Fatal("the worker changed durable state without authority")
	}
}

// A revoked lease must also stop durable writes that no dispatch guards.
func TestWorkerRetainsNoReceiptWithoutTheLease(t *testing.T) {
	h := newHarness(t, agentScript{drained: true, admit: true}.answer)
	if err := h.worker.Prepare(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, sent := h.step(t); sent != 1 {
		t.Fatalf("committed=%d, want the first step", sent)
	}
	h.receive(t, h.plan.Steps[0].Command, "succeeded", "", false)
	if err := h.leader.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := h.worker.Run(t.Context()); !errors.Is(err, coordinator.ErrNotLeader) {
		t.Fatalf("a revoked lease kept reconciling: %v", err)
	}
	if h.count(t, `SELECT count(*) FROM operation_step_completions WHERE operation_id=$1`, h.claim.OperationID) != 0 {
		t.Fatal("a revoked lease retained a receipt")
	}
	if h.count(t, `SELECT count(*) FROM operation_progress WHERE operation_id=$1`, h.claim.OperationID) != 0 {
		t.Fatal("a revoked lease recorded progress")
	}
}

// Losing the lease part way through must stop forward dispatch.
func TestWorkerStopsDispatchWhenTheLeaseIsRevoked(t *testing.T) {
	h := newHarness(t, agentScript{drained: true, admit: true}.answer)
	if err := h.worker.Prepare(t.Context()); err != nil {
		t.Fatal(err)
	}
	state, completed, err := h.worker.resume(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if err := h.leader.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := h.worker.dispatch(t.Context(), state, completed); !errors.Is(err, coordinator.ErrNotLeader) {
		t.Fatalf("a revoked lease still dispatched: %v", err)
	}
	if h.count(t, `SELECT count(*) FROM command_dispatches`) != 0 {
		t.Fatal("a revoked lease committed a dispatch")
	}
}

func TestWorkerStopsAfterClaimTakeover(t *testing.T) {
	h := newHarness(t, agentScript{drained: true, admit: true}.answer)
	if err := h.worker.Prepare(t.Context()); err != nil {
		t.Fatal(err)
	}
	// Hold state that the worker read before the replacement owner arrived.
	state, completed, err := h.worker.resume(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.pool.Exec(t.Context(), `UPDATE operations SET claim_expires_at=clock_timestamp()-interval '1 second'`); err != nil {
		t.Fatal(err)
	}
	if _, err := h.store.AcquireClaim(t.Context(), postgres.ClaimRequest{
		ClaimID: uuid.New(), InstallationID: h.claim.InstallationID, OperationID: h.claim.OperationID, RecoveryGeneration: h.claim.RecoveryGeneration,
		OwnerID: "owner-2", TTL: time.Minute, LeaseID: 2, LeaseRevision: 2,
	}); err != nil {
		t.Fatal(err)
	}
	// A dispatch that started before the takeover must still fail on the durable claim.
	if _, err := h.worker.dispatch(t.Context(), state, completed); !errors.Is(err, operation.ErrStaleOwner) {
		t.Fatalf("the replaced owner dispatched: %v", err)
	}
	if err := h.worker.Run(t.Context()); !errors.Is(err, operation.ErrStaleOwner) {
		t.Fatalf("the replaced owner continued: %v", err)
	}
	if h.count(t, `SELECT count(*) FROM command_dispatches`) != 0 {
		t.Fatal("the replaced owner committed a dispatch")
	}
}

func TestWorkerRefusesActivationWithoutDrainEvidence(t *testing.T) {
	h := newHarness(t, agentScript{}.answer)
	err := h.worker.Run(t.Context())
	if !errors.Is(err, ErrEvidencePending) || !strings.Contains(err.Error(), h.node.String()) {
		t.Fatalf("activation without drain evidence: %v", err)
	}
	if h.count(t, `SELECT count(*) FROM operations WHERE claim_active`) != 0 {
		t.Fatal("an undrained node activated the claim")
	}
	if h.count(t, `SELECT count(*) FROM claim_barriers WHERE reconciled_at IS NOT NULL`) != 0 {
		t.Fatal("an active effect was reconciled")
	}
	if h.count(t, `SELECT count(*) FROM command_dispatches`) != 0 {
		t.Fatal("the worker dispatched before activation")
	}
}

func TestWorkerRejectsUnboundEvidence(t *testing.T) {
	h := newHarness(t, agentScript{drained: true, admit: true}.answer)
	report := &agentv1.BarrierResult{Epoch: h.plan.Steps[0].Command.Epoch, AdmissionsClosed: true, Drained: true}
	message := &agentv1.AgentMessage{Message: &agentv1.AgentMessage_BarrierResult{BarrierResult: report}}
	for name, peer := range map[string]controlstream.Peer{
		"no session":         {InstallationID: h.claim.InstallationID, NodeID: h.node},
		"other installation": {InstallationID: uuid.New(), NodeID: h.node, SessionID: uuid.NewString()},
	} {
		if err := h.worker.Handle(t.Context(), peer, message); !errors.Is(err, ErrUnboundEvidence) {
			t.Fatalf("%s: %v", name, err)
		}
	}
	peer := controlstream.Peer{InstallationID: h.claim.InstallationID, NodeID: h.node, SessionID: uuid.NewString()}
	if err := h.worker.Handle(t.Context(), peer, &agentv1.AgentMessage{Message: &agentv1.AgentMessage_Heartbeat{Heartbeat: &agentv1.Heartbeat{Sequence: 1}}}); !errors.Is(err, ErrUnboundEvidence) {
		t.Fatalf("worker committed an unclaimed message: %v", err)
	}
}

func TestNewWorkerChecksInputs(t *testing.T) {
	claim := operation.Claim{ID: uuid.New(), InstallationID: uuid.New(), ClusterID: uuid.New(), OperationID: uuid.New(), RecoveryGeneration: uuid.New(), OwnerID: "owner-1", ExecutionEpoch: 1}
	store, transport, authority := &postgres.Store{}, &controlstream.Server{}, staticAuthority{}
	op := operation.Operation{ID: claim.OperationID, InstallationID: claim.InstallationID, ClusterID: claim.ClusterID}
	if _, err := New(Config{Store: store, Transport: transport, Authority: authority, Claim: claim, Operation: op}); err != nil {
		t.Fatal(err)
	}
	for name, change := range map[string]func(*Config){
		"no store":         func(c *Config) { c.Store = nil },
		"no transport":     func(c *Config) { c.Transport = nil },
		"no authority":     func(c *Config) { c.Authority = nil },
		"no claim":         func(c *Config) { c.Claim.ID = uuid.Nil },
		"no epoch":         func(c *Config) { c.Claim.ExecutionEpoch = 0 },
		"no owner":         func(c *Config) { c.Claim.OwnerID = "" },
		"other operation":  func(c *Config) { c.Operation.ID = uuid.New() },
		"other cluster":    func(c *Config) { c.Operation.ClusterID = uuid.New() },
		"short wait":       func(c *Config) { c.EvidenceWait = time.Millisecond },
		"long retry":       func(c *Config) { c.RetryDelay = time.Hour },
		"long session age": func(c *Config) { c.SessionMaxAge = time.Hour },
	} {
		config := Config{Store: store, Transport: transport, Authority: authority, Claim: claim, Operation: op}
		change(&config)
		if _, err := New(config); err == nil {
			t.Fatalf("%s: accepted", name)
		}
	}
}

func waitFor(t *testing.T, done func() bool, message string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !done() {
		if time.Now().After(deadline) {
			t.Fatal(message)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// A cancellation must reach the agent that holds the command, and the operation
// must not finish until that agent's own receipt is durable.
func TestWorkerCancelsAnAdmittedCommand(t *testing.T) {
	h := newHarness(t, agentScript{drained: true, admit: true, cancel: "cancelled"}.answer)
	if err := h.worker.Prepare(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, sent := h.step(t); sent != 1 {
		t.Fatalf("sent=%d, want the first step", sent)
	}
	h.awaitAdmission(t, h.plan.Steps[0].Command.CommandId)
	if _, err := h.pool.Exec(t.Context(), `UPDATE operations SET cancellation_requested_at=clock_timestamp() WHERE id=$1`, h.claim.OperationID); err != nil {
		t.Fatal(err)
	}
	// The first pass sends the cancel and cannot finish: no receipt exists yet.
	if err := h.worker.Run(t.Context()); !errors.Is(err, ErrEvidencePending) {
		t.Fatalf("the worker finished before the agent answered: %v", err)
	}
	if !h.cancelled(h.plan.Steps[0].Command.CommandId) {
		t.Fatal("the worker never asked the holding agent to stop the command")
	}
	// The receipt is the proof, and it must be durable before the status changes.
	h.awaitReceipt(t, h.plan.Steps[0].Command.CommandId)
	if err := h.worker.Run(t.Context()); err != nil {
		t.Fatal(err)
	}
	if h.count(t, `SELECT count(*) FROM operations WHERE id=$1 AND status='cancelled' AND finished_at IS NOT NULL`, h.claim.OperationID) != 1 {
		t.Fatal("a cancelled command did not finish the cancellation")
	}
	// A stopped step proves no postcondition, so it is never retained as complete.
	if h.count(t, `SELECT count(*) FROM operation_step_completions WHERE operation_id=$1`, h.claim.OperationID) != 0 {
		t.Fatal("a cancelled step was retained as a completion")
	}
	// The second step was never dispatched, because cancellation stops forward work.
	if total := h.count(t, `SELECT count(*) FROM command_dispatches`); total != 1 {
		t.Fatalf("dispatches=%d, want only the step that was in flight", total)
	}
}

// An agent that has passed the boundary cannot stop. The operation must wait for
// the command's own result rather than claiming it stopped.
func TestWorkerKeepsCancellingWhenTheAgentCannotStop(t *testing.T) {
	h := newHarness(t, agentScript{drained: true, admit: true, cancel: "not_cancellable"}.answer)
	if err := h.worker.Prepare(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, sent := h.step(t); sent != 1 {
		t.Fatalf("sent=%d, want the first step", sent)
	}
	h.awaitAdmission(t, h.plan.Steps[0].Command.CommandId)
	if _, err := h.pool.Exec(t.Context(), `UPDATE operations SET cancellation_requested_at=clock_timestamp() WHERE id=$1`, h.claim.OperationID); err != nil {
		t.Fatal(err)
	}
	if err := h.worker.Run(t.Context()); !errors.Is(err, ErrEvidencePending) {
		t.Fatalf("first cancellation pass: %v", err)
	}
	// The refusal names the boundary, so an operator reads why it cannot stop.
	err := h.worker.Run(t.Context())
	if !errors.Is(err, ErrEvidencePending) || !strings.Contains(err.Error(), "boundary") {
		t.Fatalf("the refusal did not name the boundary: %v", err)
	}
	if h.count(t, `SELECT count(*) FROM operations WHERE id=$1 AND status='cancelling' AND finished_at IS NULL`, h.claim.OperationID) != 1 {
		t.Fatal("a refused cancellation moved the operation")
	}
	if h.count(t, `SELECT count(*) FROM operation_progress WHERE operation_id=$1 AND state='cancelled'`, h.claim.OperationID) != 0 {
		t.Fatal("the worker cancelled work the agent still holds")
	}
}

// Silence proves nothing. A lost answer must leave the operation cancelling, and
// the worker must not keep asking.
func TestWorkerSurvivesALostCancellationReply(t *testing.T) {
	h := newHarness(t, agentScript{drained: true, admit: true}.answer)
	if err := h.worker.Prepare(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, sent := h.step(t); sent != 1 {
		t.Fatalf("sent=%d, want the first step", sent)
	}
	h.awaitAdmission(t, h.plan.Steps[0].Command.CommandId)
	if _, err := h.pool.Exec(t.Context(), `UPDATE operations SET cancellation_requested_at=clock_timestamp() WHERE id=$1`, h.claim.OperationID); err != nil {
		t.Fatal(err)
	}
	for range 3 {
		if err := h.worker.Run(t.Context()); !errors.Is(err, ErrEvidencePending) {
			t.Fatalf("a lost reply finished the cancellation: %v", err)
		}
	}
	if total := h.cancelCount(h.plan.Steps[0].Command.CommandId); total != 1 {
		t.Fatalf("cancel requests=%d, want one; silence is not a reason to resend", total)
	}
	if h.count(t, `SELECT count(*) FROM operations WHERE id=$1 AND status='cancelling' AND finished_at IS NULL`, h.claim.OperationID) != 1 {
		t.Fatal("a lost reply moved the operation")
	}
	// One cancelling record, however many passes ran.
	if total := h.count(t, `SELECT count(*) FROM operation_progress WHERE operation_id=$1`, h.claim.OperationID); total != 1 {
		t.Fatalf("progress rows=%d, want one cancelling record", total)
	}
}

// A cancellation that leaves a native effect running is not a clean stop.
// The operation must stay cancelling and never claim the effect is gone.
func TestWorkerKeepsCancellingOnAnActiveEffect(t *testing.T) {
	h := newHarness(t, agentScript{drained: true, admit: true, cancel: "cancelled", cancelEffect: true}.answer)
	if err := h.worker.Prepare(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, sent := h.step(t); sent != 1 {
		t.Fatalf("sent=%d, want the first step", sent)
	}
	h.awaitAdmission(t, h.plan.Steps[0].Command.CommandId)
	if _, err := h.pool.Exec(t.Context(), `UPDATE operations SET cancellation_requested_at=clock_timestamp() WHERE id=$1`, h.claim.OperationID); err != nil {
		t.Fatal(err)
	}
	if err := h.worker.Run(t.Context()); !errors.Is(err, ErrEvidencePending) {
		t.Fatalf("first cancellation pass: %v", err)
	}
	h.awaitReceipt(t, h.plan.Steps[0].Command.CommandId)
	for range 2 {
		err := h.worker.Run(t.Context())
		if !errors.Is(err, ErrEvidencePending) || !strings.Contains(err.Error(), "effect_unresolved") {
			t.Fatalf("an active effect finished the cancellation: %v", err)
		}
	}
	if h.count(t, `SELECT count(*) FROM operations WHERE id=$1 AND status='cancelled'`, h.claim.OperationID) != 0 {
		t.Fatal("the worker cancelled over an effect that is still running")
	}
}

// A cancellation answer carries no durable evidence, so an answer this run never
// asked for is dropped. Failing would close the stream after an owner changed.
func TestWorkerDropsAnUnrequestedCancellationAnswer(t *testing.T) {
	h := newHarness(t, agentScript{drained: true, admit: true}.answer)
	peer := controlstream.Peer{InstallationID: h.claim.InstallationID, NodeID: h.node, SessionID: uuid.NewString()}
	message := &agentv1.AgentMessage{Message: &agentv1.AgentMessage_CancellationResult{
		CancellationResult: &agentv1.CancellationResult{Epoch: h.worker.epoch(h.node), CommandId: h.plan.Steps[0].Command.CommandId, State: "cancelled"},
	}}
	if err := h.worker.Handle(t.Context(), peer, message); err != nil {
		t.Fatalf("a late answer closed the agent stream: %v", err)
	}
}

// An answer bound to other work is a protocol violation, not a late message.
func TestWorkerRefusesAMisboundCancellationAnswer(t *testing.T) {
	h := newHarness(t, agentScript{drained: true, admit: true}.answer)
	peer := controlstream.Peer{InstallationID: h.claim.InstallationID, NodeID: h.node, SessionID: uuid.NewString()}
	epoch := h.worker.epoch(h.node)
	epoch.ClusterId = uuid.NewString()
	for _, answer := range []*agentv1.CancellationResult{
		{Epoch: epoch, CommandId: h.plan.Steps[0].Command.CommandId, State: "cancelled"},
		{Epoch: h.worker.epoch(h.node), CommandId: h.plan.Steps[0].Command.CommandId, State: "made_up"},
		{Epoch: h.worker.epoch(h.node), CommandId: "", State: "cancelled"},
	} {
		message := &agentv1.AgentMessage{Message: &agentv1.AgentMessage_CancellationResult{CancellationResult: answer}}
		if err := h.worker.Handle(t.Context(), peer, message); !errors.Is(err, ErrUnboundEvidence) {
			t.Fatalf("the worker accepted an answer bound to other work: %v", err)
		}
	}
}

// No session has admitted a command that was only sent, so no session provably
// holds it. Nothing is asked to stop it, and the operation waits for admission.
func TestWorkerAsksNoAgentForAnUnadmittedCommand(t *testing.T) {
	h := newHarness(t, agentScript{drained: true}.answer)
	if err := h.worker.Prepare(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, sent := h.step(t); sent != 1 {
		t.Fatalf("sent=%d, want the first step", sent)
	}
	if _, err := h.pool.Exec(t.Context(), `UPDATE operations SET cancellation_requested_at=clock_timestamp() WHERE id=$1`, h.claim.OperationID); err != nil {
		t.Fatal(err)
	}
	err := h.worker.Run(t.Context())
	if !errors.Is(err, ErrEvidencePending) || !strings.Contains(err.Error(), "in flight") {
		t.Fatalf("an unadmitted command did not hold the cancellation: %v", err)
	}
	if h.cancelled(h.plan.Steps[0].Command.CommandId) {
		t.Fatal("the worker asked a session to stop a command it never admitted")
	}
	if h.count(t, `SELECT count(*) FROM operations WHERE id=$1 AND status='cancelling' AND finished_at IS NULL`, h.claim.OperationID) != 1 {
		t.Fatal("the worker did not record the cancellation transition")
	}
}

func TestWorkerCancelsAfterEveryDeliveryIsRefused(t *testing.T) {
	h := newHarness(t, agentScript{drained: true, reject: "agent_busy"}.answer)
	if err := h.worker.Prepare(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, sent := h.step(t); sent != 1 {
		t.Fatalf("sent=%d, want one attempt", sent)
	}
	waitFor(t, func() bool {
		return h.count(t, `SELECT count(*) FROM command_delivery_attempts WHERE rejection_bytes IS NOT NULL`) == 1
	}, "refusal did not commit")
	if _, err := h.pool.Exec(t.Context(), `UPDATE operations SET cancellation_requested_at=clock_timestamp() WHERE id=$1`, h.claim.OperationID); err != nil {
		t.Fatal(err)
	}
	if err := h.worker.Run(t.Context()); err != nil {
		t.Fatalf("cancellation waited for a refused command: %v", err)
	}
	if h.count(t, `SELECT count(*) FROM operations WHERE id=$1 AND status='cancelled' AND NOT claim_active AND finished_at IS NOT NULL`, h.claim.OperationID) != 1 {
		t.Fatal("known refusals did not finish cancellation")
	}
	if h.cancelled(h.plan.Steps[0].Command.CommandId) {
		t.Fatal("a refused command received cancellation")
	}
	if h.count(t, `SELECT count(*) FROM command_delivery_attempts`) != 1 {
		t.Fatal("cancellation retried forward work")
	}
}

// dropTimeout bounds one test database teardown. It exists to stop a hang, not to
// assert how fast PostgreSQL drops a database under load.
const dropTimeout = 60 * time.Second

func TestWorkerKeepsActiveEffectsAfterFailure(t *testing.T) {
	for _, failureKind := range []string{"delivery_exhausted", "failed_receipt"} {
		t.Run(failureKind, func(t *testing.T) {
			// The start and its readiness check run beside the volume on a second node. One node
			// never runs two commands of a cluster at once, so siblings are on different nodes.
			second := uuid.New()
			reply := func(message *agentv1.ControllerMessage) []*agentv1.AgentMessage {
				script := agentScript{drained: true, admit: true}
				if command := message.GetCommand(); command != nil && command.Input.GetStartContainer() != nil {
					if failureKind == "delivery_exhausted" {
						script.admit, script.reject = false, "agent_busy"
					} else {
						script.receipt = "failed"
					}
				}
				return script.answer(message)
			}
			h := newHarness(t, reply, func(plan *agentv1.ExecutionPlan) {
				plan.Steps[1].DependsOn = nil
				// The readiness check follows the start on its node.
				for _, step := range plan.Steps[1:] {
					step.Command.Epoch.NodeId = second.String()
					rehash(t, step.Command)
				}
			})
			h.enroll(t, h.claim.InstallationID, second)
			h.connect(t, h.claim.InstallationID, second, reply)
			if err := h.worker.Run(t.Context()); !errors.Is(err, ErrEvidencePending) {
				t.Fatalf("worker ended with an active sibling: %v", err)
			}
			if h.count(t, `SELECT count(*) FROM operations WHERE id=$1 AND status='waiting' AND claim_active AND finished_at IS NULL`, h.claim.OperationID) != 1 {
				t.Fatal("failure released the active effect")
			}
			// A fresh worker must reconstruct the failure from durable evidence.
			resumed, err := New(h.worker.config)
			if err != nil {
				t.Fatal(err)
			}
			if err := resumed.Run(t.Context()); !errors.Is(err, ErrEvidencePending) {
				t.Fatalf("restart forgot the active effect: %v", err)
			}
			if total := h.count(t, `SELECT count(*) FROM command_dispatches`); total != 2 {
				t.Fatalf("failure dispatched dependent work: %d", total)
			}
			h.receive(t, h.plan.Steps[0].Command, "succeeded", "", false)
			if err := resumed.Run(t.Context()); err != nil {
				t.Fatal(err)
			}
			if h.count(t, `SELECT count(*) FROM operations WHERE id=$1 AND status='failed' AND NOT claim_active AND finished_at IS NOT NULL`, h.claim.OperationID) != 1 {
				t.Fatal("settled effects did not permit failure")
			}
		})
	}
}

func TestWorkerWaitsAfterExhaustionWithoutAnyReply(t *testing.T) {
	h := newHarness(t, agentScript{drained: true}.answer)
	for range int(h.plan.Steps[0].MaxAttempts) + 1 {
		if err := h.worker.Run(t.Context()); !errors.Is(err, ErrEvidencePending) {
			t.Fatalf("lost replies became a known failure: %v", err)
		}
	}
	if h.count(t, `SELECT count(*) FROM operations WHERE id=$1 AND status='waiting' AND failure_code='outcome_unknown' AND claim_active AND finished_at IS NULL`, h.claim.OperationID) != 1 {
		t.Fatal("lost reply released the mutation slot")
	}
	if total := h.count(t, `SELECT count(*) FROM command_delivery_attempts`); total != int(h.plan.Steps[0].MaxAttempts) {
		t.Fatalf("unbounded delivery attempts: %d", total)
	}
}

func TestWorkerDoesNotSettleUnansweredDeliveryWithAnotherRefusal(t *testing.T) {
	for _, missing := range []int{1, 3} {
		t.Run(fmt.Sprint(missing), func(t *testing.T) {
			attempt := 0
			unanswered := make(chan *agentv1.ControllerMessage, 1)
			h := newHarness(t, func(message *agentv1.ControllerMessage) []*agentv1.AgentMessage {
				if message.GetCommand() != nil {
					attempt++
					if attempt == missing {
						unanswered <- message
						return nil
					}
				}
				return agentScript{drained: true, reject: "agent_busy"}.answer(message)
			})
			for range int(h.plan.Steps[0].MaxAttempts) + 1 {
				if err := h.worker.Run(t.Context()); !errors.Is(err, ErrEvidencePending) {
					t.Fatalf("another refusal settled unanswered attempt %d: %v", missing, err)
				}
			}
			if h.count(t, `SELECT count(*) FROM operations WHERE id=$1 AND status='waiting' AND claim_active AND finished_at IS NULL`, h.claim.OperationID) != 1 {
				t.Fatal("unanswered attempt released the mutation slot")
			}
			resumed, err := New(h.worker.config)
			if err != nil {
				t.Fatal(err)
			}
			if err := resumed.Run(t.Context()); !errors.Is(err, ErrEvidencePending) {
				t.Fatalf("restart lost the unanswered attempt: %v", err)
			}
			peer, ok := h.transport.Session(h.node)
			if !ok {
				t.Fatal("agent disconnected")
			}
			for _, reply := range (agentScript{reject: "agent_busy"}).answer(<-unanswered) {
				if err := resumed.Handle(t.Context(), peer, reply); err != nil {
					t.Fatal(err)
				}
			}
			if err := resumed.Run(t.Context()); err != nil {
				t.Fatal(err)
			}
			if h.count(t, `SELECT count(*) FROM operations WHERE id=$1 AND status='failed' AND NOT claim_active AND finished_at IS NOT NULL`, h.claim.OperationID) != 1 {
				t.Fatal("all refused attempts did not permit failure")
			}
		})
	}
}
