package postgres

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"io"
	"math"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/sirrobot01/synod/internal/controlstream"
	"github.com/sirrobot01/synod/internal/operation"
	"github.com/sirrobot01/synod/internal/pki"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/protobuf/proto"
)

func resultFixture(t *testing.T) (*Store, operation.Claim, *agentv1.TerminalResult) {
	t.Helper()
	s, _, claim := claimFixture(t)
	plan := claimPlan(t, claim)
	if err := s.InstallPlan(t.Context(), claim, plan); err != nil {
		t.Fatal(err)
	}
	reconcilePlan(t, s, claim, plan)
	if err := s.ActivateClaim(t.Context(), claim); err != nil {
		t.Fatal(err)
	}
	command := plan.Steps[0].Command
	if err := s.RecordDispatch(t.Context(), claim, command); err != nil {
		t.Fatal(err)
	}
	return s, claim, &agentv1.TerminalResult{Epoch: proto.Clone(command.Epoch).(*agentv1.Epoch), CommandId: command.CommandId, InputHash: command.InputHash, Sequence: 1, State: "succeeded", Effects: []*agentv1.Effect{{CommandId: command.CommandId, ResourceId: command.Input.ResourceId, NativeOperationId: "docker-command:" + command.CommandId, State: "running"}}}
}

func TestConcurrentAgentResultReceipt(t *testing.T) {
	s, claim, result := resultFixture(t)
	node := uuid.MustParse(result.Epoch.NodeId)
	start := make(chan struct{})
	errs := make(chan error, 16)
	var wg sync.WaitGroup
	for range 16 {
		wg.Go(func() { <-start; errs <- s.RecordAgentResult(t.Context(), claim.InstallationID, node, result) })
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	var count int
	if err := s.pool.QueryRow(t.Context(), "SELECT count(*) FROM command_results").Scan(&count); err != nil || count != 1 {
		t.Fatalf("receipts=%d err=%v", count, err)
	}
	stored, err := s.GetAgentResult(t.Context(), claim.InstallationID, node, uuid.MustParse(result.CommandId), result.Epoch)
	if err != nil || !proto.Equal(stored, result) {
		t.Fatalf("receipt mismatch: %v", err)
	}
	stored.State = "failed"
	again, err := s.GetAgentResult(t.Context(), claim.InstallationID, node, uuid.MustParse(result.CommandId), result.Epoch)
	if err != nil || !proto.Equal(again, result) {
		t.Fatal("receipt was not detached")
	}
	for _, edit := range []func(*agentv1.TerminalResult){
		func(r *agentv1.TerminalResult) { r.Sequence++ },
		func(r *agentv1.TerminalResult) { r.State = "failed"; r.FailureCode = "runtime_failure" },
		func(r *agentv1.TerminalResult) { r.Effects[0].State = "stopped" },
	} {
		changed := proto.Clone(result).(*agentv1.TerminalResult)
		edit(changed)
		if err := s.RecordAgentResult(t.Context(), claim.InstallationID, node, changed); !errors.Is(err, operation.ErrCommandConflict) {
			t.Fatalf("changed result accepted: %v", err)
		}
	}
	current, err := s.GetOperation(t.Context(), claim.InstallationID, claim.OperationID)
	if err != nil || current.Status != operation.Running {
		t.Fatalf("receipt changed operation state: %v %v", current.Status, err)
	}
}

func TestAgentResultValidation(t *testing.T) {
	s, claim, result := resultFixture(t)
	node := uuid.MustParse(result.Epoch.NodeId)
	for _, test := range []struct {
		name string
		edit func(*agentv1.TerminalResult)
	}{
		{"missing epoch", func(r *agentv1.TerminalResult) { r.Epoch = nil }},
		{"wrong node", func(r *agentv1.TerminalResult) { r.Epoch.NodeId = uuid.NewString() }},
		{"wrong app", func(r *agentv1.TerminalResult) { r.Epoch.AppId = uuid.NewString() }},
		{"wrong generation", func(r *agentv1.TerminalResult) { r.Epoch.RecoveryGeneration = uuid.NewString() }},
		{"wrong epoch", func(r *agentv1.TerminalResult) { r.Epoch.ExecutionEpoch++ }},
		{"unknown command", func(r *agentv1.TerminalResult) { r.CommandId = uuid.NewString() }},
		{"changed input", func(r *agentv1.TerminalResult) { r.InputHash[0] ^= 1 }},
		{"zero sequence", func(r *agentv1.TerminalResult) { r.Sequence = 0 }},
		{"overflow sequence", func(r *agentv1.TerminalResult) { r.Sequence = math.MaxUint64 }},
		{"invalid state", func(r *agentv1.TerminalResult) { r.State = "running" }},
		{"active success", func(r *agentv1.TerminalResult) { r.Effects[0].Active = true }},
		{"unknown success", func(r *agentv1.TerminalResult) { r.Effects[0].OutcomeUnknown = true }},
		{"nil effect", func(r *agentv1.TerminalResult) { r.Effects[0] = nil }},
		{"foreign effect", func(r *agentv1.TerminalResult) { r.Effects[0].ResourceId = uuid.NewString() }},
		{"foreign command effect", func(r *agentv1.TerminalResult) { r.Effects[0].CommandId = uuid.NewString() }},
		{"duplicate effect", func(r *agentv1.TerminalResult) {
			r.Effects = append(r.Effects, proto.Clone(r.Effects[0]).(*agentv1.Effect))
		}},
		{"native identity too large", func(r *agentv1.TerminalResult) { r.Effects[0].NativeOperationId = strings.Repeat("x", 257) }},
		{"unsafe effect state", func(r *agentv1.TerminalResult) { r.Effects[0].State = "private diagnostic" }},
		{"unsafe failure code", func(r *agentv1.TerminalResult) { r.State = "failed"; r.FailureCode = "private diagnostic" }},
		{"failure with success", func(r *agentv1.TerminalResult) { r.FailureCode = "runtime_failure" }},
		{"too large", func(r *agentv1.TerminalResult) { r.FailureCode = strings.Repeat("x", 1<<20) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			changed := proto.Clone(result).(*agentv1.TerminalResult)
			test.edit(changed)
			if err := s.RecordAgentResult(t.Context(), claim.InstallationID, node, changed); err == nil {
				t.Fatal("accepted invalid result")
			}
		})
	}
	var count int
	if err := s.pool.QueryRow(t.Context(), "SELECT count(*) FROM command_results").Scan(&count); err != nil || count != 0 {
		t.Fatalf("invalid receipt wrote data: %d %v", count, err)
	}
	result.State = "failed"
	result.FailureCode = "runtime_outcome_unknown"
	result.Effects[0].Active = true
	result.Effects[0].OutcomeUnknown = true
	if err := s.RecordAgentResult(t.Context(), claim.InstallationID, node, result); err != nil {
		t.Fatal(err)
	}
	otherEpoch := proto.Clone(result.Epoch).(*agentv1.Epoch)
	otherEpoch.NodeId = uuid.NewString()
	if _, err := s.GetAgentResult(t.Context(), claim.InstallationID, uuid.MustParse(otherEpoch.NodeId), uuid.MustParse(result.CommandId), otherEpoch); err == nil {
		t.Fatal("read another node's receipt")
	}
}

func TestHistoricalAgentReceiptDoesNotGrantAuthority(t *testing.T) {
	s, claim, result := resultFixture(t)
	if _, err := s.pool.Exec(t.Context(), "UPDATE installation_generations SET recovery_generation=$1 WHERE installation_id=$2", uuid.New(), claim.InstallationID); err != nil {
		t.Fatal(err)
	}
	if err := s.RecordAgentResult(t.Context(), claim.InstallationID, uuid.MustParse(result.Epoch.NodeId), result); err != nil {
		t.Fatal(err)
	}
	if err := s.ValidateClaim(t.Context(), claim); !errors.Is(err, operation.ErrStaleOwner) {
		t.Fatalf("historical receipt revived authority: %v", err)
	}
}

func TestAgentReceiptUnknownCommitAndRollback(t *testing.T) {
	for _, committed := range []bool{false, true} {
		t.Run(map[bool]string{false: "rollback", true: "lost reply"}[committed], func(t *testing.T) {
			s, claim, result := resultFixture(t)
			node := uuid.MustParse(result.Epoch.NodeId)
			s.commitTx = func(ctx context.Context, tx pgx.Tx) error {
				if committed {
					if err := tx.Commit(ctx); err != nil {
						return err
					}
				} else {
					if err := tx.Rollback(ctx); err != nil {
						return err
					}
				}
				return io.ErrUnexpectedEOF
			}
			err := s.RecordAgentResult(t.Context(), claim.InstallationID, node, result)
			failure, ok := errors.AsType[*AdmissionError](err)
			if !ok || failure.Code != OutcomeUnknown {
				t.Fatalf("unexpected commit classification: %v", err)
			}
			var count int
			if err := s.pool.QueryRow(t.Context(), "SELECT count(*) FROM command_results").Scan(&count); err != nil {
				t.Fatal(err)
			}
			if committed && count != 1 || !committed && count != 0 {
				t.Fatalf("receipts=%d committed=%v", count, committed)
			}
			s.commitTx = nil
			if err := s.RecordAgentResult(t.Context(), claim.InstallationID, node, result); err != nil {
				t.Fatal(err)
			}
			if err := s.pool.QueryRow(t.Context(), "SELECT count(*) FROM command_results").Scan(&count); err != nil || count != 1 {
				t.Fatalf("recovery receipts=%d err=%v", count, err)
			}
		})
	}
}

// This boundary test uses actual enrollment, certificate authorization, PostgreSQL,
// TCP/TLS gRPC, and the transport's automatic durable-result acknowledgement.
func TestAgentReceiptOverMutualTLS(t *testing.T) {
	s, claim, result := resultFixture(t)
	node := uuid.MustParse(result.Epoch.NodeId)
	caCert, caKey, err := pki.CreateCA(claim.InstallationID, time.Now(), 7*24*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	ca, err := pki.LoadCA(claim.InstallationID, caCert, caKey)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(caCert) {
		t.Fatal("invalid fixture CA")
	}
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
		var leaf []byte
		if role == pki.Agent {
			verifier := sha256.Sum256([]byte(uuid.NewString()))
			if _, err := s.RegisterEnrollmentToken(t.Context(), TokenRegistration{InstallationID: claim.InstallationID, NodeID: node, ActorID: "test-admin", RequestID: uuid.NewString(), IdempotencyKey: uuid.NewString(), TokenVerifier: verifier, TTLSeconds: 600}); err != nil {
				t.Fatal(err)
			}
			enrolled, err := s.EnrollAgent(t.Context(), EnrollmentRequest{InstallationID: claim.InstallationID, NodeID: node, RequestID: uuid.New(), TokenVerifier: verifier, CSRDER: csr}, ca)
			if err != nil {
				t.Fatal(err)
			}
			leaf = []byte(enrolled.CertificatePEM)
		} else {
			leaf, err = ca.Issue(csr, pki.Identity{InstallationID: claim.InstallationID, Role: role, SubjectID: uuid.New()}, []string{"127.0.0.1"}, time.Now(), time.Hour)
			if err != nil {
				t.Fatal(err)
			}
		}
		key, err := x509.MarshalPKCS8PrivateKey(private)
		if err != nil {
			t.Fatal(err)
		}
		pair, err := tls.X509KeyPair(leaf, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: key}))
		if err != nil {
			t.Fatal(err)
		}
		pairs[role] = pair
	}
	entered, release := make(chan struct{}), make(chan struct{})
	var interrupted atomic.Bool
	s.commitTx = func(ctx context.Context, tx pgx.Tx) error {
		if interrupted.CompareAndSwap(false, true) {
			close(entered)
			select {
			case <-release:
			case <-ctx.Done():
				return ctx.Err()
			}
			if err := tx.Commit(ctx); err != nil {
				return err
			}
			return io.ErrUnexpectedEOF
		}
		return tx.Commit(ctx)
	}
	server, err := controlstream.New(controlstream.Config{InstallationID: claim.InstallationID, TLS: &tls.Config{MinVersion: tls.VersionTLS12, ClientCAs: roots, Certificates: []tls.Certificate{pairs[pki.Controller]}}, Authorize: s.CheckAgentCertificate, Handle: func(ctx context.Context, peer controlstream.Peer, message *agentv1.AgentMessage) error {
		if terminal := message.GetTerminalResult(); terminal != nil {
			return s.RecordAgentResult(ctx, peer.InstallationID, peer.NodeID, terminal)
		}
		return nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.Serve(listener) }()
	t.Cleanup(func() { server.Stop(); <-serveDone })
	connection, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots, Certificates: []tls.Certificate{pairs[pki.Agent]}})))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { connection.Close() })
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	hello := &agentv1.AgentMessage{Message: &agentv1.AgentMessage_Hello{Hello: &agentv1.Hello{InstallationId: claim.InstallationID.String(), NodeId: node.String(), BootId: uuid.NewString(), JournalGeneration: uuid.NewString(), Protocol: &agentv1.VersionRange{Minimum: 1, Maximum: 1}, ActivePayloadVersions: []uint32{1}}}}
	stream, err := agentv1.NewAgentServiceClient(connection).Connect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(hello); err != nil {
		t.Fatal(err)
	}
	if message, err := stream.Recv(); err != nil || message.GetSession() == nil {
		t.Fatalf("session failed: %v", err)
	}
	if err := stream.Send(&agentv1.AgentMessage{Message: &agentv1.AgentMessage_TerminalResult{TerminalResult: result}}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("receipt did not reach commit")
	}
	received := make(chan error, 1)
	go func() {
		message, err := stream.Recv()
		if err == nil && message.GetResultAck() != nil {
			received <- errors.New("acknowledged unknown result")
			return
		}
		received <- err
	}()
	select {
	case err := <-received:
		t.Fatalf("response before commit: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	select {
	case err := <-received:
		if err == nil || err.Error() == "acknowledged unknown result" {
			t.Fatalf("uncertain commit acknowledged: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("uncertain stream did not close")
	}
	stored, err := s.GetAgentResult(t.Context(), claim.InstallationID, node, uuid.MustParse(result.CommandId), result.Epoch)
	if err != nil || !proto.Equal(stored, result) {
		t.Fatalf("real committed receipt missing: %v", err)
	}
	replay, err := agentv1.NewAgentServiceClient(connection).Connect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := replay.Send(hello); err != nil {
		t.Fatal(err)
	}
	if message, err := replay.Recv(); err != nil || message.GetSession() == nil {
		t.Fatalf("replay session: %v", err)
	}
	if err := replay.Send(&agentv1.AgentMessage{Message: &agentv1.AgentMessage_TerminalResult{TerminalResult: result}}); err != nil {
		t.Fatal(err)
	}
	ack, err := replay.Recv()
	if err != nil || ack.GetResultAck() == nil || ack.GetResultAck().CommandId != result.CommandId || ack.GetResultAck().Sequence != result.Sequence || !proto.Equal(ack.GetResultAck().Epoch, result.Epoch) {
		t.Fatalf("durable replay not acknowledged: %v", err)
	}
	var count int
	if err := s.pool.QueryRow(t.Context(), "SELECT count(*) FROM command_results").Scan(&count); err != nil || count != 1 {
		t.Fatalf("replay receipt count=%d err=%v", count, err)
	}
}
