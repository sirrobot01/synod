package transport_test

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
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/agent/journal"
	"github.com/sirrobot01/synod/internal/agent/transport"
	"github.com/sirrobot01/synod/internal/controlstream"
	"github.com/sirrobot01/synod/internal/operation"
	"github.com/sirrobot01/synod/internal/pki"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type fixture struct {
	ca      *pki.CA
	server  *controlstream.Server
	address string
	tls     *tls.Config
	hello   *agentv1.Hello
	revoked atomic.Bool
}

// Each scenario uses a real installation CA and TCP/TLS gRPC listener.
func newFixture(t *testing.T, handle func(context.Context, controlstream.Peer, *agentv1.AgentMessage) error, change func(*controlstream.Config), shortLife ...pki.Role) *fixture {
	t.Helper()
	installation, node := uuid.New(), uuid.New()
	now := time.Now()
	root, key, err := pki.CreateCA(installation, now, 24*time.Hour)
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
		request, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{}, private)
		if err != nil {
			t.Fatal(err)
		}
		subject := node
		var names []string
		if role == pki.Controller {
			subject = uuid.New()
			names = []string{"127.0.0.1"}
		}
		cert, err := ca.Issue(request, pki.Identity{InstallationID: installation, Role: role, SubjectID: subject}, names, now, time.Hour)
		if err != nil {
			t.Fatal(err)
		}
		der, err := x509.MarshalPKCS8PrivateKey(private)
		if err != nil {
			t.Fatal(err)
		}
		pair, err := tls.X509KeyPair(cert, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}))
		if err != nil {
			t.Fatal(err)
		}
		if len(shortLife) > 0 && role == shortLife[0] {
			leaf := *pair.Leaf
			leaf.NotAfter = time.Now().Truncate(time.Second).Add(2 * time.Second)
			rootBlock, _ := pem.Decode(root)
			rootCert, err := x509.ParseCertificate(rootBlock.Bytes)
			if err != nil {
				t.Fatal(err)
			}
			keyBlock, _ := pem.Decode(key)
			rootPrivate, err := x509.ParsePKCS8PrivateKey(keyBlock.Bytes)
			if err != nil {
				t.Fatal(err)
			}
			raw, err := x509.CreateCertificate(rand.Reader, &leaf, rootCert, leaf.PublicKey, rootPrivate)
			if err != nil {
				t.Fatal(err)
			}
			pair, err = tls.X509KeyPair(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: raw}), pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}))
			if err != nil {
				t.Fatal(err)
			}
		}
		pairs[role] = pair
	}
	f := &fixture{ca: ca, tls: &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots, Certificates: []tls.Certificate{pairs[pki.Agent]}}, hello: &agentv1.Hello{InstallationId: installation.String(), NodeId: node.String(), BootId: uuid.NewString(), JournalGeneration: uuid.NewString(), Protocol: &agentv1.VersionRange{Minimum: 1, Maximum: 1}, Capabilities: []string{"runtime.docker", "unsupported"}, ActivePayloadVersions: []uint32{1}}}
	config := controlstream.Config{InstallationID: installation, TLS: &tls.Config{MinVersion: tls.VersionTLS12, ClientCAs: roots, Certificates: []tls.Certificate{pairs[pki.Controller]}}, Capabilities: []string{"runtime.docker"}, HelloTimeout: 500 * time.Millisecond, HeartbeatInterval: 20 * time.Millisecond, StaleAfter: 2 * time.Second, RecheckInterval: 20 * time.Millisecond, AuthorizationTimeout: time.Second, MaxSessions: 1, QueueSize: 2, Handle: handle}
	config.Authorize = func(ctx context.Context, i, n uuid.UUID, cert *x509.Certificate) error {
		if i != installation || n != node || cert == nil || !cert.Equal(pairs[pki.Agent].Leaf) {
			return errors.New("wrong exact certificate")
		}
		if f.revoked.Load() {
			return errors.New("revoked-secret-never-send")
		}
		return nil
	}
	if change != nil {
		change(&config)
	}
	f.server, err = controlstream.New(config)
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	f.address = listener.Addr().String()
	t.Cleanup(f.server.Stop)
	go func() { _ = f.server.Serve(listener) }()
	return f
}

func (f *fixture) stream(t *testing.T, hello *agentv1.Hello) (agentv1.AgentService_ConnectClient, *agentv1.Session) {
	t.Helper()
	connection, err := grpc.NewClient(f.address, grpc.WithTransportCredentials(credentials.NewTLS(f.tls)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { connection.Close() })
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	t.Cleanup(cancel)
	stream, err := agentv1.NewAgentServiceClient(connection).Connect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(&agentv1.AgentMessage{Message: &agentv1.AgentMessage_Hello{Hello: hello}}); err != nil {
		t.Fatal(err)
	}
	message, err := stream.Recv()
	if err != nil {
		t.Fatal(err)
	}
	if message.GetSession() == nil {
		t.Fatal("missing session")
	}
	return stream, message.GetSession()
}

func result(f *fixture) *agentv1.AgentMessage {
	return &agentv1.AgentMessage{Message: &agentv1.AgentMessage_TerminalResult{TerminalResult: &agentv1.TerminalResult{CommandId: uuid.NewString(), InputHash: make([]byte, 32), Sequence: 1, State: "succeeded", Epoch: &agentv1.Epoch{InstallationId: f.hello.InstallationId, NodeId: f.hello.NodeId, AppId: uuid.NewString(), RecoveryGeneration: uuid.NewString(), ExecutionEpoch: 1}}}}
}

func TestDurableAcknowledgementAndReplacement(t *testing.T) {
	entered, commit := make(chan struct{}), make(chan struct{})
	f := newFixture(t, func(ctx context.Context, _ controlstream.Peer, message *agentv1.AgentMessage) error {
		if message.GetTerminalResult() != nil {
			close(entered)
			select {
			case <-commit:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	}, nil)
	stream, session := f.stream(t, f.hello)
	if len(session.Capabilities) != 1 || session.Capabilities[0] != "runtime.docker" {
		t.Fatal("capability negotiation failed")
	}
	terminal := result(f)
	if err := stream.Send(terminal); err != nil {
		t.Fatal(err)
	}
	<-entered
	received := make(chan *agentv1.ControllerMessage, 1)
	go func() { message, _ := stream.Recv(); received <- message }()
	select {
	case <-received:
		t.Fatal("acknowledged before durable handling")
	case <-time.After(50 * time.Millisecond):
	}
	close(commit)
	select {
	case message := <-received:
		if message.GetResultAck() == nil || message.GetResultAck().CommandId != terminal.GetTerminalResult().CommandId {
			t.Fatal("wrong durable acknowledgement")
		}
	case <-time.After(time.Second):
		t.Fatal("no acknowledgement")
	}
	replacement, next := f.stream(t, f.hello)
	if next.SessionId == session.SessionId {
		t.Fatal("replacement reused session ID")
	}
	if _, err := stream.Recv(); err == nil {
		t.Fatal("old session remained open")
	}
	node := uuid.MustParse(f.hello.NodeId)
	current, ok := f.server.Session(node)
	if !ok || current.SessionID != next.SessionId {
		t.Fatal("old teardown removed replacement")
	}
	inventory := &agentv1.ControllerMessage{Message: &agentv1.ControllerMessage_Inventory{Inventory: &agentv1.InventoryRequest{PageSize: 5}}}
	if err := f.server.Send(node, session.SessionId, inventory); !errors.Is(err, controlstream.ErrDisconnected) {
		t.Fatal("stale send handle accepted")
	}
	if err := f.server.Send(node, next.SessionId, inventory); err != nil {
		t.Fatal(err)
	}
	message, err := replacement.Recv()
	if err != nil || message.GetInventory() == nil {
		t.Fatal("replacement did not receive send")
	}
}

func TestHelloRejectionAndRevocation(t *testing.T) {
	f := newFixture(t, func(context.Context, controlstream.Peer, *agentv1.AgentMessage) error { return nil }, nil)
	for _, kind := range []string{"node", "boot", "protocol", "payload", "missing", "duplicate"} {
		t.Run(kind, func(t *testing.T) {
			hello := proto.Clone(f.hello).(*agentv1.Hello)
			switch kind {
			case "node":
				hello.NodeId = uuid.NewString()
			case "boot":
				hello.BootId = "invalid"
			case "protocol":
				hello.Protocol.Maximum = 0
			case "payload":
				hello.ActivePayloadVersions = []uint32{99}
			}
			conn, err := grpc.NewClient(f.address, grpc.WithTransportCredentials(credentials.NewTLS(f.tls)))
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
			defer cancel()
			stream, err := agentv1.NewAgentServiceClient(conn).Connect(ctx)
			if err != nil {
				t.Fatal(err)
			}
			message := &agentv1.AgentMessage{Message: &agentv1.AgentMessage_Hello{Hello: hello}}
			if kind == "missing" {
				message = &agentv1.AgentMessage{Message: &agentv1.AgentMessage_Heartbeat{Heartbeat: &agentv1.Heartbeat{}}}
			}
			if err := stream.Send(message); err != nil {
				t.Fatal(err)
			}
			reply, err := stream.Recv()
			if kind == "duplicate" {
				if err != nil || reply.GetSession() == nil {
					t.Fatal("initial Hello failed")
				}
				stream.Send(message)
				_, err = stream.Recv()
			}
			if err == nil {
				t.Fatal("invalid Hello accepted")
			}
		})
	}
	stream, _ := f.stream(t, f.hello)
	f.revoked.Store(true)
	_, err := stream.Recv()
	if status.Code(err) != codes.PermissionDenied || strings.Contains(err.Error(), "revoked-secret") {
		t.Fatalf("revocation not enforced or redacted: %v", err)
	}
}

func TestMutualTLSRequired(t *testing.T) {
	f := newFixture(t, func(context.Context, controlstream.Peer, *agentv1.AgentMessage) error { return nil }, nil)
	for _, kind := range []string{"no certificate", "wrong server trust", "wrong name"} {
		t.Run(kind, func(t *testing.T) {
			config := f.tls.Clone()
			switch kind {
			case "no certificate":
				config.Certificates = nil
			case "wrong server trust":
				config.RootCAs = x509.NewCertPool()
			case "wrong name":
				config.ServerName = "wrong.example"
			}
			conn, err := grpc.NewClient(f.address, grpc.WithTransportCredentials(credentials.NewTLS(config)))
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			stream, err := agentv1.NewAgentServiceClient(conn).Connect(ctx)
			if err == nil {
				stream.Send(&agentv1.AgentMessage{Message: &agentv1.AgentMessage_Hello{Hello: f.hello}})
				_, err = stream.Recv()
			}
			if err == nil {
				t.Fatal("invalid TLS accepted")
			}
		})
	}
}

func TestReconnectReplaysOnlyUnacknowledgedResults(t *testing.T) {
	var persisted, hellos, acks atomic.Int32
	f := newFixture(t, func(_ context.Context, _ controlstream.Peer, message *agentv1.AgentMessage) error {
		if message.GetHello() != nil {
			hellos.Add(1)
		}
		if message.GetTerminalResult() != nil && persisted.Add(1) == 1 {
			return errors.New("unknown commit secret")
		}
		return nil
	}, nil)
	terminal := result(f)
	var client *transport.Client
	var err error
	acknowledged := make(chan struct{})
	client, err = transport.New(transport.Config{Endpoints: []string{"127.0.0.1:1", f.address}, TLS: f.tls, Hello: f.hello, HandshakeTimeout: 150 * time.Millisecond, BackoffMin: 10 * time.Millisecond, BackoffMax: 30 * time.Millisecond, Handle: func(ctx context.Context, message *agentv1.ControllerMessage) (*agentv1.AgentMessage, error) {
		if message.GetSession() != nil && acks.Load() == 0 {
			return terminal, nil
		}
		if message.GetResultAck() != nil {
			if message.GetResultAck().CommandId != terminal.GetTerminalResult().CommandId {
				return nil, errors.New("wrong ack")
			}
			if acks.Add(1) == 1 {
				close(acknowledged)
			}
		}
		return nil, nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- client.Run(ctx) }()
	select {
	case <-acknowledged:
	case <-time.After(4 * time.Second):
		t.Fatalf("no replay acknowledgement; hellos=%d results=%d", hellos.Load(), persisted.Load())
	}
	if hellos.Load() < 2 || persisted.Load() != 2 || acks.Load() != 1 {
		t.Fatal("lost reply did not replay correctly")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("shutdown blocked")
	}
	if err := client.Send(terminal); !errors.Is(err, controlstream.ErrDisconnected) {
		t.Fatal("send after shutdown accepted")
	}
}

func TestServerQueueBoundsAndSnapshots(t *testing.T) {
	var block atomic.Bool
	entered, release := make(chan struct{}, 1), make(chan struct{})
	f := newFixture(t, func(context.Context, controlstream.Peer, *agentv1.AgentMessage) error { return nil }, func(config *controlstream.Config) {
		original := config.Authorize
		config.QueueSize = 1
		config.Authorize = func(ctx context.Context, i, n uuid.UUID, cert *x509.Certificate) error {
			if block.Load() {
				select {
				case entered <- struct{}{}:
				default:
				}
				select {
				case <-release:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			return original(ctx, i, n, cert)
		}
	})
	stream, session := f.stream(t, f.hello)
	node := uuid.MustParse(f.hello.NodeId)
	block.Store(true)
	message := &agentv1.ControllerMessage{Message: &agentv1.ControllerMessage_Inventory{Inventory: &agentv1.InventoryRequest{Cursor: "first"}}}
	if err := f.server.Send(node, session.SessionId, message); err != nil {
		t.Fatal(err)
	}
	<-entered
	message.GetInventory().Cursor = "second"
	if err := f.server.Send(node, session.SessionId, message); err != nil {
		t.Fatal(err)
	}
	message.GetInventory().Cursor = "mutated"
	if err := f.server.Send(node, session.SessionId, message); !errors.Is(err, controlstream.ErrBusy) {
		t.Fatalf("queue limit ignored: %v", err)
	}
	close(release)
	for _, want := range []string{"first", "second"} {
		reply, err := stream.Recv()
		if err != nil || reply.GetInventory().Cursor != want {
			t.Fatalf("queued snapshot changed: %v %v", reply, err)
		}
	}
	oversized := &agentv1.ControllerMessage{Message: &agentv1.ControllerMessage_Inventory{Inventory: &agentv1.InventoryRequest{Cursor: strings.Repeat("x", (1<<20)+1)}}}
	if err := f.server.Send(node, session.SessionId, oversized); !errors.Is(err, controlstream.ErrMessage) {
		t.Fatal("oversized outbound message accepted")
	}
}

func TestExpiryAndAuthorizationFailure(t *testing.T) {
	t.Run("agent expiry", func(t *testing.T) {
		f := newFixture(t, func(context.Context, controlstream.Peer, *agentv1.AgentMessage) error { return nil }, nil, pki.Agent)
		stream, _ := f.stream(t, f.hello)
		if _, err := stream.Recv(); status.Code(err) != codes.Unauthenticated {
			t.Fatalf("expired session accepted: %v", err)
		}
	})
	t.Run("dependency unavailable", func(t *testing.T) {
		var unavailable atomic.Bool
		f := newFixture(t, func(context.Context, controlstream.Peer, *agentv1.AgentMessage) error { return nil }, func(config *controlstream.Config) {
			original := config.Authorize
			config.Authorize = func(ctx context.Context, i, n uuid.UUID, cert *x509.Certificate) error {
				if unavailable.Load() {
					return errors.New("private database DSN")
				}
				return original(ctx, i, n, cert)
			}
		})
		stream, _ := f.stream(t, f.hello)
		unavailable.Store(true)
		if _, err := stream.Recv(); status.Code(err) != codes.PermissionDenied || strings.Contains(err.Error(), "private database") {
			t.Fatalf("authorization failure not closed or redacted: %v", err)
		}
	})
	t.Run("controller expiry", func(t *testing.T) {
		f := newFixture(t, func(context.Context, controlstream.Peer, *agentv1.AgentMessage) error { return nil }, nil, pki.Controller)
		ready := make(chan context.Context, 1)
		client, err := transport.New(transport.Config{Endpoints: []string{f.address}, TLS: f.tls, Hello: f.hello, HandshakeTimeout: time.Second, BackoffMin: 10 * time.Millisecond, BackoffMax: 20 * time.Millisecond, Handle: func(ctx context.Context, message *agentv1.ControllerMessage) (*agentv1.AgentMessage, error) {
			if message.GetSession() != nil {
				ready <- ctx
			}
			return nil, nil
		}})
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		done := make(chan error, 1)
		go func() { done <- client.Run(ctx) }()
		var sessionContext context.Context
		select {
		case sessionContext = <-ready:
		case <-time.After(time.Second):
			t.Fatal("client did not connect")
		}
		select {
		case <-sessionContext.Done():
		case <-time.After(3 * time.Second):
			t.Fatal("controller expiry did not close session")
		}
		cancel()
		<-done
	})
}

func TestInboundBoundsAndCancelledHandling(t *testing.T) {
	t.Run("message bytes", func(t *testing.T) {
		f := newFixture(t, func(context.Context, controlstream.Peer, *agentv1.AgentMessage) error { return nil }, func(config *controlstream.Config) { config.MaxMessageBytes = 1024 })
		stream, _ := f.stream(t, f.hello)
		stream.Send(&agentv1.AgentMessage{Message: &agentv1.AgentMessage_Inventory{Inventory: &agentv1.InventoryPage{NextCursor: strings.Repeat("x", 2048)}}})
		if _, err := stream.Recv(); err == nil {
			t.Fatal("oversized inbound message accepted")
		}
	})
	t.Run("Hello deadline", func(t *testing.T) {
		f := newFixture(t, func(context.Context, controlstream.Peer, *agentv1.AgentMessage) error { return nil }, func(config *controlstream.Config) { config.HelloTimeout = 50 * time.Millisecond })
		conn, err := grpc.NewClient(f.address, grpc.WithTransportCredentials(credentials.NewTLS(f.tls)))
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		stream, err := agentv1.NewAgentServiceClient(conn).Connect(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := stream.Recv(); status.Code(err) != codes.DeadlineExceeded {
			t.Fatalf("missing Hello did not expire: %v", err)
		}
	})
	t.Run("durable handler cancellation", func(t *testing.T) {
		entered, exited := make(chan struct{}), make(chan struct{})
		f := newFixture(t, func(ctx context.Context, _ controlstream.Peer, message *agentv1.AgentMessage) error {
			if message.GetTerminalResult() != nil {
				close(entered)
				<-ctx.Done()
				close(exited)
				return ctx.Err()
			}
			return nil
		}, nil)
		stream, _ := f.stream(t, f.hello)
		stream.Send(result(f))
		<-entered
		f.server.Stop()
		select {
		case <-exited:
		case <-time.After(time.Second):
			t.Fatal("handler was not cancelled")
		}
		if message, err := stream.Recv(); err == nil || message.GetResultAck() != nil {
			t.Fatal("cancelled durable handling acknowledged")
		}
	})
}

func TestAgentQueuePressure(t *testing.T) {
	f := newFixture(t, func(context.Context, controlstream.Peer, *agentv1.AgentMessage) error { return nil }, nil)
	ready := make(chan struct{}, 1)
	client, err := transport.New(transport.Config{Endpoints: []string{f.address}, TLS: f.tls, Hello: f.hello, QueueSize: 1, Handle: func(ctx context.Context, message *agentv1.ControllerMessage) (*agentv1.AgentMessage, error) {
		if message.GetSession() != nil {
			ready <- struct{}{}
		}
		return nil, nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- client.Run(ctx) }()
	select {
	case <-ready:
	case <-time.After(time.Second):
		t.Fatal("client did not connect")
	}
	message := &agentv1.AgentMessage{Message: &agentv1.AgentMessage_Inventory{Inventory: &agentv1.InventoryPage{NextCursor: strings.Repeat("x", 500<<10)}}}
	busy := false
	for range 100 {
		if err := client.Send(message); errors.Is(err, controlstream.ErrBusy) {
			busy = true
			break
		} else if err != nil {
			t.Fatal(err)
		}
	}
	if !busy {
		t.Fatal("agent queue did not apply backpressure")
	}
	cancel()
	<-done
}

func TestExactCertificateAuthorization(t *testing.T) {
	f := newFixture(t, func(context.Context, controlstream.Peer, *agentv1.AgentMessage) error { return nil }, nil)
	_, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	request, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{}, private)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := f.ca.Issue(request, pki.Identity{InstallationID: uuid.MustParse(f.hello.InstallationId), Role: pki.Agent, SubjectID: uuid.MustParse(f.hello.NodeId)}, nil, time.Now(), time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	der, err := x509.MarshalPKCS8PrivateKey(private)
	if err != nil {
		t.Fatal(err)
	}
	pair, err := tls.X509KeyPair(cert, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}))
	if err != nil {
		t.Fatal(err)
	}
	config := f.tls.Clone()
	config.Certificates = []tls.Certificate{pair}
	conn, err := grpc.NewClient(f.address, grpc.WithTransportCredentials(credentials.NewTLS(config)))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	stream, err := agentv1.NewAgentServiceClient(conn).Connect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	stream.Send(&agentv1.AgentMessage{Message: &agentv1.AgentMessage_Hello{Hello: f.hello}})
	if _, err := stream.Recv(); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("same node with unregistered serial accepted: %v", err)
	}
}

func TestRefusedMessageKeepsTheConnection(t *testing.T) {
	for name, refusal := range map[string]error{
		"stale owner":    operation.ErrStaleOwner,
		"journal busy":   journal.ErrBusy,
		"transport busy": controlstream.ErrBusy,
	} {
		t.Run(name, func(t *testing.T) {
			var hellos atomic.Int32
			peers := make(chan controlstream.Peer, 4)
			f := newFixture(t, func(_ context.Context, peer controlstream.Peer, message *agentv1.AgentMessage) error {
				if message.GetHello() != nil {
					hellos.Add(1)
					select {
					case peers <- peer:
					default:
					}
				}
				return nil
			}, nil)
			logs, err := os.CreateTemp(t.TempDir(), "transport-log")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { logs.Close() })
			refused, accepted := make(chan struct{}), make(chan struct{})
			var seen atomic.Int32
			client, err := transport.New(transport.Config{
				Endpoints: []string{f.address}, TLS: f.tls, Hello: f.hello,
				Logger:           slog.New(slog.NewJSONHandler(logs, nil)),
				HandshakeTimeout: time.Second, BackoffMin: 10 * time.Millisecond, BackoffMax: 30 * time.Millisecond,
				Handle: func(_ context.Context, message *agentv1.ControllerMessage) (*agentv1.AgentMessage, error) {
					if message.GetCapacityRequest() == nil {
						return nil, nil
					}
					if seen.Add(1) == 1 {
						close(refused)
						return nil, fmt.Errorf("private-refusal-token: %w", refusal)
					}
					close(accepted)
					return nil, nil
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(t.Context())
			done := make(chan struct{})
			go func() { defer close(done); _ = client.Run(ctx) }()
			t.Cleanup(func() { cancel(); <-done })
			var peer controlstream.Peer
			select {
			case peer = <-peers:
			case <-time.After(4 * time.Second):
				t.Fatal("no connection established")
			}
			challenge := &agentv1.ControllerMessage{Message: &agentv1.ControllerMessage_CapacityRequest{
				CapacityRequest: &agentv1.CapacityRequest{
					ProbeId: uuid.NewString(), ProbeRevision: 1, ExpiresUnixMs: time.Now().Add(time.Minute).UnixMilli(),
					SessionId: peer.SessionID, BootId: peer.BootID, JournalGeneration: peer.JournalGeneration,
				},
			}}
			if err := f.server.Send(peer.NodeID, peer.SessionID, challenge); err != nil {
				t.Fatal(err)
			}
			select {
			case <-refused:
			case <-time.After(4 * time.Second):
				t.Fatal("the first challenge never reached the agent")
			}
			if err := f.server.Send(peer.NodeID, peer.SessionID, challenge); err != nil {
				t.Fatalf("the refusal closed the session: %v", err)
			}
			select {
			case <-accepted:
			case <-time.After(4 * time.Second):
				t.Fatal("the connection did not survive a refused message")
			}
			if hellos.Load() != 1 {
				t.Fatalf("hellos=%d, want one connection", hellos.Load())
			}
			logged, err := os.ReadFile(logs.Name())
			if err != nil {
				t.Fatal(err)
			}
			code := "agent_busy"
			if errors.Is(refusal, operation.ErrStaleOwner) {
				code = "stale_owner"
			}
			if !strings.Contains(string(logged), `"reason":"`+code+`"`) || !strings.Contains(string(logged), "agent_message_refused") || !strings.Contains(string(logged), "CapacityRequest") || strings.Contains(string(logged), "private-refusal-token") {
				t.Fatalf("refusal log lacks context or exposes private text: %s", logged)
			}
		})
	}
}

func TestFatalMessageHandlingReconnectsAndRedacts(t *testing.T) {
	for name, test := range map[string]struct {
		session bool
		failure error
		reason  string
	}{
		"session refusal":             {session: true, failure: fmt.Errorf("private-failure-token: %w", operation.ErrStaleOwner), reason: "session_installation_failed"},
		"recovery required with busy": {failure: errors.Join(journal.ErrRecoveryRequired, journal.ErrBusy, errors.New("private-failure-token")), reason: "recovery_required"},
		"protocol fault with busy":    {failure: errors.Join(controlstream.ErrMessage, journal.ErrBusy, errors.New("private-failure-token")), reason: "invalid_controller_message"},
		"private handler fault":       {failure: errors.New("private-failure-token"), reason: "handler_failed"},
	} {
		t.Run(name, func(t *testing.T) {
			peers := make(chan controlstream.Peer, 4)
			f := newFixture(t, func(_ context.Context, peer controlstream.Peer, message *agentv1.AgentMessage) error {
				if message.GetHello() != nil {
					select {
					case peers <- peer:
					default:
					}
				}
				return nil
			}, nil)
			logs, err := os.CreateTemp(t.TempDir(), "transport-log")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { logs.Close() })
			installed := make(chan string, 4)
			var sessions atomic.Int32
			client, err := transport.New(transport.Config{
				Endpoints: []string{f.address}, TLS: f.tls, Hello: f.hello,
				Logger:           slog.New(slog.NewJSONHandler(logs, nil)),
				HandshakeTimeout: time.Second, BackoffMin: 10 * time.Millisecond, BackoffMax: 30 * time.Millisecond,
				Handle: func(_ context.Context, message *agentv1.ControllerMessage) (*agentv1.AgentMessage, error) {
					if session := message.GetSession(); session != nil {
						if sessions.Add(1) == 1 && test.session {
							return nil, test.failure
						}
						select {
						case installed <- session.SessionId:
						default:
						}
					}
					if message.GetCapacityRequest() != nil {
						return nil, test.failure
					}
					return nil, nil
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(t.Context())
			done := make(chan struct{})
			go func() { defer close(done); _ = client.Run(ctx) }()
			t.Cleanup(func() { cancel(); <-done })
			var first controlstream.Peer
			select {
			case first = <-peers:
			case <-time.After(4 * time.Second):
				t.Fatal("no connection established")
			}
			if !test.session {
				select {
				case <-installed:
				case <-time.After(4 * time.Second):
					t.Fatal("first session was not installed")
				}
				challenge := &agentv1.ControllerMessage{Message: &agentv1.ControllerMessage_CapacityRequest{
					CapacityRequest: &agentv1.CapacityRequest{
						ProbeId: uuid.NewString(), ProbeRevision: 1, ExpiresUnixMs: time.Now().Add(time.Minute).UnixMilli(),
						SessionId: first.SessionID, BootId: first.BootID, JournalGeneration: first.JournalGeneration,
					},
				}}
				if err := f.server.Send(first.NodeID, first.SessionID, challenge); err != nil {
					t.Fatal(err)
				}
			}
			select {
			case next := <-installed:
				if next == first.SessionID {
					t.Fatal("fatal handling reused the refused session")
				}
			case <-time.After(4 * time.Second):
				t.Fatal("fatal handling did not reconnect")
			}
			logged, err := os.ReadFile(logs.Name())
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(string(logged), `"reason":"`+test.reason+`"`) || !strings.Contains(string(logged), "agent_stream_ended") || !strings.Contains(string(logged), `"message":"*agentv1.ControllerMessage_`) || strings.Contains(string(logged), "private-failure-token") || strings.Contains(string(logged), "agent_message_refused") {
				t.Fatalf("fatal log misclassified the error or exposed private text: %s", logged)
			}
		})
	}
}

func TestHeartbeatCarriesBoundedAppObservations(t *testing.T) {
	received := make(chan *agentv1.AgentMessage, 1)
	f := newFixture(t, func(_ context.Context, _ controlstream.Peer, message *agentv1.AgentMessage) error {
		if message.GetHeartbeat() != nil {
			select {
			case received <- proto.Clone(message).(*agentv1.AgentMessage):
			default:
			}
		}
		return nil
	}, func(c *controlstream.Config) { c.MaxMessageBytes = 1024 })
	observation := &agentv1.Observation{ResourceId: uuid.NewString(), Lifecycle: "running", Health: "healthy", Ports: []*agentv1.PortBinding{{ContainerPort: 80, HostAddress: "0.0.0.0", HostPort: 32100, Transport: "tcp"}}}
	client, err := transport.New(transport.Config{Endpoints: []string{f.address}, TLS: f.tls, Hello: f.hello, Observations: func() []*agentv1.Observation {
		return []*agentv1.Observation{proto.Clone(observation).(*agentv1.Observation), {ResourceId: strings.Repeat("x", 2048)}}
	}, Handle: func(context.Context, *agentv1.ControllerMessage) (*agentv1.AgentMessage, error) { return nil, nil }})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- client.Run(ctx) }()
	defer func() { cancel(); <-done }()
	select {
	case message := <-received:
		got := message.GetHeartbeat().Observations
		if len(got) != 1 || !proto.Equal(got[0], observation) || proto.Size(message) > 1024 {
			t.Fatalf("heartbeat observations: %v", message)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no observation reached the controller")
	}
}

func TestHeartbeatRotatesObservationsAcrossFrames(t *testing.T) {
	received := make(chan *agentv1.AgentMessage, 16)
	f := newFixture(t, func(_ context.Context, _ controlstream.Peer, message *agentv1.AgentMessage) error {
		if message.GetHeartbeat() != nil {
			select {
			case received <- message:
			default:
			}
		}
		return nil
	}, func(c *controlstream.Config) { c.MaxMessageBytes = 1024 })
	var observations []*agentv1.Observation
	for range 5 {
		observation := &agentv1.Observation{ResourceId: uuid.NewString(), Lifecycle: "running", Health: "healthy"}
		for port := range 20 {
			observation.Ports = append(observation.Ports, &agentv1.PortBinding{ContainerPort: uint32(80 + port), HostPort: uint32(32000 + port), HostAddress: "127.0.0.1", Transport: "tcp"})
		}
		observations = append(observations, observation)
	}
	client, err := transport.New(transport.Config{Endpoints: []string{f.address}, TLS: f.tls, Hello: f.hello, Observations: func() []*agentv1.Observation { return observations }, Handle: func(context.Context, *agentv1.ControllerMessage) (*agentv1.AgentMessage, error) { return nil, nil }})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	done := make(chan error, 1)
	go func() { done <- client.Run(ctx) }()
	defer func() { cancel(); <-done }()
	seen := map[string]bool{}
	for len(seen) < len(observations) {
		select {
		case message := <-received:
			if proto.Size(message) > 1024 {
				t.Fatal("heartbeat exceeds negotiated limit")
			}
			for _, observation := range message.GetHeartbeat().Observations {
				seen[observation.ResourceId] = true
			}
		case <-ctx.Done():
			t.Fatalf("observations starved: received %d of %d", len(seen), len(observations))
		}
	}
}

func TestHeartbeatCarriesReplicaStatus(t *testing.T) {
	received := make(chan *agentv1.AgentMessage, 1)
	f := newFixture(t, func(_ context.Context, _ controlstream.Peer, message *agentv1.AgentMessage) error {
		if message.GetHeartbeat() != nil {
			select {
			case received <- proto.Clone(message).(*agentv1.AgentMessage):
			default:
			}
		}
		return nil
	}, nil)
	status := &agentv1.ReplicaStatus{AppId: uuid.NewString(), Role: "standby", Epoch: 2, Sequence: 7}
	client, err := transport.New(transport.Config{Endpoints: []string{f.address}, TLS: f.tls, Hello: f.hello, Replicas: func() []*agentv1.ReplicaStatus {
		return []*agentv1.ReplicaStatus{proto.Clone(status).(*agentv1.ReplicaStatus)}
	}, Handle: func(context.Context, *agentv1.ControllerMessage) (*agentv1.AgentMessage, error) { return nil, nil }})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- client.Run(ctx) }()
	defer func() { cancel(); <-done }()
	select {
	case message := <-received:
		if got := message.GetHeartbeat().Replicas; len(got) != 1 || !proto.Equal(got[0], status) {
			t.Fatalf("heartbeat replicas: %v", message)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no replica status reached the controller")
	}
}
