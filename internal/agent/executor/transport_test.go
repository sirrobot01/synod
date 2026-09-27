package executor

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/agent/transport"
	"github.com/sirrobot01/synod/internal/controlstream"
	"github.com/sirrobot01/synod/internal/pki"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/proto"
)

type controllerFixture struct {
	server *controlstream.Server
	client *transport.Client
	node   uuid.UUID
	events chan *agentv1.AgentMessage
	cancel context.CancelFunc
}

func startController(t *testing.T, f *fixture, handle func(context.Context, controlstream.Peer, *agentv1.AgentMessage) error) *controllerFixture {
	t.Helper()
	installation := uuid.MustParse(f.epoch.InstallationId)
	node := uuid.MustParse(f.epoch.NodeId)
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
		subject := node
		var names []string
		if role == pki.Controller {
			subject = uuid.New()
			names = []string{"127.0.0.1"}
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
	fixture := &controllerFixture{node: node, events: make(chan *agentv1.AgentMessage, 256)}
	capabilities := []string{controlstream.CommandInspectionCapability}
	if f.e.options.Router != nil {
		capabilities = append(capabilities, controlstream.RouterCapability)
	}
	fixture.server, err = controlstream.New(controlstream.Config{InstallationID: installation, Capabilities: capabilities, TLS: &tls.Config{MinVersion: tls.VersionTLS12, ClientCAs: roots, Certificates: []tls.Certificate{pairs[pki.Controller]}}, HeartbeatInterval: 50 * time.Millisecond, StaleAfter: 5 * time.Second, Authorize: func(_ context.Context, i, n uuid.UUID, leaf *x509.Certificate) error {
		if i != installation || n != node || !leaf.Equal(pairs[pki.Agent].Leaf) {
			return errors.New("unexpected TLS identity")
		}
		return nil
	}, Handle: func(ctx context.Context, peer controlstream.Peer, message *agentv1.AgentMessage) error {
		if handle != nil {
			if err := handle(ctx, peer, message); err != nil {
				return err
			}
		}
		if message.GetHeartbeat() == nil {
			select {
			case fixture.events <- proto.Clone(message).(*agentv1.AgentMessage):
			case <-ctx.Done():
				return ctx.Err()
			}
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
	served := make(chan error, 1)
	go func() { served <- fixture.server.Serve(listener) }()
	t.Cleanup(func() { fixture.server.Stop(); <-served })
	fixture.client, err = transport.New(transport.Config{Endpoints: []string{listener.Addr().String()}, TLS: &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots, Certificates: []tls.Certificate{pairs[pki.Agent]}}, Hello: &agentv1.Hello{InstallationId: installation.String(), NodeId: node.String(), BootId: uuid.NewString(), JournalGeneration: uuid.NewString(), Protocol: &agentv1.VersionRange{Minimum: 1, Maximum: 1}, ActivePayloadVersions: []uint32{1}, Capabilities: capabilities}, BackoffMin: 10 * time.Millisecond, BackoffMax: 30 * time.Millisecond, Handle: f.e.Handle})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	fixture.cancel = cancel
	executorDone, clientDone := make(chan error, 1), make(chan error, 1)
	go func() { executorDone <- f.e.Run(ctx, fixture.client) }()
	go func() { clientDone <- fixture.client.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		for _, done := range []chan error{executorDone, clientDone} {
			select {
			case err := <-done:
				if err != nil && !errors.Is(err, context.Canceled) {
					t.Error(err)
				}
			case <-time.After(15 * time.Second):
				t.Error("shutdown did not finish")
			}
		}
	})
	return fixture
}

func (f *controllerFixture) send(t *testing.T, message *agentv1.ControllerMessage) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for {
		if peer, ok := f.server.Session(f.node); ok {
			if err := f.server.Send(f.node, peer.SessionID, message); err == nil {
				return
			} else if !errors.Is(err, controlstream.ErrBusy) && !errors.Is(err, controlstream.ErrDisconnected) {
				t.Fatal(err)
			}
		}
		select {
		case <-ctx.Done():
			t.Fatal("session send timed out")
		case <-tick.C:
		}
	}
}

func (f *controllerFixture) wait(t *testing.T, match func(*agentv1.AgentMessage) bool) *agentv1.AgentMessage {
	t.Helper()
	timer := time.NewTimer(10 * time.Second)
	defer timer.Stop()
	for {
		select {
		case message := <-f.events:
			if match(message) {
				return message
			}
		case <-timer.C:
			t.Fatal("expected agent event did not arrive")
			return nil
		}
	}
}

func (f *controllerFixture) activate(t *testing.T, epoch *agentv1.Epoch) {
	t.Helper()
	f.send(t, &agentv1.ControllerMessage{Message: &agentv1.ControllerMessage_PrepareEpoch{PrepareEpoch: &agentv1.PrepareEpoch{Epoch: epoch}}})
	f.wait(t, func(message *agentv1.AgentMessage) bool {
		return message.GetBarrierResult() != nil && message.GetBarrierResult().Drained && proto.Equal(message.GetBarrierResult().Epoch, epoch)
	})
	f.send(t, &agentv1.ControllerMessage{Message: &agentv1.ControllerMessage_ActivateEpoch{ActivateEpoch: &agentv1.ActivateEpoch{Epoch: epoch}}})
}

func TestAuthenticatedLoopCompletesAndAcknowledges(t *testing.T) {
	f := newFixture(t, nil)
	controller := startController(t, f, nil)
	controller.activate(t, f.epoch)
	command := volumeCommand(t, f.epoch)
	controller.send(t, &agentv1.ControllerMessage{DeliveryAttemptId: uuid.NewString(), Message: &agentv1.ControllerMessage_Command{Command: command}})
	ack := controller.wait(t, func(message *agentv1.AgentMessage) bool { return message.GetAdmissionAck() != nil })
	if ack.GetAdmissionAck().State != "queued" {
		t.Fatal("admission rejected", ack)
	}
	terminal := controller.wait(t, func(message *agentv1.AgentMessage) bool { return message.GetTerminalResult() != nil }).GetTerminalResult()
	if terminal.State != "succeeded" || terminal.CommandId != command.CommandId {
		t.Fatal(terminal)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for {
		record, err := f.e.journal.Get(command.CommandId)
		if err != nil {
			t.Fatal(err)
		}
		if record.Acknowledged {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("ACK was not durable")
		case <-tick.C:
		}
	}
}
