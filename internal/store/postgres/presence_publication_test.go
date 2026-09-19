package postgres

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/controlstream"
	"github.com/sirrobot01/synod/internal/pki"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestAgentPresencePublicationOverMutualTLS(t *testing.T) {
	store, registration, request, ca := enrollmentFixture(t)
	roots := x509.NewCertPool()
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
			if _, err := store.RegisterEnrollmentToken(t.Context(), registration); err != nil {
				t.Fatal(err)
			}
			request.CSRDER = csr
			enrolled, err := store.EnrollAgent(t.Context(), request, ca)
			if err != nil {
				t.Fatal(err)
			}
			leaf = []byte(enrolled.CertificatePEM)
			if !roots.AppendCertsFromPEM([]byte(enrolled.CACertificatePEM)) {
				t.Fatal("invalid fixture CA")
			}
		} else {
			leaf, err = ca.Issue(csr, pki.Identity{InstallationID: request.InstallationID, Role: role, SubjectID: uuid.New()}, []string{"127.0.0.1"}, time.Now(), time.Hour)
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
	firstCommitted := make(chan uuid.UUID, 1)
	releaseFirst := make(chan struct{})
	secondAuthorized := make(chan struct{})
	secondEntered := make(chan struct{}, 1)
	heartbeatStored := make(chan error, 1)
	var authorizations, hellos atomic.Int32
	server, err := controlstream.New(controlstream.Config{
		InstallationID:  request.InstallationID,
		TLS:             &tls.Config{MinVersion: tls.VersionTLS12, ClientCAs: roots, Certificates: []tls.Certificate{pairs[pki.Controller]}},
		RecheckInterval: time.Hour,
		Authorize: func(ctx context.Context, installation, node uuid.UUID, leaf *x509.Certificate) error {
			if err := store.CheckAgentCertificate(ctx, installation, node, leaf); err != nil {
				return err
			}
			// The first stream has two checks before its paused Hello callback.
			// No writer or periodic checks can run before publication.
			if authorizations.Add(1) == 4 {
				close(secondAuthorized)
			}
			return nil
		},
		Handle: func(ctx context.Context, peer controlstream.Peer, message *agentv1.AgentMessage) error {
			sessionID, err := uuid.Parse(peer.SessionID)
			if err != nil {
				return err
			}
			if hello := message.GetHello(); hello != nil {
				order := hellos.Add(1)
				if order == 2 {
					secondEntered <- struct{}{}
				}
				if err := store.RecordAgentSession(ctx, peer.InstallationID, peer.NodeID, sessionID, hello); err != nil {
					return err
				}
				if order == 1 {
					firstCommitted <- sessionID
					select {
					case <-releaseFirst:
					case <-ctx.Done():
						return ctx.Err()
					}
				}
				return nil
			}
			if heartbeat := message.GetHeartbeat(); heartbeat != nil {
				err := store.RecordAgentHeartbeat(ctx, peer.InstallationID, peer.NodeID, sessionID, heartbeat)
				heartbeatStored <- err
				return err
			}
			return errors.New("unexpected fixture message")
		},
	})
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
	hello := &agentv1.AgentMessage{Message: &agentv1.AgentMessage_Hello{Hello: &agentv1.Hello{InstallationId: request.InstallationID.String(), NodeId: request.NodeID.String(), BootId: uuid.NewString(), JournalGeneration: uuid.NewString(), Protocol: &agentv1.VersionRange{Minimum: 1, Maximum: 1}, ActivePayloadVersions: []uint32{1}}}}
	first, err := agentv1.NewAgentServiceClient(connection).Connect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Send(hello); err != nil {
		t.Fatal(err)
	}
	var firstID uuid.UUID
	select {
	case firstID = <-firstCommitted:
	case <-ctx.Done():
		t.Fatal("first Hello did not commit")
	}
	if _, ok := server.Session(request.NodeID); ok {
		t.Fatal("first session published before durable callback completed")
	}
	presence, err := store.GetAgentPresence(ctx, request.InstallationID, request.NodeID)
	if err != nil || presence.SessionID != firstID {
		t.Fatal("first commit was not visible")
	}
	second, err := agentv1.NewAgentServiceClient(connection).Connect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := second.Send(hello); err != nil {
		t.Fatal(err)
	}
	select {
	case <-secondAuthorized:
	case <-ctx.Done():
		t.Fatal("second Hello did not reach publication boundary")
	}
	// The same-package synctest proves the guard wait deterministically. This
	// real-network check catches entry into Store while the first commit is paused.
	select {
	case <-secondEntered:
		t.Fatal("second Hello passed the paused publication boundary")
	case <-time.After(100 * time.Millisecond):
	}
	close(releaseFirst)
	response, err := second.Recv()
	if err != nil || response.GetSession() == nil {
		t.Fatalf("second session was not published: %v", err)
	}
	current, ok := server.Session(request.NodeID)
	presence, err = store.GetAgentPresence(ctx, request.InstallationID, request.NodeID)
	if err != nil || !ok || current.SessionID != response.GetSession().SessionId || presence.SessionID.String() != current.SessionID || presence.SessionID == firstID {
		t.Fatalf("live and durable sessions disagree: %v", err)
	}
	if err := second.Send(&agentv1.AgentMessage{Message: &agentv1.AgentMessage_Heartbeat{Heartbeat: &agentv1.Heartbeat{Sequence: 1, ObservedAt: timestamppb.Now()}}}); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-heartbeatStored:
		if err != nil {
			t.Fatalf("current heartbeat rejected: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("current heartbeat did not commit")
	}
	presence, err = store.GetAgentPresence(ctx, request.InstallationID, request.NodeID)
	if err != nil || presence.Heartbeat == nil || presence.Heartbeat.Sequence != 1 || presence.SessionID.String() != current.SessionID {
		t.Fatal("heartbeat did not preserve current session")
	}
}
