package daemon

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/agent/enrollment"
	"github.com/sirrobot01/synod/internal/controlstream"
	"github.com/sirrobot01/synod/internal/pki"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
)

type lockedBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *lockedBuffer) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *lockedBuffer) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

// The agent asks for a certificate inside the renewal window, saves it, and connects with it.
func TestAgentRenewsItsCertificateAndReconnectsWithIt(t *testing.T) {
	host := os.Getenv("SYNOD_TEST_DOCKER_HOST")
	if !strings.HasPrefix(host, "unix://") {
		t.Skip("set SYNOD_TEST_DOCKER_HOST=unix://SOCKET for the agent runtime")
	}
	renewalSettle = 0
	t.Cleanup(func() { renewalSettle = 2 * time.Minute })
	installation, node := uuid.New(), uuid.New()
	rootPEM, rootKey, err := pki.CreateCA(installation, time.Now(), 60*24*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	ca, err := pki.LoadCA(installation, rootPEM, rootKey)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AppendCertsFromPEM(rootPEM)

	// Enroll with a five-day certificate. That is inside the renewal window.
	enrollmentServer := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request enrollment.Request
		if json.NewDecoder(r.Body).Decode(&request) != nil {
			w.WriteHeader(400)
			return
		}
		certificate, err := ca.Issue(request.CSRDER, pki.Identity{InstallationID: installation, Role: pki.Agent, SubjectID: node}, nil, time.Now(), 5*24*time.Hour)
		if err != nil {
			w.WriteHeader(400)
			return
		}
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(enrollment.Response{NodeID: node, CertificateID: uuid.New(), CertificatePEM: string(certificate), CACertificatePEM: string(rootPEM), AcceptedAt: time.Now().UTC()})
	}))
	enrollmentServer.Config.ErrorLog = log.New(io.Discard, "", 0)
	enrollmentServer.StartTLS()
	defer enrollmentServer.Close()
	enrollmentRoots := x509.NewCertPool()
	enrollmentRoots.AddCert(enrollmentServer.Certificate())
	dir := t.TempDir()
	tokenFile := filepath.Join(dir, "token")
	if err := os.WriteFile(tokenFile, []byte(base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{7}, 32))), 0o600); err != nil {
		t.Fatal(err)
	}
	stateDir := filepath.Join(dir, "enrollment")
	state, err := enrollment.Open(stateDir, enrollment.Options{Identity: enrollment.Identity{InstallationID: installation, NodeID: node}, AllowCreate: true})
	if err != nil {
		t.Fatal(err)
	}
	enrolled, err := state.Submit(t.Context(), enrollment.Config{Endpoint: enrollmentServer.URL, TokenFile: tokenFile, RootCAs: enrollmentRoots, Attempts: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err := state.Close(); err != nil {
		t.Fatal(err)
	}

	// The controller transport issues a full certificate for the enrolled key.
	_, controllerKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	csr, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{}, controllerKey)
	if err != nil {
		t.Fatal(err)
	}
	controllerPEM, err := ca.Issue(csr, pki.Identity{InstallationID: installation, Role: pki.Controller, SubjectID: uuid.New()}, []string{"127.0.0.1"}, time.Now(), 24*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	der, _ := x509.MarshalPKCS8PrivateKey(controllerKey)
	controllerPair, err := tls.X509KeyPair(controllerPEM, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}))
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var renewedSerial string
	sessionSerials := make(chan string, 16)
	var server *controlstream.Server
	server, err = controlstream.New(controlstream.Config{
		InstallationID: installation, TLS: &tls.Config{MinVersion: tls.VersionTLS12, ClientCAs: roots, Certificates: []tls.Certificate{controllerPair}},
		Capabilities: []string{"runtime.docker", "apps.discovery", "destinations", "destination.serve", "destination.health", "backup.exec", "restore.exec", "proof.exec", "files.read", "certificate.renew"},
		Authorize: func(_ context.Context, _, subject uuid.UUID, certificate *x509.Certificate) error {
			if subject != node {
				return errors.New("unknown node")
			}
			select {
			case sessionSerials <- certificate.SerialNumber.Text(16):
			default:
			}
			return nil
		},
		Handle: func(_ context.Context, peer controlstream.Peer, message *agentv1.AgentMessage) error {
			request := message.GetCertificateRequest()
			if request == nil {
				return nil
			}
			parsed, err := x509.ParseCertificateRequest(request.CsrDer)
			if err != nil || !parsed.PublicKey.(ed25519.PublicKey).Equal(enrolled.Certificate.Leaf.PublicKey) {
				return errors.New("the renewal request does not carry the enrolled key")
			}
			certificate, err := ca.Issue(request.CsrDer, pki.Identity{InstallationID: installation, Role: pki.Agent, SubjectID: node}, nil, time.Now(), 30*24*time.Hour)
			if err != nil {
				return err
			}
			block, _ := pem.Decode(certificate)
			leaf, _ := x509.ParseCertificate(block.Bytes)
			mu.Lock()
			renewedSerial = leaf.SerialNumber.Text(16)
			mu.Unlock()
			return server.Send(peer.NodeID, peer.SessionID, &agentv1.ControllerMessage{Message: &agentv1.ControllerMessage_CertificateIssued{CertificateIssued: &agentv1.CertificateIssued{CertificateId: uuid.NewString(), CertificatePem: string(certificate), CaCertificatePem: string(rootPEM)}}})
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer server.Stop()
	go server.Serve(listener)

	free, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	blobAddress := free.Addr().String()
	free.Close()
	ctx, cancel := context.WithCancel(t.Context())
	var logs lockedBuffer
	done := make(chan error, 1)
	go func() {
		done <- Run(ctx, Config{InstallationID: installation.String(), NodeID: node.String(), EnrollmentDir: stateDir, ControllerEndpoints: []string{listener.Addr().String()}, DockerSocket: strings.TrimPrefix(host, "unix://"), DestinationListen: blobAddress}, &logs)
	}()
	first := enrolled.Certificate.Leaf.SerialNumber.Text(16)
	deadline := time.After(90 * time.Second)
	for reconnected := false; !reconnected; {
		select {
		case serial := <-sessionSerials:
			mu.Lock()
			reconnected = renewedSerial != "" && serial == renewedSerial
			mu.Unlock()
			if serial != first && !reconnected {
				t.Fatalf("the agent presented an unknown certificate %s", serial)
			}
		case err := <-done:
			t.Fatalf("the agent stopped: %v\n%s", err, logs.String())
		case <-deadline:
			t.Fatalf("the agent did not reconnect with a renewed certificate\n%s", logs.String())
		}
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("the agent did not stop cleanly: %v", err)
	}
	if !strings.Contains(logs.String(), "certificate_renewed") || !strings.Contains(logs.String(), "agent_restarting") {
		t.Fatalf("the agent did not report its renewal\n%s", logs.String())
	}
	reopened, err := enrollment.Open(stateDir, enrollment.Options{Identity: enrollment.Identity{InstallationID: installation, NodeID: node}})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	saved, err := reopened.Credentials()
	if err != nil || saved.Certificate.Leaf.SerialNumber.Text(16) != renewedSerial || !bytes.Equal(saved.Certificate.Leaf.RawSubjectPublicKeyInfo, enrolled.Certificate.Leaf.RawSubjectPublicKeyInfo) {
		t.Fatal("the agent did not save the renewed certificate for its key", err)
	}
}
