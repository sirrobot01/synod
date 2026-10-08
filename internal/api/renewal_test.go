package api

import (
	"bytes"
	"crypto/rand"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/agent/enrollment"
	"github.com/sirrobot01/synod/internal/auth"
	"github.com/sirrobot01/synod/internal/pki"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
)

func TestDeviceRenewsWithBoundTokenAndKeepsItsKeyAndApproval(t *testing.T) {
	metadata, pool := apiTestDatabase(t)
	identity := enrollment.Identity{InstallationID: uuid.New(), NodeID: uuid.New()}
	adminToken := strings.Repeat("renewal-admin-token-", 3)
	admin, err := auth.NewAdminToken(adminToken, "test-admin")
	if err != nil {
		t.Fatal(err)
	}
	caCert, caKey, err := pki.CreateCA(identity.InstallationID, time.Now(), 60*24*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	authority, err := pki.LoadCA(identity.InstallationID, caCert, caKey)
	if err != nil {
		t.Fatal(err)
	}
	server, err := NewServer(metadata, admin, identity.InstallationID, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	server.EnrollmentAuthority = authority
	https := httptest.NewTLSServer(server)
	defer https.Close()
	roots := x509.NewCertPool()
	roots.AddCert(https.Certificate())
	parent := t.TempDir()
	token := func(name string, bound bool) string {
		t.Helper()
		raw := make([]byte, 32)
		rand.Read(raw)
		secret := base64.RawURLEncoding.EncodeToString(raw)
		body := map[string]any{"token": secret, "ttl_seconds": 600}
		if bound {
			body["node_id"] = identity.NodeID
		}
		encoded, _ := json.Marshal(body)
		request, err := http.NewRequestWithContext(t.Context(), "POST", https.URL+"/v1/enrollment-tokens", bytes.NewReader(encoded))
		if err != nil {
			t.Fatal(err)
		}
		request.Header.Set("Authorization", "Bearer "+adminToken)
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("Idempotency-Key", name)
		response, err := https.Client().Do(request)
		if err != nil {
			t.Fatal(err)
		}
		io.Copy(io.Discard, response.Body)
		response.Body.Close()
		if response.StatusCode != 201 {
			t.Fatalf("token %s status=%d", name, response.StatusCode)
		}
		path := filepath.Join(parent, name)
		if err := os.WriteFile(path, []byte(secret), 0o600); err != nil {
			t.Fatal(err)
		}
		return path
	}
	state, err := enrollment.Open(filepath.Join(parent, "enrollment"), enrollment.Options{Identity: identity, AllowCreate: true})
	if err != nil {
		t.Fatal(err)
	}
	defer state.Close()
	first, err := state.Submit(t.Context(), enrollment.Config{Endpoint: https.URL, TokenFile: token("join", false), RootCAs: roots, Attempts: 1})
	if err != nil {
		t.Fatal(err)
	}
	session := uuid.New()
	if err := metadata.RecordAgentSession(t.Context(), identity.NodeID, session, &agentv1.Hello{NodeId: identity.NodeID.String(), Architecture: "arm64"}); err != nil {
		t.Fatal(err)
	}
	if _, err := metadata.ApproveNode(t.Context(), identity.NodeID, "admin", "approve"); err != nil {
		t.Fatal(err)
	}

	renewal := enrollment.Config{Endpoint: https.URL, TokenFile: token("renew", true), RootCAs: roots, Attempts: 1}
	renewed, err := state.RenewWithToken(t.Context(), renewal)
	if err != nil {
		t.Fatal(err)
	}
	if renewed.CertificateID == first.CertificateID || !bytes.Equal(renewed.Certificate.Leaf.RawSubjectPublicKeyInfo, first.Certificate.Leaf.RawSubjectPublicKeyInfo) {
		t.Fatal("renewal did not issue a new certificate for the same key")
	}
	if renewed.AgentEndpoint != first.AgentEndpoint {
		t.Fatal("renewal lost the agent endpoint")
	}
	again, err := state.RenewWithToken(t.Context(), renewal)
	if err != nil || again.CertificateID != renewed.CertificateID {
		t.Fatalf("a repeated renewal with the same token issued another certificate: %v", err)
	}
	node, err := metadata.GetNode(t.Context(), identity.NodeID)
	if err != nil || node.ApprovedAt == nil {
		t.Fatal("renewal lost the device approval", err)
	}
	var certificates int
	if err := pool.QueryRowContext(t.Context(), "SELECT count(*) FROM node_certificates WHERE node_id=?", identity.NodeID).Scan(&certificates); err != nil || certificates != 2 {
		t.Fatalf("certificates=%d err=%v", certificates, err)
	}
	if err := metadata.CheckAgentCertificate(t.Context(), identity.NodeID, renewed.Certificate.Leaf); err != nil {
		t.Fatalf("the controller does not accept the renewed certificate: %v", err)
	}

	// A bound token does not let another key take over the device.
	other, err := enrollment.Open(filepath.Join(parent, "other"), enrollment.Options{Identity: identity, AllowCreate: true})
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	if _, err := other.Submit(t.Context(), enrollment.Config{Endpoint: https.URL, TokenFile: token("takeover", true), RootCAs: roots, Attempts: 1}); !errors.Is(err, enrollment.ErrConflict) {
		t.Fatalf("another key took over the device: %v", err)
	}
}
