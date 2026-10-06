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
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/agent/enrollment"
	"github.com/sirrobot01/synod/internal/auth"
	"github.com/sirrobot01/synod/internal/pki"
)

func TestAgentEnrollmentRecoversCommittedHTTPReply(t *testing.T) {
	metadata, pool := apiTestDatabase(t)
	identity := enrollment.Identity{InstallationID: uuid.New(), NodeID: uuid.New()}
	adminToken := strings.Repeat("integration-admin-", 3)
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
	var drop atomic.Bool
	drop.Store(true)
	var enrollmentCalls atomic.Int32
	https := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/enrollments" {
			enrollmentCalls.Add(1)
			if drop.CompareAndSwap(true, false) {
				// Complete the actual Store transaction, then lose its entire HTTP reply.
				recorded := httptest.NewRecorder()
				server.ServeHTTP(recorded, r)
				if recorded.Code != http.StatusCreated {
					t.Errorf("fixture enrollment did not commit: %d", recorded.Code)
				}
				connection, _, err := w.(http.Hijacker).Hijack()
				if err != nil {
					t.Error(err)
					return
				}
				connection.Close()
				return
			}
		}
		server.ServeHTTP(w, r)
	}))
	defer https.Close()
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		t.Fatal(err)
	}
	token := base64.RawURLEncoding.EncodeToString(raw)
	registration, err := json.Marshal(map[string]any{"node_id": identity.NodeID, "token": token, "ttl_seconds": 600})
	if err != nil {
		t.Fatal(err)
	}
	req, err := http.NewRequestWithContext(t.Context(), "POST", https.URL+"/v1/enrollment-tokens", bytes.NewReader(registration))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+adminToken)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", "register-agent")
	resp, err := https.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 201 {
		t.Fatalf("registration status=%d", resp.StatusCode)
	}
	parent := t.TempDir()
	tokenPath := filepath.Join(parent, "token")
	if err := os.WriteFile(tokenPath, []byte(token), 0o600); err != nil {
		t.Fatal(err)
	}
	statePath := filepath.Join(parent, "enrollment")
	state, err := enrollment.Open(statePath, enrollment.Options{Identity: identity, AllowCreate: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if state != nil {
			state.Close()
		}
	}()
	first, err := state.Request()
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AddCert(https.Certificate())
	config := enrollment.Config{Endpoint: https.URL, TokenFile: tokenPath, RootCAs: roots, Attempts: 1}
	if _, err := state.Submit(t.Context(), config); !errors.Is(err, enrollment.ErrTransport) {
		t.Fatalf("lost reply outcome=%v", err)
	}
	var count int
	if err := pool.QueryRowContext(t.Context(), "SELECT count(*) FROM node_certificates").Scan(&count); err != nil || count != 1 {
		t.Fatalf("committed certificate count=%d err=%v", count, err)
	}
	if err := state.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.ExecContext(t.Context(), "UPDATE enrollment_tokens SET created_at=created_at-120000, expires_at=created_at-119000 WHERE node_id=?", identity.NodeID); err != nil {
		t.Fatal(err)
	}
	state, err = enrollment.Open(statePath, enrollment.Options{Identity: identity})
	if err != nil {
		t.Fatal(err)
	}
	replay, err := state.Request()
	if err != nil || replay.RequestID != first.RequestID || !bytes.Equal(replay.CSRDER, first.CSRDER) {
		t.Fatal("restart changed the enrollment request")
	}
	credentials, err := state.Submit(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	if credentials.Identity != identity || credentials.RequestID != first.RequestID || credentials.CACertificatePEM != string(caCert) {
		t.Fatal("incorrect enrolled identity")
	}
	if err := metadata.CheckAgentCertificate(t.Context(), identity.NodeID, credentials.Certificate.Leaf); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRowContext(t.Context(), "SELECT count(*) FROM node_certificates").Scan(&count); err != nil || count != 1 {
		t.Fatalf("replay created certificates: %d %v", count, err)
	}
	if err := state.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(tokenPath); err != nil {
		t.Fatal(err)
	}
	state, err = enrollment.Open(statePath, enrollment.Options{Identity: identity})
	if err != nil {
		t.Fatal(err)
	}
	local, err := state.Submit(t.Context(), enrollment.Config{})
	if err != nil || local.CertificateID != credentials.CertificateID || enrollmentCalls.Load() != 2 {
		t.Fatalf("durable completion repeated HTTP: calls=%d err=%v", enrollmentCalls.Load(), err)
	}
}
