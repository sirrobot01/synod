package cli_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"os/exec"

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
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	agentcli "github.com/sirrobot01/synod/internal/agent/cli"
	"github.com/sirrobot01/synod/internal/agent/enrollment"
	"github.com/sirrobot01/synod/internal/api"
	"github.com/sirrobot01/synod/internal/auth"
	"github.com/sirrobot01/synod/internal/pki"
	"github.com/sirrobot01/synod/internal/store/postgres"
)

func TestExecutableRecoversCommittedEnrollment(t *testing.T) {
	dsn := os.Getenv("SYNOD_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("run scripts/test-postgres.sh")
	}
	adminPool, err := pgxpool.New(t.Context(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(adminPool.Close)
	database := "agent_cli_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if _, err := adminPool.Exec(t.Context(), "CREATE DATABASE "+pgx.Identifier{database}.Sanitize()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if _, err := adminPool.Exec(ctx, "DROP DATABASE "+pgx.Identifier{database}.Sanitize()+" WITH (FORCE)"); err != nil {
			t.Error(err)
		}
	})
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	config.ConnConfig.Database = database
	pool, err := pgxpool.NewWithConfig(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	store := postgres.New(pool)
	if err := store.Migrate(t.Context()); err != nil {
		t.Fatal(err)
	}
	binary := filepath.Join(t.TempDir(), "synod")
	build := exec.CommandContext(t.Context(), "go", "build", "-race", "-o", binary, "../../../cmd/synod")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build: %v %s", err, output)
	}
	identity := enrollment.Identity{InstallationID: uuid.New(), NodeID: uuid.New()}
	adminToken := strings.Repeat("integration-admin-", 3)
	admin, err := auth.NewAdminToken(adminToken, "test-admin")
	if err != nil {
		t.Fatal(err)
	}
	caCert, caKey, err := pki.CreateCA(identity.InstallationID, time.Now(), 7*24*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	authority, err := pki.LoadCA(identity.InstallationID, caCert, caKey)
	if err != nil {
		t.Fatal(err)
	}
	server, err := api.NewServer(store, admin, identity.InstallationID, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
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
	caPath := filepath.Join(parent, "api-ca.pem")
	if err := os.WriteFile(caPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: https.Certificate().Raw}), 0o644); err != nil {
		t.Fatal(err)
	}
	common := []string{"--installation-id", identity.InstallationID.String(), "--node-id", identity.NodeID.String(), "--state-dir", statePath}
	remote := []string{"--api-url", https.URL, "--token-file", tokenPath, "--ca-file", caPath, "--attempts", "1"}
	run := func(mode string, extra []string) ([]byte, []byte, error) {
		t.Helper()
		args := append([]string{"agent", "enroll", mode}, common...)
		args = append(args, extra...)
		command := exec.CommandContext(t.Context(), binary, args...)
		var stdout, stderr bytes.Buffer
		command.Stdout = &stdout
		command.Stderr = &stderr
		err := command.Run()
		if bytes.Contains(stdout.Bytes(), []byte(token)) || bytes.Contains(stderr.Bytes(), []byte(token)) || bytes.Contains(stdout.Bytes(), []byte("PRIVATE KEY")) {
			t.Fatal("secret in command output")
		}
		return stdout.Bytes(), stderr.Bytes(), err
	}
	stdout, stderr, err := run("bootstrap", remote)
	var failure agentcli.Failure
	if err == nil || len(stdout) != 0 || json.Unmarshal(stderr, &failure) != nil || failure.Code != "transport_failed" {
		t.Fatalf("lost reply: %v %s", err, stderr)
	}
	var count int
	if err := pool.QueryRow(t.Context(), "SELECT count(*) FROM node_certificates").Scan(&count); err != nil || count != 1 {
		t.Fatalf("committed count=%d err=%v", count, err)
	}
	state, err := enrollment.Open(statePath, enrollment.Options{Identity: identity})
	if err != nil {
		t.Fatal(err)
	}
	first, err := state.Request()
	if err != nil {
		t.Fatal(err)
	}
	if err := state.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(t.Context(), "UPDATE enrollment_tokens SET created_at=clock_timestamp()-interval '2 minutes', expires_at=clock_timestamp()-interval '1 second' WHERE node_id=$1", identity.NodeID); err != nil {
		t.Fatal(err)
	}
	stdout, stderr, err = run("retry", remote)
	if err != nil || len(stderr) != 0 {
		t.Fatalf("retry: %v %s", err, stderr)
	}
	var public agentcli.Identity
	decoder := json.NewDecoder(bytes.NewReader(stdout))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&public); err != nil {
		t.Fatal(err)
	}
	if public.InstallationID != identity.InstallationID || public.NodeID != identity.NodeID || public.RequestID != first.RequestID || public.CertificateID == uuid.Nil || public.ExpiresAt.IsZero() || public.AcceptedAt.IsZero() {
		t.Fatal("wrong public identity")
	}
	state, err = enrollment.Open(statePath, enrollment.Options{Identity: identity})
	if err != nil {
		t.Fatal(err)
	}
	credentials, err := state.Credentials()
	if err != nil {
		t.Fatal(err)
	}
	replay, err := state.Request()
	if err != nil || !bytes.Equal(replay.CSRDER, first.CSRDER) {
		t.Fatal("retry changed CSR")
	}
	if err := store.CheckAgentCertificate(t.Context(), identity.InstallationID, identity.NodeID, credentials.Certificate.Leaf); err != nil {
		t.Fatal(err)
	}
	if err := state.Close(); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(t.Context(), "SELECT count(*) FROM node_certificates").Scan(&count); err != nil || count != 1 {
		t.Fatal("retry created a certificate")
	}
	if err := os.Remove(tokenPath); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(caPath); err != nil {
		t.Fatal(err)
	}
	https.Close()
	local, stderr, err := run("retry", nil)
	if err != nil || len(stderr) != 0 || !bytes.Equal(local, stdout) || enrollmentCalls.Load() != 2 {
		t.Fatalf("local reuse: %v %s", err, stderr)
	}
	entries, err := os.ReadDir(statePath)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 {
		t.Fatal("unexpected state or journal files")
	}
}
