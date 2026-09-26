package cli

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"log/slog"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/sirrobot01/synod/internal/agent/daemon"
	"github.com/sirrobot01/synod/internal/agent/enrollment"
	"github.com/sirrobot01/synod/internal/api"
	"github.com/sirrobot01/synod/internal/auth"
	"github.com/sirrobot01/synod/internal/jointoken"
	"github.com/sirrobot01/synod/internal/pki"
	"github.com/sirrobot01/synod/internal/store/postgres"
)

// One join token enrolls one node, and a rerun resumes a join that stopped early.
func TestJoinEnrollsAndConfiguresTheAgent(t *testing.T) {
	dsn := os.Getenv("SYNOD_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("run scripts/test-postgres.sh")
	}
	adminPool, err := pgxpool.New(t.Context(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(adminPool.Close)
	database := "agent_join_" + strings.ReplaceAll(uuid.NewString(), "-", "")
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

	installation := uuid.New()
	admin, err := auth.NewAdminToken(strings.Repeat("join-admin-", 4), "test-admin")
	if err != nil {
		t.Fatal(err)
	}
	caCert, caKey, err := pki.CreateCA(installation, time.Now(), 7*24*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	authority, err := pki.LoadCA(installation, caCert, caKey)
	if err != nil {
		t.Fatal(err)
	}
	server, err := api.NewServer(store, admin, installation, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	server.EnrollmentAuthority = authority
	server.AgentListen = "0.0.0.0:8444"
	// The httptest certificate is a self-signed CA, so it is also the pinned API CA.
	https := httptest.NewTLSServer(server)
	t.Cleanup(https.Close)

	token := jointoken.Token{Address: https.Listener.Addr().String(), InstallationID: installation, Fingerprint: jointoken.Fingerprint(https.Certificate().Raw), Secret: jointoken.NewSecret()}
	if _, err := store.RegisterEnrollmentToken(t.Context(), postgres.TokenRegistration{InstallationID: installation, ActorID: "test", RequestID: uuid.NewString(), IdempotencyKey: uuid.NewString(), TokenVerifier: sha256.Sum256([]byte(token.Secret)), TTLSeconds: 600}); err != nil {
		t.Fatal(err)
	}
	parent := t.TempDir()
	if err := os.Chmod(parent, 0o700); err != nil {
		t.Fatal(err)
	}
	options := joinOptions{dir: filepath.Join(parent, "agent"), storageID: "local", dockerSocket: strings.TrimPrefix(os.Getenv("SYNOD_TEST_DOCKER_HOST"), "unix://"), systemdDir: filepath.Join(parent, "absent")}
	var stdout, stderr bytes.Buffer

	wrong := token
	wrong.Fingerprint = jointoken.Fingerprint([]byte("another CA"))
	if err := join(t.Context(), jointoken.Encode(wrong), joinOptions{dir: filepath.Join(parent, "wrong")}, &stdout, &stderr); !errors.Is(err, enrollment.ErrCertificate) {
		t.Fatalf("join with a wrong pin: %v, want a certificate error", err)
	}

	// Before a controller runner records a generation, the token stays unconsumed.
	if err := join(t.Context(), jointoken.Encode(token), options, &stdout, &stderr); !errors.Is(err, enrollment.ErrUnavailable) {
		t.Fatalf("join before a recovery generation: %v, want outcome unavailable", err)
	}
	var consumed bool
	if err := pool.QueryRow(t.Context(), `SELECT consumed_at IS NOT NULL FROM enrollment_tokens`).Scan(&consumed); err != nil || consumed {
		t.Fatalf("consumed=%v err=%v, want the join token unconsumed", consumed, err)
	}
	generation := uuid.New()
	if err := store.InitializeGeneration(t.Context(), installation, generation); err != nil {
		t.Fatal(err)
	}
	if options.dockerSocket == "" {
		t.Skip("set SYNOD_TEST_DOCKER_HOST to initialize the agent journal")
	}
	// The rerun repeats the saved request and completes the join.
	if err := join(t.Context(), jointoken.Encode(token), options, &stdout, &stderr); err != nil {
		t.Fatalf("resumed join: %v\n%s", err, stderr.String())
	}
	joined, err := daemon.Load(filepath.Join(options.dir, "agent.json"))
	if err != nil {
		t.Fatal(err)
	}
	if joined.InstallationID != installation.String() || joined.RecoveryGeneration != generation.String() || len(joined.ControllerEndpoints) != 1 || joined.ControllerEndpoints[0] != "127.0.0.1:8444" {
		t.Fatalf("config=%+v, want the installation, generation, and the dialed host with the agent port", joined)
	}
	if !strings.Contains(stdout.String(), "synod node approve "+joined.NodeID) {
		t.Fatalf("output does not name the approval command: %s", stdout.String())
	}
	for name, want := range map[string]bool{"join.token": false, "synod-agent.service": true, "journal": true} {
		if _, err := os.Stat(filepath.Join(options.dir, name)); (err == nil) != want {
			t.Fatalf("%s present=%v, want %v", name, err == nil, want)
		}
	}
	if err := join(t.Context(), jointoken.Encode(token), options, &stdout, &stderr); !errors.Is(err, enrollment.ErrState) {
		t.Fatalf("second join: %v, want a state error", err)
	}
}
