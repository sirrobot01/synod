package postgres

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sirrobot01/synod/internal/app"
)

func testStore(t *testing.T) *Store {
	t.Helper()
	dsn := os.Getenv("SYNOD_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set SYNOD_TEST_DATABASE_URL or run scripts/test-postgres.sh")
	}
	admin, err := pgxpool.New(t.Context(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	name := "task010_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if _, err := admin.Exec(t.Context(), "CREATE DATABASE "+pgx.Identifier{name}.Sanitize()); err != nil {
		admin.Close()
		t.Fatal(err)
	}
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	config.ConnConfig.Database = name
	config.MaxConns = 40
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
	store := New(pool)
	if err := store.Migrate(t.Context()); err != nil {
		t.Fatal(err)
	}
	return store
}

// testRequest admits one tier 3 app in a new installation.
func testRequest() CreateAppRequest {
	return CreateAppRequest{
		Identity:  RequestIdentity{InstallationID: uuid.New(), ActorID: "actor-1", APIOperation: CreateAppAPIOperation, IdempotencyKey: uuid.NewString()},
		RequestID: "request-1",
		Spec:      app.AppSpec{SchemaVersion: 1, Name: "web", Tier: app.TierParallel, Copies: 1, Service: app.ServiceSpec{Image: "nginx", Ports: []app.PortSpec{{ContainerPort: 80}}}, Router: app.RouterSpec{Protocol: "tcp", Port: 8080}},
	}
}

func requireCode(t *testing.T, err error, want ErrorCode) {
	t.Helper()
	got, ok := errors.AsType[*AdmissionError](err)
	if !ok || got.Code != want {
		t.Fatalf("error = %#v, want %s", err, want)
	}
}

func waitForKeyWait(t *testing.T, s *Store) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		var blocked bool
		if err := s.pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND wait_event='transactionid')`).Scan(&blocked); err != nil {
			t.Fatal(err)
		}
		if blocked {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatal("no transaction key wait observed")
		case <-ticker.C:
		}
	}
}

func TestErrorClassification(t *testing.T) {
	for constraint, want := range map[string]ErrorCode{"apps_active_name": NameConflict, "operations_active_app": OperationConflict, "unrelated": DependencyUnavailable} {
		cause := &pgconn.PgError{Code: "23505", ConstraintName: constraint, Message: "secret connection detail"}
		err := admissionError(fmt.Errorf("wrapped: %w", cause))
		requireCode(t, err, want)
		if !errors.Is(err, cause) || strings.Contains(err.Error(), "secret") {
			t.Fatal("cause or redaction lost")
		}
	}
}

func TestMigrations(t *testing.T) {
	s := testStore(t)
	if err := s.Migrate(t.Context()); err != nil {
		t.Fatal(err)
	}
	entries, err := migrations.ReadDir("migrations")
	if err != nil {
		t.Fatal(err)
	}
	var count int
	if err := s.pool.QueryRow(t.Context(), `SELECT count(*) FROM schema_migrations`).Scan(&count); err != nil || count != len(entries) {
		t.Fatalf("migration count %d, want %d: %v", count, len(entries), err)
	}
	for _, entry := range entries {
		prefix, _, _ := strings.Cut(entry.Name(), "_")
		version, err := strconv.Atoi(prefix)
		if err != nil {
			t.Fatal(err)
		}
		var checksum []byte
		if err := s.pool.QueryRow(t.Context(), `SELECT checksum FROM schema_migrations WHERE version=$1`, version).Scan(&checksum); err != nil {
			t.Fatal(err)
		}
		if _, err := s.pool.Exec(t.Context(), `UPDATE schema_migrations SET checksum='bad'::bytea WHERE version=$1`, version); err != nil {
			t.Fatal(err)
		}
		if err := s.Migrate(t.Context()); err == nil {
			t.Fatalf("accepted changed migration %d", version)
		}
		if _, err := s.pool.Exec(t.Context(), `UPDATE schema_migrations SET checksum=$1 WHERE version=$2`, checksum, version); err != nil {
			t.Fatal(err)
		}
	}
}

// dropTimeout bounds one test database teardown. It exists to stop a hang, not to
// assert how fast PostgreSQL drops a database under load.
const dropTimeout = 60 * time.Second
