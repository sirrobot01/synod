package coordinator

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sirrobot01/synod/internal/store/postgres"
)

func testPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	databaseURL := os.Getenv("SYNOD_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("run scripts/test-postgres.sh")
	}
	cfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	admin, err := pgx.Connect(t.Context(), databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	name := "synod_test_" + strings.ReplaceAll(uuid.NewString(), "-", "_")
	if _, err := admin.Exec(t.Context(), "CREATE DATABASE "+pgx.Identifier{name}.Sanitize()); err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.Database = name
	pool, err := pgxpool.NewWithConfig(t.Context(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	store := postgres.New(pool)
	if err := store.Migrate(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		pool.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := admin.Exec(ctx, "DROP DATABASE "+pgx.Identifier{name}.Sanitize()+" WITH (FORCE)"); err != nil {
			t.Error(err)
		}
		admin.Close(ctx)
	})
	return pool
}

func TestLeadershipRenewalAndRevocation(t *testing.T) {
	pool := testPool(t)
	coord, err := New(pool)
	if err != nil {
		t.Fatal(err)
	}
	leader, err := coord.Campaign(t.Context(), uuid.New(), uuid.New(), "owner", 3)
	if err != nil {
		t.Fatal(err)
	}
	if leader.LeaseID == 0 || leader.Revision < 1 {
		t.Fatalf("leadership evidence: %+v", leader)
	}
	if err := leader.Verify(t.Context()); err != nil {
		t.Fatal(err)
	}
	select {
	case <-time.After(4 * time.Second):
	case <-leader.Done:
		t.Fatal("lease expired while renewal was active")
	case <-t.Context().Done():
		t.Fatal(t.Context().Err())
	}
	if err := leader.Verify(t.Context()); err != nil {
		t.Fatal("renewal did not preserve lease", err)
	}
	if err := leader.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	select {
	case <-leader.Done:
	case <-time.After(time.Second):
		t.Fatal("lease loss was not signalled")
	}
	if err := leader.Verify(t.Context()); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("revoked owner verified: %v", err)
	}
}

func TestDisconnectedOwnerAndContender(t *testing.T) {
	pool := testPool(t)
	firstCoordinator, err := New(pool)
	if err != nil {
		t.Fatal(err)
	}
	secondCoordinator, err := New(pool)
	if err != nil {
		t.Fatal(err)
	}
	installation, appID := uuid.New(), uuid.New()
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	first, err := firstCoordinator.Campaign(ctx, installation, appID, "first", 3)
	if err != nil {
		t.Fatal(err)
	}
	type outcome struct {
		leader *Leadership
		err    error
	}
	result := make(chan outcome, 1)
	go func() {
		leader, err := secondCoordinator.Campaign(ctx, installation, appID, "second", 3)
		result <- outcome{leader, err}
	}()
	select {
	case res := <-result:
		t.Fatalf("contender bypassed live leader: %v", res.err)
	case <-time.After(200 * time.Millisecond):
	}
	if err := first.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	select {
	case <-first.Done:
	case <-ctx.Done():
		t.Fatal("closed lease did not signal loss")
	}
	if err := first.Verify(ctx); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("closed owner verified: %v", err)
	}
	select {
	case next := <-result:
		if next.err != nil {
			t.Fatal(next.err)
		}
		defer next.leader.Close(t.Context())
		if next.leader.Revision <= first.Revision || next.leader.LeaseID == first.LeaseID {
			t.Fatalf("takeover reused lease evidence: rev=%d vs %d, lease=%d vs %d", next.leader.Revision, first.Revision, next.leader.LeaseID, first.LeaseID)
		}
		if err := next.leader.Verify(ctx); err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("lease release did not admit contender")
	}
}

func TestAuthorityValueAndUnavailableClient(t *testing.T) {
	pool := testPool(t)
	coord, err := New(pool)
	if err != nil {
		t.Fatal(err)
	}
	installationID, appID := uuid.New(), uuid.New()
	leader, err := coord.Campaign(t.Context(), installationID, appID, "owner", 3)
	if err != nil {
		t.Fatal(err)
	}
	defer leader.Close(t.Context())
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := leader.Verify(ctx); err == nil {
		t.Fatal("unavailable verification succeeded")
	}
	if _, err := pool.Exec(t.Context(), `UPDATE app_leases SET owner_id = 'different-owner' WHERE installation_id = $1 AND app_id = $2`, installationID, appID); err != nil {
		t.Fatal(err)
	}
	if err := leader.Verify(t.Context()); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("changed election value verified: %v", err)
	}
}
