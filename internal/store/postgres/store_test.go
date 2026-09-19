package postgres

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sirrobot01/synod/internal/cluster"
	"github.com/sirrobot01/synod/internal/operation"
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
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
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

func testRequest() CreateClusterRequest {
	return CreateClusterRequest{
		Identity:  RequestIdentity{InstallationID: uuid.New(), ActorID: "actor-1", APIOperation: CreateClusterAPIOperation, IdempotencyKey: uuid.NewString()},
		RequestID: "request-1",
		Spec:      cluster.CreateSpec{SchemaVersion: 1, Name: "db", Engine: cluster.PostgreSQL, PostgreSQL: &cluster.PostgreSQLSpec{Version: "17.11", Topology: "standalone"}},
	}
}

func requireCode(t *testing.T, err error, want ErrorCode) {
	t.Helper()
	got, ok := errors.AsType[*AdmissionError](err)
	if !ok || got.Code != want {
		t.Fatalf("error = %#v, want %s", err, want)
	}
}

func requireCounts(t *testing.T, s *Store, want int) {
	t.Helper()
	for _, table := range []string{"clusters", "operations", "idempotency_results", "outbox", "audit_events"} {
		var count int
		if err := s.pool.QueryRow(t.Context(), "SELECT count(*) FROM "+pgx.Identifier{table}.Sanitize()).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != want {
			t.Fatalf("%s count = %d, want %d", table, count, want)
		}
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

func TestRequestValidation(t *testing.T) {
	request := testRequest()
	_, want, err := encodeRequest(request.Identity, request.Spec)
	if err != nil {
		t.Fatal(err)
	}
	for _, change := range []func(*RequestIdentity){
		func(i *RequestIdentity) { i.InstallationID = uuid.Nil },
		func(i *RequestIdentity) { i.ActorID = "" },
		func(i *RequestIdentity) { i.ActorID = "bad\x00actor" },
		func(i *RequestIdentity) { i.IdempotencyKey = "has space" },
		func(i *RequestIdentity) { i.IdempotencyKey = strings.Repeat("a", 129) },
		func(i *RequestIdentity) { i.APIOperation = "cluster.create" },
	} {
		identity := request.Identity
		change(&identity)
		_, _, err := encodeRequest(identity, request.Spec)
		requireCode(t, err, InvalidRequest)
	}
	identity := request.Identity
	identity.ActorID = "actor-2"
	identity.IdempotencyKey = "different"
	_, got, err := encodeRequest(identity, request.Spec)
	if err != nil || got != want {
		t.Fatalf("transport identity changed hash: %v", err)
	}
}

func TestErrorClassification(t *testing.T) {
	for constraint, want := range map[string]ErrorCode{"clusters_active_name": NameConflict, "operations_active_cluster": OperationConflict, "unrelated": DependencyUnavailable} {
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

func TestAdmissionReplayAndRead(t *testing.T) {
	s := testStore(t)
	request := testRequest()
	first, err := s.CreateCluster(t.Context(), request)
	if err != nil {
		t.Fatalf("admit: %v (%+v)", err, errors.Unwrap(err))
	}
	if first.Replayed || first.ClusterID == uuid.Nil || first.OperationID == uuid.Nil || first.DesiredRevision != 1 || first.AcceptedAt.IsZero() {
		t.Fatalf("bad admission: %+v", first)
	}
	request.RequestID = "request-2"
	second, err := s.CreateCluster(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	want := first
	want.Replayed = true
	if second != want {
		t.Fatalf("replay = %+v, want %+v", second, want)
	}
	resolved, err := s.LookupAdmission(t.Context(), request.Identity, request.Spec)
	if err != nil || resolved != want {
		t.Fatalf("lookup = %+v: %v", resolved, err)
	}
	gotCluster, err := s.GetCluster(t.Context(), request.Identity.InstallationID, first.ClusterID)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(gotCluster.Spec, request.Spec) || gotCluster.DesiredRevision != 1 || gotCluster.ObservedRevision != 0 || gotCluster.ExecutionEpoch != 0 || gotCluster.DesiredLifecycle != cluster.DesiredRunning || gotCluster.ObservedLifecycle != cluster.ObservedProvisioning || gotCluster.DeletedAt != nil {
		t.Fatalf("bad cluster: %+v", gotCluster)
	}
	gotOp, err := s.GetOperation(t.Context(), request.Identity.InstallationID, first.OperationID)
	if err != nil {
		t.Fatal(err)
	}
	if gotOp.Status != operation.Pending || gotOp.PlanVersion != 0 || gotOp.OwnerID != nil || gotOp.ClaimExpiresAt != nil || gotOp.ExecutionEpoch != 0 || gotOp.Attempts != 0 || gotOp.FinishedAt != nil || gotOp.CancellationRequestedAt != nil || gotOp.FailureCode != nil {
		t.Fatalf("bad operation: %+v", gotOp)
	}
	if gotOp.CreatedAt != first.AcceptedAt || gotCluster.CreatedAt != first.AcceptedAt {
		t.Fatal("admission timestamps differ")
	}
	events, err := s.ListPendingOutbox(t.Context(), request.Identity.InstallationID, 0, 100)
	if err != nil || len(events) != 1 || events[0].OperationID != first.OperationID || events[0].Kind != "operation.ready" || events[0].SchemaVersion != 1 {
		t.Fatalf("events = %+v: %v", events, err)
	}
	var auditRequest string
	if err := s.pool.QueryRow(t.Context(), `SELECT request_id FROM audit_events`).Scan(&auditRequest); err != nil || auditRequest != "request-1" {
		t.Fatalf("audit request %s: %v", auditRequest, err)
	}
	requireCounts(t, s, 1)
	request.Spec.PostgreSQL.Version = "17.10"
	_, err = s.CreateCluster(t.Context(), request)
	requireCode(t, err, IdempotencyConflict)
	_, err = s.LookupAdmission(t.Context(), request.Identity, request.Spec)
	requireCode(t, err, IdempotencyConflict)
	requireCounts(t, s, 1)
}

func TestRedisAndScopes(t *testing.T) {
	s := testStore(t)
	request := testRequest()
	request.Spec = cluster.CreateSpec{SchemaVersion: 1, Name: "cache", Engine: cluster.Redis, Redis: &cluster.RedisSpec{Version: "8.2.1", Topology: "standalone", Persistence: "aof"}}
	first, err := s.CreateCluster(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	request.Identity.ActorID = "actor-2"
	request.Spec.Name = "cache-2"
	if _, err := s.CreateCluster(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	otherInstall := uuid.New()
	request.Identity.InstallationID = otherInstall
	request.Spec.Name = "cache"
	if _, err := s.CreateCluster(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	_, err = s.GetCluster(t.Context(), otherInstall, first.ClusterID)
	requireCode(t, err, NotFound)
	_, err = s.GetOperation(t.Context(), otherInstall, first.OperationID)
	requireCode(t, err, NotFound)
	requireCounts(t, s, 3)
}

func TestConcurrentAdmission(t *testing.T) {
	for _, scenario := range []string{"identical", "payload-conflict", "name-conflict"} {
		t.Run(scenario, func(t *testing.T) {
			s := testStore(t)
			request := testRequest()
			start := make(chan struct{})
			var wg sync.WaitGroup
			type outcome struct {
				result Admission
				err    error
			}
			results := make(chan outcome, 32)
			for i := range 32 {
				wg.Go(func() {
					r := request
					if scenario == "payload-conflict" {
						r.Spec.Name = fmt.Sprintf("db-%d", i)
					}
					if scenario == "name-conflict" {
						r.Identity.IdempotencyKey = fmt.Sprintf("key-%d", i)
					}
					<-start
					result, err := New(s.pool).CreateCluster(t.Context(), r)
					results <- outcome{result, err}
				})
			}
			close(start)
			wg.Wait()
			close(results)
			var winner uuid.UUID
			successes := 0
			for result := range results {
				if result.err != nil {
					if scenario == "identical" {
						t.Fatal(result.err)
					}
					code := IdempotencyConflict
					if scenario == "name-conflict" {
						code = NameConflict
					}
					requireCode(t, result.err, code)
					continue
				}
				successes++
				if winner == uuid.Nil {
					winner = result.result.OperationID
				} else if winner != result.result.OperationID {
					t.Fatal("duplicate operation")
				}
			}
			want := 1
			if scenario == "identical" {
				want = 32
			}
			if successes != want {
				t.Fatalf("successes %d, want %d", successes, want)
			}
			requireCounts(t, s, 1)
		})
	}
}

func TestRollbackAtEveryInsert(t *testing.T) {
	for _, table := range []string{"idempotency_results", "clusters", "operations", "outbox", "audit_events"} {
		t.Run(table, func(t *testing.T) {
			s := testStore(t)
			_, err := s.pool.Exec(t.Context(), `CREATE FUNCTION fail_insert() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected insertion failure'; END $$;`)
			if err != nil {
				t.Fatal(err)
			}
			_, err = s.pool.Exec(t.Context(), `CREATE TRIGGER fail_insert AFTER INSERT ON `+pgx.Identifier{table}.Sanitize()+` FOR EACH ROW EXECUTE FUNCTION fail_insert()`)
			if err != nil {
				t.Fatal(err)
			}
			_, err = s.CreateCluster(t.Context(), testRequest())
			requireCode(t, err, DependencyUnavailable)
			requireCounts(t, s, 0)
		})
	}
}

func TestKeyWaitCommitRollbackAndCancellation(t *testing.T) {
	for _, scenario := range []string{"commit", "rollback", "cancel-waiter"} {
		t.Run(scenario, func(t *testing.T) {
			s := testStore(t)
			request := testRequest()
			ready := make(chan struct{})
			release := make(chan struct{})
			owner := New(s.pool)
			owner.commitTx = func(ctx context.Context, tx pgx.Tx) error {
				close(ready)
				<-release
				if scenario == "rollback" {
					if err := tx.Rollback(ctx); err != nil {
						return err
					}
					return pgx.ErrTxCommitRollback
				}
				return tx.Commit(ctx)
			}
			type outcome struct {
				result Admission
				err    error
			}
			first := make(chan outcome, 1)
			second := make(chan outcome, 1)
			go func() { result, err := owner.CreateCluster(t.Context(), request); first <- outcome{result, err} }()
			<-ready
			_, err := s.LookupAdmission(t.Context(), request.Identity, request.Spec)
			requireCode(t, err, NotFound)
			waitCtx, cancel := context.WithCancel(t.Context())
			defer cancel()
			go func() { result, err := s.CreateCluster(waitCtx, request); second <- outcome{result, err} }()
			waitForKeyWait(t, s)
			if scenario == "cancel-waiter" {
				cancel()
				waiter := <-second
				requireCode(t, waiter.err, DependencyUnavailable)
				if !errors.Is(waiter.err, context.Canceled) {
					t.Fatal("lost cancellation cause")
				}
			}
			close(release)
			winner := <-first
			if scenario == "rollback" {
				requireCode(t, winner.err, DependencyUnavailable)
			} else if winner.err != nil {
				t.Fatal(winner.err)
			}
			if scenario != "cancel-waiter" {
				waiter := <-second
				if waiter.err != nil {
					t.Fatal(waiter.err)
				}
				if scenario == "commit" && (!waiter.result.Replayed || waiter.result.OperationID != winner.result.OperationID) {
					t.Fatal("did not replay committed winner")
				}
				if scenario == "rollback" && waiter.result.Replayed {
					t.Fatal("replayed rolled-back request")
				}
			}
			requireCounts(t, s, 1)
		})
	}
}

func TestUnknownCommitRecovery(t *testing.T) {
	s := testStore(t)
	request := testRequest()
	s.commitTx = func(ctx context.Context, tx pgx.Tx) error {
		if err := tx.Commit(ctx); err != nil {
			return err
		}
		return io.ErrUnexpectedEOF
	}
	result, err := s.CreateCluster(t.Context(), request)
	requireCode(t, err, OutcomeUnknown)
	if result.ClusterID != uuid.Nil || result.OperationID != uuid.Nil {
		t.Fatal("returned candidate IDs as acceptance")
	}
	requireCounts(t, s, 1)
	// Block the first recovery query, then terminate its real backend.
	locked, err := s.pool.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer locked.Rollback(t.Context())
	if _, err := locked.Exec(t.Context(), `LOCK TABLE idempotency_results IN ACCESS EXCLUSIVE MODE`); err != nil {
		t.Fatal(err)
	}
	config := s.pool.Config()
	config.ConnConfig.RuntimeParams["application_name"] = "task010-recovery"
	lost, err := pgxpool.NewWithConfig(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	defer lost.Close()
	lookup := make(chan error, 1)
	go func() {
		_, err := New(lost).LookupAdmission(t.Context(), request.Identity, request.Spec)
		lookup <- err
	}()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	var pid int32
	for pid == 0 {
		err := s.pool.QueryRow(ctx, `SELECT COALESCE(max(pid),0) FROM pg_stat_activity
			WHERE datname=current_database() AND application_name='task010-recovery' AND wait_event='relation'`).Scan(&pid)
		if err != nil {
			t.Fatal(err)
		}
		if pid != 0 {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("recovery query did not wait on relation")
		case <-ticker.C:
		}
	}
	var terminated bool
	if err := s.pool.QueryRow(t.Context(), `SELECT pg_terminate_backend($1,5000)`, pid).Scan(&terminated); err != nil || !terminated {
		t.Fatalf("terminate recovery backend: %v", err)
	}
	requireCode(t, <-lookup, DependencyUnavailable)
	if err := locked.Rollback(t.Context()); err != nil {
		t.Fatal(err)
	}
	fresh, err := pgxpool.NewWithConfig(t.Context(), s.pool.Config())
	if err != nil {
		t.Fatal(err)
	}
	defer fresh.Close()
	recovered, err := New(fresh).LookupAdmission(t.Context(), request.Identity, request.Spec)
	if err != nil {
		t.Fatal(err)
	}
	replayed, err := New(fresh).CreateCluster(t.Context(), request)
	if err != nil || replayed != recovered {
		t.Fatalf("recovery mismatch: %+v / %+v: %v", recovered, replayed, err)
	}
	requireCounts(t, s, 1)
}

func TestBackendLossBeforeCommit(t *testing.T) {
	s := testStore(t)
	request := testRequest()
	s.commitTx = func(ctx context.Context, tx pgx.Tx) error {
		var terminated bool
		if err := s.pool.QueryRow(ctx, `SELECT pg_terminate_backend($1, 5000)`, tx.Conn().PgConn().PID()).Scan(&terminated); err != nil {
			return err
		}
		return tx.Commit(ctx)
	}
	_, err := s.CreateCluster(t.Context(), request)
	if err == nil {
		t.Fatal("backend loss accepted")
	}
	requireCounts(t, s, 0)
	if _, err := New(s.pool).CreateCluster(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	requireCounts(t, s, 1)
}

func TestDatabaseConstraints(t *testing.T) {
	s := testStore(t)
	request := testRequest()
	if _, err := s.CreateCluster(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct{ name, sql, code string }{
		{"uuid", `SELECT 'bad'::uuid`, "22P02"},
		{"hash", `UPDATE operations SET request_hash='bad'::bytea`, "23514"},
		{"state", `UPDATE operations SET status='unknown'`, "23514"},
		{"negative-revision", `UPDATE clusters SET desired_revision=0`, "23514"},
		{"name", `UPDATE clusters SET name='Upper'`, "23514"},
		{"spec-name", `UPDATE clusters SET spec=spec-'name'`, "23514"},
		{"spec-engine", `UPDATE clusters SET spec=jsonb_set(spec,'{engine}','"redis"')`, "23514"},
		{"claim", `UPDATE operations SET owner_id='worker'`, "23514"},
		{"key", `UPDATE idempotency_results SET idempotency_key='has space'`, "23514"},
		{"cross-installation", `UPDATE outbox SET installation_id='00000000-0000-0000-0000-000000000001'`, "23503"},
		{"cross-cluster", `UPDATE audit_events SET cluster_id='00000000-0000-0000-0000-000000000001'`, "23503"},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := s.pool.Exec(t.Context(), test.sql)
			got, ok := errors.AsType[*pgconn.PgError](err)
			if !ok || got.Code != test.code {
				t.Fatalf("constraint error = %v, want %s", err, test.code)
			}
		})
	}
	requireCounts(t, s, 1)
}

func TestDeferredKeyConstraint(t *testing.T) {
	s := testStore(t)
	request := testRequest()
	hash := sha256.Sum256([]byte("request"))
	tx, err := s.pool.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(t.Context())
	_, err = tx.Exec(t.Context(), `INSERT INTO idempotency_results
		(installation_id,actor_id,api_operation,idempotency_key,request_hash,hash_version,cluster_id,operation_id,desired_revision,accepted_at)
		VALUES ($1,$2,$3,$4,$5,1,$6,$7,1,transaction_timestamp())`, request.Identity.InstallationID, request.Identity.ActorID, request.Identity.APIOperation, request.Identity.IdempotencyKey, hash[:], uuid.New(), uuid.New())
	if err != nil {
		t.Fatal(err)
	}
	err = tx.Commit(t.Context())
	pgErr, ok := errors.AsType[*pgconn.PgError](err)
	if !ok || pgErr.Code != "23503" {
		t.Fatalf("commit = %v, want deferred FK failure", err)
	}
	requireCounts(t, s, 0)
}

func TestActiveMutationSlot(t *testing.T) {
	s := testStore(t)
	request := testRequest()
	admitted, err := s.CreateCluster(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	const clone = `INSERT INTO operations SELECT (jsonb_populate_record(NULL::operations,
		to_jsonb(o)||jsonb_build_object('id',$1::text,'status','pending'))).* FROM operations o WHERE id=$2`
	for _, status := range []operation.Status{operation.Pending, operation.Running, operation.Waiting, operation.Cancelling} {
		if _, err := s.pool.Exec(t.Context(), `UPDATE operations SET status=$1 WHERE id=$2`, status, admitted.OperationID); err != nil {
			t.Fatal(err)
		}
		_, err := s.pool.Exec(t.Context(), clone, uuid.NewString(), admitted.OperationID)
		requireCode(t, admissionError(err), OperationConflict)
	}
	for _, status := range []operation.Status{operation.Succeeded, operation.Failed, operation.Cancelled} {
		if _, err := s.pool.Exec(t.Context(), `UPDATE operations SET status=$1 WHERE id=$2`, status, admitted.OperationID); err != nil {
			t.Fatal(err)
		}
		id := uuid.New()
		if _, err := s.pool.Exec(t.Context(), clone, id.String(), admitted.OperationID); err != nil {
			t.Fatal(err)
		}
		if _, err := s.pool.Exec(t.Context(), `DELETE FROM operations WHERE id=$1`, id); err != nil {
			t.Fatal(err)
		}
	}
	requireCounts(t, s, 1)
}

func TestReplayAfterTerminalAndNameRelease(t *testing.T) {
	s := testStore(t)
	request := testRequest()
	first, err := s.CreateCluster(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.pool.Exec(t.Context(), `UPDATE operations SET status='succeeded', finished_at=transaction_timestamp();
		UPDATE clusters SET desired_revision=2, desired_lifecycle='deleted', observed_lifecycle='deleting'`); err != nil {
		t.Fatal(err)
	}
	second := request
	second.Identity.IdempotencyKey = "new-key"
	_, err = s.CreateCluster(t.Context(), second)
	requireCode(t, err, NameConflict)
	replayed, err := s.CreateCluster(t.Context(), request)
	if err != nil || replayed.OperationID != first.OperationID || replayed.DesiredRevision != 1 {
		t.Fatalf("terminal replay: %+v: %v", replayed, err)
	}
	if _, err := s.pool.Exec(t.Context(), `UPDATE clusters SET deleted_at=transaction_timestamp(),observed_lifecycle='deleted'`); err != nil {
		t.Fatal(err)
	}
	newAdmission, err := s.CreateCluster(t.Context(), second)
	if err != nil {
		t.Fatal(err)
	}
	if newAdmission.ClusterID == first.ClusterID {
		t.Fatal("reused deleted cluster ID")
	}
	replayed, err = s.CreateCluster(t.Context(), request)
	if err != nil || replayed.OperationID != first.OperationID {
		t.Fatalf("name-reuse replay: %+v: %v", replayed, err)
	}
	requireCounts(t, s, 2)
}

func TestOutboxCommitOrder(t *testing.T) {
	s := testStore(t)
	firstRequest := testRequest()
	ready := make(chan struct{})
	release := make(chan struct{})
	owner := New(s.pool)
	owner.commitTx = func(ctx context.Context, tx pgx.Tx) error { close(ready); <-release; return tx.Commit(ctx) }
	firstResult := make(chan error, 1)
	go func() { _, err := owner.CreateCluster(t.Context(), firstRequest); firstResult <- err }()
	<-ready
	second := firstRequest
	second.Spec.Name = "later"
	second.Identity.IdempotencyKey = "later-key"
	admitted, err := s.CreateCluster(t.Context(), second)
	if err != nil {
		t.Fatal(err)
	}
	events, err := s.ListPendingOutbox(t.Context(), firstRequest.Identity.InstallationID, 0, 1)
	if err != nil || len(events) != 1 || events[0].OperationID != admitted.OperationID {
		t.Fatalf("early scan: %+v: %v", events, err)
	}
	highID := events[0].ID
	close(release)
	if err := <-firstResult; err != nil {
		t.Fatal(err)
	}
	rescan, err := s.ListPendingOutbox(t.Context(), firstRequest.Identity.InstallationID, 0, 100)
	if err != nil || len(rescan) != 2 || rescan[0].ID >= highID {
		t.Fatalf("rescan lost older commit: %+v: %v", rescan, err)
	}
	page, err := s.ListPendingOutbox(t.Context(), firstRequest.Identity.InstallationID, rescan[0].ID, 1)
	if err != nil || len(page) != 1 || page[0].ID != highID {
		t.Fatalf("page: %+v: %v", page, err)
	}
	if _, err := s.pool.Exec(t.Context(), `UPDATE outbox SET delivered_at=transaction_timestamp() WHERE id=$1`, rescan[0].ID); err != nil {
		t.Fatal(err)
	}
	pending, err := s.ListPendingOutbox(t.Context(), firstRequest.Identity.InstallationID, 0, 100)
	if err != nil || len(pending) != 1 || pending[0].ID != highID {
		t.Fatalf("pending: %+v: %v", pending, err)
	}
}

func TestPendingSurvivesProcessRestart(t *testing.T) {
	s := testStore(t)
	request := testRequest()
	admitted, err := s.CreateCluster(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	database := s.pool.Config().ConnConfig.Database
	s.pool.Close()
	command := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestRestartReader$", "-test.v")
	command.Env = append(os.Environ(), "SYNOD_RESTART_READER=1", "SYNOD_RESTART_DATABASE="+database,
		"SYNOD_RESTART_INSTALLATION="+request.Identity.InstallationID.String(), "SYNOD_RESTART_CLUSTER="+admitted.ClusterID.String(), "SYNOD_RESTART_OPERATION="+admitted.OperationID.String())
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("restart reader: %v\n%s", err, output)
	}
}

func TestRestartReader(t *testing.T) {
	if os.Getenv("SYNOD_RESTART_READER") != "1" {
		t.Skip("subprocess fixture")
	}
	config, err := pgxpool.ParseConfig(os.Getenv("SYNOD_TEST_DATABASE_URL"))
	if err != nil {
		t.Fatal(err)
	}
	config.ConnConfig.Database = os.Getenv("SYNOD_RESTART_DATABASE")
	pool, err := pgxpool.NewWithConfig(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	s := New(pool)
	installation := uuid.MustParse(os.Getenv("SYNOD_RESTART_INSTALLATION"))
	clusterID := uuid.MustParse(os.Getenv("SYNOD_RESTART_CLUSTER"))
	operationID := uuid.MustParse(os.Getenv("SYNOD_RESTART_OPERATION"))
	if _, err := s.GetCluster(t.Context(), installation, clusterID); err != nil {
		t.Fatal(err)
	}
	op, err := s.GetOperation(t.Context(), installation, operationID)
	if err != nil || op.Status != operation.Pending {
		t.Fatalf("operation state: %+v: %v", op, err)
	}
	events, err := s.ListPendingOutbox(t.Context(), installation, 0, 100)
	if err != nil || len(events) != 1 || events[0].OperationID != operationID {
		t.Fatalf("restart events: %+v: %v", events, err)
	}
	requireCounts(t, s, 1)
}

func TestChangedSemanticInput(t *testing.T) {
	for _, scenario := range []string{"name", "postgres-version", "engine", "redis-version", "redis-persistence"} {
		t.Run(scenario, func(t *testing.T) {
			s := testStore(t)
			request := testRequest()
			if strings.HasPrefix(scenario, "redis-") {
				request.Spec = cluster.CreateSpec{SchemaVersion: 1, Name: "cache", Engine: cluster.Redis, Redis: &cluster.RedisSpec{Version: "8.2.1", Topology: "standalone", Persistence: "aof"}}
			}
			if _, err := s.CreateCluster(t.Context(), request); err != nil {
				t.Fatal(err)
			}
			switch scenario {
			case "name":
				request.Spec.Name = "different"
			case "postgres-version":
				request.Spec.PostgreSQL.Version = "17.10"
			case "engine":
				request.Spec.Engine = cluster.Redis
				request.Spec.PostgreSQL = nil
				request.Spec.Redis = &cluster.RedisSpec{Version: "8.2.1", Topology: "standalone", Persistence: "rdb"}
			case "redis-version":
				request.Spec.Redis.Version = "8.2.0"
			case "redis-persistence":
				request.Spec.Redis.Persistence = "rdb+aof"
			}
			_, err := s.CreateCluster(t.Context(), request)
			requireCode(t, err, IdempotencyConflict)
			requireCounts(t, s, 1)
		})
	}
}

func TestKnownCommitRejection(t *testing.T) {
	s := testStore(t)
	request := testRequest()
	s.commitTx = func(ctx context.Context, tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `UPDATE idempotency_results SET operation_id=$1`, uuid.New()); err != nil {
			return err
		}
		return tx.Commit(ctx)
	}
	_, err := s.CreateCluster(t.Context(), request)
	requireCode(t, err, DependencyUnavailable)
	pgErr, ok := errors.AsType[*pgconn.PgError](err)
	if !ok || pgErr.Code != "23503" {
		t.Fatalf("commit cause: %v", err)
	}
	requireCounts(t, s, 0)
}

func TestAdmissionFieldConsistency(t *testing.T) {
	s := testStore(t)
	request := testRequest()
	if _, err := s.CreateCluster(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	var consistent bool
	err := s.pool.QueryRow(t.Context(), `SELECT
		c.id=i.cluster_id AND c.id=o.cluster_id AND c.id=b.cluster_id AND c.id=a.cluster_id
		AND c.installation_id=i.installation_id AND c.installation_id=o.installation_id
		AND c.installation_id=b.installation_id AND c.installation_id=a.installation_id
		AND o.id=i.operation_id AND o.id=b.operation_id AND o.id=a.operation_id
		AND o.request_hash=i.request_hash AND o.hash_version=i.hash_version
		AND o.actor_id=i.actor_id AND o.actor_id=a.actor_id
		AND o.api_operation=i.api_operation AND o.idempotency_key=i.idempotency_key
		AND o.desired_revision=i.desired_revision AND o.desired_revision=c.desired_revision
		AND c.created_at=c.updated_at AND c.created_at=o.created_at AND c.created_at=o.updated_at
		AND c.created_at=i.accepted_at AND c.created_at=b.created_at AND c.created_at=a.created_at
		AND a.action='cluster.create.accepted' AND a.outcome='accepted' AND b.delivered_at IS NULL
		FROM clusters c CROSS JOIN operations o CROSS JOIN idempotency_results i CROSS JOIN outbox b CROSS JOIN audit_events a`).Scan(&consistent)
	if err != nil || !consistent {
		t.Fatalf("inconsistent admission: %v", err)
	}
}

func TestInvalidReadsAndRequest(t *testing.T) {
	s := testStore(t)
	request := testRequest()
	for _, id := range []string{"", "has space", strings.Repeat("r", 129), "new\nline"} {
		request.RequestID = id
		_, err := s.CreateCluster(t.Context(), request)
		requireCode(t, err, InvalidRequest)
	}
	for _, limit := range []int{-1, 0, 1001} {
		_, err := s.ListPendingOutbox(t.Context(), request.Identity.InstallationID, 0, limit)
		requireCode(t, err, InvalidRequest)
	}
	_, err := s.ListPendingOutbox(t.Context(), request.Identity.InstallationID, -1, 10)
	requireCode(t, err, InvalidRequest)
	_, err = s.GetCluster(t.Context(), uuid.Nil, uuid.New())
	requireCode(t, err, InvalidRequest)
	_, err = s.GetOperation(t.Context(), uuid.New(), uuid.Nil)
	requireCode(t, err, InvalidRequest)
	requireCounts(t, s, 0)
}
