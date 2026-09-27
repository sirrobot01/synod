package postgres

import (
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/sirrobot01/synod/internal/app"
	"github.com/sirrobot01/synod/internal/operation"
)

func TestAppActionsCleanUnplannedImportsAndReplayLostReplies(t *testing.T) {
	s := testStore(t)
	installation, generation := uuid.New(), uuid.New()
	if err := s.InitializeGeneration(t.Context(), installation, generation); err != nil {
		t.Fatal(err)
	}
	created, err := s.AdmitApp(t.Context(), CreateAppRequest{Identity: RequestIdentity{InstallationID: installation, ActorID: "operator", APIOperation: CreateAppAPIOperation, IdempotencyKey: "create"}, RequestID: "create", Spec: app.AppSpec{SchemaVersion: 1, Name: "web", Tier: app.TierParallel, Copies: 1, Service: app.ServiceSpec{Image: "unavailable.invalid/image", Ports: []app.PortSpec{{ContainerPort: 80}}}, Router: app.RouterSpec{Protocol: "http", Port: 8080}}})
	if err != nil {
		t.Fatal(err)
	}
	claim, err := s.AcquireClaim(t.Context(), ClaimRequest{ClaimID: uuid.New(), InstallationID: installation, OperationID: created.OperationID, RecoveryGeneration: generation, OwnerID: "controller", TTL: time.Minute, LeaseID: 1, LeaseRevision: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.RecordAppPlanningFailure(t.Context(), claim, "image_unavailable"); err != nil {
		t.Fatal(err)
	}
	request := AppActionRequest{Identity: RequestIdentity{InstallationID: installation, ActorID: "operator", APIOperation: StopAppAPIOperation, IdempotencyKey: "stop"}, RequestID: "stop", AppID: created.AppID, ExpectedRevision: 1}
	results := make([]app.Admission, 8)
	failures := make([]error, 8)
	var wg sync.WaitGroup
	for i := range results {
		wg.Go(func() { results[i], failures[i] = s.AdmitAppAction(t.Context(), request) })
	}
	wg.Wait()
	for i, result := range results {
		if failures[i] != nil || result.OperationID != results[0].OperationID || result.DesiredRevision != 2 {
			t.Fatalf("concurrent action: %+v %v", result, failures[i])
		}
	}
	op, err := s.GetOperation(t.Context(), installation, created.OperationID)
	if err != nil || op.Status != operation.Cancelled {
		t.Fatalf("unplanned import survived: %+v %v", op, err)
	}
	if err := s.RecordAppPlanningFailure(t.Context(), claim, "image_unavailable"); !errors.Is(err, operation.ErrStaleOwner) {
		t.Fatalf("old owner stayed active: %v", err)
	}
	item, err := s.GetAppStatus(t.Context(), installation, created.AppID)
	if err != nil || item.Status.State != "stopped" || item.ObservedRevision != 2 {
		t.Fatalf("stop: %+v %v", item, err)
	}
	request.ExpectedRevision = 2
	_, err = s.AdmitAppAction(t.Context(), request)
	requireCode(t, err, IdempotencyConflict)
	request.Identity.IdempotencyKey = "stale"
	request.ExpectedRevision = 1
	_, err = s.AdmitAppAction(t.Context(), request)
	requireCode(t, err, RevisionConflict)
	request.ExpectedRevision = 2
	request.Identity.APIOperation = StartAppAPIOperation
	request.Identity.IdempotencyKey = "start"
	started, err := s.AdmitAppAction(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	op, err = s.GetOperation(t.Context(), installation, started.OperationID)
	if err != nil || op.Status != operation.Pending || op.Kind != "app.start" {
		t.Fatalf("start did not plan: %+v %v", op, err)
	}
	request.ExpectedRevision = 3
	request.Identity.APIOperation = DeleteAppAPIOperation
	request.Identity.IdempotencyKey = "delete"
	lost := &Store{pool: s.pool, commitTx: func(ctx context.Context, tx pgx.Tx) error {
		if err := tx.Commit(ctx); err != nil {
			return err
		}
		return io.EOF
	}}
	_, err = lost.AdmitAppAction(t.Context(), request)
	requireCode(t, err, OutcomeUnknown)
	deleted, err := New(s.pool).AdmitAppAction(t.Context(), request)
	if err != nil || !deleted.Replayed {
		t.Fatalf("delete retry: %+v %v", deleted, err)
	}
	item, err = s.GetAppStatus(t.Context(), installation, created.AppID)
	if err != nil || item.Status.State != "deleted" || item.DeletedAt == nil || item.ObservedRevision != 4 {
		t.Fatalf("delete: %+v %v", item, err)
	}
	var outbox int
	if err := s.pool.QueryRow(t.Context(), `SELECT count(*) FROM outbox WHERE operation_id=$1`, deleted.OperationID).Scan(&outbox); err != nil || outbox != 0 {
		t.Fatalf("empty delete dispatched work: %d %v", outbox, err)
	}
}
