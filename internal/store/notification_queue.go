package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/notification"
	"github.com/sirrobot01/synod/internal/secret"
)

// enqueueNotification bounds unfinished work. The caller retries admission when full.
func enqueueNotification(ctx context.Context, tx *sql.Tx, alert notification.Alert, resource string, now time.Time) (bool, error) {
	var count int
	if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM notification_alerts WHERE state IN ('pending','sending')`).Scan(&count); err != nil {
		return false, storeError(err)
	}
	if count >= 512 {
		return false, nil
	}
	_, err := tx.ExecContext(ctx, `INSERT INTO notification_alerts (id,kind,node_id,app_id,destination_id,operation_id,message,resource,created_at,state,next_attempt_at) VALUES (?,?,?,?,?,?,?,?,?,'pending',?) ON CONFLICT(id) DO NOTHING`, alert.ID, alert.Kind, alert.NodeID, alert.AppID, alert.DestinationID, alert.OperationID, alert.Message, resource, now.UnixMilli(), now.UnixMilli())
	if err != nil {
		return false, storeError(err)
	}
	return true, nil
}

func (s *Store) TestNotification(ctx context.Context, input notification.TestRequest, actorID, requestID string) error {
	if input.ID == uuid.Nil || input.ExpectedRevision < 1 || !printableWithin(actorID, 256) || !printableWithin(requestID, 128) {
		return &Error{Code: InvalidRequest}
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return storeError(err)
	}
	defer tx.Rollback()
	var actor string
	var previous int64
	err = tx.QueryRowContext(ctx, `SELECT actor_id,revision FROM notification_tests WHERE id=?`, input.ID).Scan(&actor, &previous)
	if err == nil {
		if actor != actorID || previous != input.ExpectedRevision {
			return &Error{Code: IdempotencyConflict}
		}
		return nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return storeError(err)
	}
	var revision int64
	var configured bool
	if err := tx.QueryRowContext(ctx, `SELECT revision,json_extract(CAST(public AS TEXT),'$.configured') FROM notification_settings`).Scan(&revision, &configured); err != nil {
		return storeError(err)
	}
	if revision != input.ExpectedRevision {
		return &Error{Code: NotificationConflict}
	}
	if !configured {
		return &Error{Code: InvalidRequest}
	}
	var exists bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM notification_alerts WHERE id=?)`, input.ID).Scan(&exists); err != nil {
		return storeError(err)
	}
	if exists {
		return &Error{Code: IdempotencyConflict}
	}
	admitted, err := enqueueNotification(ctx, tx, notification.Alert{ID: input.ID, Kind: "test", Message: "Synod notification test. Delivery is working."}, "", s.now())
	if err != nil {
		return err
	}
	if !admitted {
		return &Error{Code: NotificationQueueFull}
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO notification_tests VALUES (?,?,?)`, input.ID, actorID, revision); err != nil {
		return storeError(err)
	}
	if err := audit(ctx, tx, actorID, requestID, "notification.test_requested", input.ID, s.now()); err != nil {
		return err
	}
	return storeCommit(tx)
}

// NotificationDelivery holds one leased send. Provider credentials stay private.
type NotificationDelivery struct {
	ID       uuid.UUID
	Attempt  int
	Message  string
	Settings notification.Settings
}

func (NotificationDelivery) String() string   { return "[private notification delivery]" }
func (NotificationDelivery) GoString() string { return "[private notification delivery]" }

// ClaimNotification recovers expired leases after a controller restart.
func (s *Store) ClaimNotification(ctx context.Context, installationID uuid.UUID, ring *secret.Keyring) (*NotificationDelivery, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, storeError(err)
	}
	defer tx.Rollback()
	settings, _, _, err := notificationSettings(ctx, tx, installationID, ring)
	if err != nil {
		return nil, err
	}
	if settings.URL == "" {
		return nil, nil
	}
	now := s.now()
	if _, err := tx.ExecContext(ctx, `UPDATE notification_alerts SET state='failed',failure_code='delivery_unconfirmed' WHERE state='sending' AND next_attempt_at<=? AND attempts=5`, now.UnixMilli()); err != nil {
		return nil, storeError(err)
	}
	delivery := &NotificationDelivery{Settings: settings}
	err = tx.QueryRowContext(ctx, `SELECT id,message,attempts FROM notification_alerts WHERE state IN ('pending','sending') AND next_attempt_at<=? AND attempts<5 ORDER BY next_attempt_at,created_at,id LIMIT 1`, now.UnixMilli()).Scan(&delivery.ID, &delivery.Message, &delivery.Attempt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, storeCommit(tx)
	}
	if err != nil {
		return nil, storeError(err)
	}
	delivery.Attempt++
	if _, err := tx.ExecContext(ctx, `UPDATE notification_alerts SET state='sending',attempts=?,next_attempt_at=? WHERE id=?`, delivery.Attempt, now.Add(30*time.Second).UnixMilli(), delivery.ID); err != nil {
		return nil, storeError(err)
	}
	delivery.Message = fmt.Sprintf("%s\nAlert: %s", delivery.Message, delivery.ID)
	return delivery, storeCommit(tx)
}

// CompleteNotification ignores a cancelled or superseded lease.
func (s *Store) CompleteNotification(ctx context.Context, id uuid.UUID, attempt int, sendErr error) error {
	if id == uuid.Nil || attempt < 1 || attempt > 5 {
		return &Error{Code: InvalidRequest}
	}
	now := s.now()
	state, code := notification.Sent, ""
	var delivered any = now.UnixMilli()
	next := now
	if sendErr != nil {
		state, code, delivered = notification.Pending, "delivery_failed", nil
		if errors.Is(sendErr, notification.ErrRejected) {
			code = "delivery_rejected"
		}
		if attempt == 5 || errors.Is(sendErr, notification.ErrInvalid) || errors.Is(sendErr, notification.ErrRejected) {
			state = notification.Failed
		} else {
			next = now.Add([]time.Duration{time.Second, 10 * time.Second, time.Minute, 5 * time.Minute}[attempt-1])
		}
	}
	_, err := s.db.ExecContext(ctx, `UPDATE notification_alerts SET state=?,failure_code=?,delivered_at=?,next_attempt_at=? WHERE id=? AND state='sending' AND attempts=?`, state, code, delivered, next.UnixMilli(), id, attempt)
	if err != nil {
		return storeError(err)
	}
	return nil
}
