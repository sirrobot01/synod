package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	json "encoding/json/v2"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/notification"
	"github.com/sirrobot01/synod/internal/secret"
)

const (
	NotificationConflict  ErrorCode = "notification_settings_changed"
	NotificationQueueFull ErrorCode = "notification_queue_full"
)

func (s *Store) ConfigureNotifications(ctx context.Context, installationID uuid.UUID, ring *secret.Keyring, input notification.Update, actorID, requestID string) (int64, error) {
	if installationID == uuid.Nil || input.ID == uuid.Nil || input.ExpectedRevision < 1 || ring == nil || notification.ValidateService(input.Settings) != nil || !printableWithin(actorID, 256) || !printableWithin(requestID, 128) {
		return 0, &Error{Code: InvalidRequest}
	}
	raw, _ := json.Marshal(input)
	hash := sha256.Sum256(raw)
	clear(raw)
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, storeError(err)
	}
	defer tx.Rollback()
	var previous []byte
	var actor string
	var revision int64
	err = tx.QueryRowContext(ctx, `SELECT actor_id,request_hash,revision FROM notification_updates WHERE id=?`, input.ID).Scan(&actor, &previous, &revision)
	if err == nil {
		if actor != actorID || !bytes.Equal(hash[:], previous) {
			return 0, &Error{Code: IdempotencyConflict}
		}
		return revision, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return 0, storeError(err)
	}
	if err := tx.QueryRowContext(ctx, `SELECT revision FROM notification_settings`).Scan(&revision); err != nil {
		return 0, storeError(err)
	}
	if revision != input.ExpectedRevision {
		return 0, &Error{Code: NotificationConflict}
	}
	revision++
	var sealed []byte
	if input.Settings.URL != "" {
		raw, _ := json.Marshal(input.Settings)
		envelope, err := ring.Seal(secret.Binding{InstallationID: installationID, OwnerType: "notification", OwnerID: installationID, CredentialID: installationID, Version: revision}, raw)
		clear(raw)
		if err != nil {
			return 0, storeError(err)
		}
		sealed, _ = json.Marshal(envelope)
	}
	public, _ := json.Marshal(input.Settings.Public())
	if _, err := tx.ExecContext(ctx, `UPDATE notification_settings SET revision=?,changed_at=?,public=?,credentials=?`, revision, s.now().UnixMilli(), public, sealed); err != nil {
		return 0, storeError(err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO notification_updates (id,actor_id,request_hash,revision) VALUES (?,?,?,?)`, input.ID, actorID, hash[:], revision); err != nil {
		return 0, storeError(err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE notification_alerts SET state='cancelled' WHERE state IN ('pending','sending')`); err != nil {
		return 0, storeError(err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM notification_conditions`); err != nil {
		return 0, storeError(err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE destination_revision SET revision=revision+1`); err != nil {
		return 0, storeError(err)
	}
	if err := audit(ctx, tx, actorID, requestID, "notification.configured", input.ID, s.now()); err != nil {
		return 0, err
	}
	return revision, storeCommit(tx)
}

// notificationSettings binds the private URL to the exact public revision.
func notificationSettings(ctx context.Context, tx *sql.Tx, installationID uuid.UUID, ring *secret.Keyring) (notification.Settings, int64, time.Time, error) {
	var raw, sealed []byte
	var revision, changed int64
	if err := tx.QueryRowContext(ctx, `SELECT revision,changed_at,public,credentials FROM notification_settings`).Scan(&revision, &changed, &raw, &sealed); err != nil {
		return notification.Settings{}, 0, time.Time{}, storeError(err)
	}
	var public notification.PublicSettings
	if json.Unmarshal(raw, &public) != nil {
		return notification.Settings{}, 0, time.Time{}, &Error{Code: Unavailable}
	}
	settings := notification.Settings{OverdueGraceHours: public.OverdueGraceHours, MissingAfterHours: public.MissingAfterHours, FullPercent: public.FullPercent}
	if public.Configured {
		var envelope secret.Envelope
		if ring == nil || json.Unmarshal(sealed, &envelope) != nil {
			return notification.Settings{}, 0, time.Time{}, &Error{Code: Unavailable}
		}
		plain, err := ring.Open(secret.Binding{InstallationID: installationID, OwnerType: "notification", OwnerID: installationID, CredentialID: installationID, Version: revision}, envelope)
		if err != nil {
			return notification.Settings{}, 0, time.Time{}, storeError(err)
		}
		defer clear(plain)
		if json.Unmarshal(plain, &settings) != nil {
			return notification.Settings{}, 0, time.Time{}, &Error{Code: Unavailable}
		}
	}
	if settings.Validate() != nil || settings.Public() != public {
		return notification.Settings{}, 0, time.Time{}, &Error{Code: Unavailable}
	}
	return settings, revision, time.UnixMilli(changed).UTC(), nil
}

func (s *Store) NotificationStatus(ctx context.Context) (notification.Status, error) {
	result := notification.Status{Alerts: []notification.Alert{}}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return result, storeError(err)
	}
	defer tx.Rollback()
	var public []byte
	if err := tx.QueryRowContext(ctx, `SELECT revision,public FROM notification_settings`).Scan(&result.Revision, &public); err != nil {
		return result, storeError(err)
	}
	if json.Unmarshal(public, &result.Settings) != nil {
		return result, &Error{Code: Unavailable}
	}
	rows, err := tx.QueryContext(ctx, `SELECT id,kind,node_id,app_id,destination_id,operation_id,message,created_at,state,attempts,next_attempt_at,delivered_at,failure_code FROM notification_alerts ORDER BY created_at DESC,id LIMIT 100`)
	if err != nil {
		return result, storeError(err)
	}
	defer rows.Close()
	for rows.Next() {
		var alert notification.Alert
		if err := rows.Scan(&alert.ID, &alert.Kind, &alert.NodeID, &alert.AppID, &alert.DestinationID, &alert.OperationID, &alert.Message, millis{&alert.CreatedAt}, &alert.State, &alert.Attempts, millis{&alert.NextAttemptAt}, optionalMillis{&alert.DeliveredAt}, &alert.FailureCode); err != nil {
			return result, storeError(err)
		}
		result.Alerts = append(result.Alerts, alert)
	}
	if err := rows.Err(); err != nil {
		return result, storeError(err)
	}
	return result, storeCommit(tx)
}
