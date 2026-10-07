package store

import (
	"context"
	json "encoding/json/v2"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/backup"
	"github.com/sirrobot01/synod/internal/notification"
	"github.com/sirrobot01/synod/internal/secret"
)

// RefreshNotifications evaluates durable evidence in one snapshot of metadata.
func (s *Store) RefreshNotifications(ctx context.Context, installationID uuid.UUID, ring *secret.Keyring) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return storeError(err)
	}
	defer tx.Rollback()
	settings, _, changed, err := notificationSettings(ctx, tx, installationID, ring)
	if err != nil {
		return err
	}
	if settings.URL == "" {
		return nil
	}
	now := s.now()
	rows, err := tx.QueryContext(ctx, `SELECT o.id,o.kind,o.node_id,o.app_id,coalesce(json_extract(CAST(o.input AS TEXT),'$.destination_id'),?),o.failure_code FROM operations o WHERE o.status='failed' AND o.kind IN ('backup','restore_drill') AND o.finished_at>=? AND NOT EXISTS(SELECT 1 FROM notification_alerts a WHERE a.operation_id=o.id AND a.kind IN ('backup_failed','drill_failed')) ORDER BY o.finished_at,o.id LIMIT 64`, uuid.Nil, changed.UnixMilli())
	if err != nil {
		return storeError(err)
	}
	var failures []notification.Alert
	for rows.Next() {
		var a notification.Alert
		var kind, code string
		if err := rows.Scan(&a.OperationID, &kind, &a.NodeID, &a.AppID, &a.DestinationID, &code); err != nil {
			rows.Close()
			return storeError(err)
		}
		a.ID = uuid.NewSHA1(a.OperationID, []byte("notification"))
		a.Kind = "backup_failed"
		label := "Backup"
		if kind == "restore_drill" {
			a.Kind = "drill_failed"
			label = "Restore drill"
		}
		a.Message = fmt.Sprintf("%s failed for app %s on device %s. Code: %s. Run synod operation get %s.", label, a.AppID, a.NodeID, code, a.OperationID)
		failures = append(failures, a)
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return storeError(err)
	}
	for _, a := range failures {
		if _, err := enqueueNotification(ctx, tx, a, "", now); err != nil {
			return err
		}
	}

	previous := map[string]bool{}
	rows, err = tx.QueryContext(ctx, `SELECT resource,active FROM notification_conditions`)
	if err != nil {
		return storeError(err)
	}
	for rows.Next() {
		var key string
		var active bool
		if err := rows.Scan(&key, &active); err != nil {
			rows.Close()
			return storeError(err)
		}
		previous[key] = active
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return storeError(err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE notification_conditions SET active=0`); err != nil {
		return storeError(err)
	}
	// Each condition has one active episode. Admission and state change commit together.
	conditions := map[string]notification.Alert{}
	rows, err = tx.QueryContext(ctx, `SELECT id,coalesce(last_seen_at,approved_at) FROM nodes WHERE approved_at IS NOT NULL AND revoked_at IS NULL`)
	if err != nil {
		return storeError(err)
	}
	for rows.Next() {
		var id uuid.UUID
		var seen time.Time
		if err := rows.Scan(&id, millis{&seen}); err != nil {
			rows.Close()
			return storeError(err)
		}
		if now.Sub(seen) >= time.Duration(settings.MissingAfterHours)*time.Hour {
			conditions["missing/"+id.String()] = notification.Alert{Kind: "device_missing", NodeID: id, Message: fmt.Sprintf("Device %s has not connected since %s. Check its power and agent connection. Run synod node list.", id, seen.Format(time.RFC3339))}
		}
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return storeError(err)
	}
	rows, err = tx.QueryContext(ctx, `SELECT h.destination_id,h.node_id,h.total_bytes,h.available_bytes FROM destination_health h JOIN destinations d ON d.id=h.destination_id JOIN nodes n ON n.id=h.node_id WHERE d.removed_at IS NULL AND n.approved_at IS NOT NULL AND n.revoked_at IS NULL AND h.observed_at>=?`, now.Add(-5*time.Minute).UnixMilli())
	if err != nil {
		return storeError(err)
	}
	for rows.Next() {
		var id, node uuid.UUID
		var total, available int64
		if err := rows.Scan(&id, &node, &total, &available); err != nil {
			rows.Close()
			return storeError(err)
		}
		percent := 100 * (1 - float64(available)/float64(total))
		if percent >= float64(settings.FullPercent) {
			conditions["full/"+id.String()] = notification.Alert{Kind: "destination_full", NodeID: node, DestinationID: id, Message: fmt.Sprintf("Destination %s on device %s is %.1f%% full (%d bytes available). Add space or another destination.", id, node, percent, available)}
		}
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return storeError(err)
	}
	// Offline devices are expected. Reconnection leaves time for journal replay and catch-up.
	rows, err = tx.QueryContext(ctx, `SELECT b.definition FROM app_protection p JOIN backup_schedules b ON b.id=p.schedule_id JOIN nodes n ON n.id=b.node_id WHERE p.enabled=1 AND n.approved_at IS NOT NULL AND n.revoked_at IS NULL AND n.last_seen_at>=? AND n.connected_at<=?`, now.Add(-2*time.Minute).UnixMilli(), now.Add(-5*time.Minute).UnixMilli())
	if err != nil {
		return storeError(err)
	}
	var schedules []backup.Schedule
	for rows.Next() {
		var raw []byte
		var schedule backup.Schedule
		if err := rows.Scan(&raw); err != nil {
			rows.Close()
			return storeError(err)
		}
		if json.Unmarshal(raw, &schedule) != nil || schedule.Validate() != nil {
			rows.Close()
			return &Error{Code: Unavailable}
		}
		schedules = append(schedules, schedule)
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return storeError(err)
	}
	for _, schedule := range schedules {
		due := schedule.Due(now.Add(-time.Duration(settings.OverdueGraceHours) * time.Hour))
		if due.IsZero() {
			continue
		}
		for _, destination := range schedule.Settings.Destinations {
			var covered, busy bool
			err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM operations WHERE kind='backup' AND app_id=? AND json_extract(CAST(input AS TEXT),'$.destination_id')=? AND status='succeeded' AND unixepoch(json_extract(CAST(result AS TEXT),'$.started_at'),'subsec')*1000>=?),EXISTS(SELECT 1 FROM operations WHERE kind='backup' AND app_id=? AND json_extract(CAST(input AS TEXT),'$.destination_id')=? AND status IN ('pending','running','waiting','cancelling') AND updated_at>=?)`, schedule.AppID, destination, due.UnixMilli(), schedule.AppID, destination, now.Add(-2*time.Hour).UnixMilli()).Scan(&covered, &busy)
			if err != nil {
				return storeError(err)
			}
			if !covered && !busy {
				conditions["overdue/"+schedule.AppID.String()+"/"+destination.String()] = notification.Alert{Kind: "backup_overdue", NodeID: schedule.NodeID, AppID: schedule.AppID, DestinationID: destination, Message: fmt.Sprintf("App %s on device %s has no good backup to destination %s for the slot at %s. Run synod app status, then synod backup %s.", schedule.AppID, schedule.NodeID, destination, due.Format(time.RFC3339), schedule.AppID)}
			}
		}
	}
	for resource, a := range conditions {
		if !previous[resource] {
			a.ID = uuid.New()
			admitted, err := enqueueNotification(ctx, tx, a, resource, now)
			if err != nil {
				return err
			}
			if !admitted {
				continue
			}
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO notification_conditions VALUES (?,1) ON CONFLICT(resource) DO UPDATE SET active=1`, resource); err != nil {
			return storeError(err)
		}
	}
	if _, err := tx.ExecContext(ctx, `UPDATE notification_alerts SET state='cancelled' WHERE state IN ('pending','sending') AND resource!='' AND NOT EXISTS(SELECT 1 FROM notification_conditions c WHERE c.resource=notification_alerts.resource AND c.active=1)`); err != nil {
		return storeError(err)
	}
	return storeCommit(tx)
}
