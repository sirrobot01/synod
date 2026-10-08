package store

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"slices"
	"time"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/backup"
	"github.com/sirrobot01/synod/internal/destination"
	"github.com/sirrobot01/synod/internal/operation"
	"github.com/sirrobot01/synod/internal/proof"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/proto"
)

const PlanConflict ErrorCode = "protect_plan_changed"

// UpdateProtection confirms selected apps and saves immutable schedules in one transaction.
func (s *Store) UpdateProtection(ctx context.Context, installationID uuid.UUID, update backup.ProtectionUpdate, actorID, requestID string) (int64, error) {
	if installationID == uuid.Nil || update.ID == uuid.Nil || update.ExpectedRevision < 1 || (update.All && len(update.AppIDs) != 0) || (!update.All && len(update.AppIDs) == 0) || len(update.AppIDs) > 512 || (update.Enabled && update.Settings.Validate() != nil) || !printableWithin(actorID, 256) || !printableWithin(requestID, 128) {
		return 0, &Error{Code: InvalidRequest}
	}
	for i, id := range update.AppIDs {
		if id == uuid.Nil || slices.Contains(update.AppIDs[:i], id) {
			return 0, &Error{Code: InvalidRequest}
		}
	}
	input, err := json.Marshal(update)
	if err != nil || len(input) > 64<<10 {
		return 0, &Error{Code: InvalidRequest}
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, storeError(err)
	}
	defer tx.Rollback()
	var revision int64
	var previous []byte
	var actor string
	err = tx.QueryRowContext(ctx, `SELECT input,actor_id,revision FROM protection_updates WHERE id=?`, update.ID).Scan(&previous, &actor, &revision)
	if err == nil {
		if !bytes.Equal(input, previous) || actor != actorID {
			return 0, &Error{Code: IdempotencyConflict}
		}
		return revision, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return 0, storeError(err)
	}
	if err := tx.QueryRowContext(ctx, `SELECT revision FROM destination_revision`).Scan(&revision); err != nil {
		return 0, storeError(err)
	}
	if revision != update.ExpectedRevision {
		return 0, &Error{Code: PlanConflict}
	}
	// Included paths belong to one app. The default for new apps cannot name them.
	if update.All && len(update.Settings.IncludeLarge) > 0 {
		return 0, &Error{Code: InvalidRequest}
	}
	if update.Enabled {
		for _, id := range update.Settings.Destinations {
			var available bool
			if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM destinations WHERE id=? AND removed_at IS NULL)`, id).Scan(&available); err != nil {
				return 0, storeError(err)
			}
			if !available {
				return 0, &Error{Code: InvalidRequest}
			}
		}
	}
	var bound sql.Null[uuid.UUID]
	if err := tx.QueryRowContext(ctx, `SELECT installation_id FROM protection_defaults`).Scan(&bound); err != nil {
		return 0, storeError(err)
	}
	if bound.Valid && bound.V != installationID {
		return 0, &Error{Code: InvalidRequest}
	}
	if _, err := tx.ExecContext(ctx, `UPDATE protection_defaults SET installation_id=?`, installationID); err != nil {
		return 0, storeError(err)
	}
	ids := update.AppIDs
	if update.All {
		rows, err := tx.QueryContext(ctx, `SELECT a.id FROM apps a JOIN nodes n ON n.id=a.node_id WHERE ?=0 OR (n.approved_at IS NOT NULL AND n.revoked_at IS NULL AND a.present=1) ORDER BY a.id`, update.Enabled)
		if err != nil {
			return 0, storeError(err)
		}
		defer rows.Close()
		for rows.Next() {
			var id uuid.UUID
			if err := rows.Scan(&id); err != nil {
				return 0, storeError(err)
			}
			ids = append(ids, id)
		}
		if err := rows.Err(); err != nil {
			return 0, storeError(err)
		}
		if err := rows.Close(); err != nil {
			return 0, storeError(err)
		}
	}
	for _, id := range ids {
		var node uuid.UUID
		var key string
		if err := tx.QueryRowContext(ctx, `SELECT a.node_id,a.source_key FROM apps a JOIN nodes n ON n.id=a.node_id WHERE a.id=? AND (?=0 OR (n.approved_at IS NOT NULL AND n.revoked_at IS NULL AND a.present=1))`, id, update.Enabled).Scan(&node, &key); err != nil {
			return 0, storeError(err)
		}
		if update.Enabled && len(update.Settings.IncludeLarge) > 0 {
			var raw []byte
			if err := tx.QueryRowContext(ctx, `SELECT definition FROM apps WHERE id=?`, id).Scan(&raw); err != nil {
				return 0, storeError(err)
			}
			definition := new(agentv1.DiscoveredApp)
			if proto.Unmarshal(raw, definition) != nil {
				return 0, &Error{Code: Unavailable}
			}
			if backup.CheckIncluded(definition, update.Settings.IncludeLarge) != nil {
				return 0, &Error{Code: InvalidRequest}
			}
		}
		if update.Enabled {
			for _, destinationID := range update.Settings.Destinations {
				var available bool
				if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM destinations WHERE id=? AND removed_at IS NULL AND (kind!='local' OR node_id=?))`, destinationID, node).Scan(&available); err != nil {
					return 0, storeError(err)
				}
				if !available {
					return 0, &Error{Code: InvalidRequest}
				}
			}
		}
		if err := setAppProtection(ctx, tx, installationID, id, node, key, update.Enabled, update.Settings, s.now()); err != nil {
			return 0, err
		}
	}
	if update.All {
		settings := update.Settings
		if !update.Enabled {
			var raw []byte
			if err := tx.QueryRowContext(ctx, `SELECT settings FROM protection_defaults`).Scan(&raw); err != nil {
				return 0, storeError(err)
			}
			if err := json.Unmarshal(raw, &settings); err != nil {
				return 0, storeError(err)
			}
		}
		raw, _ := json.Marshal(settings)
		if _, err := tx.ExecContext(ctx, `UPDATE protection_defaults SET auto_protect=?,settings=?`, update.Enabled, raw); err != nil {
			return 0, storeError(err)
		}
	}
	revision++
	if _, err := tx.ExecContext(ctx, `UPDATE destination_revision SET revision=?`, revision); err != nil {
		return 0, storeError(err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO protection_updates (id,actor_id,input,revision) VALUES (?,?,?,?)`, update.ID, actorID, input, revision); err != nil {
		return 0, storeError(err)
	}
	if err := audit(ctx, tx, actorID, requestID, "protection.changed", update.ID, s.now()); err != nil {
		return 0, err
	}
	return revision, storeCommit(tx)
}

// setAppProtection keeps the current selection and its immutable history in the same transaction.
func setAppProtection(ctx context.Context, tx *sql.Tx, installationID, appID, nodeID uuid.UUID, sourceKey string, enabled bool, settings backup.Settings, now time.Time) error {
	var previous []byte
	var wasEnabled bool
	err := tx.QueryRowContext(ctx, `SELECT enabled,settings FROM app_protection WHERE app_id=?`, appID).Scan(&wasEnabled, &previous)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return storeError(err)
	}
	if !enabled {
		settings = backup.DefaultSettings()
		if len(previous) > 0 && json.Unmarshal(previous, &settings) != nil {
			return &Error{Code: Unavailable}
		}
	}
	raw, _ := json.Marshal(settings)
	if err == nil && wasEnabled == enabled && bytes.Equal(raw, previous) {
		return nil
	}
	var scheduleID sql.Null[uuid.UUID]
	if enabled {
		schedule := backup.Schedule{ID: uuid.New(), InstallationID: installationID, AppID: appID, NodeID: nodeID, SourceKey: sourceKey, Settings: settings, CreatedAt: now.UTC().Truncate(time.Millisecond)}
		if schedule.Validate() != nil {
			return &Error{Code: InvalidRequest}
		}
		definition, _ := json.Marshal(schedule)
		if _, err := tx.ExecContext(ctx, `INSERT INTO backup_schedules (id,app_id,node_id,definition) VALUES (?,?,?,?)`, schedule.ID, appID, nodeID, definition); err != nil {
			return storeError(err)
		}
		scheduleID = sql.Null[uuid.UUID]{V: schedule.ID, Valid: true}
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO app_protection (app_id,enabled,settings,schedule_id,changed_at) VALUES (?,?,?,?,?) ON CONFLICT(app_id) DO UPDATE SET enabled=excluded.enabled,settings=excluded.settings,schedule_id=excluded.schedule_id,changed_at=excluded.changed_at`, appID, enabled, raw, scheduleID, now.UnixMilli())
	if err != nil {
		return storeError(err)
	}
	return nil
}

// ProtectionPlan shows saved choices, proposed paths, and the last result at each destination.
func (s *Store) ProtectionPlan(ctx context.Context) (backup.ProtectionPlan, error) {
	plan := backup.ProtectionPlan{Apps: []backup.AppPlan{}}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return plan, storeError(err)
	}
	defer tx.Rollback()
	var defaults []byte
	var installation sql.Null[uuid.UUID]
	if err := tx.QueryRowContext(ctx, `SELECT r.revision,p.auto_protect,p.settings,p.installation_id FROM destination_revision r CROSS JOIN protection_defaults p`).Scan(&plan.Revision, &plan.AutoProtect, &defaults, &installation); err != nil {
		return plan, storeError(err)
	}
	if err := json.Unmarshal(defaults, &plan.Defaults); err != nil {
		return plan, storeError(err)
	}
	destinations := make(map[uuid.UUID]destination.Destination)
	rows, err := tx.QueryContext(ctx, `SELECT definition FROM destinations WHERE removed_at IS NULL`)
	if err != nil {
		return plan, storeError(err)
	}
	defer rows.Close()
	for rows.Next() {
		var raw []byte
		var d destination.Destination
		if err := rows.Scan(&raw); err != nil {
			return plan, storeError(err)
		}
		if err := json.Unmarshal(raw, &d); err != nil {
			return plan, storeError(err)
		}
		destinations[d.ID] = d
	}
	if err := rows.Err(); err != nil {
		return plan, storeError(err)
	}
	if err := rows.Close(); err != nil {
		return plan, storeError(err)
	}
	rows, err = tx.QueryContext(ctx, `SELECT a.id,a.node_id,n.name,a.observed_at,a.present,a.definition,n.approved_at IS NOT NULL AND n.revoked_at IS NULL,coalesce(p.enabled,0),p.settings,p.schedule_id,p.changed_at,coalesce(d.revision=? AND d.session_id=n.session_id,0) FROM apps a JOIN nodes n ON n.id=a.node_id LEFT JOIN app_protection p ON p.app_id=a.id LEFT JOIN destination_deliveries d ON d.node_id=n.id ORDER BY n.name,a.source_key,a.id`, plan.Revision)
	if err != nil {
		return plan, storeError(err)
	}
	defer rows.Close()
	for rows.Next() {
		entry := backup.AppPlan{Settings: plan.Defaults, Targets: []backup.TargetStatus{}}
		var raw, settings []byte
		var approved bool
		var scheduleID sql.Null[uuid.UUID]
		var changed sql.NullInt64
		if err := rows.Scan(&entry.App.ID, &entry.App.NodeID, &entry.App.NodeName, millis{&entry.App.ObservedAt}, &entry.App.Present, &raw, &approved, &entry.Enabled, &settings, &scheduleID, &changed, &entry.SettingsCurrent); err != nil {
			return plan, storeError(err)
		}
		entry.App.Definition = new(agentv1.DiscoveredApp)
		if proto.Unmarshal(raw, entry.App.Definition) != nil || (settings != nil && json.Unmarshal(settings, &entry.Settings) != nil) {
			return plan, &Error{Code: Unavailable}
		}
		entry.ScheduleID = scheduleID.V
		if changed.Valid {
			entry.ChangedAt = time.UnixMilli(changed.Int64).UTC()
		}
		entry.BlockedCode = backup.BlockedCode(entry.App)
		if !approved {
			entry.BlockedCode = "device_not_approved"
		}
		var selected []destination.Destination
		for _, id := range entry.Settings.Destinations {
			d, ok := destinations[id]
			if !ok || (d.Location.Kind == "local" && d.NodeID != entry.App.NodeID) {
				entry.BlockedCode = "destination_unavailable"
			}
			selected = append(selected, d)
		}
		entry.OnlyOnSource = destination.OnlyOnSource(entry.App.NodeID, selected)
		plan.Apps = append(plan.Apps, entry)
	}
	if err := rows.Err(); err != nil {
		return plan, storeError(err)
	}
	if err := rows.Close(); err != nil {
		return plan, storeError(err)
	}
	type target struct{ app, destination uuid.UUID }
	latest := make(map[target]operation.Operation)
	good := make(map[target]backup.Result)
	for _, successful := range []bool{false, true} {
		rows, err := tx.QueryContext(ctx, `SELECT id,app_id,status,failure_code,result,input FROM (
 SELECT *,row_number() OVER (
  PARTITION BY app_id,json_extract(CAST(input AS TEXT),'$.destination_id')
  ORDER BY coalesce(unixepoch(nullif(json_extract(CAST(result AS TEXT),'$.started_at'),'0001-01-01T00:00:00Z'),'subsec')*1000,created_at) DESC,created_at DESC,id DESC
 ) AS position FROM operations WHERE kind='backup' AND (?=0 OR status='succeeded')
) WHERE position=1`, successful)
		if err != nil {
			return plan, storeError(err)
		}
		for rows.Next() {
			var op operation.Operation
			if err := rows.Scan(&op.ID, &op.AppID, &op.Status, &op.FailureCode, operationResult{&op.Result}, &op.Input); err != nil {
				rows.Close()
				return plan, storeError(err)
			}
			var job backup.Request
			if json.Unmarshal(op.Input, &job) != nil || job.Validate() != nil {
				continue
			}
			key := target{job.AppID, job.DestinationID}
			if successful {
				var result backup.Result
				if json.Unmarshal(op.Result, &result) != nil || (backup.Event{ID: job.ID, Status: "succeeded", Result: result, Request: &job}).Validate(job) != nil {
					rows.Close()
					return plan, &Error{Code: Unavailable}
				}
				good[key] = result
			} else {
				latest[key] = op
			}
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return plan, storeError(err)
		}
		if err := rows.Close(); err != nil {
			return plan, storeError(err)
		}
	}
	drills := make(map[uuid.UUID]*proof.Status)
	tested := make(map[uuid.UUID]*time.Time)
	for _, successful := range []bool{false, true} {
		rows, err := tx.QueryContext(ctx, `SELECT id,app_id,node_id,status,result FROM (
 SELECT *,row_number() OVER (PARTITION BY app_id ORDER BY coalesce(unixepoch(json_extract(CAST(result AS TEXT),'$.started_at'),'subsec')*1000,created_at) DESC,created_at DESC,id DESC) AS position
 FROM operations WHERE kind='restore_drill' AND (?=0 OR status='succeeded')) WHERE position=1`, successful)
		if err != nil {
			return plan, storeError(err)
		}
		for rows.Next() {
			status := new(proof.Status)
			var appID uuid.UUID
			var raw []byte
			if err := rows.Scan(&status.ID, &appID, &status.NodeID, &status.State, &raw); err != nil {
				rows.Close()
				return plan, storeError(err)
			}
			if len(raw) > 0 && json.Unmarshal(raw, &status.Result) != nil {
				rows.Close()
				return plan, &Error{Code: Unavailable}
			}
			status.DestinationID = status.Result.DestinationID
			if successful {
				tested[appID] = new(status.Result.CompletedAt)
			} else {
				drills[appID] = status
			}
		}
		if err := errors.Join(rows.Err(), rows.Close()); err != nil {
			return plan, storeError(err)
		}
	}
	now := s.now()
	for i := range plan.Apps {
		entry := &plan.Apps[i]
		entry.LastDrill, entry.RestoreTestedAt = drills[entry.App.ID], tested[entry.App.ID]
		entry.State = backup.Unprotected
		if entry.Enabled {
			entry.State = backup.Protected
		}
		targetIDs := slices.Clone(entry.Settings.Destinations)
		for key := range latest {
			if key.app == entry.App.ID && !slices.Contains(targetIDs, key.destination) {
				targetIDs = append(targetIDs, key.destination)
			}
		}
		slices.SortFunc(targetIDs, func(a, b uuid.UUID) int { return bytes.Compare(a[:], b[:]) })
		states := make(map[backup.ProtectionState]bool)
		for _, id := range targetIDs {
			status := backup.TargetStatus{DestinationID: id, State: backup.Pending, Selected: entry.Enabled && slices.Contains(entry.Settings.Destinations, id)}
			if status.Selected {
				schedule := backup.Schedule{ID: entry.ScheduleID, InstallationID: installation.V, AppID: entry.App.ID, NodeID: entry.App.NodeID, SourceKey: entry.App.Definition.Key, Settings: entry.Settings, CreatedAt: entry.ChangedAt}
				status.DueAt = schedule.Due(now)
			}
			key := target{entry.App.ID, id}
			if result, ok := good[key]; ok {
				status.LastGood = &result
				status.DataLossWindowSeconds = new(max(0, int64(now.Sub(result.StartedAt).Seconds())))
				status.State = backup.Protected
				if !status.Selected {
					status.State = backup.Retained
				} else if result.StartedAt.Before(status.DueAt) {
					status.State = backup.Overdue
				}
				if entry.DataLossWindowSeconds == nil || *status.DataLossWindowSeconds < *entry.DataLossWindowSeconds {
					entry.DataLossWindowSeconds = status.DataLossWindowSeconds
				}
			}
			if op, ok := latest[key]; ok {
				status.LastOperationID = op.ID
				status.FailureCode = op.FailureCode
				if !op.Status.Terminal() {
					status.State = backup.ProtectionState(op.Status)
				} else if op.Status == operation.Failed {
					status.State = backup.Failed
				} else if status.State == backup.Protected && status.LastGood != nil && len(status.LastGood.WarningCodes) > 0 {
					status.State = backup.Warning
				}
			}
			if status.Selected {
				states[status.State] = true
			}
			entry.Targets = append(entry.Targets, status)
		}
		if len(states) > 1 {
			entry.State = backup.Partial
		} else {
			for state := range states {
				entry.State = state
			}
		}
		if entry.Enabled && entry.BlockedCode != "" {
			entry.State = backup.Blocked
		}
	}
	return plan, storeCommit(tx)
}
