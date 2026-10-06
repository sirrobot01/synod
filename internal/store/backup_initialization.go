package store

import (
	"context"
	"encoding/json"
	"time"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/backup"
	"github.com/sirrobot01/synod/internal/inventory"
	"github.com/sirrobot01/synod/internal/operation"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/proto"
)

type BackupInitialization struct {
	Schedule      backup.Schedule
	DestinationID uuid.UUID
}

// PendingInitializations lists usable apps whose selected repositories have not opened yet.
func (s *Store) PendingInitializations(ctx context.Context) ([]BackupInitialization, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT s.definition,d.id,a.definition FROM app_protection p JOIN backup_schedules s ON s.id=p.schedule_id JOIN apps a ON a.id=p.app_id JOIN nodes n ON n.id=a.node_id JOIN json_each(CAST(p.settings AS TEXT),'$.destinations') selected JOIN destinations d ON d.id=selected.value WHERE p.enabled=1 AND a.present=1 AND n.approved_at IS NOT NULL AND n.revoked_at IS NULL AND d.initialized_at IS NULL AND d.removed_at IS NULL AND (d.kind!='local' OR d.node_id=a.node_id) ORDER BY d.id,a.id LIMIT 512`)
	if err != nil {
		return nil, storeError(err)
	}
	defer rows.Close()
	var result []BackupInitialization
	for rows.Next() {
		var candidate BackupInitialization
		var raw, definition []byte
		if err := rows.Scan(&raw, &candidate.DestinationID, &definition); err != nil {
			return nil, storeError(err)
		}
		app := inventory.App{Present: true, Definition: new(agentv1.DiscoveredApp)}
		if json.Unmarshal(raw, &candidate.Schedule) != nil || candidate.Schedule.Validate() != nil || proto.Unmarshal(definition, app.Definition) != nil {
			return nil, &Error{Code: Unavailable}
		}
		if backup.BlockedCode(app) == "" {
			result = append(result, candidate)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, storeError(err)
	}
	return result, nil
}

// CreateInitializationBackup gives a repository one online owner before offline schedules use it.
// One schedule can make one attempt per daily slot. An exact retry returns the saved operation.
func (s *Store) CreateInitializationBackup(ctx context.Context, scheduleID, destinationID uuid.UUID, slot time.Time) (operation.Operation, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return operation.Operation{}, storeError(err)
	}
	defer tx.Rollback()
	var raw []byte
	if err := tx.QueryRowContext(ctx, `SELECT s.definition FROM backup_schedules s JOIN app_protection p ON p.schedule_id=s.id JOIN destinations d ON d.id=? WHERE s.id=? AND p.enabled=1 AND d.initialized_at IS NULL AND d.removed_at IS NULL`, destinationID, scheduleID).Scan(&raw); err != nil {
		return operation.Operation{}, storeError(err)
	}
	var schedule backup.Schedule
	if json.Unmarshal(raw, &schedule) != nil {
		return operation.Operation{}, &Error{Code: Unavailable}
	}
	request, err := schedule.Request(destinationID, slot)
	if err != nil || slot.After(s.now().Add(5*time.Minute)) {
		return operation.Operation{}, &Error{Code: InvalidRequest}
	}
	input := backup.Submission{ID: uuid.NewSHA1(scheduleID, []byte("initialize/"+destinationID.String()+"/"+slot.UTC().Format(time.RFC3339Nano))), AppID: request.AppID, DestinationID: destinationID, Method: request.Method}
	op, err := s.createBackup(ctx, tx, schedule.InstallationID, input, "schedule:"+scheduleID.String(), input.ID.String())
	if err != nil {
		return operation.Operation{}, err
	}
	return op, storeCommit(tx)
}
