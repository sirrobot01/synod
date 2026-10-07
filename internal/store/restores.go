package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/operation"
	"github.com/sirrobot01/synod/internal/restore"
)

// CreateRestore accepts historical apps and requires an approved target device.
func (s *Store) CreateRestore(ctx context.Context, installationID uuid.UUID, input restore.Submission, actorID, requestID string) (operation.Operation, error) {
	job := restore.Request{Submission: input, Kind: "restore", InstallationID: installationID, RequestedAt: s.now().UTC()}
	if job.Validate() != nil || !printableWithin(actorID, 256) || !printableWithin(requestID, 128) {
		return operation.Operation{}, &Error{Code: InvalidRequest}
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return operation.Operation{}, storeError(err)
	}
	defer tx.Rollback()
	var op operation.Operation
	err = tx.QueryRowContext(ctx, `SELECT `+operationColumns+` FROM operations WHERE id=?`, input.ID).Scan(operationFields(&op)...)
	if err == nil {
		var saved restore.Request
		if json.Unmarshal(op.Input, &saved) != nil || saved.Validate() != nil || op.Kind != "restore" || op.ActorID != actorID || saved.InstallationID != installationID || saved.Submission != input {
			return operation.Operation{}, &Error{Code: IdempotencyConflict}
		}
		return op, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return operation.Operation{}, storeError(err)
	}
	var allowed bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM apps WHERE id=?) AND EXISTS(SELECT 1 FROM nodes WHERE id=? AND approved_at IS NOT NULL AND revoked_at IS NULL)`, input.AppID, input.NodeID).Scan(&allowed); err != nil {
		return operation.Operation{}, storeError(err)
	}
	if !allowed {
		return operation.Operation{}, &Error{Code: NotFound}
	}
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM destinations WHERE removed_at IS NULL AND (kind!='local' OR node_id=?) AND (? OR id=?))`, input.NodeID, input.DestinationID == uuid.Nil, input.DestinationID).Scan(&allowed); err != nil {
		return operation.Operation{}, storeError(err)
	}
	if !allowed {
		return operation.Operation{}, &Error{Code: NotFound}
	}
	var count int
	if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM operations WHERE node_id=? AND status NOT IN ('succeeded','failed','cancelled')`, job.NodeID).Scan(&count); err != nil {
		return operation.Operation{}, storeError(err)
	}
	if count >= 64 {
		return operation.Operation{}, &Error{Code: OperationConflict}
	}
	raw, _ := json.Marshal(job)
	now := s.now().UnixMilli()
	if _, err := tx.ExecContext(ctx, `INSERT INTO operations (id,app_id,node_id,actor_id,kind,status,input,created_at,updated_at) VALUES (?,?,?,?,'restore','pending',?,?,?)`, job.ID, job.AppID, job.NodeID, actorID, raw, now, now); err != nil {
		return operation.Operation{}, storeError(err)
	}
	if err := audit(ctx, tx, actorID, requestID, "restore.requested", job.ID, s.now()); err != nil {
		return operation.Operation{}, err
	}
	if err := tx.QueryRowContext(ctx, `SELECT `+operationColumns+` FROM operations WHERE id=?`, job.ID).Scan(operationFields(&op)...); err != nil {
		return operation.Operation{}, storeError(err)
	}
	return op, storeCommit(tx)
}

func (s *Store) RestoreRequests(ctx context.Context) ([]restore.Request, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT input FROM operations WHERE kind='restore' AND status IN ('pending','running','waiting') ORDER BY created_at,id LIMIT 512`)
	if err != nil {
		return nil, storeError(err)
	}
	defer rows.Close()
	var requests []restore.Request
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			return nil, storeError(err)
		}
		var request restore.Request
		if json.Unmarshal(raw, &request) != nil || request.Validate() != nil {
			continue
		}
		requests = append(requests, request)
	}
	if err := rows.Err(); err != nil {
		return nil, storeError(err)
	}
	return requests, nil
}

// RecordRestoreEvent accepts only the target's current approved session.
func (s *Store) RecordRestoreEvent(ctx context.Context, nodeID, sessionID uuid.UUID, event restore.Event) error {
	if nodeID == uuid.Nil || sessionID == uuid.Nil || event.ID == uuid.Nil {
		return &Error{Code: InvalidRequest}
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return storeError(err)
	}
	defer tx.Rollback()
	var approved bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM nodes WHERE id=? AND session_id=? AND approved_at IS NOT NULL AND revoked_at IS NULL)`, nodeID, sessionID).Scan(&approved); err != nil {
		return storeError(err)
	}
	if !approved {
		return ErrStaleSession
	}
	var op operation.Operation
	if err := tx.QueryRowContext(ctx, `SELECT `+operationColumns+` FROM operations WHERE id=? AND node_id=? AND kind='restore'`, event.ID, nodeID).Scan(operationFields(&op)...); err != nil {
		return storeError(err)
	}
	var request restore.Request
	if json.Unmarshal(op.Input, &request) != nil || event.Validate(request) != nil || event.Result.StartedAt.After(s.now().Add(5*time.Minute)) || event.Result.CompletedAt.After(s.now().Add(5*time.Minute)) {
		return &Error{Code: InvalidRequest}
	}
	result, _ := json.Marshal(event.Result)
	if len(result) > 64<<10 {
		return &Error{Code: InvalidRequest}
	}
	changed, err := s.recordOperationEvent(ctx, tx, op, event.Status, result, event.Result.FailureCode)
	if err != nil || !changed {
		return err
	}
	return storeCommit(tx)
}
