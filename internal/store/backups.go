package store

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/backup"
	"github.com/sirrobot01/synod/internal/operation"
)

const BackupInitializing ErrorCode = "backup_initializing"

// CreateBackup commits an immutable request and assigns at most one repository initializer.
func (s *Store) CreateBackup(ctx context.Context, installationID uuid.UUID, input backup.Submission, actorID, requestID string) (operation.Operation, error) {
	if installationID == uuid.Nil || input.ID == uuid.Nil || input.AppID == uuid.Nil || input.DestinationID == uuid.Nil || (input.Method != backup.Stop && input.Method != backup.Live) || !printableWithin(actorID, 256) || !printableWithin(requestID, 128) {
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
		var saved backup.Request
		if json.Unmarshal(op.Input, &saved) != nil || op.ActorID != actorID || op.Kind != "backup" || saved.InstallationID != installationID || saved.AppID != input.AppID || saved.DestinationID != input.DestinationID || saved.Method != input.Method {
			return operation.Operation{}, &Error{Code: IdempotencyConflict}
		}
		return op, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return operation.Operation{}, storeError(err)
	}
	job := backup.Request{ID: input.ID, AppID: input.AppID, InstallationID: installationID, DestinationID: input.DestinationID, Method: input.Method, RequestedAt: s.now().UTC()}
	if err := tx.QueryRowContext(ctx, `SELECT a.node_id,a.source_key FROM apps a JOIN nodes n ON n.id=a.node_id WHERE a.id=? AND a.present=1 AND n.approved_at IS NOT NULL AND n.revoked_at IS NULL`, input.AppID).Scan(&job.NodeID, &job.SourceKey); err != nil {
		return operation.Operation{}, storeError(err)
	}
	var kind string
	var owner, initializer sql.NullString
	var initialized sql.NullInt64
	if err := tx.QueryRowContext(ctx, `SELECT kind,node_id,initializer_id,initialized_at FROM destinations WHERE id=? AND removed_at IS NULL`, input.DestinationID).Scan(&kind, &owner, &initializer, &initialized); err != nil {
		return operation.Operation{}, storeError(err)
	}
	if kind == "local" && owner.String != job.NodeID.String() {
		return operation.Operation{}, &Error{Code: InvalidRequest}
	}

	if !initialized.Valid {
		// Different destination names can refer to the same storage. Serialize creation for the installation.
		var creating bool
		if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM destinations d JOIN operations o ON o.id=d.initializer_id WHERE d.initialized_at IS NULL AND o.status NOT IN ('succeeded','failed','cancelled'))`).Scan(&creating); err != nil {
			return operation.Operation{}, storeError(err)
		}
		if creating {
			return operation.Operation{}, &Error{Code: BackupInitializing}
		}

		if initializer.Valid {
			var status operation.Status
			if err := tx.QueryRowContext(ctx, `SELECT status FROM operations WHERE id=?`, initializer.String).Scan(&status); err != nil {
				return operation.Operation{}, storeError(err)
			}
			if !status.Terminal() {
				return operation.Operation{}, &Error{Code: BackupInitializing}
			}
		}
		job.Initialize = true
	}
	var count int
	if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM operations WHERE node_id=? AND status NOT IN ('succeeded','failed','cancelled')`, job.NodeID).Scan(&count); err != nil {
		return operation.Operation{}, storeError(err)
	}
	if count >= 64 {
		return operation.Operation{}, &Error{Code: OperationConflict}
	}
	if job.Validate() != nil {
		return operation.Operation{}, &Error{Code: InvalidRequest}
	}
	raw, _ := json.Marshal(job)
	now := s.now().UnixMilli()
	if _, err := tx.ExecContext(ctx, `INSERT INTO operations (id,app_id,node_id,actor_id,kind,status,input,created_at,updated_at) VALUES (?,?,?,?,'backup','pending',?,?,?)`, job.ID, job.AppID, job.NodeID, actorID, raw, now, now); err != nil {
		return operation.Operation{}, storeError(err)
	}
	if job.Initialize {
		if _, err := tx.ExecContext(ctx, `UPDATE destinations SET initializer_id=? WHERE id=?`, job.ID, job.DestinationID); err != nil {
			return operation.Operation{}, storeError(err)
		}
	}
	if err := audit(ctx, tx, actorID, requestID, "backup.requested", job.ID, s.now()); err != nil {
		return operation.Operation{}, err
	}
	if err := tx.QueryRowContext(ctx, `SELECT `+operationColumns+` FROM operations WHERE id=?`, job.ID).Scan(operationFields(&op)...); err != nil {
		return operation.Operation{}, storeError(err)
	}
	return op, storeCommit(tx)
}

// BackupRequests lists unfinished work. The transport still checks approval and session identity.
func (s *Store) BackupRequests(ctx context.Context) ([]backup.Request, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT input FROM operations WHERE kind='backup' AND status IN ('pending','running','waiting') ORDER BY created_at,id LIMIT 512`)
	if err != nil {
		return nil, storeError(err)
	}
	defer rows.Close()
	var requests []backup.Request
	for rows.Next() {
		var input []byte
		if err := rows.Scan(&input); err != nil {
			return nil, storeError(err)
		}
		var request backup.Request
		if json.Unmarshal(input, &request) != nil || request.Validate() != nil {
			continue
		} // Generic operation records carry no executable request.
		requests = append(requests, request)
	}
	if err := rows.Err(); err != nil {
		return nil, storeError(err)
	}
	return requests, nil
}

// RecordBackupEvent accepts only the current approved device session. A terminal result is immutable.
func (s *Store) RecordBackupEvent(ctx context.Context, nodeID, sessionID uuid.UUID, event backup.Event) error {
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
	if err := tx.QueryRowContext(ctx, `SELECT `+operationColumns+` FROM operations WHERE id=? AND node_id=? AND kind='backup'`, event.ID, nodeID).Scan(operationFields(&op)...); err != nil {
		return storeError(err)
	}
	var request backup.Request
	if json.Unmarshal(op.Input, &request) != nil || request.Validate() != nil || event.Validate(request) != nil {
		return &Error{Code: InvalidRequest}
	}
	result, _ := json.Marshal(event.Result)
	if len(result) > 64<<10 {
		return &Error{Code: InvalidRequest}
	}
	next := operation.Status(event.Status)
	if event.Status == "queued" {
		next = operation.Pending
	}
	if op.Status.Terminal() {
		if next == op.Status && bytes.Equal(result, op.Result) {
			return nil
		}
		if !next.Terminal() {
			return nil
		}
		return &Error{Code: OperationConflict}
	}
	// A reconnect may deliver the terminal event before an intermediate event.
	if op.Status == operation.Pending && next != operation.Pending {
		if !op.Status.CanTransition(operation.Running) {
			return &Error{Code: OperationConflict}
		}
		if _, err := tx.ExecContext(ctx, `UPDATE operations SET status='running' WHERE id=?`, op.ID); err != nil {
			return storeError(err)
		}
		op.Status = operation.Running
	}
	if op.Status == operation.Waiting && next.Terminal() {
		if !op.Status.CanTransition(operation.Running) {
			return &Error{Code: OperationConflict}
		}
		if _, err := tx.ExecContext(ctx, `UPDATE operations SET status='running' WHERE id=?`, op.ID); err != nil {
			return storeError(err)
		}
		op.Status = operation.Running
	}
	if next == operation.Pending && op.Status != operation.Pending {
		return nil
	}
	if next != op.Status && !op.Status.CanTransition(next) {
		return &Error{Code: OperationConflict}
	}
	if next == op.Status && bytes.Equal(result, op.Result) {
		return nil
	}
	var finished any
	now := s.now().UnixMilli()
	if next.Terminal() {
		finished = now
	}
	if _, err := tx.ExecContext(ctx, `UPDATE operations SET status=?,failure_code=?,result=?,updated_at=?,finished_at=? WHERE id=?`, next, event.Result.FailureCode, result, now, finished, op.ID); err != nil {
		return storeError(err)
	}
	if event.Result.RepositoryOpened {
		if _, err := tx.ExecContext(ctx, `UPDATE destinations SET initialized_at=coalesce(initialized_at,?) WHERE id=? AND (initialized_at IS NOT NULL OR initializer_id=?)`, now, request.DestinationID, op.ID); err != nil {
			return storeError(err)
		}
	}
	return storeCommit(tx)
}
