package store

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/operation"
)

const OperationConflict ErrorCode = "operation_conflict"

const operationColumns = `id,app_id,node_id,actor_id,kind,status,failure_code,created_at,updated_at,finished_at,input,result`

// operationFields keeps every operation query in the same column order.
func operationFields(op *operation.Operation) []any {
	return []any{&op.ID, &op.AppID, &op.NodeID, &op.ActorID, &op.Kind, &op.Status, &op.FailureCode, millis{&op.CreatedAt}, millis{&op.UpdatedAt}, optionalMillis{&op.FinishedAt}, &op.Input, operationResult{&op.Result}}
}

// CreateOperation records pending work on an approved device. An exact retry keeps the first record.
func (s *Store) CreateOperation(ctx context.Context, op operation.Operation) (operation.Operation, error) {
	if len(op.Input) == 0 {
		op.Input = []byte("{}")
	}
	if len(op.Input) > 8192 || !json.Valid(op.Input) || op.ID == uuid.Nil || op.AppID == uuid.Nil || op.NodeID == uuid.Nil || !printableWithin(op.ActorID, 256) || !printableWithin(op.Kind, 64) {
		return operation.Operation{}, &Error{Code: InvalidRequest}
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return operation.Operation{}, storeError(err)
	}
	defer tx.Rollback()
	var prior operation.Operation
	err = tx.QueryRowContext(ctx, `SELECT `+operationColumns+` FROM operations WHERE id=?`, op.ID).Scan(operationFields(&prior)...)
	if err == nil {
		if prior.AppID != op.AppID || prior.NodeID != op.NodeID || prior.ActorID != op.ActorID || prior.Kind != op.Kind || !bytes.Equal(prior.Input, op.Input) {
			return operation.Operation{}, &Error{Code: IdempotencyConflict}
		}
		return prior, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return operation.Operation{}, storeError(err)
	}
	var approved bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM nodes WHERE id=? AND approved_at IS NOT NULL AND revoked_at IS NULL)`, op.NodeID).Scan(&approved); err != nil {
		return operation.Operation{}, storeError(err)
	}
	if !approved {
		return operation.Operation{}, &Error{Code: OperationConflict}
	}
	now := s.now().UnixMilli()
	if _, err := tx.ExecContext(ctx, `INSERT INTO operations (id,app_id,node_id,actor_id,kind,status,created_at,updated_at,input) VALUES (?,?,?,?,?,'pending',?,?,?)`, op.ID, op.AppID, op.NodeID, op.ActorID, op.Kind, now, now, op.Input); err != nil {
		return operation.Operation{}, storeError(err)
	}
	if err := tx.QueryRowContext(ctx, `SELECT `+operationColumns+` FROM operations WHERE id=?`, op.ID).Scan(operationFields(&op)...); err != nil {
		return operation.Operation{}, storeError(err)
	}
	if err := tx.Commit(); err != nil {
		return operation.Operation{}, storeError(err)
	}
	return op, nil
}

func (s *Store) GetOperation(ctx context.Context, id uuid.UUID) (operation.Operation, error) {
	var op operation.Operation
	if err := s.db.QueryRowContext(ctx, `SELECT `+operationColumns+` FROM operations WHERE id=?`, id).Scan(operationFields(&op)...); err != nil {
		return operation.Operation{}, storeError(err)
	}
	return op, nil
}

// ListOperations returns at most 100 recent operations, newest first.
func (s *Store) ListOperations(ctx context.Context) ([]operation.Operation, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT `+operationColumns+` FROM operations ORDER BY created_at DESC,id LIMIT 100`)
	if err != nil {
		return nil, storeError(err)
	}
	defer rows.Close()
	ops := make([]operation.Operation, 0)
	for rows.Next() {
		var op operation.Operation
		if err := rows.Scan(operationFields(&op)...); err != nil {
			return nil, storeError(err)
		}
		ops = append(ops, op)
	}
	if err := rows.Err(); err != nil {
		return nil, storeError(err)
	}
	return ops, nil
}

// TransitionOperation changes state only when the saved state matches from.
func (s *Store) TransitionOperation(ctx context.Context, id uuid.UUID, from, to operation.Status, failureCode string) error {
	if !from.CanTransition(to) || (to == operation.Failed) != (failureCode != "") || (failureCode != "" && !printableWithin(failureCode, 128)) {
		return &Error{Code: InvalidRequest}
	}
	var finished any
	now := s.now().UnixMilli()
	if to.Terminal() {
		finished = now
	}
	result, err := s.db.ExecContext(ctx, `UPDATE operations SET status=?,failure_code=?,updated_at=?,finished_at=? WHERE id=? AND status=? AND NOT (kind IN ('backup','restore') AND coalesce(json_extract(CAST(input AS TEXT),'$.id'),'')=id)`, to, failureCode, now, finished, id, from)
	if err != nil {
		return storeError(err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return storeError(err)
	}
	if changed == 0 {
		return &Error{Code: OperationConflict}
	}
	return nil
}

// operationResult preserves SQL NULL as an absent JSON result.
type operationResult struct{ target *json.RawMessage }

func (r operationResult) Scan(value any) error {
	switch v := value.(type) {
	case nil:
		*r.target = nil
	case []byte:
		*r.target = bytes.Clone(v)
	case string:
		*r.target = json.RawMessage(v)
	default:
		return errors.New("invalid operation result storage type")
	}
	return nil
}

// recordOperationEvent preserves terminal results and accepts replay after a reconnect.
func (s *Store) recordOperationEvent(ctx context.Context, tx *sql.Tx, op operation.Operation, status string, result []byte, failureCode string) (bool, error) {
	next := operation.Status(status)
	if status == "queued" {
		next = operation.Pending
	}
	if op.Status.Terminal() {
		if next == op.Status && bytes.Equal(result, op.Result) {
			return false, nil
		}
		if !next.Terminal() {
			return false, nil
		}
		return false, &Error{Code: OperationConflict}
	}
	// A reconnect may deliver the terminal event before an intermediate event.
	if op.Status == operation.Pending && next != operation.Pending {
		if !op.Status.CanTransition(operation.Running) {
			return false, &Error{Code: OperationConflict}
		}
		if _, err := tx.ExecContext(ctx, `UPDATE operations SET status='running' WHERE id=?`, op.ID); err != nil {
			return false, storeError(err)
		}
		op.Status = operation.Running
	}
	if op.Status == operation.Waiting && next.Terminal() {
		if !op.Status.CanTransition(operation.Running) {
			return false, &Error{Code: OperationConflict}
		}
		if _, err := tx.ExecContext(ctx, `UPDATE operations SET status='running' WHERE id=?`, op.ID); err != nil {
			return false, storeError(err)
		}
		op.Status = operation.Running
	}
	if next == operation.Pending && op.Status != operation.Pending {
		return false, nil
	}
	if next != op.Status && !op.Status.CanTransition(next) {
		return false, &Error{Code: OperationConflict}
	}
	if next == op.Status && bytes.Equal(result, op.Result) {
		return false, nil
	}
	var finished any
	now := s.now().UnixMilli()
	if next.Terminal() {
		finished = now
	}
	if _, err := tx.ExecContext(ctx, `UPDATE operations SET status=?,failure_code=?,result=?,updated_at=?,finished_at=? WHERE id=?`, next, failureCode, result, now, finished, op.ID); err != nil {
		return false, storeError(err)
	}
	return true, nil
}
