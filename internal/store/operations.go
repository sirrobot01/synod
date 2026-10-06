package store

import (
	"context"
	"database/sql"
	"errors"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/operation"
)

const OperationConflict ErrorCode = "operation_conflict"

const operationColumns = `id,app_id,node_id,actor_id,kind,status,failure_code,created_at,updated_at,finished_at`

// operationFields keeps every operation query in the same column order.
func operationFields(op *operation.Operation) []any {
	return []any{&op.ID, &op.AppID, &op.NodeID, &op.ActorID, &op.Kind, &op.Status, &op.FailureCode, millis{&op.CreatedAt}, millis{&op.UpdatedAt}, optionalMillis{&op.FinishedAt}}
}

// CreateOperation records pending work on an approved device. An exact retry keeps the first record.
func (s *Store) CreateOperation(ctx context.Context, op operation.Operation) (operation.Operation, error) {
	if op.ID == uuid.Nil || op.AppID == uuid.Nil || op.NodeID == uuid.Nil || !printableWithin(op.ActorID, 256) || !printableWithin(op.Kind, 64) {
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
		if prior.AppID != op.AppID || prior.NodeID != op.NodeID || prior.ActorID != op.ActorID || prior.Kind != op.Kind {
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
	if _, err := tx.ExecContext(ctx, `INSERT INTO operations (id,app_id,node_id,actor_id,kind,status,created_at,updated_at) VALUES (?,?,?,?,?,'pending',?,?)`, op.ID, op.AppID, op.NodeID, op.ActorID, op.Kind, now, now); err != nil {
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
	result, err := s.db.ExecContext(ctx, `UPDATE operations SET status=?,failure_code=?,updated_at=?,finished_at=? WHERE id=? AND status=?`, to, failureCode, now, finished, id, from)
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
