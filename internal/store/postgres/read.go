package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/sirrobot01/synod/internal/operation"
)

func (s *Store) GetOperation(ctx context.Context, installationID, operationID uuid.UUID) (operation.Operation, error) {
	if installationID == uuid.Nil || operationID == uuid.Nil {
		return operation.Operation{}, &AdmissionError{Code: InvalidRequest}
	}
	var result operation.Operation
	var hash []byte
	err := s.pool.QueryRow(ctx, `SELECT id, installation_id, app_id, actor_id, api_operation, request_hash,
		idempotency_key, desired_revision, kind, status, plan_version, owner_id, execution_epoch,
		claim_expires_at, attempts, next_attempt_at, cancellation_requested_at, created_at, updated_at, finished_at, failure_code
		FROM operations WHERE installation_id=$1 AND id=$2`, installationID, operationID).
		Scan(&result.ID, &result.InstallationID, &result.AppID, &result.ActorID, &result.APIOperation, &hash,
			&result.IdempotencyKey, &result.DesiredRevision, &result.Kind, &result.Status, &result.PlanVersion, &result.OwnerID,
			&result.ExecutionEpoch, &result.ClaimExpiresAt, &result.Attempts, &result.NextAttemptAt, &result.CancellationRequestedAt,
			&result.CreatedAt, &result.UpdatedAt, &result.FinishedAt, &result.FailureCode)
	if err != nil {
		return operation.Operation{}, admissionError(err)
	}
	if len(hash) != len(result.RequestHash) {
		return operation.Operation{}, admissionError(fmt.Errorf("invalid operation %s hash length", operationID))
	}
	copy(result.RequestHash[:], hash)
	return result, nil
}

// RecoveryGeneration returns the installation recovery generation.
// It returns the nil UUID when the installation has none. That is not an error.
// A claim must carry the current generation, so a caller reads it before it claims.
func (s *Store) RecoveryGeneration(ctx context.Context, installationID uuid.UUID) (uuid.UUID, error) {
	if installationID == uuid.Nil {
		return uuid.Nil, &AdmissionError{Code: InvalidRequest}
	}
	var generation uuid.UUID
	err := s.pool.QueryRow(ctx, `SELECT recovery_generation FROM installation_generations WHERE installation_id=$1`, installationID).Scan(&generation)
	if errors.Is(err, pgx.ErrNoRows) {
		return uuid.Nil, nil
	}
	if err != nil {
		return uuid.Nil, admissionError(err)
	}
	return generation, nil
}

// OutboxEvent is one committed notification waiting for delivery.
type OutboxEvent struct {
	ID             int64
	EventID        uuid.UUID
	InstallationID uuid.UUID
	AppID          uuid.UUID
	OperationID    uuid.UUID
	Kind           string
	SchemaVersion  int
	CreatedAt      time.Time
}

// ListPendingOutbox reads committed notifications without granting dispatch authority.
// Restart scans at zero. Sequence allocation order is not transaction commit order.
func (s *Store) ListPendingOutbox(ctx context.Context, installationID uuid.UUID, afterID int64, limit int) ([]OutboxEvent, error) {
	if installationID == uuid.Nil || afterID < 0 || limit < 1 || limit > 1000 {
		return nil, &AdmissionError{Code: InvalidRequest}
	}
	rows, err := s.pool.Query(ctx, `SELECT id, event_id, installation_id, app_id, operation_id, kind, schema_version, created_at
		FROM outbox WHERE installation_id=$1 AND id>$2 AND delivered_at IS NULL ORDER BY id LIMIT $3`, installationID, afterID, limit)
	if err != nil {
		return nil, admissionError(err)
	}
	defer rows.Close()
	result := make([]OutboxEvent, 0)
	for rows.Next() {
		var event OutboxEvent
		if err := rows.Scan(&event.ID, &event.EventID, &event.InstallationID, &event.AppID, &event.OperationID, &event.Kind, &event.SchemaVersion, &event.CreatedAt); err != nil {
			return nil, admissionError(err)
		}
		result = append(result, event)
	}
	if err := rows.Err(); err != nil {
		return nil, admissionError(err)
	}
	return result, nil
}
