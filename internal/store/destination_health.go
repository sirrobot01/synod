package store

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/destination"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
)

func (s *Store) RecordDestinationHealth(ctx context.Context, nodeID, sessionID uuid.UUID, report *agentv1.DestinationHealth) error {
	if nodeID == uuid.Nil || sessionID == uuid.Nil || destination.ValidateHealth(report) != nil {
		return &Error{Code: InvalidRequest}
	}
	now := s.now()
	observed := report.ObservedAt.AsTime()
	if observed.After(now.Add(time.Minute)) || observed.Before(now.Add(-5*time.Minute)) {
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
	for _, space := range report.Destinations {
		var owns bool
		if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM destinations WHERE id=? AND node_id=? AND kind IN ('local','device') AND removed_at IS NULL)`, space.DestinationId, nodeID).Scan(&owns); err != nil {
			return storeError(err)
		}
		// Old cached plans can still report a removed destination.
		if !owns {
			continue
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO destination_health VALUES (?,?,?,?,?) ON CONFLICT(destination_id) DO UPDATE SET node_id=excluded.node_id,observed_at=excluded.observed_at,total_bytes=excluded.total_bytes,available_bytes=excluded.available_bytes WHERE excluded.observed_at>destination_health.observed_at`, space.DestinationId, nodeID, observed.UnixMilli(), space.TotalBytes, space.AvailableBytes); err != nil {
			return storeError(err)
		}
	}
	return storeCommit(tx)
}
