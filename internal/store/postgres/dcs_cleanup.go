package postgres

import (
	"context"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// ListPendingDCSCleanups answers the deleted Patroni clusters whose DCS scope still needs removal.
func (s *Store) ListPendingDCSCleanups(ctx context.Context, installationID uuid.UUID) ([]uuid.UUID, error) {
	if installationID == uuid.Nil {
		return nil, &AdmissionError{Code: InvalidRequest}
	}
	rows, err := s.pool.Query(ctx, `SELECT cluster_id FROM patroni_dcs_cleanups
 WHERE installation_id=$1 AND completed_at IS NULL ORDER BY requested_at, cluster_id LIMIT 100`, installationID)
	if err != nil {
		return nil, admissionError(err)
	}
	clusters, err := pgx.CollectRows(rows, pgx.RowTo[uuid.UUID])
	if err != nil {
		return nil, admissionError(err)
	}
	return clusters, nil
}

// CompleteDCSCleanup records that the controller removed one cluster's DCS scope. A repeat changes nothing.
func (s *Store) CompleteDCSCleanup(ctx context.Context, installationID, clusterID uuid.UUID) error {
	if installationID == uuid.Nil || clusterID == uuid.Nil {
		return &AdmissionError{Code: InvalidRequest}
	}
	if _, err := s.pool.Exec(ctx, `UPDATE patroni_dcs_cleanups SET completed_at=clock_timestamp()
 WHERE installation_id=$1 AND cluster_id=$2 AND completed_at IS NULL`, installationID, clusterID); err != nil {
		return admissionError(err)
	}
	return nil
}
