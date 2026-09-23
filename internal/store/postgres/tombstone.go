package postgres

import (
	"context"

	"github.com/google/uuid"
)

// BackupDisposition is stated by a delete rather than implied: backups have their own
// retention policy, and purging workload data never purges them.
type BackupDisposition string

const BackupsNoPolicy BackupDisposition = "no_policy"

// Tombstone marks a deleted cluster and what its delete left behind.
type Tombstone struct {
	InstallationID     uuid.UUID
	ClusterID          uuid.UUID
	DeletedByOperation uuid.UUID
	RetainedVolumes    int
	BackupDisposition  BackupDisposition
	// Pending names every node that has not proved its objects are gone.
	Pending []uuid.UUID
	Cleared bool
}

// ReadClusterTombstone answers what a delete left behind and who has not answered.
func (s *Store) ReadClusterTombstone(ctx context.Context, installationID, clusterID uuid.UUID) (Tombstone, error) {
	var found Tombstone
	if installationID == uuid.Nil || clusterID == uuid.Nil {
		return found, &AdmissionError{Code: InvalidRequest}
	}
	var disposition string
	var cleared *string
	if err := s.pool.QueryRow(ctx, `SELECT installation_id,cluster_id,deleted_by_operation,retained_volumes,
 backup_disposition,cleared_at::text FROM cluster_tombstones WHERE installation_id=$1 AND cluster_id=$2`,
		installationID, clusterID).Scan(&found.InstallationID, &found.ClusterID, &found.DeletedByOperation,
		&found.RetainedVolumes, &disposition, &cleared); err != nil {
		return Tombstone{}, admissionError(err)
	}
	found.BackupDisposition = BackupDisposition(disposition)
	found.Cleared = cleared != nil
	rows, err := s.pool.Query(ctx, `SELECT node_id FROM cluster_tombstone_nodes
 WHERE installation_id=$1 AND cluster_id=$2 AND acknowledged_at IS NULL ORDER BY node_id`, installationID, clusterID)
	if err != nil {
		return Tombstone{}, admissionError(err)
	}
	defer rows.Close()
	for rows.Next() {
		var node uuid.UUID
		if err := rows.Scan(&node); err != nil {
			return Tombstone{}, admissionError(err)
		}
		found.Pending = append(found.Pending, node)
	}
	if rows.Err() != nil {
		return Tombstone{}, admissionError(rows.Err())
	}
	return found, nil
}
