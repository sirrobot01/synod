package postgres

import (
	"context"
	"errors"

	"github.com/google/uuid"
)

// ErrTombstoneHeld reports a delete that is not finished because a node has not
// proved its runtime objects are gone. An offline node holds a tombstone open.
var ErrTombstoneHeld = errors.New("tombstone_held")

// BackupDisposition is what a cluster delete did about its backup artifacts.
//
// Backups have their own retention and deletion policy, so a delete states this
// rather than implying it. Purging workload data never implies purging backups.
type BackupDisposition string

const (
	BackupsRetainedUnderPolicy BackupDisposition = "retained_under_policy"
	BackupsNoPolicy            BackupDisposition = "no_policy"
)

// Tombstone marks a deleted cluster and what its delete left behind.
type Tombstone struct {
	InstallationID     uuid.UUID
	ClusterID          uuid.UUID
	DeletedByOperation uuid.UUID
	RetainedVolumes    int
	BackupDisposition  BackupDisposition
	// Pending names every node that has not yet proved its objects are gone.
	// A tombstone with a pending node is not cleared, whatever the controller did.
	Pending []uuid.UUID
	Cleared bool
}

// TombstoneRequest records one delete.
type TombstoneRequest struct {
	InstallationID     uuid.UUID
	ClusterID          uuid.UUID
	DeletedByOperation uuid.UUID
	RetainedVolumes    int
	BackupDisposition  BackupDisposition
	// Nodes are every node that held part of this cluster. Each must acknowledge
	// before the tombstone clears.
	Nodes []uuid.UUID
}

// CreateClusterTombstone records a delete and the nodes that must prove cleanup.
//
// A repeat with the same content is an ordinary replay. A repeat that disagrees is a
// conflict rather than an update: a delete that changed what it retained is a delete
// nobody can account for afterwards.
func (s *Store) CreateClusterTombstone(ctx context.Context, request TombstoneRequest) error {
	if request.InstallationID == uuid.Nil || request.ClusterID == uuid.Nil || request.DeletedByOperation == uuid.Nil ||
		request.RetainedVolumes < 0 || len(request.Nodes) == 0 || len(request.Nodes) > maxTombstoneNodes ||
		(request.BackupDisposition != BackupsRetainedUnderPolicy && request.BackupDisposition != BackupsNoPolicy) {
		return &AdmissionError{Code: InvalidRequest}
	}
	seen := make(map[uuid.UUID]bool, len(request.Nodes))
	for _, node := range request.Nodes {
		if node == uuid.Nil || seen[node] {
			return &AdmissionError{Code: InvalidRequest}
		}
		seen[node] = true
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return admissionError(err)
	}
	defer tx.Rollback(ctx)
	tag, err := tx.Exec(ctx, `INSERT INTO cluster_tombstones
 (installation_id,cluster_id,deleted_by_operation,retained_volumes,backup_disposition)
 VALUES ($1,$2,$3,$4,$5) ON CONFLICT (installation_id,cluster_id) DO NOTHING`,
		request.InstallationID, request.ClusterID, request.DeletedByOperation,
		request.RetainedVolumes, string(request.BackupDisposition))
	if err != nil {
		return admissionError(err)
	}
	if tag.RowsAffected() == 0 {
		var operationID uuid.UUID
		var retained int
		var disposition string
		if err := tx.QueryRow(ctx, `SELECT deleted_by_operation,retained_volumes,backup_disposition
 FROM cluster_tombstones WHERE installation_id=$1 AND cluster_id=$2`,
			request.InstallationID, request.ClusterID).Scan(&operationID, &retained, &disposition); err != nil {
			return admissionError(err)
		}
		if operationID != request.DeletedByOperation || retained != request.RetainedVolumes || disposition != string(request.BackupDisposition) {
			return ErrTombstoneConflict
		}
		return nil
	}
	for _, node := range request.Nodes {
		if _, err := tx.Exec(ctx, `INSERT INTO cluster_tombstone_nodes (installation_id,cluster_id,node_id)
 VALUES ($1,$2,$3) ON CONFLICT DO NOTHING`, request.InstallationID, request.ClusterID, node); err != nil {
			return admissionError(err)
		}
	}
	return s.commitMetadata(ctx, tx)
}

// AcknowledgeTombstone records that one node proved its runtime objects are gone.
//
// The tombstone clears only when the last named node acknowledges. Nothing here
// removes data: the volumes a delete retained stay retained.
func (s *Store) AcknowledgeTombstone(ctx context.Context, installationID, clusterID, nodeID uuid.UUID) error {
	if installationID == uuid.Nil || clusterID == uuid.Nil || nodeID == uuid.Nil {
		return &AdmissionError{Code: InvalidRequest}
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return admissionError(err)
	}
	defer tx.Rollback(ctx)
	// Lock the tombstone so two acknowledgements cannot both read a pending node
	// and both decide they were not the last.
	var cleared *string
	if err := tx.QueryRow(ctx, `SELECT cleared_at::text FROM cluster_tombstones
 WHERE installation_id=$1 AND cluster_id=$2 FOR UPDATE`, installationID, clusterID).Scan(&cleared); err != nil {
		return admissionError(err)
	}
	tag, err := tx.Exec(ctx, `UPDATE cluster_tombstone_nodes SET acknowledged_at=clock_timestamp()
 WHERE installation_id=$1 AND cluster_id=$2 AND node_id=$3 AND acknowledged_at IS NULL`,
		installationID, clusterID, nodeID)
	if err != nil {
		return admissionError(err)
	}
	if tag.RowsAffected() == 0 {
		// Either this node was never named, or it already acknowledged. A repeat is
		// ordinary; a node that was never part of the cluster is not.
		var named bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM cluster_tombstone_nodes
 WHERE installation_id=$1 AND cluster_id=$2 AND node_id=$3)`, installationID, clusterID, nodeID).Scan(&named); err != nil {
			return admissionError(err)
		}
		if !named {
			return &AdmissionError{Code: InvalidRequest}
		}
	}
	var pending int
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM cluster_tombstone_nodes
 WHERE installation_id=$1 AND cluster_id=$2 AND acknowledged_at IS NULL`, installationID, clusterID).Scan(&pending); err != nil {
		return admissionError(err)
	}
	if pending == 0 {
		if _, err := tx.Exec(ctx, `UPDATE cluster_tombstones SET cleared_at=clock_timestamp()
 WHERE installation_id=$1 AND cluster_id=$2 AND cleared_at IS NULL`, installationID, clusterID); err != nil {
			return admissionError(err)
		}
	}
	return s.commitMetadata(ctx, tx)
}

// ReleaseTombstonedPlacement frees the capacity and ports a deleted cluster held.
//
// Specification 33 keeps the tombstone and the reservations for an offline node until
// cleanup is proved. Until every node has answered, this refuses: releasing a port or
// a capacity reservation tells the planner that a host resource is free, and a node
// that is offline may still be holding it.
//
// It releases reservations only. No data is removed, and a repeat is ordinary.
func (s *Store) ReleaseTombstonedPlacement(ctx context.Context, installationID, clusterID uuid.UUID) error {
	if installationID == uuid.Nil || clusterID == uuid.Nil {
		return &AdmissionError{Code: InvalidRequest}
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return admissionError(err)
	}
	defer tx.Rollback(ctx)
	// The tombstone row is what serializes this against the last acknowledgement, so
	// a release and a final acknowledgement cannot both decide they went first.
	var cleared *string
	if err := tx.QueryRow(ctx, `SELECT cleared_at::text FROM cluster_tombstones
 WHERE installation_id=$1 AND cluster_id=$2 FOR UPDATE`, installationID, clusterID).Scan(&cleared); err != nil {
		return admissionError(err)
	}
	if cleared == nil {
		return ErrTombstoneHeld
	}
	if _, err := tx.Exec(ctx, `UPDATE resource_allocations SET released_at=clock_timestamp()
 WHERE installation_id=$1 AND cluster_id=$2 AND released_at IS NULL`, installationID, clusterID); err != nil {
		return admissionError(err)
	}
	if _, err := tx.Exec(ctx, `UPDATE allocation_ports SET released_at=clock_timestamp()
 WHERE installation_id=$1 AND allocation_id IN
 (SELECT id FROM resource_allocations WHERE installation_id=$1 AND cluster_id=$2) AND released_at IS NULL`,
		installationID, clusterID); err != nil {
		return admissionError(err)
	}
	return s.commitMetadata(ctx, tx)
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

// ErrTombstoneConflict reports a second delete record that disagrees with the first.
var ErrTombstoneConflict = errors.New("tombstone_conflict")

// maxTombstoneNodes bounds one delete. A cluster spans far fewer nodes than this.
const maxTombstoneNodes = 256
