package postgres

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"slices"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/sirrobot01/synod/internal/placement"
)

// Node maintenance refusals. Each names what is unhandled, because an operator who
// reads "cannot remove node" learns nothing about what to do next.
var (
	ErrNodeNotCordoned   = errors.New("node_not_cordoned")
	ErrWorkloadOnNode    = errors.New("workload_on_node")
	ErrStorageUndisposed = errors.New("retained_storage_undisposed")
)

// SetNodeCordon stops or resumes new placement on one node. It edits the policy only,
// so everything already placed stays where it is.
//
// A change bumps the policy revision, which invalidates the node's capacity probe: an
// uncordoned node must be measured again before it takes work. Setting the value it
// already has writes nothing, so a repeat does not keep throwing evidence away.
func (s *Store) SetNodeCordon(ctx context.Context, installationID, nodeID uuid.UUID, cordoned bool) (int64, error) {
	if installationID == uuid.Nil || nodeID == uuid.Nil {
		return 0, &AdmissionError{Code: InvalidRequest}
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, admissionError(err)
	}
	defer tx.Rollback(ctx)
	var revision int64
	var encoded []byte
	if err := tx.QueryRow(ctx, `SELECT revision,policy_bytes FROM node_placement_policies
 WHERE installation_id=$1 AND node_id=$2 FOR UPDATE`, installationID, nodeID).Scan(&revision, &encoded); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			// The planner refuses a node with no policy, so there is nothing to cordon.
			return 0, &AdmissionError{Code: NotFound}
		}
		return 0, admissionError(err)
	}
	var policy placement.NodePolicy
	if json.Unmarshal(encoded, &policy) != nil {
		return 0, &AdmissionError{Code: DependencyUnavailable}
	}
	if policy.Cordoned == cordoned {
		return revision, nil
	}
	policy.Cordoned, policy.Revision = cordoned, revision+1
	if err := policy.Validate(); err != nil {
		return 0, &AdmissionError{Code: InvalidRequest, Cause: err}
	}
	next, err := json.Marshal(policy)
	if err != nil || len(next) > maxPolicyBytes {
		return 0, &AdmissionError{Code: InvalidRequest, Cause: err}
	}
	if _, err := tx.Exec(ctx, `UPDATE node_placement_policies SET revision=$1,policy_bytes=$2
 WHERE installation_id=$3 AND node_id=$4`, policy.Revision, next, installationID, nodeID); err != nil {
		return 0, admissionError(err)
	}
	if err := s.commitMetadata(ctx, tx); err != nil {
		return 0, err
	}
	return policy.Revision, nil
}

// RemoveNode revokes one node once its workload and storage are accounted for, as
// specification 33 requires. Three things must hold and each refusal names which
// failed: the node is cordoned, it holds no live reservation, and every volume on it
// that is not already purged is named in acknowledged.
//
// The acknowledgement must match exactly: a caller naming a volume the node does not
// hold is reading a stale list, which is how storage gets signed off unexamined.
//
// Nothing here deletes data. The volume identities stay so an operator can find it.
func (s *Store) RemoveNode(ctx context.Context, installationID, nodeID uuid.UUID, acknowledged []uuid.UUID) error {
	if installationID == uuid.Nil || nodeID == uuid.Nil || len(acknowledged) > maxVolumeIdentities {
		return &AdmissionError{Code: InvalidRequest}
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return admissionError(err)
	}
	defer tx.Rollback(ctx)
	var revoked *string
	if err := tx.QueryRow(ctx, `SELECT revoked_at::text FROM nodes
 WHERE installation_id=$1 AND id=$2 FOR UPDATE`, installationID, nodeID).Scan(&revoked); err != nil {
		return admissionError(err)
	}
	if revoked != nil {
		// Already removed. A repeat is ordinary.
		return nil
	}
	var encoded []byte
	if err := tx.QueryRow(ctx, `SELECT policy_bytes FROM node_placement_policies
 WHERE installation_id=$1 AND node_id=$2 FOR UPDATE`, installationID, nodeID).Scan(&encoded); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrNodeNotCordoned
		}
		return admissionError(err)
	}
	var policy placement.NodePolicy
	if json.Unmarshal(encoded, &policy) != nil {
		return &AdmissionError{Code: DependencyUnavailable}
	}
	if !policy.Cordoned {
		return ErrNodeNotCordoned
	}
	var live int
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM resource_allocations
 WHERE installation_id=$1 AND node_id=$2 AND released_at IS NULL`, installationID, nodeID).Scan(&live); err != nil {
		return admissionError(err)
	}
	if live > 0 {
		return ErrWorkloadOnNode
	}
	rows, err := tx.Query(ctx, `SELECT resource_id FROM volume_identities
 WHERE installation_id=$1 AND node_id=$2 AND retention<>'purged' ORDER BY resource_id`, installationID, nodeID)
	if err != nil {
		return admissionError(err)
	}
	defer rows.Close()
	var held []uuid.UUID
	for rows.Next() {
		var resource uuid.UUID
		if err := rows.Scan(&resource); err != nil {
			return admissionError(err)
		}
		held = append(held, resource)
	}
	if rows.Err() != nil {
		return admissionError(rows.Err())
	}
	rows.Close()
	// PostgreSQL orders uuid byte-wise, so the two lists compare directly.
	seen := slices.Clone(acknowledged)
	slices.SortFunc(seen, func(a, b uuid.UUID) int { return bytes.Compare(a[:], b[:]) })
	if !slices.Equal(seen, held) {
		return ErrStorageUndisposed
	}
	if _, err := tx.Exec(ctx, `UPDATE nodes SET revoked_at=clock_timestamp()
 WHERE installation_id=$1 AND id=$2`, installationID, nodeID); err != nil {
		return admissionError(err)
	}
	return s.commitMetadata(ctx, tx)
}
