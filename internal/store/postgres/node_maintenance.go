package postgres

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/sirrobot01/synod/internal/placement"
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
