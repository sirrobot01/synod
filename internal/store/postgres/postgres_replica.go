package postgres

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/sirrobot01/synod/internal/operation"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/proto"
)

// ReadPostgresReplicaClone reads recorded copy intent for one live replica under the current claim.
func (s *Store) ReadPostgresReplicaClone(ctx context.Context, claim operation.Claim, memberID uuid.UUID) (*agentv1.PostgresContainerConfig, uuid.UUID, error) {
	if memberID == uuid.Nil {
		return nil, uuid.Nil, &AdmissionError{Code: InvalidRequest}
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, uuid.Nil, admissionError(err)
	}
	defer tx.Rollback(ctx)
	if _, _, _, err := lockClaim(ctx, tx, claim); err != nil {
		return nil, uuid.Nil, err
	}
	var resourceID, nodeID, createdBy uuid.UUID
	var createdRevision, currentRevision int64
	err = tx.QueryRow(ctx, `SELECT m.resource_id,a.node_id,m.created_operation_id,m.created_revision,current.desired_revision
 FROM cluster_members m JOIN clusters c ON c.installation_id=m.installation_id AND c.id=m.cluster_id
 JOIN resource_allocations a ON a.installation_id=m.installation_id AND a.cluster_id=m.cluster_id AND a.resource_id=m.resource_id AND a.released_at IS NULL
 JOIN operations current ON current.installation_id=m.installation_id AND current.cluster_id=m.cluster_id
 WHERE m.installation_id=$1 AND m.cluster_id=$2 AND m.member_id=$3 AND current.id=$4
 AND m.role='replica' AND m.removed_at IS NULL AND c.engine='postgres'`, claim.InstallationID, claim.ClusterID, memberID, claim.OperationID).
		Scan(&resourceID, &nodeID, &createdBy, &createdRevision, &currentRevision)
	if err != nil {
		return nil, uuid.Nil, admissionError(err)
	}
	prior := operation.Operation{InstallationID: claim.InstallationID, ClusterID: claim.ClusterID}
	var encoded []byte
	err = tx.QueryRow(ctx, `WITH RECURSIVE history AS (
 SELECT id,desired_revision,status FROM operations
 WHERE installation_id=$1 AND cluster_id=$2 AND id=$3 AND desired_revision=$4 AND desired_revision<$5 AND kind IN ('cluster.scale','cluster.rebuild_replica')
 UNION ALL
 SELECT o.id,o.desired_revision,o.status FROM operations o JOIN history h ON o.retry_of=h.id
 WHERE o.installation_id=$1 AND o.cluster_id=$2 AND o.kind IN ('cluster.scale','cluster.rebuild_replica')
 AND h.status IN ('failed','cancelled') AND o.desired_revision>h.desired_revision AND o.desired_revision<$5
 ) SELECT o.id,o.desired_revision,o.kind,o.status,o.plan_bytes FROM history h JOIN operations o ON o.id=h.id
 WHERE o.plan_bytes IS NOT NULL AND (o.kind='cluster.scale' OR o.id=$3) ORDER BY o.desired_revision DESC LIMIT 1`, claim.InstallationID, claim.ClusterID, createdBy, createdRevision, currentRevision).
		Scan(&prior.ID, &prior.DesiredRevision, &prior.Kind, &prior.Status, &encoded)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, uuid.Nil, operation.ErrUnsupportedPlan
	}
	if err != nil {
		return nil, uuid.Nil, admissionError(err)
	}
	if len(encoded) > maxExecutionStateBytes {
		return nil, uuid.Nil, ErrExecutionStateTooLarge
	}
	if prior.Status != operation.Succeeded && prior.Status != operation.Failed && prior.Status != operation.Cancelled {
		return nil, uuid.Nil, operation.ErrCommandConflict
	}
	plan := new(agentv1.ExecutionPlan)
	if proto.Unmarshal(encoded, plan) != nil {
		return nil, uuid.Nil, operation.ErrCommandConflict
	}
	if err := operation.ValidatePlan(plan, prior); err != nil {
		return nil, uuid.Nil, err
	}
	var config *agentv1.PostgresContainerConfig
	var cloneID uuid.UUID
	for _, step := range plan.Steps {
		command := step.Command
		if command.Input.ResourceId != resourceID.String() {
			continue
		}
		var candidate *agentv1.PostgresContainerConfig
		var original string
		if clone := command.Input.GetClonePostgresReplica(); clone != nil {
			candidate, original = clone.Postgres, command.CommandId
		}
		if verify := command.Input.GetVerifyPostgresReplicaClone(); verify != nil {
			candidate, original = verify.Postgres, verify.CloneCommandId
		}
		if candidate == nil {
			continue
		}
		if cloneID != uuid.Nil || command.Epoch.NodeId != nodeID.String() {
			return nil, uuid.Nil, operation.ErrCommandConflict
		}
		config, cloneID = candidate, uuid.MustParse(original)
	}
	if config == nil {
		return nil, uuid.Nil, operation.ErrUnsupportedPlan
	}
	return config, cloneID, nil
}
