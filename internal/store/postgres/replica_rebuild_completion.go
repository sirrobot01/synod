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

// settleReplicaRebuild records only proved removals and undispatched reservations at a terminal boundary.
func settleReplicaRebuild(ctx context.Context, tx pgx.Tx, claim operation.Claim, encoded []byte, succeeded bool) error {
	op := operation.Operation{ID: claim.OperationID, InstallationID: claim.InstallationID, ClusterID: claim.ClusterID}
	if err := tx.QueryRow(ctx, `SELECT kind,desired_revision,plan_version FROM operations WHERE installation_id=$1 AND id=$2`, claim.InstallationID, claim.OperationID).Scan(&op.Kind, &op.DesiredRevision, &op.PlanVersion); err != nil {
		return admissionError(err)
	}
	if op.Kind != "cluster.rebuild_replica" {
		return nil
	}
	if len(encoded) == 0 {
		if succeeded {
			return operation.ErrBarrierPending
		}
		return nil
	}
	if len(encoded) > maxExecutionStateBytes {
		return ErrExecutionStateTooLarge
	}
	var plan agentv1.ExecutionPlan
	if proto.Unmarshal(encoded, &plan) != nil {
		return operation.ErrCommandConflict
	}
	if err := operation.ValidatePlan(&plan, op); err != nil {
		return err
	}
	if int(plan.Version) != op.PlanVersion {
		return operation.ErrCommandConflict
	}
	var oldMember, oldResource, newMember, newResource, oldNode, newNode uuid.UUID
	var revision int64
	var lifecycle string
	var desired int
	err := tx.QueryRow(ctx, `SELECT p.member_id,p.resource_id,c.desired_revision,c.desired_lifecycle,c.desired_replicas
 FROM operations o JOIN cluster_action_plans p ON p.installation_id=o.installation_id AND p.cluster_id=o.cluster_id AND p.actor_id=o.actor_id AND p.plan_token=o.action_plan_token
 JOIN clusters c ON c.installation_id=o.installation_id AND c.id=o.cluster_id
 WHERE o.installation_id=$1 AND o.id=$2 AND p.action='rebuild_replica' AND p.expected_revision+1=o.desired_revision AND c.deleted_at IS NULL`, claim.InstallationID, claim.OperationID).Scan(&oldMember, &oldResource, &revision, &lifecycle, &desired)
	if err != nil {
		return admissionError(err)
	}
	if revision != op.DesiredRevision || lifecycle != "running" || desired != 1 || oldResource.String() != plan.Steps[0].Command.Input.ResourceId {
		return operation.ErrCommandConflict
	}
	// The installed placement owns exactly one fresh replica for this operation.
	rows, err := tx.Query(ctx, `SELECT m.member_id,m.resource_id,a.node_id FROM cluster_members m JOIN resource_allocations a ON a.installation_id=m.installation_id AND a.resource_id=m.resource_id AND a.released_at IS NULL WHERE m.installation_id=$1 AND m.cluster_id=$2 AND m.created_operation_id=$3 AND m.role='replica' AND m.removed_at IS NULL FOR UPDATE OF m,a`, claim.InstallationID, claim.ClusterID, claim.OperationID)
	if err != nil {
		return admissionError(err)
	}
	count := 0
	for rows.Next() {
		count++
		if err := rows.Scan(&newMember, &newResource, &newNode); err != nil {
			rows.Close()
			return admissionError(err)
		}
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return admissionError(err)
	}
	if count != 1 || newResource.String() != plan.Steps[5].Command.Input.ResourceId || newNode.String() != plan.Steps[5].Command.Epoch.NodeId {
		return operation.ErrCommandConflict
	}
	if err := tx.QueryRow(ctx, `SELECT a.node_id FROM cluster_members m JOIN resource_allocations a ON a.installation_id=m.installation_id AND a.resource_id=m.resource_id AND a.released_at IS NULL WHERE m.installation_id=$1 AND m.cluster_id=$2 AND m.member_id=$3 AND m.resource_id=$4 AND m.role='replica' AND m.removed_at IS NULL FOR UPDATE OF m,a`, claim.InstallationID, claim.ClusterID, oldMember, oldResource).Scan(&oldNode); err != nil {
		return admissionError(err)
	}
	if oldNode.String() != plan.Steps[0].Command.Epoch.NodeId {
		return operation.ErrCommandConflict
	}
	var primary bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM cluster_members m JOIN resource_allocations a ON a.installation_id=m.installation_id AND a.resource_id=m.resource_id AND a.released_at IS NULL WHERE m.installation_id=$1 AND m.cluster_id=$2 AND m.role='primary' AND m.removed_at IS NULL AND m.resource_id=$3 AND a.node_id=$4)`, claim.InstallationID, claim.ClusterID, plan.Steps[3].Command.Input.ResourceId, plan.Steps[3].Command.Epoch.NodeId).Scan(&primary); err != nil {
		return admissionError(err)
	}
	if !primary {
		return operation.ErrCommandConflict
	}
	proof := &agentv1.ExecutionPlan{Version: plan.Version, Steps: plan.Steps[:3]}
	if succeeded {
		proof = &plan
	}
	err = checkPlanCompletions(ctx, tx, op, proof)
	retired := oldResource
	if err != nil {
		if succeeded || !errors.Is(err, operation.ErrBarrierPending) {
			return err
		}
		// Without old removal proof, new work must be undispatched before its reservation can be discarded.
		ids := make([]uuid.UUID, 0, 9)
		for _, step := range plan.Steps[3:] {
			ids = append(ids, uuid.MustParse(step.Command.CommandId))
		}
		var dispatched bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM command_dispatches WHERE installation_id=$1 AND operation_id=$2 AND command_id=ANY($3))`, claim.InstallationID, claim.OperationID, ids).Scan(&dispatched); err != nil {
			return admissionError(err)
		}
		if dispatched {
			return operation.ErrCommandConflict
		}
		retired = newResource
	}
	if _, err := tx.Exec(ctx, `UPDATE cluster_members SET removed_at=clock_timestamp(),updated_at=clock_timestamp() WHERE installation_id=$1 AND cluster_id=$2 AND resource_id=$3 AND removed_at IS NULL`, claim.InstallationID, claim.ClusterID, retired); err != nil {
		return admissionError(err)
	}
	if _, err := tx.Exec(ctx, `UPDATE allocation_ports SET released_at=clock_timestamp() WHERE installation_id=$1 AND cluster_id=$2 AND released_at IS NULL AND allocation_id IN (SELECT id FROM resource_allocations WHERE installation_id=$1 AND cluster_id=$2 AND resource_id=$3)`, claim.InstallationID, claim.ClusterID, retired); err != nil {
		return admissionError(err)
	}
	if _, err := tx.Exec(ctx, `UPDATE resource_allocations SET released_at=clock_timestamp() WHERE installation_id=$1 AND cluster_id=$2 AND resource_id=$3 AND released_at IS NULL`, claim.InstallationID, claim.ClusterID, retired); err != nil {
		return admissionError(err)
	}
	if succeeded {
		if _, err := tx.Exec(ctx, `UPDATE cluster_members SET state='active',updated_at=clock_timestamp() WHERE installation_id=$1 AND member_id=$2`, claim.InstallationID, newMember); err != nil {
			return admissionError(err)
		}
		if _, err := tx.Exec(ctx, `UPDATE clusters SET observed_revision=$1,observed_lifecycle='running',updated_at=clock_timestamp() WHERE installation_id=$2 AND id=$3`, op.DesiredRevision, claim.InstallationID, claim.ClusterID); err != nil {
			return admissionError(err)
		}
	}
	return nil
}
