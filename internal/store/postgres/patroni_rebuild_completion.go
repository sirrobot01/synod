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

// settleMemberRebuild records a Patroni member rebuild at a terminal boundary.
//
// A resume activates the residual member on success and changes nothing on failure.
// A replace retires the old member and activates the new one on success. Failure retires
// the old member only with removal proof. Without it, the new reservation is discarded
// only when no new-member command was dispatched. Otherwise both stay for review.
func settleMemberRebuild(ctx context.Context, tx pgx.Tx, claim operation.Claim, encoded []byte, succeeded bool) error {
	op := operation.Operation{ID: claim.OperationID, InstallationID: claim.InstallationID, ClusterID: claim.ClusterID}
	if err := tx.QueryRow(ctx, `SELECT kind,desired_revision,plan_version FROM operations WHERE installation_id=$1 AND id=$2`, claim.InstallationID, claim.OperationID).Scan(&op.Kind, &op.DesiredRevision, &op.PlanVersion); err != nil {
		return admissionError(err)
	}
	if op.Kind != "cluster.rebuild_member" {
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
	// peers checks every other member. fresh is the index of the member volume step. A resume
	// has no removal. Route steps of a routed cluster follow the member steps.
	prefix, peers := operation.MemberRebuildPrefix(&plan), operation.LeadingChecks(plan.Steps)
	fresh := prefix - 4
	old, created := plan.Steps[peers].Command, plan.Steps[fresh+1].Command
	resume := prefix-peers == 4
	if resume {
		old = created
	}
	var oldMember, oldResource, newMember, newResource, oldNode, newNode uuid.UUID
	var revision int64
	var lifecycle string
	err := tx.QueryRow(ctx, `SELECT p.member_id,p.resource_id,c.desired_revision,c.desired_lifecycle
 FROM operations o JOIN cluster_action_plans p ON p.installation_id=o.installation_id AND p.cluster_id=o.cluster_id AND p.actor_id=o.actor_id AND p.plan_token=o.action_plan_token
 JOIN clusters c ON c.installation_id=o.installation_id AND c.id=o.cluster_id
 WHERE o.installation_id=$1 AND o.id=$2 AND p.action='rebuild_member' AND p.expected_revision+1=o.desired_revision AND c.deleted_at IS NULL`, claim.InstallationID, claim.OperationID).Scan(&oldMember, &oldResource, &revision, &lifecycle)
	if err != nil {
		return admissionError(err)
	}
	if revision != op.DesiredRevision || lifecycle != "running" || oldResource.String() != old.Input.ResourceId {
		return operation.ErrCommandConflict
	}
	if resume {
		return settleMemberResume(ctx, tx, claim, op, &plan, oldMember, oldResource, succeeded)
	}
	// The installed placement owns exactly one fresh member for this operation.
	rows, err := tx.Query(ctx, `SELECT m.member_id,m.resource_id,a.node_id FROM cluster_members m JOIN resource_allocations a ON a.installation_id=m.installation_id AND a.resource_id=m.resource_id AND a.released_at IS NULL WHERE m.installation_id=$1 AND m.cluster_id=$2 AND m.created_operation_id=$3 AND m.role='patroni' AND m.removed_at IS NULL FOR UPDATE OF m,a`, claim.InstallationID, claim.ClusterID, claim.OperationID)
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
	if count != 1 || newResource.String() != created.Input.ResourceId || newNode.String() != created.Epoch.NodeId {
		return operation.ErrCommandConflict
	}
	if err := tx.QueryRow(ctx, `SELECT a.node_id FROM cluster_members m JOIN resource_allocations a ON a.installation_id=m.installation_id AND a.resource_id=m.resource_id AND a.released_at IS NULL WHERE m.installation_id=$1 AND m.cluster_id=$2 AND m.member_id=$3 AND m.resource_id=$4 AND m.role='patroni' AND m.removed_at IS NULL FOR UPDATE OF m,a`, claim.InstallationID, claim.ClusterID, oldMember, oldResource).Scan(&oldNode); err != nil {
		return admissionError(err)
	}
	if oldNode.String() != old.Epoch.NodeId || newNode != oldNode {
		return operation.ErrCommandConflict
	}
	retired := oldResource
	if succeeded {
		elected, primaries, err := checkPlanCompletions(ctx, tx, op, &plan)
		if err != nil {
			return err
		}
		// Patroni elects its primary. A rebuild that proves no writer, or two, is not running.
		// The peer checks and the fresh member's check prove every member once.
		if elected != peers+1 || primaries != 1 {
			return operation.ErrPostconditionUnproved
		}
	} else if _, _, err := checkPlanCompletions(ctx, tx, op, &agentv1.ExecutionPlan{Version: plan.Version, Steps: plan.Steps[fresh-3 : fresh]}); err != nil {
		if !errors.Is(err, operation.ErrBarrierPending) {
			return err
		}
		// Without old removal proof, new work must be undispatched before its reservation can be discarded.
		ids := make([]uuid.UUID, 0, 4)
		for _, step := range plan.Steps[fresh:] {
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

// settleMemberResume activates a resumed residual member after one writer is proved.
// A failed resume leaves the member residual, because its storage is unchanged.
func settleMemberResume(ctx context.Context, tx pgx.Tx, claim operation.Claim, op operation.Operation, plan *agentv1.ExecutionPlan, member, resource uuid.UUID, succeeded bool) error {
	var node uuid.UUID
	if err := tx.QueryRow(ctx, `SELECT a.node_id FROM cluster_members m JOIN resource_allocations a ON a.installation_id=m.installation_id AND a.resource_id=m.resource_id AND a.released_at IS NULL WHERE m.installation_id=$1 AND m.cluster_id=$2 AND m.member_id=$3 AND m.resource_id=$4 AND m.role='patroni' AND m.state='residual' AND m.removed_at IS NULL FOR UPDATE OF m,a`, claim.InstallationID, claim.ClusterID, member, resource).Scan(&node); err != nil {
		return admissionError(err)
	}
	if node.String() != plan.Steps[3].Command.Epoch.NodeId {
		return operation.ErrCommandConflict
	}
	if !succeeded {
		return nil
	}
	elected, primaries, err := checkPlanCompletions(ctx, tx, op, plan)
	if err != nil {
		return err
	}
	if elected != operation.LeadingChecks(plan.Steps)+1 || primaries != 1 {
		return operation.ErrPostconditionUnproved
	}
	if _, err := tx.Exec(ctx, `UPDATE cluster_members SET state='active',updated_at=clock_timestamp() WHERE installation_id=$1 AND member_id=$2`, claim.InstallationID, member); err != nil {
		return admissionError(err)
	}
	if _, err := tx.Exec(ctx, `UPDATE clusters SET observed_revision=$1,observed_lifecycle='running',updated_at=clock_timestamp() WHERE installation_id=$2 AND id=$3`, op.DesiredRevision, claim.InstallationID, claim.ClusterID); err != nil {
		return admissionError(err)
	}
	return nil
}
