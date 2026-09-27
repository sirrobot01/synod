package postgres

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"maps"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/sirrobot01/synod/internal/operation"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/proto"
)

// completeLifecycle runs with the cluster and operation locked. Historical receipts
// prove the completed plan; they do not claim continuous health after completion.
func completeLifecycle(ctx context.Context, tx pgx.Tx, claim operation.Claim, planBytes []byte) error {
	if len(planBytes) == 0 {
		return operation.ErrBarrierPending
	}
	if len(planBytes) > maxExecutionStateBytes {
		return ErrExecutionStateTooLarge
	}
	op := operation.Operation{ID: claim.OperationID, InstallationID: claim.InstallationID, ClusterID: claim.ClusterID}
	var desiredRevision int64
	var desiredLifecycle string
	var deleted bool
	if err := tx.QueryRow(ctx, `SELECT kind FROM operations WHERE installation_id=$1 AND id=$2`, claim.InstallationID, claim.OperationID).Scan(&op.Kind); err != nil {
		return admissionError(err)
	}
	workloadTable := "clusters"
	if strings.HasPrefix(op.Kind, "app.") {
		workloadTable = "apps"
	}
	if err := tx.QueryRow(ctx, fmt.Sprintf(`SELECT o.kind,o.desired_revision,o.plan_version,c.desired_revision,c.desired_lifecycle,c.deleted_at IS NOT NULL
 FROM operations o JOIN %s c ON c.installation_id=o.installation_id AND c.id=o.cluster_id
 WHERE o.installation_id=$1 AND o.id=$2`, workloadTable), claim.InstallationID, claim.OperationID).
		Scan(&op.Kind, &op.DesiredRevision, &op.PlanVersion, &desiredRevision, &desiredLifecycle, &deleted); err != nil {
		return admissionError(err)
	}
	var plan agentv1.ExecutionPlan
	if proto.Unmarshal(planBytes, &plan) != nil {
		return &AdmissionError{Code: DependencyUnavailable}
	}
	if err := operation.ValidatePlan(&plan, op); err != nil {
		return err
	}
	if int(plan.Version) != op.PlanVersion {
		return operation.ErrCommandConflict
	}
	if err := checkPlanCompletions(ctx, tx, op, &plan); err != nil {
		return err
	}
	if strings.HasPrefix(op.Kind, "app.") {
		if deleted || desiredRevision != op.DesiredRevision {
			return operation.ErrCommandConflict
		}
		return completeAppLifecycle(ctx, tx, claim, op, &plan, desiredLifecycle)
	}
	var target string
	var postcondition agentv1.Postcondition
	// A whole-cluster action must prove every live member. A scale adds one member and
	// deliberately leaves the others alone, so it proves only the members its plan
	// names. Requiring the untouched ones would refuse every scale.
	planned, removing := false, false
	switch op.Kind {
	case "cluster.create", "cluster.start", "cluster.recover_setup":
		target, postcondition = "running", agentv1.Postcondition_POSTCONDITION_ENGINE_READY
	case "cluster.stop":
		target, postcondition = "stopped", agentv1.Postcondition_POSTCONDITION_CONTAINER_STOPPED
	case "cluster.delete":
		target, postcondition = "deleted", agentv1.Postcondition_POSTCONDITION_CONTAINER_ABSENT
	case "cluster.scale":
		// A scale down proves the member it removed is absent. A scale up proves the
		// member it added is ready. The desired count says which this is.
		target, postcondition, planned = "running", agentv1.Postcondition_POSTCONDITION_ENGINE_READY, true
		var desired int
		if err := tx.QueryRow(ctx, `SELECT desired_replicas FROM clusters WHERE installation_id=$1 AND id=$2`,
			claim.InstallationID, claim.ClusterID).Scan(&desired); err != nil {
			return admissionError(err)
		}
		var live int
		if err := tx.QueryRow(ctx, `SELECT count(*) FROM cluster_members
 WHERE installation_id=$1 AND cluster_id=$2 AND role='replica' AND removed_at IS NULL`,
			claim.InstallationID, claim.ClusterID).Scan(&live); err != nil {
			return admissionError(err)
		}
		if desired < live {
			postcondition = agentv1.Postcondition_POSTCONDITION_CONTAINER_ABSENT
			removing = true
		}
	default:
		return nil
	}
	if deleted || desiredRevision != op.DesiredRevision || desiredLifecycle != target {
		return operation.ErrCommandConflict
	}
	// Dependencies, not slice order, decide whether a proof is final. A later
	// observation can contradict it even when that step changes no native state.
	ancestors := make(map[string]map[string]bool, len(plan.Steps))
	for _, step := range plan.Steps {
		prior := make(map[string]bool)
		for _, dependency := range step.DependsOn {
			prior[dependency] = true
			maps.Copy(prior, ancestors[dependency])
		}
		ancestors[step.Command.StepId] = prior
	}
	// Current allocations all represent instances. A future retained-disk allocation
	// must have an explicit kind before it can bypass instance completion proofs.
	rows, err := tx.Query(ctx, `SELECT node_id,resource_id FROM resource_allocations
 WHERE installation_id=$1 AND cluster_id=$2 AND released_at IS NULL ORDER BY id FOR UPDATE`, claim.InstallationID, claim.ClusterID)
	if err != nil {
		return admissionError(err)
	}
	nodes := make(map[uuid.UUID]bool)
	var activated []uuid.UUID
	for rows.Next() {
		var nodeID, resourceID uuid.UUID
		if err := rows.Scan(&nodeID, &resourceID); err != nil {
			rows.Close()
			return admissionError(err)
		}
		proved := false
		for _, step := range plan.Steps {
			command := step.Command
			if command.Epoch.NodeId != nodeID.String() || command.Input.ResourceId != resourceID.String() || step.Postcondition != postcondition {
				continue
			}
			ordered := true
			for _, other := range plan.Steps {
				previous := other.Command
				if previous.StepId == command.StepId || previous.Epoch.NodeId != command.Epoch.NodeId || previous.Input.ResourceId != command.Input.ResourceId {
					continue
				}
				if !ancestors[command.StepId][previous.StepId] {
					ordered = false
					break
				}
			}
			if ordered {
				proved = true
				break
			}
		}
		if !proved {
			if planned && !names(&plan, nodeID, resourceID) {
				// A member this plan never mentioned keeps whatever its own operation
				// proved. It is not evidence for or against this one.
				continue
			}
			rows.Close()
			return operation.ErrCommandConflict
		}
		activated = append(activated, resourceID)
		nodes[nodeID] = true
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return admissionError(err)
	}
	if len(nodes) == 0 {
		return operation.ErrBarrierPending
	}
	if target == "deleted" {
		if _, err := tx.Exec(ctx, `INSERT INTO cluster_tombstones
 (installation_id,cluster_id,deleted_by_operation,retained_volumes,backup_disposition,cleared_at)
 SELECT $1,$2,$3,count(*),'no_policy',clock_timestamp() FROM volume_identities
 WHERE installation_id=$1 AND cluster_id=$2 AND retention<>'purged'`, claim.InstallationID, claim.ClusterID, claim.OperationID); err != nil {
			return admissionError(err)
		}
		if _, err := tx.Exec(ctx, `INSERT INTO cluster_tombstone_nodes (installation_id,cluster_id,node_id,acknowledged_at)
 SELECT DISTINCT installation_id,cluster_id,node_id,transaction_timestamp() FROM resource_allocations
 WHERE installation_id=$1 AND cluster_id=$2 AND released_at IS NULL`, claim.InstallationID, claim.ClusterID); err != nil {
			return admissionError(err)
		}
		if _, err := tx.Exec(ctx, `UPDATE allocation_ports SET released_at=clock_timestamp()
 WHERE installation_id=$1 AND cluster_id=$2 AND released_at IS NULL`, claim.InstallationID, claim.ClusterID); err != nil {
			return admissionError(err)
		}
		if _, err := tx.Exec(ctx, `UPDATE resource_allocations SET released_at=clock_timestamp()
 WHERE installation_id=$1 AND cluster_id=$2 AND released_at IS NULL`, claim.InstallationID, claim.ClusterID); err != nil {
			return admissionError(err)
		}
	}
	if removing {
		// The member row stays. A later add must receive a new identity, because a node
		// can still hold the data this member wrote.
		if _, err := tx.Exec(ctx, `UPDATE cluster_members SET removed_at=clock_timestamp(),updated_at=clock_timestamp()
 WHERE installation_id=$1 AND cluster_id=$2 AND resource_id = ANY($3) AND removed_at IS NULL`,
			claim.InstallationID, claim.ClusterID, activated); err != nil {
			return admissionError(err)
		}
		if _, err := tx.Exec(ctx, `UPDATE allocation_ports SET released_at=clock_timestamp()
 WHERE installation_id=$1 AND cluster_id=$2 AND released_at IS NULL AND allocation_id IN
 (SELECT id FROM resource_allocations WHERE installation_id=$1 AND cluster_id=$2 AND resource_id = ANY($3))`,
			claim.InstallationID, claim.ClusterID, activated); err != nil {
			return admissionError(err)
		}
		if _, err := tx.Exec(ctx, `UPDATE resource_allocations SET released_at=clock_timestamp()
 WHERE installation_id=$1 AND cluster_id=$2 AND released_at IS NULL AND resource_id = ANY($3)`,
			claim.InstallationID, claim.ClusterID, activated); err != nil {
			return admissionError(err)
		}
	} else if _, err := tx.Exec(ctx, `UPDATE cluster_members SET state='active',updated_at=clock_timestamp()
 WHERE installation_id=$1 AND cluster_id=$2 AND resource_id = ANY($3) AND removed_at IS NULL`,
		claim.InstallationID, claim.ClusterID, activated); err != nil {
		return admissionError(err)
	}
	if _, err := tx.Exec(ctx, `UPDATE clusters SET observed_revision=$1,observed_lifecycle=$2,updated_at=clock_timestamp(),
 deleted_at=CASE WHEN $2='deleted' THEN clock_timestamp() ELSE deleted_at END WHERE installation_id=$3 AND id=$4`, op.DesiredRevision, target, claim.InstallationID, claim.ClusterID); err != nil {
		return admissionError(err)
	}
	return nil
}

// checkPlanCompletions verifies every step's stored receipt.
func checkPlanCompletions(ctx context.Context, tx pgx.Tx, op operation.Operation, plan *agentv1.ExecutionPlan) error {
	for _, step := range plan.Steps {
		command := step.Command
		var savedHash, dispatchBytes, receiptBytes []byte
		var epoch int64
		var generation uuid.UUID
		err := tx.QueryRow(ctx, `SELECT c.receipt_hash,d.command_bytes,r.result_bytes,c.execution_epoch,c.recovery_generation
 FROM operation_step_completions c
 JOIN command_dispatches d USING (command_id,execution_epoch,recovery_generation)
 JOIN command_results r USING (command_id,execution_epoch,recovery_generation)
 WHERE c.operation_id=$1 AND c.step_id=$2 AND c.command_id=$3
 AND d.installation_id=$4 AND d.cluster_id=$5 AND d.operation_id=$1 AND d.step_id=$2 AND d.node_id=$6 AND d.input_hash=$7`,
			op.ID, command.StepId, command.CommandId, op.InstallationID, op.ClusterID, command.Epoch.NodeId, command.InputHash).
			Scan(&savedHash, &dispatchBytes, &receiptBytes, &epoch, &generation)
		if errors.Is(err, pgx.ErrNoRows) {
			return operation.ErrBarrierPending
		}
		if err != nil {
			return admissionError(err)
		}
		var dispatch agentv1.Command
		var receipt agentv1.TerminalResult
		if proto.Unmarshal(dispatchBytes, &dispatch) != nil || proto.Unmarshal(receiptBytes, &receipt) != nil {
			return &AdmissionError{Code: DependencyUnavailable}
		}
		hash := sha256.Sum256(receiptBytes)
		if operation.ValidateCommand(&dispatch) != nil || dispatch.CommandId != command.CommandId || dispatch.StepId != command.StepId ||
			!bytes.Equal(dispatch.InputHash, command.InputHash) || dispatch.Epoch.ExecutionEpoch != epoch || dispatch.Epoch.RecoveryGeneration != generation.String() ||
			!proto.Equal(receipt.Epoch, dispatch.Epoch) || !bytes.Equal(savedHash, hash[:]) {
			return operation.ErrCommandConflict
		}
		if err := operation.CheckStepResult(step, &receipt); err != nil {
			return err
		}
	}
	return nil
}

// names reports whether the plan acts on one node's resource. A scale proves only the
// members it names, so it must be able to tell them from the ones it left alone.
func names(plan *agentv1.ExecutionPlan, nodeID, resourceID uuid.UUID) bool {
	for _, step := range plan.Steps {
		if step.Command.Epoch.NodeId == nodeID.String() && step.Command.Input.ResourceId == resourceID.String() {
			return true
		}
	}
	return false
}
