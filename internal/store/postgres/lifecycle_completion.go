package postgres

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/sirrobot01/synod/internal/operation"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/proto"
)

// completeLifecycle runs with the app and operation locked. Historical receipts
// prove the completed plan; they do not claim continuous health after completion.
func completeLifecycle(ctx context.Context, tx pgx.Tx, claim operation.Claim, planBytes []byte) error {
	if len(planBytes) == 0 {
		return operation.ErrBarrierPending
	}
	if len(planBytes) > maxExecutionStateBytes {
		return ErrExecutionStateTooLarge
	}
	op := operation.Operation{ID: claim.OperationID, InstallationID: claim.InstallationID, AppID: claim.AppID}
	var desiredRevision int64
	var desiredLifecycle string
	var deleted bool
	if err := tx.QueryRow(ctx, `SELECT kind FROM operations WHERE installation_id=$1 AND id=$2`, claim.InstallationID, claim.OperationID).Scan(&op.Kind); err != nil {
		return admissionError(err)
	}
	if !strings.HasPrefix(op.Kind, "app.") {
		return operation.ErrCommandConflict
	}
	if err := tx.QueryRow(ctx, `SELECT o.kind,o.desired_revision,o.plan_version,c.desired_revision,c.desired_lifecycle,c.deleted_at IS NOT NULL
 FROM operations o JOIN apps c ON c.installation_id=o.installation_id AND c.id=o.app_id
 WHERE o.installation_id=$1 AND o.id=$2`, claim.InstallationID, claim.OperationID).
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
	if deleted || desiredRevision != op.DesiredRevision {
		return operation.ErrCommandConflict
	}
	return completeAppLifecycle(ctx, tx, claim, op, &plan, desiredLifecycle)
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
 AND d.installation_id=$4 AND d.app_id=$5 AND d.operation_id=$1 AND d.step_id=$2 AND d.node_id=$6 AND d.input_hash=$7`,
			op.ID, command.StepId, command.CommandId, op.InstallationID, op.AppID, command.Epoch.NodeId, command.InputHash).
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
