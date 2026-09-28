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

// cursorFilter selects the exact dispatch identities that a read needs.
const cursorFilter = ` AND (d.command_id,d.execution_epoch,d.recovery_generation) IN (SELECT * FROM unnest($2::uuid[],$3::bigint[],$4::uuid[]))`

// LoadExecutionState returns the stored state of one claimed operation in plan order.
// Dispatch and admission records cover the current ownership run.
// Delivery records retain the command's total budget across ownership runs.
// A completion keeps the execution epoch that proved it, and the receipt is read at that epoch.
// The read requires current ownership. It does not extend the claim and it grants no authority.
func (s *Store) LoadExecutionState(ctx context.Context, claim operation.Claim) (ExecutionState, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return ExecutionState{}, admissionError(err)
	}
	defer tx.Rollback(ctx)
	active, status, planBytes, err := lockClaim(ctx, tx, claim)
	if err != nil {
		return ExecutionState{}, err
	}
	if planBytes == nil {
		return ExecutionState{}, operation.ErrBarrierPending
	}
	if len(planBytes) > maxExecutionStateBytes {
		return ExecutionState{}, ErrExecutionStateTooLarge
	}
	state := ExecutionState{Plan: new(agentv1.ExecutionPlan), Status: status, Active: active}
	if proto.Unmarshal(planBytes, state.Plan) != nil {
		return ExecutionState{}, &AdmissionError{Code: DependencyUnavailable}
	}
	if err := tx.QueryRow(ctx, `SELECT cancellation_requested_at FROM operations WHERE id=$1`, claim.OperationID).Scan(&state.CancellationRequestedAt); err != nil {
		return ExecutionState{}, admissionError(err)
	}
	if err := tx.QueryRow(ctx, `SELECT COALESCE(max(sequence),0) FROM operation_progress WHERE operation_id=$1 AND execution_epoch=$2`, claim.OperationID, claim.ExecutionEpoch).Scan(&state.ProgressSequence); err != nil {
		return ExecutionState{}, admissionError(err)
	}
	if state.Barriers, err = loadBarriers(ctx, tx, claim); err != nil {
		return ExecutionState{}, err
	}
	if state.Recovery, err = loadRecovery(ctx, tx, claim); err != nil {
		return ExecutionState{}, err
	}
	if state.Steps, err = loadSteps(ctx, tx, claim, state.Plan); err != nil {
		return ExecutionState{}, err
	}
	return state, nil
}

// LoadDispatch returns one committed dispatch by its stable identity.
// It resolves a command that an earlier ownership run sent under an earlier execution epoch.
// An identity that this operation never dispatched returns ErrCommandConflict.
func (s *Store) LoadDispatch(ctx context.Context, claim operation.Claim, cursor DispatchCursor) (DispatchEvidence, error) {
	if cursor.CommandID == uuid.Nil || cursor.ExecutionEpoch < 1 || cursor.RecoveryGeneration == uuid.Nil {
		return DispatchEvidence{}, &AdmissionError{Code: InvalidRequest}
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return DispatchEvidence{}, admissionError(err)
	}
	defer tx.Rollback(ctx)
	if _, _, _, err := lockClaim(ctx, tx, claim); err != nil {
		return DispatchEvidence{}, err
	}
	var commandBytes, acceptedBytes, resultBytes []byte
	err = tx.QueryRow(ctx, `SELECT d.command_bytes,a.accepted_bytes,r.result_bytes FROM command_dispatches d
 LEFT JOIN command_admissions a USING (command_id,execution_epoch,recovery_generation)
 LEFT JOIN command_results r USING (command_id,execution_epoch,recovery_generation)
 WHERE d.command_id=$1 AND d.execution_epoch=$2 AND d.recovery_generation=$3 AND d.installation_id=$4 AND d.app_id=$5 AND d.operation_id=$6`,
		cursor.CommandID, cursor.ExecutionEpoch, cursor.RecoveryGeneration, claim.InstallationID, claim.AppID, claim.OperationID).Scan(&commandBytes, &acceptedBytes, &resultBytes)
	if errors.Is(err, pgx.ErrNoRows) {
		return DispatchEvidence{}, operation.ErrCommandConflict
	}
	if err != nil {
		return DispatchEvidence{}, admissionError(err)
	}
	var evidence DispatchEvidence
	evidence.Command = new(agentv1.Command)
	if proto.Unmarshal(commandBytes, evidence.Command) != nil {
		return DispatchEvidence{}, &AdmissionError{Code: DependencyUnavailable}
	}
	if acceptedBytes != nil {
		evidence.Admission = new(agentv1.AdmissionAck)
		if proto.Unmarshal(acceptedBytes, evidence.Admission) != nil {
			return DispatchEvidence{}, &AdmissionError{Code: DependencyUnavailable}
		}
	}
	if resultBytes != nil {
		evidence.Result = new(agentv1.TerminalResult)
		if proto.Unmarshal(resultBytes, evidence.Result) != nil {
			return DispatchEvidence{}, &AdmissionError{Code: DependencyUnavailable}
		}
	}
	return evidence, nil
}

// loadBarriers returns one record for every node that the plan or an earlier dispatch touched.
func loadBarriers(ctx context.Context, tx pgx.Tx, claim operation.Claim) ([]BarrierEvidence, error) {
	rows, err := tx.Query(ctx, `SELECT node_id,session_id,report_bytes,reconciled_at IS NOT NULL FROM claim_barriers WHERE claim_id=$1 ORDER BY node_id`, claim.ID)
	if err != nil {
		return nil, admissionError(err)
	}
	defer rows.Close()
	var barriers []BarrierEvidence
	for rows.Next() {
		var evidence BarrierEvidence
		var session *uuid.UUID
		var encoded []byte
		if err := rows.Scan(&evidence.NodeID, &session, &encoded, &evidence.Reconciled); err != nil {
			return nil, admissionError(err)
		}
		if session != nil {
			evidence.SessionID = *session
		}
		if encoded != nil {
			evidence.Report = new(agentv1.BarrierResult)
			if proto.Unmarshal(encoded, evidence.Report) != nil {
				return nil, &AdmissionError{Code: DependencyUnavailable}
			}
		}
		barriers = append(barriers, evidence)
	}
	if rows.Err() != nil {
		return nil, admissionError(rows.Err())
	}
	return barriers, nil
}

func loadRecovery(ctx context.Context, tx pgx.Tx, claim operation.Claim) (*RecoveryCheckpoint, error) {
	var hash []byte
	var inspections []uuid.UUID
	err := tx.QueryRow(ctx, `SELECT frontier_hash,inspection_ids FROM claim_recovery_checkpoints WHERE claim_id=$1`, claim.ID).Scan(&hash, &inspections)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, admissionError(err)
	}
	checkpoint := RecoveryCheckpoint{InspectionIDs: inspections}
	copy(checkpoint.FrontierHash[:], hash)
	return &checkpoint, nil
}

// loadSteps reads the dispatch records of the current run and the receipt that proved each step.
// It reads the exact identities the plan names. It never widens a read to another operation.
func loadSteps(ctx context.Context, tx pgx.Tx, claim operation.Claim, plan *agentv1.ExecutionPlan) ([]StepEvidence, error) {
	steps := make([]StepEvidence, len(plan.Steps))
	commands := make([]uuid.UUID, len(plan.Steps))
	byStep := make(map[uuid.UUID]int, len(plan.Steps))
	byCommand := make(map[uuid.UUID]int, len(plan.Steps))
	// proving names the dispatch identity whose receipt proves each step.
	proving := make(map[DispatchCursor]int, len(plan.Steps))
	for i, step := range plan.Steps {
		stepID, err := uuid.Parse(step.Command.StepId)
		if err != nil {
			return nil, &AdmissionError{Code: DependencyUnavailable}
		}
		commandID, err := uuid.Parse(step.Command.CommandId)
		if err != nil {
			return nil, &AdmissionError{Code: DependencyUnavailable}
		}
		steps[i], commands[i] = StepEvidence{StepID: stepID}, commandID
		byStep[stepID], byCommand[commandID] = i, i
		proving[DispatchCursor{CommandID: commandID, ExecutionEpoch: claim.ExecutionEpoch, RecoveryGeneration: claim.RecoveryGeneration}] = i
	}
	if err := loadCompletions(ctx, tx, claim, plan, steps, commands, byStep, proving); err != nil {
		return nil, err
	}
	ids, epochs, generations := make([]uuid.UUID, 0, len(steps)*2), make([]int64, 0, len(steps)*2), make([]uuid.UUID, 0, len(steps)*2)
	for cursor := range proving {
		ids, epochs, generations = append(ids, cursor.CommandID), append(epochs, cursor.ExecutionEpoch), append(generations, cursor.RecoveryGeneration)
	}
	for _, commandID := range commands {
		ids, epochs, generations = append(ids, commandID), append(epochs, claim.ExecutionEpoch), append(generations, claim.RecoveryGeneration)
	}
	commandOf, err := loadDispatchedCommands(ctx, tx, claim, ids, epochs, generations, steps, byCommand, proving)
	if err != nil {
		return nil, err
	}
	if err := loadReceipts(ctx, tx, claim, ids, epochs, generations, steps, proving, commandOf); err != nil {
		return nil, err
	}
	if err := loadAdmissions(ctx, tx, claim, ids, epochs, generations, steps, byCommand); err != nil {
		return nil, err
	}
	return steps, loadDeliveries(ctx, tx, claim, commands, steps, byCommand)
}

// loadCompletions moves a proved step to the execution epoch of its historical receipt.
func loadCompletions(ctx context.Context, tx pgx.Tx, claim operation.Claim, plan *agentv1.ExecutionPlan, steps []StepEvidence, commands []uuid.UUID, byStep map[uuid.UUID]int, proving map[DispatchCursor]int) error {
	rows, err := tx.Query(ctx, `SELECT step_id,command_id,execution_epoch,recovery_generation,receipt_hash FROM operation_step_completions WHERE operation_id=$1`, claim.OperationID)
	if err != nil {
		return admissionError(err)
	}
	defer rows.Close()
	for rows.Next() {
		var completion StepCompletion
		var cursor DispatchCursor
		var hash []byte
		if err := rows.Scan(&completion.StepID, &cursor.CommandID, &cursor.ExecutionEpoch, &cursor.RecoveryGeneration, &hash); err != nil {
			return admissionError(err)
		}
		i, ok := byStep[completion.StepID]
		if !ok || cursor.CommandID != commands[i] {
			return operation.ErrCommandConflict
		}
		copy(completion.ReceiptHash[:], hash)
		completion.ReceiptEpoch = &agentv1.Epoch{
			InstallationId: claim.InstallationID.String(), NodeId: plan.Steps[i].Command.Epoch.NodeId, AppId: claim.AppID.String(),
			RecoveryGeneration: cursor.RecoveryGeneration.String(), ExecutionEpoch: cursor.ExecutionEpoch,
		}
		steps[i].Completion = &completion
		delete(proving, DispatchCursor{CommandID: commands[i], ExecutionEpoch: claim.ExecutionEpoch, RecoveryGeneration: claim.RecoveryGeneration})
		proving[cursor] = i
	}
	if rows.Err() != nil {
		return admissionError(rows.Err())
	}
	return nil
}

// loadDispatchedCommands returns the proving command of each step and fills the current dispatch.
func loadDispatchedCommands(ctx context.Context, tx pgx.Tx, claim operation.Claim, ids []uuid.UUID, epochs []int64, generations []uuid.UUID, steps []StepEvidence, byCommand map[uuid.UUID]int, proving map[DispatchCursor]int) (map[int]*agentv1.Command, error) {
	rows, err := tx.Query(ctx, `SELECT d.command_id,d.execution_epoch,d.recovery_generation,d.command_bytes FROM command_dispatches d WHERE d.operation_id=$1`+cursorFilter, claim.OperationID, ids, epochs, generations)
	if err != nil {
		return nil, admissionError(err)
	}
	defer rows.Close()
	commandOf, total := make(map[int]*agentv1.Command, len(steps)), 0
	for rows.Next() {
		var cursor DispatchCursor
		var encoded []byte
		if err := rows.Scan(&cursor.CommandID, &cursor.ExecutionEpoch, &cursor.RecoveryGeneration, &encoded); err != nil {
			return nil, admissionError(err)
		}
		if total += len(encoded); total > maxExecutionStateBytes {
			return nil, ErrExecutionStateTooLarge
		}
		command := new(agentv1.Command)
		if proto.Unmarshal(encoded, command) != nil {
			return nil, &AdmissionError{Code: DependencyUnavailable}
		}
		if i, ok := byCommand[cursor.CommandID]; ok && cursor.ExecutionEpoch == claim.ExecutionEpoch && cursor.RecoveryGeneration == claim.RecoveryGeneration {
			steps[i].Dispatch = command
		}
		if i, ok := proving[cursor]; ok {
			commandOf[i] = command
		}
	}
	if rows.Err() != nil {
		return nil, admissionError(rows.Err())
	}
	return commandOf, nil
}

// loadReceipts attaches a terminal receipt only with the command that produced it.
func loadReceipts(ctx context.Context, tx pgx.Tx, claim operation.Claim, ids []uuid.UUID, epochs []int64, generations []uuid.UUID, steps []StepEvidence, proving map[DispatchCursor]int, commandOf map[int]*agentv1.Command) error {
	rows, err := tx.Query(ctx, `SELECT d.command_id,d.execution_epoch,d.recovery_generation,r.result_bytes FROM command_results r
 JOIN command_dispatches d USING (command_id,execution_epoch,recovery_generation) WHERE d.operation_id=$1`+cursorFilter, claim.OperationID, ids, epochs, generations)
	if err != nil {
		return admissionError(err)
	}
	defer rows.Close()
	total := 0
	for rows.Next() {
		var cursor DispatchCursor
		var encoded []byte
		if err := rows.Scan(&cursor.CommandID, &cursor.ExecutionEpoch, &cursor.RecoveryGeneration, &encoded); err != nil {
			return admissionError(err)
		}
		i, ok := proving[cursor]
		if !ok {
			continue
		}
		if total += len(encoded); total > maxExecutionStateBytes {
			return ErrExecutionStateTooLarge
		}
		steps[i].Result = new(agentv1.TerminalResult)
		if proto.Unmarshal(encoded, steps[i].Result) != nil {
			return &AdmissionError{Code: DependencyUnavailable}
		}
		steps[i].ResultCommand = commandOf[i]
	}
	if rows.Err() != nil {
		return admissionError(rows.Err())
	}
	return nil
}

// loadAdmissions reads acknowledgements of the current run only. A rejection never erases one.
func loadAdmissions(ctx context.Context, tx pgx.Tx, claim operation.Claim, ids []uuid.UUID, epochs []int64, generations []uuid.UUID, steps []StepEvidence, byCommand map[uuid.UUID]int) error {
	rows, err := tx.Query(ctx, `SELECT d.command_id,a.accepted_bytes,a.accepted_session_id,a.rejection_bytes,a.rejection_session_id FROM command_admissions a
 JOIN command_dispatches d USING (command_id,execution_epoch,recovery_generation)
 WHERE d.operation_id=$1 AND d.execution_epoch=$5 AND d.recovery_generation=$6`+cursorFilter, claim.OperationID, ids, epochs, generations, claim.ExecutionEpoch, claim.RecoveryGeneration)
	if err != nil {
		return admissionError(err)
	}
	defer rows.Close()
	for rows.Next() {
		var commandID uuid.UUID
		var accepted, rejected []byte
		var acceptedSession, rejectedSession *uuid.UUID
		if err := rows.Scan(&commandID, &accepted, &acceptedSession, &rejected, &rejectedSession); err != nil {
			return admissionError(err)
		}
		i, ok := byCommand[commandID]
		if !ok {
			continue
		}
		if accepted != nil {
			steps[i].Admission = new(agentv1.AdmissionAck)
			if proto.Unmarshal(accepted, steps[i].Admission) != nil {
				return &AdmissionError{Code: DependencyUnavailable}
			}
			steps[i].AdmissionSessionID = *acceptedSession
		}
		if rejected != nil {
			steps[i].Rejection = new(agentv1.AdmissionAck)
			if proto.Unmarshal(rejected, steps[i].Rejection) != nil {
				return &AdmissionError{Code: DependencyUnavailable}
			}
			steps[i].RejectionSessionID = *rejectedSession
		}
	}
	if rows.Err() != nil {
		return admissionError(rows.Err())
	}
	return nil
}

// loadDeliveries returns the latest attempt across claims, matching the enforced command budget.
func loadDeliveries(ctx context.Context, tx pgx.Tx, claim operation.Claim, commands []uuid.UUID, steps []StepEvidence, byCommand map[uuid.UUID]int) error {
	rows, err := tx.Query(ctx, `SELECT DISTINCT ON (a.command_id) a.command_id,a.attempt_id,a.session_id,a.attempt_count,a.created_at,a.next_attempt_at,
 count(*) FILTER (WHERE a.claim_id=$3 AND a.rejection_bytes IS NULL) OVER (PARTITION BY a.command_id)
 FROM command_delivery_attempts a JOIN command_dispatches d USING (command_id,execution_epoch,recovery_generation)
 WHERE d.operation_id=$1 AND a.command_id = ANY($2) ORDER BY a.command_id,a.attempt_count DESC`, claim.OperationID, commands, claim.ID)
	if err != nil {
		return admissionError(err)
	}
	defer rows.Close()
	for rows.Next() {
		var attempt DeliveryAttempt
		var unanswered int
		if err := rows.Scan(&attempt.CommandID, &attempt.ID, &attempt.SessionID, &attempt.Count, &attempt.CreatedAt, &attempt.NextAttemptAt, &unanswered); err != nil {
			return admissionError(err)
		}
		if i, ok := byCommand[attempt.CommandID]; ok {
			steps[i].Delivery = &attempt
			steps[i].UnansweredDeliveries = unanswered
		}
	}
	if rows.Err() != nil {
		return admissionError(rows.Err())
	}
	return nil
}
