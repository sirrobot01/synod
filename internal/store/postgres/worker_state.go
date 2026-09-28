package postgres

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/sirrobot01/synod/internal/operation"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/proto"
)

// CheckAgentSession checks database-observed presence and journal continuity.
// It does not grant claim authority. The worker must also verify PostgreSQL and etcd ownership.
func (s *Store) CheckAgentSession(ctx context.Context, claim operation.Claim, nodeID, sessionID uuid.UUID, maxAge time.Duration) (AgentPresence, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return AgentPresence{}, admissionError(err)
	}
	defer tx.Rollback(ctx)
	if _, _, _, err := lockClaim(ctx, tx, claim); err != nil {
		return AgentPresence{}, err
	}
	return checkAgentSession(ctx, tx, claim, nodeID, sessionID, maxAge)
}

// checkAgentSession holds node and presence locks through the caller's transaction.
func checkAgentSession(ctx context.Context, tx pgx.Tx, claim operation.Claim, nodeID, sessionID uuid.UUID, maxAge time.Duration) (AgentPresence, error) {
	if nodeID == uuid.Nil || sessionID == uuid.Nil || maxAge < time.Second || maxAge > time.Minute {
		return AgentPresence{}, &AdmissionError{Code: InvalidRequest}
	}
	var revoked *time.Time
	if err := tx.QueryRow(ctx, `SELECT revoked_at FROM nodes WHERE installation_id=$1 AND id=$2 FOR SHARE`, claim.InstallationID, nodeID).Scan(&revoked); err != nil {
		return AgentPresence{}, admissionError(err)
	}
	if revoked != nil {
		return AgentPresence{}, ErrStaleSession
	}
	var affected bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM claim_barriers WHERE claim_id=$1 AND node_id=$2)`, claim.ID, nodeID).Scan(&affected); err != nil {
		return AgentPresence{}, admissionError(err)
	}
	if !affected {
		return AgentPresence{}, operation.ErrStaleOwner
	}
	result := AgentPresence{InstallationID: claim.InstallationID, NodeID: nodeID}
	var hello, heartbeat []byte
	if err := tx.QueryRow(ctx, `SELECT p.session_id,s.hello_bytes,p.heartbeat_bytes,s.connected_at,p.last_seen_at,p.recovery_required
 FROM agent_presence p JOIN agent_sessions s USING (installation_id,node_id,session_id)
 WHERE p.installation_id=$1 AND p.node_id=$2 FOR SHARE OF p`, claim.InstallationID, nodeID).Scan(&result.SessionID, &hello, &heartbeat, &result.ConnectedAt, &result.LastSeenAt, &result.RecoveryRequired); err != nil {
		return AgentPresence{}, admissionError(err)
	}
	var now time.Time
	if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&now); err != nil {
		return AgentPresence{}, admissionError(err)
	}
	if result.SessionID != sessionID || result.RecoveryRequired || !result.LastSeenAt.After(now.Add(-maxAge)) {
		return AgentPresence{}, ErrStaleSession
	}
	result.Hello = new(agentv1.Hello)
	if proto.Unmarshal(hello, result.Hello) != nil {
		return AgentPresence{}, &AdmissionError{Code: DependencyUnavailable}
	}
	if len(heartbeat) != 0 {
		result.Heartbeat = new(agentv1.Heartbeat)
		if proto.Unmarshal(heartbeat, result.Heartbeat) != nil {
			return AgentPresence{}, &AdmissionError{Code: DependencyUnavailable}
		}
	}
	if _, _, _, err := lockClaim(ctx, tx, claim); err != nil {
		return AgentPresence{}, err
	}
	return result, nil
}

// RecordAgentAdmission records certificate-bound receipt of a committed dispatch.
// Accepted states advance monotonically. A rejection cannot erase prior admission.
// Neither receipt nor rejection proves that a native command completed.
func (s *Store) RecordAgentAdmission(ctx context.Context, installationID, nodeID, sessionID uuid.UUID, ack *agentv1.AdmissionAck) error {
	if installationID == uuid.Nil || nodeID == uuid.Nil || sessionID == uuid.Nil || ack == nil || ack.Epoch == nil || len(ack.InputHash) != 32 || proto.Size(ack) > 4096 {
		return &AdmissionError{Code: InvalidRequest}
	}
	epoch := ack.Epoch
	if epoch.InstallationId != installationID.String() || epoch.NodeId != nodeID.String() || epoch.ExecutionEpoch < 1 {
		return operation.ErrStaleOwner
	}
	for _, id := range []string{ack.CommandId, ack.DeliveryAttemptId, epoch.AppId, epoch.RecoveryGeneration} {
		parsed, err := uuid.Parse(id)
		if err != nil || parsed == uuid.Nil || parsed.String() != id {
			return &AdmissionError{Code: InvalidRequest}
		}
	}
	rank := 0
	switch ack.State {
	case "queued":
		rank = 1
	case "running":
		rank = 2
	case "terminal":
		rank = 3
	case "compacted":
		rank = 4
	case "rejected":
		if ack.ErrorCode == "" || len(ack.ErrorCode) > 128 || strings.ContainsFunc(ack.ErrorCode, func(r rune) bool { return !(r >= 'a' && r <= 'z' || r >= '0' && r <= '9' || r == '_') }) {
			return &AdmissionError{Code: InvalidRequest}
		}
	default:
		return &AdmissionError{Code: InvalidRequest}
	}
	if rank != 0 && ack.ErrorCode != "" {
		return &AdmissionError{Code: InvalidRequest}
	}
	encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(ack)
	if err != nil {
		return &AdmissionError{Code: InvalidRequest}
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return admissionError(err)
	}
	defer tx.Rollback(ctx)
	// Lock presence through the commit so a replacement cannot interleave receipt.
	var currentSession uuid.UUID
	if err := tx.QueryRow(ctx, `SELECT session_id FROM agent_presence WHERE installation_id=$1 AND node_id=$2 FOR SHARE`, installationID, nodeID).Scan(&currentSession); err != nil {
		return admissionError(err)
	}
	if currentSession != sessionID {
		return ErrStaleSession
	}
	var commandBytes []byte
	if err := tx.QueryRow(ctx, `SELECT command_bytes FROM command_dispatches WHERE command_id=$1
 AND execution_epoch=$2 AND recovery_generation=$3 AND installation_id=$4 AND node_id=$5 AND app_id=$6`, ack.CommandId, epoch.ExecutionEpoch, epoch.RecoveryGeneration, installationID, nodeID, epoch.AppId).Scan(&commandBytes); err != nil {
		return admissionError(err)
	}
	var command agentv1.Command
	if proto.Unmarshal(commandBytes, &command) != nil || operation.ValidateCommand(&command) != nil {
		return &AdmissionError{Code: DependencyUnavailable}
	}
	if command.CommandId != ack.CommandId || !proto.Equal(command.Epoch, epoch) || !bytes.Equal(command.InputHash, ack.InputHash) {
		return operation.ErrCommandConflict
	}
	var matches bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM command_delivery_attempts
 WHERE attempt_id=$1 AND command_id=$2 AND execution_epoch=$3 AND recovery_generation=$4 AND session_id=$5)`,
		ack.DeliveryAttemptId, ack.CommandId, epoch.ExecutionEpoch, epoch.RecoveryGeneration, sessionID).Scan(&matches); err != nil {
		return admissionError(err)
	}
	if !matches {
		return operation.ErrCommandConflict
	}
	if rank == 0 {
		if _, err := tx.Exec(ctx, `UPDATE command_delivery_attempts SET rejection_bytes=$2 WHERE attempt_id=$1`, ack.DeliveryAttemptId, encoded); err != nil {
			return admissionError(err)
		}
		_, err = tx.Exec(ctx, `INSERT INTO command_admissions (command_id,execution_epoch,recovery_generation,rejection_bytes,rejection_session_id)
 VALUES ($1,$2,$3,$4,$5) ON CONFLICT (command_id,execution_epoch,recovery_generation)
 DO UPDATE SET rejection_bytes=EXCLUDED.rejection_bytes,rejection_session_id=EXCLUDED.rejection_session_id,updated_at=clock_timestamp()`, ack.CommandId, epoch.ExecutionEpoch, epoch.RecoveryGeneration, encoded, sessionID)
	} else {
		_, err = tx.Exec(ctx, `INSERT INTO command_admissions (command_id,execution_epoch,recovery_generation,accepted_rank,accepted_bytes,accepted_session_id)
 VALUES ($1,$2,$3,$4,$5,$6) ON CONFLICT (command_id,execution_epoch,recovery_generation)
 DO UPDATE SET accepted_rank=EXCLUDED.accepted_rank,accepted_bytes=EXCLUDED.accepted_bytes,accepted_session_id=EXCLUDED.accepted_session_id,updated_at=clock_timestamp()
 WHERE command_admissions.accepted_rank <= EXCLUDED.accepted_rank`, ack.CommandId, epoch.ExecutionEpoch, epoch.RecoveryGeneration, rank, encoded, sessionID)
	}
	if err != nil {
		return admissionError(err)
	}
	return s.commitMetadata(ctx, tx)
}

// RecordSessionBarrier retains drain evidence for the exact current agent session.
// The worker separately reconciles residual state before it can activate authority.
func (s *Store) RecordSessionBarrier(ctx context.Context, claim operation.Claim, sessionID uuid.UUID, report *agentv1.BarrierResult) error {
	if report == nil || report.Epoch == nil || proto.Size(report) > 1<<20 || len(report.Effects) > 1024 {
		return &AdmissionError{Code: InvalidRequest}
	}
	epoch := report.Epoch
	if epoch.InstallationId != claim.InstallationID.String() || epoch.AppId != claim.AppID.String() || epoch.RecoveryGeneration != claim.RecoveryGeneration.String() || epoch.ExecutionEpoch != claim.ExecutionEpoch {
		return operation.ErrStaleOwner
	}
	nodeID, err := uuid.Parse(epoch.NodeId)
	if err != nil || nodeID == uuid.Nil || nodeID.String() != epoch.NodeId {
		return &AdmissionError{Code: InvalidRequest}
	}
	if len(report.FailureCode) > 128 || strings.ContainsFunc(report.FailureCode, func(r rune) bool { return !(r >= 'a' && r <= 'z' || r >= '0' && r <= '9' || r == '_') }) {
		return &AdmissionError{Code: InvalidRequest}
	}
	seen := make(map[string]bool, len(report.Effects))
	for _, effect := range report.Effects {
		if effect == nil || len(effect.NativeOperationId) > 256 || len(effect.State) < 1 || len(effect.State) > 64 || strings.ContainsFunc(effect.NativeOperationId, func(r rune) bool { return r < '!' || r > '~' }) || strings.ContainsFunc(effect.State, func(r rune) bool { return !(r >= 'a' && r <= 'z' || r >= '0' && r <= '9' || r == '_') }) {
			return &AdmissionError{Code: InvalidRequest}
		}
		resourceID, err := uuid.Parse(effect.ResourceId)
		if err != nil || resourceID == uuid.Nil || resourceID.String() != effect.ResourceId {
			return &AdmissionError{Code: InvalidRequest}
		}
		if effect.CommandId != "" {
			commandID, err := uuid.Parse(effect.CommandId)
			if err != nil || commandID == uuid.Nil || commandID.String() != effect.CommandId {
				return &AdmissionError{Code: InvalidRequest}
			}
		}
		key := effect.CommandId + "/" + effect.ResourceId + "/" + effect.NativeOperationId
		if seen[key] {
			return &AdmissionError{Code: InvalidRequest}
		}
		seen[key] = true
	}
	encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(report)
	if err != nil {
		return &AdmissionError{Code: InvalidRequest}
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return admissionError(err)
	}
	defer tx.Rollback(ctx)
	if _, _, _, err := lockClaim(ctx, tx, claim); err != nil {
		return err
	}
	if _, err := checkAgentSession(ctx, tx, claim, nodeID, sessionID, 20*time.Second); err != nil {
		return err
	}
	if err := recordBarrier(ctx, tx, claim, nodeID, sessionID, encoded); err != nil {
		return err
	}
	return s.commitMetadata(ctx, tx)
}

var ErrRetryPending = errors.New("retry_pending")
var ErrAttemptsExhausted = errors.New("attempts_exhausted")
var ErrExecutionStateTooLarge = errors.New("execution_state_too_large")

const maxExecutionStateBytes = 16 << 20

// BarrierEvidence is one node barrier report and its reconciliation state.
type BarrierEvidence struct {
	NodeID     uuid.UUID
	SessionID  uuid.UUID
	Report     *agentv1.BarrierResult
	Reconciled bool
}

// DeliveryAttempt records one command delivery try and its next retry time.
type DeliveryAttempt struct {
	ID            uuid.UUID
	CommandID     uuid.UUID
	SessionID     uuid.UUID
	Count         int
	CreatedAt     time.Time
	NextAttemptAt time.Time
}

// StepCompletion is a checked historical success for one plan step.
type StepCompletion struct {
	StepID       uuid.UUID
	ReceiptEpoch *agentv1.Epoch
	ReceiptHash  [32]byte
}

// StepEvidence is every stored record for one plan step.
type StepEvidence struct {
	StepID             uuid.UUID
	Dispatch           *agentv1.Command
	Admission          *agentv1.AdmissionAck
	AdmissionSessionID uuid.UUID
	Rejection          *agentv1.AdmissionAck
	RejectionSessionID uuid.UUID
	ResultCommand      *agentv1.Command
	Result             *agentv1.TerminalResult
	Completion         *StepCompletion
	Delivery           *DeliveryAttempt
	// UnansweredDeliveries counts current-run attempts without a recorded refusal.
	// Accepted commands still require a terminal result, regardless of this count.
	UnansweredDeliveries int
}

// RecoveryCheckpoint is the stored frontier that a worker resumes from.
type RecoveryCheckpoint struct {
	FrontierHash  [32]byte
	InspectionIDs []uuid.UUID
}

// ExecutionState is the complete stored state of one claimed operation.
type ExecutionState struct {
	Plan                    *agentv1.ExecutionPlan
	Status                  operation.Status
	Active                  bool
	CancellationRequestedAt *time.Time
	ProgressSequence        int64
	Barriers                []BarrierEvidence
	Steps                   []StepEvidence
	Recovery                *RecoveryCheckpoint
}

// DispatchCursor names one command by its stable dispatch identity.
type DispatchCursor struct {
	CommandID          uuid.UUID
	ExecutionEpoch     int64
	RecoveryGeneration uuid.UUID
}

// DispatchEvidence is the stored command, admission, and result for one dispatch.
type DispatchEvidence struct {
	Command   *agentv1.Command
	Admission *agentv1.AdmissionAck
	Result    *agentv1.TerminalResult
}

// RecordStepCompletion retains a checked historical success. It does not assert current host state.
func (s *Store) RecordStepCompletion(ctx context.Context, claim operation.Claim, stepID uuid.UUID, receiptEpoch *agentv1.Epoch, receiptHash [32]byte) error {
	if stepID == uuid.Nil || receiptEpoch == nil {
		return &AdmissionError{Code: InvalidRequest}
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return admissionError(err)
	}
	defer tx.Rollback(ctx)
	_, state, planBytes, err := lockClaim(ctx, tx, claim)
	if err != nil {
		return err
	}
	if state == operation.Succeeded || state == operation.Failed || state == operation.Cancelled {
		return operation.ErrStaleOwner
	}
	if len(planBytes) > maxExecutionStateBytes {
		return ErrExecutionStateTooLarge
	}
	var plan agentv1.ExecutionPlan
	if planBytes == nil {
		return operation.ErrBarrierPending
	}
	if proto.Unmarshal(planBytes, &plan) != nil {
		return &AdmissionError{Code: DependencyUnavailable}
	}
	index := slices.IndexFunc(plan.Steps, func(step *agentv1.PlanStep) bool {
		return step != nil && step.Command != nil && step.Command.StepId == stepID.String()
	})
	if index < 0 {
		return operation.ErrCommandConflict
	}
	step := plan.Steps[index]
	command := step.Command
	if receiptEpoch.InstallationId != claim.InstallationID.String() || receiptEpoch.AppId != claim.AppID.String() || receiptEpoch.NodeId != command.Epoch.NodeId || receiptEpoch.ExecutionEpoch < 1 {
		return operation.ErrCommandConflict
	}
	generation, err := uuid.Parse(receiptEpoch.RecoveryGeneration)
	if err != nil || generation == uuid.Nil || generation.String() != receiptEpoch.RecoveryGeneration {
		return &AdmissionError{Code: InvalidRequest}
	}
	var receiptBytes []byte
	if err := tx.QueryRow(ctx, `SELECT r.result_bytes FROM command_results r JOIN command_dispatches d USING (command_id,execution_epoch,recovery_generation)
 WHERE r.command_id=$1 AND r.execution_epoch=$2 AND r.recovery_generation=$3 AND d.installation_id=$4 AND d.operation_id=$5 AND d.node_id=$6 AND d.step_id=$7 AND d.input_hash=$8`, command.CommandId, receiptEpoch.ExecutionEpoch, generation, claim.InstallationID, claim.OperationID, command.Epoch.NodeId, stepID, command.InputHash).Scan(&receiptBytes); err != nil {
		return admissionError(err)
	}
	var receipt agentv1.TerminalResult
	if proto.Unmarshal(receiptBytes, &receipt) != nil {
		return &AdmissionError{Code: DependencyUnavailable}
	}
	if sha256.Sum256(receiptBytes) != receiptHash || !proto.Equal(receipt.Epoch, receiptEpoch) || receipt.State != "succeeded" || receipt.FailureCode != "" || slices.ContainsFunc(receipt.Effects, func(effect *agentv1.Effect) bool { return effect == nil || effect.Active || effect.OutcomeUnknown }) {
		return operation.ErrCommandConflict
	}
	if err := operation.CheckStepResult(step, &receipt); err != nil {
		return err
	}
	for _, dependency := range step.DependsOn {
		var completed bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM operation_step_completions WHERE operation_id=$1 AND step_id=$2)`, claim.OperationID, dependency).Scan(&completed); err != nil {
			return admissionError(err)
		}
		if !completed {
			return operation.ErrBarrierPending
		}
	}
	tag, err := tx.Exec(ctx, `INSERT INTO operation_step_completions (operation_id,step_id,command_id,execution_epoch,recovery_generation,receipt_hash)
 VALUES ($1,$2,$3,$4,$5,$6) ON CONFLICT DO NOTHING`, claim.OperationID, stepID, command.CommandId, receiptEpoch.ExecutionEpoch, generation, receiptHash[:])
	if err != nil {
		return admissionError(err)
	}
	if tag.RowsAffected() == 0 {
		var savedHash []byte
		var savedEpoch int64
		var savedGeneration uuid.UUID
		if err := tx.QueryRow(ctx, `SELECT receipt_hash,execution_epoch,recovery_generation FROM operation_step_completions WHERE operation_id=$1 AND step_id=$2`, claim.OperationID, stepID).Scan(&savedHash, &savedEpoch, &savedGeneration); err != nil {
			return admissionError(err)
		}
		if !bytes.Equal(savedHash, receiptHash[:]) || savedEpoch != receiptEpoch.ExecutionEpoch || savedGeneration != generation {
			return operation.ErrCommandConflict
		}
		return nil
	}
	return s.commitMetadata(ctx, tx)
}

// RecordDeliveryAttempt commits the retry budget before a command can enter the stream.
// A lost send consumes its attempt until durable evidence resolves the outcome.
func (s *Store) RecordDeliveryAttempt(ctx context.Context, claim operation.Claim, commandID, sessionID, attemptID uuid.UUID, retryDelay time.Duration) (DeliveryAttempt, error) {
	var result DeliveryAttempt
	if commandID == uuid.Nil || sessionID == uuid.Nil || attemptID == uuid.Nil || retryDelay < time.Millisecond || retryDelay > time.Minute {
		return result, &AdmissionError{Code: InvalidRequest}
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return result, admissionError(err)
	}
	defer tx.Rollback(ctx)
	active, state, planBytes, err := lockClaim(ctx, tx, claim)
	if err != nil {
		return result, err
	}
	if !active || state != operation.Running {
		return result, operation.ErrBarrierPending
	}
	if len(planBytes) > maxExecutionStateBytes {
		return result, ErrExecutionStateTooLarge
	}
	var plan agentv1.ExecutionPlan
	if proto.Unmarshal(planBytes, &plan) != nil {
		return result, &AdmissionError{Code: DependencyUnavailable}
	}
	index := slices.IndexFunc(plan.Steps, func(step *agentv1.PlanStep) bool {
		return step != nil && step.Command != nil && step.Command.CommandId == commandID.String()
	})
	if index < 0 {
		return result, operation.ErrCommandConflict
	}
	step := plan.Steps[index]
	nodeID, err := uuid.Parse(step.Command.Epoch.NodeId)
	if err != nil {
		return result, &AdmissionError{Code: DependencyUnavailable}
	}
	if _, err := checkAgentSession(ctx, tx, claim, nodeID, sessionID, 20*time.Second); err != nil {
		return result, err
	}
	var existingClaim uuid.UUID
	var delay int64
	err = tx.QueryRow(ctx, `SELECT attempt_id,command_id,session_id,attempt_count,created_at,next_attempt_at,claim_id,retry_delay_us FROM command_delivery_attempts WHERE attempt_id=$1`, attemptID).Scan(&result.ID, &result.CommandID, &result.SessionID, &result.Count, &result.CreatedAt, &result.NextAttemptAt, &existingClaim, &delay)
	if err == nil {
		if existingClaim != claim.ID || result.CommandID != commandID || result.SessionID != sessionID || delay != retryDelay.Microseconds() {
			return DeliveryAttempt{}, operation.ErrCommandConflict
		}
		return result, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return result, admissionError(err)
	}
	var recovery bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM claim_recovery_checkpoints WHERE claim_id=$1)`, claim.ID).Scan(&recovery); err != nil {
		return result, admissionError(err)
	}
	if !recovery {
		return result, operation.ErrBarrierPending
	}
	for _, dependency := range step.DependsOn {
		var completed bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM operation_step_completions WHERE operation_id=$1 AND step_id=$2)`, claim.OperationID, dependency).Scan(&completed); err != nil {
			return result, admissionError(err)
		}
		if !completed {
			return result, operation.ErrBarrierPending
		}
	}
	var hash []byte
	if err := tx.QueryRow(ctx, `SELECT input_hash FROM command_dispatches WHERE command_id=$1 AND execution_epoch=$2 AND recovery_generation=$3 AND installation_id=$4 AND operation_id=$5 AND node_id=$6`, commandID, claim.ExecutionEpoch, claim.RecoveryGeneration, claim.InstallationID, claim.OperationID, nodeID).Scan(&hash); err != nil {
		return result, admissionError(err)
	}
	if !bytes.Equal(hash, step.Command.InputHash) {
		return result, operation.ErrCommandConflict
	}
	var count int
	var next *time.Time
	if err := tx.QueryRow(ctx, `SELECT count(*),max(next_attempt_at) FROM command_delivery_attempts WHERE command_id=$1`, commandID).Scan(&count, &next); err != nil {
		return result, admissionError(err)
	}
	if count >= int(step.MaxAttempts) {
		return result, ErrAttemptsExhausted
	}
	var now time.Time
	if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&now); err != nil {
		return result, admissionError(err)
	}
	if next != nil && now.Before(*next) {
		return result, ErrRetryPending
	}
	if _, _, _, err := lockClaim(ctx, tx, claim); err != nil {
		return result, err
	}
	result = DeliveryAttempt{ID: attemptID, CommandID: commandID, SessionID: sessionID, Count: count + 1, CreatedAt: now, NextAttemptAt: now.Add(retryDelay)}
	if _, err := tx.Exec(ctx, `INSERT INTO command_delivery_attempts (attempt_id,claim_id,command_id,execution_epoch,recovery_generation,session_id,attempt_count,retry_delay_us,created_at,next_attempt_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`, attemptID, claim.ID, commandID, claim.ExecutionEpoch, claim.RecoveryGeneration, sessionID, result.Count, retryDelay.Microseconds(), now, result.NextAttemptAt); err != nil {
		return DeliveryAttempt{}, admissionError(err)
	}
	if err := s.commitMetadata(ctx, tx); err != nil {
		return DeliveryAttempt{}, err
	}
	return result, nil
}
