package postgres

import (
	"bytes"
	"context"
	"errors"
	"slices"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/sirrobot01/synod/internal/controlstream"
	"github.com/sirrobot01/synod/internal/operation"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/proto"
)

// InspectionEvidence is one command inspection request and its reply.
type InspectionEvidence struct {
	Request    *agentv1.CommandInspectionRequest
	Result     *agentv1.CommandInspectionResult
	SessionID  uuid.UUID
	CreatedAt  time.Time
	ReceivedAt *time.Time
}

// RecordInspectionRequest fixes the read request before sending it to an agent.
// One claim permits at most 512 requests. A read is not a native execution attempt.
func (s *Store) RecordInspectionRequest(ctx context.Context, claim operation.Claim, sessionID uuid.UUID, request *agentv1.CommandInspectionRequest) error {
	if request == nil || request.Command == nil || request.Command.Epoch == nil || controlstream.CheckCommandInspectionRequest(request, claim.InstallationID.String(), request.Command.Epoch.NodeId) != nil {
		return &AdmissionError{Code: InvalidRequest}
	}
	command := request.Command
	if command.Epoch.ClusterId != claim.ClusterID.String() || command.Epoch.ExecutionEpoch != claim.ExecutionEpoch || command.Epoch.RecoveryGeneration != claim.RecoveryGeneration.String() {
		return operation.ErrStaleOwner
	}
	nodeID := uuid.MustParse(command.Epoch.NodeId)
	encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(request)
	if err != nil {
		return &AdmissionError{Code: InvalidRequest}
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return admissionError(err)
	}
	defer tx.Rollback(ctx)
	if _, state, _, err := lockClaim(ctx, tx, claim); err != nil {
		return err
	} else if state == operation.Succeeded || state == operation.Failed || state == operation.Cancelled {
		return operation.ErrStaleOwner
	}
	if _, err := checkAgentSession(ctx, tx, claim, nodeID, sessionID, 20*time.Second); err != nil {
		return err
	}
	var oldClaim, oldSession uuid.UUID
	var previous []byte
	err = tx.QueryRow(ctx, `SELECT claim_id,session_id,request_bytes FROM command_inspections WHERE request_id=$1`, request.RequestId).Scan(&oldClaim, &oldSession, &previous)
	if err == nil {
		if oldClaim != claim.ID || oldSession != sessionID || !bytes.Equal(previous, encoded) {
			return operation.ErrCommandConflict
		}
		return nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return admissionError(err)
	}
	// Only an exact historical dispatch can supply the immutable inspected input.
	var known bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM command_dispatches WHERE command_id=$1 AND installation_id=$2 AND cluster_id=$3 AND node_id=$4 AND input_hash=$5)`, command.CommandId, claim.InstallationID, claim.ClusterID, nodeID, command.InputHash).Scan(&known); err != nil {
		return admissionError(err)
	}
	if !known {
		return operation.ErrCommandConflict
	}
	var count int
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM command_inspections WHERE claim_id=$1`, claim.ID).Scan(&count); err != nil {
		return admissionError(err)
	}
	if count >= 512 {
		return ErrExecutionStateTooLarge
	}
	if _, _, _, err := lockClaim(ctx, tx, claim); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO command_inspections (request_id,claim_id,installation_id,node_id,session_id,command_id,request_bytes) VALUES ($1,$2,$3,$4,$5,$6,$7)`, request.RequestId, claim.ID, claim.InstallationID, nodeID, sessionID, command.CommandId, encoded); err != nil {
		return admissionError(err)
	}
	return s.commitMetadata(ctx, tx)
}

// RecordAgentInspection accepts one immutable result for an exact saved request.
// A changed observation uses a new request UUID. Receipt does not reconcile a step.
func (s *Store) RecordAgentInspection(ctx context.Context, installationID, nodeID, sessionID uuid.UUID, result *agentv1.CommandInspectionResult) error {
	if installationID == uuid.Nil || nodeID == uuid.Nil || sessionID == uuid.Nil || controlstream.CheckCommandInspectionResult(result, installationID.String(), nodeID.String()) != nil {
		return &AdmissionError{Code: InvalidRequest}
	}
	encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(result)
	if err != nil {
		return &AdmissionError{Code: InvalidRequest}
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return admissionError(err)
	}
	defer tx.Rollback(ctx)
	var claim operation.Claim
	if err := tx.QueryRow(ctx, `SELECT c.claim_id,c.installation_id,c.cluster_id,c.operation_id,c.recovery_generation,c.owner_id,c.execution_epoch
 FROM command_inspections i JOIN execution_claims c USING(claim_id)
 WHERE i.request_id=$1 AND i.installation_id=$2 AND i.node_id=$3 AND i.session_id=$4`, result.RequestId, installationID, nodeID, sessionID).Scan(&claim.ID, &claim.InstallationID, &claim.ClusterID, &claim.OperationID, &claim.RecoveryGeneration, &claim.OwnerID, &claim.ExecutionEpoch); err != nil {
		return admissionError(err)
	}
	if _, state, _, err := lockClaim(ctx, tx, claim); err != nil {
		return err
	} else if state == operation.Succeeded || state == operation.Failed || state == operation.Cancelled {
		return operation.ErrStaleOwner
	}
	if _, err := checkAgentSession(ctx, tx, claim, nodeID, sessionID, 20*time.Second); err != nil {
		return err
	}
	var requestBytes, previous []byte
	var issuedAt time.Time
	if err := tx.QueryRow(ctx, `SELECT request_bytes,result_bytes,created_at FROM command_inspections WHERE request_id=$1 FOR UPDATE`, result.RequestId).Scan(&requestBytes, &previous, &issuedAt); err != nil {
		return admissionError(err)
	}
	var request agentv1.CommandInspectionRequest
	if proto.Unmarshal(requestBytes, &request) != nil || request.Command == nil || request.Command.Input == nil {
		return &AdmissionError{Code: DependencyUnavailable}
	}
	command := request.Command
	if !proto.Equal(result.Epoch, command.Epoch) || result.CommandId != command.CommandId || !bytes.Equal(result.InputHash, command.InputHash) {
		return operation.ErrCommandConflict
	}
	for _, effect := range result.Effects {
		if effect.ResourceId != command.Input.ResourceId {
			return operation.ErrCommandConflict
		}
	}
	for _, prior := range request.PriorEffects {
		if !prior.Active && !prior.OutcomeUnknown {
			continue
		}
		if !slices.ContainsFunc(result.Effects, func(effect *agentv1.Effect) bool {
			return effect.CommandId == prior.CommandId && effect.ResourceId == prior.ResourceId && effect.NativeOperationId == prior.NativeOperationId
		}) {
			return operation.ErrBarrierPending
		}
	}
	if previous != nil {
		var old agentv1.CommandInspectionResult
		if proto.Unmarshal(previous, &old) != nil {
			return &AdmissionError{Code: DependencyUnavailable}
		}
		if !proto.Equal(&old, result) {
			return operation.ErrCommandConflict
		}
		return nil
	}
	var now time.Time
	if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&now); err != nil {
		return admissionError(err)
	}
	if !issuedAt.After(now.Add(-20 * time.Second)) {
		return ErrStaleSession
	}
	if _, _, _, err := lockClaim(ctx, tx, claim); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `UPDATE command_inspections SET result_bytes=$1,received_at=clock_timestamp() WHERE request_id=$2`, encoded, result.RequestId); err != nil {
		return admissionError(err)
	}
	return s.commitMetadata(ctx, tx)
}

func (s *Store) GetCommandInspection(ctx context.Context, claim operation.Claim, requestID uuid.UUID) (InspectionEvidence, error) {
	var result InspectionEvidence
	if requestID == uuid.Nil {
		return result, &AdmissionError{Code: InvalidRequest}
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return result, admissionError(err)
	}
	defer tx.Rollback(ctx)
	if _, _, _, err := lockClaim(ctx, tx, claim); err != nil {
		return result, err
	}
	var requestBytes, resultBytes []byte
	if err := tx.QueryRow(ctx, `SELECT request_bytes,result_bytes,session_id,created_at,received_at FROM command_inspections WHERE claim_id=$1 AND request_id=$2`, claim.ID, requestID).Scan(&requestBytes, &resultBytes, &result.SessionID, &result.CreatedAt, &result.ReceivedAt); err != nil {
		return result, admissionError(err)
	}
	result.Request = new(agentv1.CommandInspectionRequest)
	if proto.Unmarshal(requestBytes, result.Request) != nil {
		return InspectionEvidence{}, &AdmissionError{Code: DependencyUnavailable}
	}
	if resultBytes != nil {
		result.Result = new(agentv1.CommandInspectionResult)
		if proto.Unmarshal(resultBytes, result.Result) != nil {
			return InspectionEvidence{}, &AdmissionError{Code: DependencyUnavailable}
		}
	}
	return result, nil
}

// RecordRecoveryCheckpoint stores the worker's interpretation of its current recovery frontier.
// Every referenced successful observation must belong to this claim and a current agent session.
func (s *Store) RecordRecoveryCheckpoint(ctx context.Context, claim operation.Claim, frontierHash [32]byte, inspectionIDs []uuid.UUID) error {
	if len(inspectionIDs) > 256 {
		return ErrExecutionStateTooLarge
	}
	ids := slices.Clone(inspectionIDs)
	slices.SortFunc(ids, func(a, b uuid.UUID) int { return bytes.Compare(a[:], b[:]) })
	for i, id := range ids {
		if id == uuid.Nil || (i > 0 && ids[i-1] == id) {
			return &AdmissionError{Code: InvalidRequest}
		}
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return admissionError(err)
	}
	defer tx.Rollback(ctx)
	if _, state, _, err := lockClaim(ctx, tx, claim); err != nil {
		return err
	} else if state == operation.Succeeded || state == operation.Failed || state == operation.Cancelled {
		return operation.ErrStaleOwner
	}
	var oldHash []byte
	var oldIDs []uuid.UUID
	err = tx.QueryRow(ctx, `SELECT frontier_hash,inspection_ids FROM claim_recovery_checkpoints WHERE claim_id=$1`, claim.ID).Scan(&oldHash, &oldIDs)
	if err == nil {
		if !bytes.Equal(oldHash, frontierHash[:]) || !slices.Equal(oldIDs, ids) {
			return operation.ErrCommandConflict
		}
		return nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return admissionError(err)
	}
	for _, id := range ids {
		var node, session uuid.UUID
		var resultBytes []byte
		var received *time.Time
		if err := tx.QueryRow(ctx, `SELECT node_id,session_id,result_bytes,received_at FROM command_inspections WHERE request_id=$1 AND claim_id=$2`, id, claim.ID).Scan(&node, &session, &resultBytes, &received); err != nil {
			return admissionError(err)
		}
		if received == nil {
			return operation.ErrBarrierPending
		}
		if _, err := checkAgentSession(ctx, tx, claim, node, session, 20*time.Second); err != nil {
			return err
		}
		var now time.Time
		if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&now); err != nil {
			return admissionError(err)
		}
		if !received.After(now.Add(-20 * time.Second)) {
			return ErrStaleSession
		}
		var result agentv1.CommandInspectionResult
		if proto.Unmarshal(resultBytes, &result) != nil {
			return &AdmissionError{Code: DependencyUnavailable}
		}
		if result.FailureCode != "" || slices.ContainsFunc(result.Effects, func(effect *agentv1.Effect) bool { return effect == nil || effect.Active || effect.OutcomeUnknown }) {
			return operation.ErrBarrierPending
		}
	}
	if _, _, _, err := lockClaim(ctx, tx, claim); err != nil {
		return err
	}
	if ids == nil {
		ids = []uuid.UUID{}
	}
	if _, err := tx.Exec(ctx, `INSERT INTO claim_recovery_checkpoints (claim_id,frontier_hash,inspection_ids) VALUES ($1,$2,$3)`, claim.ID, frontierHash[:], ids); err != nil {
		return admissionError(err)
	}
	return s.commitMetadata(ctx, tx)
}
