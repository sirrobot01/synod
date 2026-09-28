package postgres

import (
	"bytes"
	"context"
	"errors"
	"math"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/sirrobot01/synod/internal/operation"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/proto"
)

// RecordAgentResult receives evidence from the certificate-authenticated node.
// Nil permits a matching transport ACK. Receipt never changes operation authority.
func (s *Store) RecordAgentResult(ctx context.Context, installationID, nodeID uuid.UUID, result *agentv1.TerminalResult) error {
	if installationID == uuid.Nil || nodeID == uuid.Nil || result == nil || result.Epoch == nil || result.Sequence == 0 || result.Sequence > math.MaxInt64 || len(result.InputHash) != 32 || proto.Size(result) > 1<<20 || len(result.Effects) > 1024 {
		return &AdmissionError{Code: InvalidRequest}
	}
	epoch := result.Epoch
	if epoch.InstallationId != installationID.String() || epoch.NodeId != nodeID.String() || epoch.ExecutionEpoch < 1 {
		return operation.ErrStaleOwner
	}
	for _, id := range []string{result.CommandId, epoch.AppId, epoch.RecoveryGeneration} {
		parsed, err := uuid.Parse(id)
		if err != nil || parsed == uuid.Nil || parsed.String() != id {
			return &AdmissionError{Code: InvalidRequest}
		}
	}
	if result.State != "succeeded" && result.State != "failed" && result.State != "cancelled" {
		return &AdmissionError{Code: InvalidRequest}
	}
	if len(result.FailureCode) > 128 || strings.ContainsFunc(result.FailureCode, func(r rune) bool { return !(r >= 'a' && r <= 'z' || r >= '0' && r <= '9' || r == '_') }) || (result.State == "succeeded" && result.FailureCode != "") {
		return &AdmissionError{Code: InvalidRequest}
	}
	encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(result)
	if err != nil || len(encoded) > 1<<20 {
		return &AdmissionError{Code: InvalidRequest}
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return admissionError(err)
	}
	defer tx.Rollback(ctx)
	var commandBytes []byte
	err = tx.QueryRow(ctx, `SELECT command_bytes FROM command_dispatches
 WHERE command_id=$1 AND execution_epoch=$2 AND recovery_generation=$3
 AND installation_id=$4 AND node_id=$5 AND app_id=$6`, result.CommandId, epoch.ExecutionEpoch, epoch.RecoveryGeneration, installationID, nodeID, epoch.AppId).Scan(&commandBytes)
	if err != nil {
		return admissionError(err)
	}
	var command agentv1.Command
	if err := proto.Unmarshal(commandBytes, &command); err != nil {
		return admissionError(err)
	}
	if operation.ValidateCommand(&command) != nil || command.CommandId != result.CommandId {
		return &AdmissionError{Code: DependencyUnavailable}
	}
	if command.Input == nil || !proto.Equal(command.Epoch, epoch) || !bytes.Equal(command.InputHash, result.InputHash) {
		return operation.ErrCommandConflict
	}
	seen := make(map[string]bool, len(result.Effects))
	for _, effect := range result.Effects {
		if effect == nil || effect.CommandId != command.CommandId || effect.ResourceId != command.Input.ResourceId || len(effect.NativeOperationId) > 256 || len(effect.State) < 1 || len(effect.State) > 64 || strings.ContainsFunc(effect.NativeOperationId, func(r rune) bool { return r < '!' || r > '~' }) || strings.ContainsFunc(effect.State, func(r rune) bool { return !(r >= 'a' && r <= 'z' || r >= '0' && r <= '9' || r == '_') }) || seen[effect.NativeOperationId] {
			return &AdmissionError{Code: InvalidRequest}
		}
		seen[effect.NativeOperationId] = true
		if result.State == "succeeded" && (effect.Active || effect.OutcomeUnknown) {
			return operation.ErrBarrierPending
		}
	}
	tag, err := tx.Exec(ctx, `INSERT INTO command_results (command_id,execution_epoch,recovery_generation,result_bytes)
 VALUES ($1,$2,$3,$4) ON CONFLICT DO NOTHING`, result.CommandId, epoch.ExecutionEpoch, epoch.RecoveryGeneration, encoded)
	if err != nil {
		return admissionError(err)
	}
	if tag.RowsAffected() == 0 {
		var previousBytes []byte
		if err := tx.QueryRow(ctx, `SELECT result_bytes FROM command_results WHERE command_id=$1 AND execution_epoch=$2 AND recovery_generation=$3`, result.CommandId, epoch.ExecutionEpoch, epoch.RecoveryGeneration).Scan(&previousBytes); err != nil {
			return admissionError(err)
		}
		var previous agentv1.TerminalResult
		if proto.Unmarshal(previousBytes, &previous) != nil {
			return &AdmissionError{Code: DependencyUnavailable}
		}
		if !proto.Equal(&previous, result) {
			return operation.ErrCommandConflict
		}
		return nil
	}
	return s.commitMetadata(ctx, tx)
}

// GetAgentResult reads one exact dispatch receipt, including historical epochs.
// The caller must validate its current claim before advancing any operation.
func (s *Store) GetAgentResult(ctx context.Context, installationID, nodeID uuid.UUID, commandID uuid.UUID, epoch *agentv1.Epoch) (*agentv1.TerminalResult, error) {
	if installationID == uuid.Nil || nodeID == uuid.Nil || commandID == uuid.Nil || epoch == nil || epoch.InstallationId != installationID.String() || epoch.NodeId != nodeID.String() || epoch.ExecutionEpoch < 1 {
		return nil, &AdmissionError{Code: InvalidRequest}
	}
	for _, id := range []string{epoch.AppId, epoch.RecoveryGeneration} {
		parsed, err := uuid.Parse(id)
		if err != nil || parsed == uuid.Nil || parsed.String() != id {
			return nil, &AdmissionError{Code: InvalidRequest}
		}
	}
	var data []byte
	err := s.pool.QueryRow(ctx, `SELECT r.result_bytes FROM command_results r
 JOIN command_dispatches d USING (command_id,execution_epoch,recovery_generation)
 WHERE r.command_id=$1 AND r.execution_epoch=$2 AND r.recovery_generation=$3
 AND d.installation_id=$4 AND d.node_id=$5 AND d.app_id=$6`, commandID, epoch.ExecutionEpoch, epoch.RecoveryGeneration, installationID, nodeID, epoch.AppId).Scan(&data)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, &AdmissionError{Code: NotFound}
	}
	if err != nil {
		return nil, admissionError(err)
	}
	var result agentv1.TerminalResult
	if err := proto.Unmarshal(data, &result); err != nil {
		return nil, &AdmissionError{Code: DependencyUnavailable}
	}
	return &result, nil
}
