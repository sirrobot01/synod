package postgres

import (
	"bytes"
	"context"
	"errors"
	"math"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/sirrobot01/synod/internal/operation"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/proto"
)

// ReadScaleRetryPlan reads the latest prior plan and checked dispatch results under the current claim.
func (s *Store) ReadScaleRetryPlan(ctx context.Context, claim operation.Claim) (*agentv1.ExecutionPlan, []DispatchEvidence, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, nil, admissionError(err)
	}
	defer tx.Rollback(ctx)
	if _, _, _, err := lockClaim(ctx, tx, claim); err != nil {
		return nil, nil, err
	}
	prior := operation.Operation{InstallationID: claim.InstallationID, ClusterID: claim.ClusterID}
	var encoded []byte
	err = tx.QueryRow(ctx, `WITH RECURSIVE history AS (
 SELECT p.id,p.retry_of,p.desired_revision,p.kind,p.status,p.plan_bytes FROM operations p
 JOIN operations current ON current.retry_of=p.id
 WHERE current.id=$1 AND current.installation_id=$2 AND current.cluster_id=$3
 UNION ALL
 SELECT p.id,p.retry_of,p.desired_revision,p.kind,p.status,p.plan_bytes FROM operations p
 JOIN history h ON h.retry_of=p.id AND p.desired_revision<h.desired_revision
 WHERE p.installation_id=$2 AND p.cluster_id=$3
 ) SELECT id,desired_revision,kind,status,plan_bytes FROM history
 WHERE plan_bytes IS NOT NULL ORDER BY desired_revision DESC LIMIT 1`,
		claim.OperationID, claim.InstallationID, claim.ClusterID).
		Scan(&prior.ID, &prior.DesiredRevision, &prior.Kind, &prior.Status, &encoded)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil, nil
	}
	if err != nil {
		return nil, nil, admissionError(err)
	}
	if len(encoded) > maxExecutionStateBytes {
		return nil, nil, ErrExecutionStateTooLarge
	}
	plan := new(agentv1.ExecutionPlan)
	if prior.Kind != "cluster.scale" || (prior.Status != operation.Failed && prior.Status != operation.Cancelled) {
		return nil, nil, operation.ErrCommandConflict
	}
	if err := proto.Unmarshal(encoded, plan); err != nil {
		return nil, nil, admissionError(err)
	}
	if err := operation.ValidatePlan(plan, prior); err != nil {
		return nil, nil, err
	}
	byCommand := make(map[string]*agentv1.Command, len(plan.Steps))
	for _, step := range plan.Steps {
		byCommand[step.Command.CommandId] = step.Command
	}
	rows, err := tx.Query(ctx, `SELECT d.command_id,d.node_id,d.step_id,d.execution_epoch,d.recovery_generation,d.input_hash,d.command_bytes,r.result_bytes
 FROM command_dispatches d LEFT JOIN command_results r USING (command_id,execution_epoch,recovery_generation)
 WHERE d.installation_id=$1 AND d.cluster_id=$2 AND d.operation_id=$3
 ORDER BY d.command_id,d.execution_epoch,d.recovery_generation`, claim.InstallationID, claim.ClusterID, prior.ID)
	if err != nil {
		return nil, nil, admissionError(err)
	}
	defer rows.Close()
	var dispatched []DispatchEvidence
	total := len(encoded)
	for rows.Next() {
		var commandID, nodeID, stepID, generation uuid.UUID
		var epoch int64
		var inputHash, commandBytes, resultBytes []byte
		if err := rows.Scan(&commandID, &nodeID, &stepID, &epoch, &generation, &inputHash, &commandBytes, &resultBytes); err != nil {
			return nil, nil, admissionError(err)
		}
		if total += len(commandBytes) + len(resultBytes); total > maxExecutionStateBytes {
			return nil, nil, ErrExecutionStateTooLarge
		}
		command := new(agentv1.Command)
		if proto.Unmarshal(commandBytes, command) != nil || operation.ValidateCommand(command) != nil {
			return nil, nil, operation.ErrCommandConflict
		}
		expected := byCommand[commandID.String()]
		if expected == nil || command.CommandId != expected.CommandId || command.StepId != expected.StepId || command.StepId != stepID.String() ||
			command.OperationId != prior.ID.String() || command.DesiredRevision != prior.DesiredRevision || command.PlanVersion != plan.Version ||
			command.Epoch.InstallationId != claim.InstallationID.String() || command.Epoch.ClusterId != claim.ClusterID.String() ||
			command.Epoch.NodeId != nodeID.String() || command.Epoch.NodeId != expected.Epoch.NodeId ||
			command.Epoch.ExecutionEpoch != epoch || command.Epoch.RecoveryGeneration != generation.String() ||
			!bytes.Equal(command.InputHash, expected.InputHash) || !bytes.Equal(command.InputHash, inputHash) {
			return nil, nil, operation.ErrCommandConflict
		}
		evidence := DispatchEvidence{Command: command}
		if resultBytes != nil {
			result := new(agentv1.TerminalResult)
			if proto.Unmarshal(resultBytes, result) != nil || result.CommandId != command.CommandId ||
				!proto.Equal(result.Epoch, command.Epoch) || !bytes.Equal(result.InputHash, command.InputHash) ||
				result.Sequence == 0 || result.Sequence > math.MaxInt64 ||
				(result.State != "succeeded" && result.State != "failed" && result.State != "cancelled") {
				return nil, nil, operation.ErrCommandConflict
			}
			evidence.Result = result
		}
		dispatched = append(dispatched, evidence)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, admissionError(err)
	}
	return plan, dispatched, nil
}
