package postgres

import (
	"context"
	"errors"
	"slices"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/sirrobot01/synod/internal/operation"
	"github.com/sirrobot01/synod/internal/placement"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/proto"
)

func (s *Store) prepareDatabaseMoveStep(ctx context.Context, tx pgx.Tx, claim operation.Claim, stepID, writer uuid.UUID, epoch uint64, revision int64, plan *agentv1.ExecutionPlan) (bool, error) {
	var move AppMove
	var targetCopy uuid.UUID
	var tier int
	if err := tx.QueryRow(ctx, `SELECT m.source_node_id,m.target_node_id,m.writer_epoch,m.promoted_at,m.target_copy_id,a.tier
 FROM app_moves m JOIN apps a ON a.installation_id=m.installation_id AND a.id=m.app_id WHERE m.operation_id=$1`, claim.OperationID).Scan(&move.SourceNodeID, &move.TargetNodeID, &move.WriterEpoch, &move.PromotedAt, &targetCopy, &tier); err != nil {
		return false, admissionError(err)
	}
	index := slices.IndexFunc(plan.Steps, func(step *agentv1.PlanStep) bool { return step.Command.StepId == stepID.String() })
	if tier != 1 || index < 0 || plan.Steps[index].Command.DesiredRevision != revision {
		return false, operation.ErrCommandConflict
	}
	if move.PromotedAt != nil {
		return writer == move.TargetNodeID && epoch == move.WriterEpoch+1, nil
	}
	if writer != move.SourceNodeID || epoch != move.WriterEpoch || index > 1 {
		return false, operation.ErrCommandConflict
	}
	if index == 1 {
		if err := checkPlanCompletions(ctx, tx, operation.Operation{ID: claim.OperationID, InstallationID: claim.InstallationID, AppID: claim.AppID}, &agentv1.ExecutionPlan{Steps: plan.Steps[:1]}); err != nil {
			return false, err
		}
		var expired bool
		if err := tx.QueryRow(ctx, `SELECT created_at+($3 * interval '1 second')<clock_timestamp() FROM operation_step_completions WHERE operation_id=$1 AND step_id=$2`, claim.OperationID, plan.Steps[0].Command.StepId, plan.Steps[1].Command.Input.Timeout.AsDuration().Seconds()).Scan(&expired); err != nil {
			return false, admissionError(err)
		}
		if expired {
			return false, ErrAppMoveTargetLost
		}
	}
	command := plan.Steps[0].Command.Input.GetSwitchDatabase()
	nodes, err := readPlacementNodes(ctx, tx, claim.InstallationID)
	if err != nil {
		return false, err
	}
	nodeIndex := slices.IndexFunc(nodes, func(node placement.Node) bool { return node.ID == move.TargetNodeID })
	if nodeIndex < 0 || nodes[nodeIndex].Policy == nil || nodes[nodeIndex].Policy.ClientAddress != command.TargetHost {
		return false, ErrAppMoveTargetLost
	}
	var encoded []byte
	err = tx.QueryRow(ctx, `SELECT t.table_bytes FROM app_replicas r JOIN agent_presence p USING(installation_id,node_id)
 JOIN nodes n ON n.installation_id=p.installation_id AND n.id=p.node_id
 JOIN agent_replication t ON t.installation_id=p.installation_id AND t.node_id=p.node_id AND t.session_id=p.session_id
 JOIN app_copies c ON c.installation_id=r.installation_id AND c.app_id=r.app_id AND c.node_id=r.node_id
 WHERE r.installation_id=$1 AND r.app_id=$2 AND r.node_id=$3 AND r.writer_epoch=$4 AND r.role='standby'
 AND r.database_hold_id=$5 AND r.failure_code='' AND r.sequence>0 AND (r.state='streaming' OR ($6 AND r.state='disconnected'))
 AND r.session_id=p.session_id AND r.observed_at>clock_timestamp()-interval '20 seconds' AND r.completed_at>clock_timestamp()-interval '20 seconds'
 AND p.last_seen_at>clock_timestamp()-interval '20 seconds' AND NOT p.recovery_required AND n.revoked_at IS NULL
 AND t.acknowledged_at IS NOT NULL AND t.failure_code='' AND c.id=$7 AND c.desired AND c.role='standby' AND c.host_port=$8
 FOR SHARE OF p`, claim.InstallationID, claim.AppID, move.TargetNodeID, epoch, claim.OperationID, index == 1, targetCopy, command.TargetPort).Scan(&encoded)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, admissionError(err)
	}
	var table agentv1.ReplicationTable
	if proto.Unmarshal(encoded, &table) != nil {
		return false, operation.ErrCommandConflict
	}
	if !slices.ContainsFunc(table.Apps, func(a *agentv1.AppReplication) bool {
		return a.AppId == claim.AppID.String() && a.Epoch == epoch && a.Role == "standby" && a.Database.GetSwitchoverOperationId() == claim.OperationID.String()
	}) {
		return false, nil
	}
	if index == 0 {
		return true, nil
	}
	if _, err := tx.Exec(ctx, `UPDATE app_copies SET rebuild_required=true,rebuild_data_removed=false,role='standby',health='unknown',observed_at=NULL,observed_session_id=NULL WHERE installation_id=$1 AND app_id=$2 AND desired AND id<>$3`, claim.InstallationID, claim.AppID, targetCopy); err != nil {
		return false, admissionError(err)
	}
	if _, err := tx.Exec(ctx, `UPDATE app_copies SET rebuild_required=false,rebuild_data_removed=false,role='active',health='unknown',observed_at=NULL,observed_session_id=NULL WHERE id=$1`, targetCopy); err != nil {
		return false, admissionError(err)
	}

	if _, err := tx.Exec(ctx, `UPDATE apps SET writer_node_id=$3,writer_epoch=writer_epoch+1,writer_last_grant_at=NULL,failover_loss_window_seconds=0,updated_at=clock_timestamp() WHERE installation_id=$1 AND id=$2`, claim.InstallationID, claim.AppID, move.TargetNodeID); err != nil {
		return false, admissionError(err)
	}
	if _, err := tx.Exec(ctx, `UPDATE app_moves SET promoted_at=clock_timestamp() WHERE operation_id=$1`, claim.OperationID); err != nil {
		return false, admissionError(err)
	}
	if err := s.commitMetadata(ctx, tx); err != nil {
		return false, err
	}
	return true, nil
}
