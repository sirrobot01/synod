package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/sirrobot01/synod/internal/agent/replication"
	"github.com/sirrobot01/synod/internal/app"
	"github.com/sirrobot01/synod/internal/operation"
	"github.com/sirrobot01/synod/internal/placement"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/proto"
)

var ErrAppMoveTargetLost = errors.New("app_move_target_lost")

type AppMove struct {
	SourceNodeID uuid.UUID
	TargetNodeID uuid.UUID
	WriterEpoch  uint64
	PromotedAt   *time.Time
}

func (s *Store) GetAppMove(ctx context.Context, installation, id uuid.UUID) (AppMove, error) {
	var move AppMove
	err := s.pool.QueryRow(ctx, `SELECT source_node_id,target_node_id,writer_epoch,promoted_at FROM app_moves WHERE installation_id=$1 AND operation_id=$2`, installation, id).Scan(&move.SourceNodeID, &move.TargetNodeID, &move.WriterEpoch, &move.PromotedAt)
	if err != nil {
		return AppMove{}, admissionError(err)
	}
	return move, nil
}

// recordAppMoveTarget reserves the target as a standby. The writer does not change here.
func recordAppMoveTarget(ctx context.Context, tx pgx.Tx, claim operation.Claim, request placement.Request, decision placement.Decision, plan *agentv1.ExecutionPlan) error {
	if len(request.Resources) != 1 || len(decision.Assignments) != 1 || len(plan.Steps) != 4 {
		return operation.ErrCommandConflict
	}
	if err := operation.ValidatePlan(plan, operation.Operation{ID: claim.OperationID, InstallationID: claim.InstallationID, ClusterID: claim.ClusterID, DesiredRevision: request.DesiredRevision, Kind: MoveAppAPIOperation}); err != nil {
		return err
	}
	var source, target uuid.UUID
	var epoch uint64
	var standbys int
	var name string
	err := tx.QueryRow(ctx, `SELECT m.source_node_id,m.target_node_id,m.writer_epoch,(a.spec->'replication'->>'standbys')::integer,a.name
 FROM app_moves m JOIN apps a ON a.installation_id=m.installation_id AND a.id=m.app_id
 WHERE m.operation_id=$1 AND a.desired_revision=$2 AND a.writer_node_id=m.source_node_id AND a.writer_epoch=m.writer_epoch AND a.desired_lifecycle='running'`, claim.OperationID, request.DesiredRevision).Scan(&source, &target, &epoch, &standbys, &name)
	if err != nil {
		return admissionError(err)
	}
	resource, assigned := request.Resources[0], decision.Assignments[0]
	stop, final, ensure, start := plan.Steps[0].Command, plan.Steps[1].Command, plan.Steps[2].Command, plan.Steps[3].Command
	input := ensure.Input.GetEnsureContainer()
	if resource.Role != "active" || resource.ID != assigned.ResourceID || assigned.NodeID != target || len(assigned.Ports) != 1 || assigned.Ports[0].Name != "backend" {
		return operation.ErrCommandConflict
	}
	if input.GetGeneric() == nil || input.Generic.AppName != name || len(input.Ports) != 1 || input.Ports[0].HostPort != uint32(assigned.Ports[0].Number) || input.Ports[0].HostAddress != assigned.Ports[0].Address || input.Ports[0].Transport != assigned.Ports[0].Transport {
		return operation.ErrCommandConflict
	}
	if ensure.Input.ResourceId != resource.ID.String() || ensure.Epoch.NodeId != target.String() || start.Epoch.NodeId != target.String() || start.Input.ResourceId != resource.ID.String() || start.Input.GetStartContainer() == nil {
		return operation.ErrCommandConflict
	}
	if stop.Input.GetStopContainer() == nil || stop.Epoch.NodeId != source.String() || final.Input.GetFinalAppSnapshot().GetWriterEpoch() != epoch || final.Input.GetFinalAppSnapshot().GetTargetNodeId() != target.String() {
		return operation.ErrCommandConflict
	}
	for i := 1; i < len(plan.Steps); i++ {
		if !slices.Contains(plan.Steps[i].DependsOn, plan.Steps[i-1].Command.StepId) {
			return operation.ErrCommandConflict
		}
	}
	var valid bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM app_copies WHERE installation_id=$1 AND app_id=$2 AND node_id=$3 AND id=$4 AND desired AND role='active')`, claim.InstallationID, claim.ClusterID, source, stop.Input.ResourceId).Scan(&valid); err != nil {
		return admissionError(err)
	}
	if !valid {
		return operation.ErrCommandConflict
	}
	// Make room before activating a new or retired target. Keep retired data for deletion.
	if _, err := tx.Exec(ctx, `UPDATE app_copies SET desired=false,updated_at=clock_timestamp() WHERE id IN
 (SELECT id FROM app_copies WHERE installation_id=$1 AND app_id=$2 AND desired AND role='standby' AND node_id<>$3 ORDER BY copy_index DESC OFFSET $4)`, claim.InstallationID, claim.ClusterID, target, standbys-1); err != nil {
		return admissionError(err)
	}
	tag, err := tx.Exec(ctx, `INSERT INTO app_copies(id,installation_id,app_id,node_id,copy_index,host_port,desired_revision,role,observed_state,created_at,updated_at)
 VALUES($1,$2,$3,$4,$5,$6,$7,'standby','pending',clock_timestamp(),clock_timestamp())
 ON CONFLICT(id) DO UPDATE SET desired=true,desired_revision=EXCLUDED.desired_revision,updated_at=clock_timestamp()
 WHERE app_copies.installation_id=EXCLUDED.installation_id AND app_copies.app_id=EXCLUDED.app_id AND app_copies.node_id=EXCLUDED.node_id AND app_copies.copy_index=EXCLUDED.copy_index AND app_copies.host_port=EXCLUDED.host_port AND app_copies.role='standby'`, resource.ID, claim.InstallationID, claim.ClusterID, target, input.Generic.CopyIndex, input.Ports[0].HostPort, request.DesiredRevision)
	if err != nil {
		return admissionError(err)
	}
	if tag.RowsAffected() != 1 {
		return operation.ErrCommandConflict
	}
	_, err = tx.Exec(ctx, `UPDATE app_moves SET target_copy_id=$2 WHERE operation_id=$1`, claim.OperationID, resource.ID)
	if err != nil {
		return admissionError(err)
	}
	return nil
}

// PrepareAppMoveStep checks the replica before stopping and commits promotion before starting.
func (s *Store) PrepareAppMoveStep(ctx context.Context, claim operation.Claim, stepID uuid.UUID) (bool, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, admissionError(err)
	}
	defer tx.Rollback(ctx)
	var writer uuid.UUID
	var epoch uint64
	var revision int64
	if err := tx.QueryRow(ctx, `SELECT writer_node_id,writer_epoch,desired_revision FROM apps WHERE installation_id=$1 AND id=$2 FOR UPDATE`, claim.InstallationID, claim.ClusterID).Scan(&writer, &epoch, &revision); err != nil {
		return false, admissionError(err)
	}
	_, status, encoded, err := lockClaim(ctx, tx, claim)
	if err != nil {
		return false, err
	}
	if status == operation.Cancelled || status == operation.Cancelling || status == operation.Failed || status == operation.Succeeded {
		return false, operation.ErrStaleOwner
	}
	var plan agentv1.ExecutionPlan
	if proto.Unmarshal(encoded, &plan) != nil || len(plan.Steps) != 4 {
		return false, operation.ErrCommandConflict
	}
	var move AppMove
	var targetCopy uuid.UUID
	if err := tx.QueryRow(ctx, `SELECT source_node_id,target_node_id,writer_epoch,promoted_at,target_copy_id FROM app_moves WHERE operation_id=$1`, claim.OperationID).Scan(&move.SourceNodeID, &move.TargetNodeID, &move.WriterEpoch, &move.PromotedAt, &targetCopy); err != nil {
		return false, admissionError(err)
	}
	if move.PromotedAt != nil {
		return writer == move.TargetNodeID && epoch == move.WriterEpoch+1, nil
	}
	if writer != move.SourceNodeID || epoch != move.WriterEpoch {
		return false, operation.ErrCommandConflict
	}
	index := slices.IndexFunc(plan.Steps, func(step *agentv1.PlanStep) bool { return step.Command.StepId == stepID.String() })
	if index < 0 || plan.Steps[index].Command.DesiredRevision != revision {
		return false, operation.ErrCommandConflict
	}
	if index == 1 {
		return true, nil
	}
	if index != 0 && index != 2 {
		return false, operation.ErrCommandConflict
	}
	if _, err := tx.Exec(ctx, `SELECT 1 FROM agent_presence WHERE installation_id=$1 AND node_id=$2 FOR SHARE`, claim.InstallationID, move.TargetNodeID); err != nil {
		return false, admissionError(err)
	}
	if index == 2 {
		op := operation.Operation{ID: claim.OperationID, InstallationID: claim.InstallationID, ClusterID: claim.ClusterID}
		if err := checkPlanCompletions(ctx, tx, op, &agentv1.ExecutionPlan{Steps: plan.Steps[:2]}); err != nil {
			return false, err
		}
		var available, expired bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM agent_presence p JOIN nodes n ON n.installation_id=p.installation_id AND n.id=p.node_id WHERE p.installation_id=$1 AND p.node_id=$2 AND p.last_seen_at>clock_timestamp()-interval '20 seconds' AND NOT p.recovery_required AND n.revoked_at IS NULL)`, claim.InstallationID, move.TargetNodeID).Scan(&available); err != nil {
			return false, admissionError(err)
		}
		if err := tx.QueryRow(ctx, `SELECT created_at+($3 * interval '1 second')<clock_timestamp() FROM operation_step_completions WHERE operation_id=$1 AND step_id=$2`, claim.OperationID, plan.Steps[1].Command.StepId, plan.Steps[2].Command.Input.Timeout.AsDuration().Seconds()).Scan(&expired); err != nil {
			return false, admissionError(err)
		}
		if !available || expired {
			return false, ErrAppMoveTargetLost
		}
	}
	var tableBytes []byte
	var sequence uint64
	err = tx.QueryRow(ctx, `SELECT r.sequence,t.table_bytes FROM app_replicas r JOIN agent_presence p USING(installation_id,node_id)
 JOIN nodes n ON n.installation_id=p.installation_id AND n.id=p.node_id
 JOIN agent_replication t ON t.installation_id=p.installation_id AND t.node_id=p.node_id AND t.session_id=p.session_id
 WHERE r.installation_id=$1 AND r.app_id=$2 AND r.node_id=$3 AND r.writer_epoch=$4 AND r.role='standby' AND r.failure_code='' AND r.sequence>0 AND r.completed_at IS NOT NULL
 AND r.session_id=p.session_id AND r.observed_at>clock_timestamp()-interval '20 seconds'
 AND p.last_seen_at>clock_timestamp()-interval '20 seconds' AND NOT p.recovery_required AND n.revoked_at IS NULL
 AND t.acknowledged_at IS NOT NULL AND t.failure_code=''`, claim.InstallationID, claim.ClusterID, move.TargetNodeID, epoch).Scan(&sequence, &tableBytes)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, admissionError(err)
	}
	var table agentv1.ReplicationTable
	if proto.Unmarshal(tableBytes, &table) != nil {
		return false, operation.ErrCommandConflict
	}
	if !slices.ContainsFunc(table.Apps, func(a *agentv1.AppReplication) bool {
		return a.AppId == claim.ClusterID.String() && a.Epoch == epoch && a.Role == replication.RoleStandby
	}) {
		return false, nil
	}
	if index == 0 {
		return true, nil
	}
	var resultBytes []byte
	if err := tx.QueryRow(ctx, `SELECT r.result_bytes FROM operation_step_completions c JOIN command_results r USING(command_id,execution_epoch,recovery_generation) WHERE c.operation_id=$1 AND c.step_id=$2`, claim.OperationID, plan.Steps[1].Command.StepId).Scan(&resultBytes); err != nil {
		return false, admissionError(err)
	}
	var result agentv1.TerminalResult
	if proto.Unmarshal(resultBytes, &result) != nil {
		return false, operation.ErrCommandConflict
	}
	finalSequence := uint64(0)
	for _, effect := range result.Effects {
		if proof := effect.AppSnapshot; proof != nil && effect.NativeOperationId == "app-data:"+plan.Steps[1].Command.CommandId {
			finalSequence = proof.Sequence
		}
	}
	if finalSequence == 0 || sequence != finalSequence {
		return false, nil
	}
	// Release source compute only after the stopped writer and final copy are proved.
	if err := releaseAppCompute(ctx, tx, claim.InstallationID, claim.ClusterID, move.SourceNodeID); err != nil {
		return false, err
	}
	if _, err := tx.Exec(ctx, `UPDATE app_copies SET role='standby',health='unknown',observed_at=NULL,observed_session_id=NULL WHERE installation_id=$1 AND app_id=$2 AND desired AND role='active'`, claim.InstallationID, claim.ClusterID); err != nil {
		return false, admissionError(err)
	}
	if _, err := tx.Exec(ctx, `UPDATE app_copies SET role='active',health='unknown',observed_at=NULL,observed_session_id=NULL WHERE id=$1 AND desired`, targetCopy); err != nil {
		return false, admissionError(err)
	}
	if _, err := tx.Exec(ctx, `UPDATE apps SET writer_node_id=$3,writer_epoch=writer_epoch+1,writer_last_grant_at=NULL,updated_at=clock_timestamp() WHERE installation_id=$1 AND id=$2`, claim.InstallationID, claim.ClusterID, move.TargetNodeID); err != nil {
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

// releaseAppCompute retains disk and port ownership until app deletion.
func releaseAppCompute(ctx context.Context, tx pgx.Tx, installation, id, node uuid.UUID) error {
	rows, err := tx.Query(ctx, `SELECT allocation_bytes FROM resource_allocations WHERE installation_id=$1 AND cluster_id=$2 AND node_id=$3 AND released_at IS NULL AND resource_id IN(SELECT id FROM app_copies WHERE installation_id=$1 AND app_id=$2)`, installation, id, node)
	if err != nil {
		return admissionError(err)
	}
	allocations, err := pgx.CollectRows(rows, pgx.RowTo[[]byte])
	if err != nil {
		return err
	}
	for _, data := range allocations {
		var a placement.Allocation
		if err := json.Unmarshal(data, &a); err != nil {
			return err
		}
		a.Budget.CPUMillis, a.Budget.MemoryBytes = 0, 0
		data, err = json.Marshal(a)
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `UPDATE resource_allocations SET allocation_bytes=$2 WHERE id=$1`, a.ID, data); err != nil {
			return admissionError(err)
		}
	}
	return nil
}

// ReconcileAppMove restarts a failed source or prepares a stable preferred target.
func (s *Store) ReconcileAppMove(ctx context.Context, installation, id uuid.UUID) error {
	var failed uuid.UUID
	var revision int64
	err := s.pool.QueryRow(ctx, `SELECT m.operation_id,a.desired_revision FROM app_moves m JOIN operations o ON o.id=m.operation_id JOIN apps a ON a.installation_id=m.installation_id AND a.id=m.app_id
 WHERE a.installation_id=$1 AND a.id=$2 AND a.desired_lifecycle='running' AND a.writer_node_id=m.source_node_id AND a.writer_epoch=m.writer_epoch
 AND m.promoted_at IS NULL AND o.status IN ('failed','cancelled') AND a.desired_revision=o.desired_revision
 AND NOT EXISTS(SELECT 1 FROM operations WHERE installation_id=$1 AND app_id=$2 AND status IN ('pending','running','waiting','cancelling'))`, installation, id).Scan(&failed, &revision)
	if err == nil {
		_, err = s.AdmitAppAction(ctx, AppActionRequest{Identity: RequestIdentity{InstallationID: installation, ActorID: "controller", APIOperation: StartAppAPIOperation, IdempotencyKey: "rollback:" + failed.String()}, RequestID: uuid.NewString(), AppID: id, ExpectedRevision: revision, RollbackMoveID: failed})
		return err
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return admissionError(err)
	}
	item, err := s.GetApp(ctx, installation, id)
	if err != nil {
		return err
	}
	if item.Spec.Replication == nil || item.Spec.Replication.Failback != "auto" {
		return nil
	}
	snapshot, err := s.ReadPlacementSnapshot(ctx, installation)
	if err != nil {
		return err
	}
	for _, name := range item.Spec.DevicePreference.Preferred {
		for _, node := range snapshot.Nodes {
			if node.ID.String() != name && (node.Policy == nil || node.Policy.Labels["synod.name"] != name) {
				continue
			}
			tx, err := s.pool.Begin(ctx)
			if err != nil {
				return err
			}
			ready, err := appFailbackReady(ctx, tx, installation, id, node.ID)
			_ = tx.Rollback(ctx)
			if err != nil {
				return err
			}
			if !ready {
				continue
			}
			_, err = s.AdmitAppAction(ctx, AppActionRequest{Identity: RequestIdentity{InstallationID: installation, ActorID: "controller", APIOperation: MoveAppAPIOperation, IdempotencyKey: fmt.Sprintf("failback:%s:%d", id, item.DesiredRevision)}, RequestID: uuid.NewString(), AppID: id, ExpectedRevision: item.DesiredRevision, TargetNodeID: node.ID, Automatic: true})
			return err
		}
	}
	return nil
}

// appFailbackReady requires an uninterrupted online period and a better preference rank.
// The move prepares a replica before its first stop command can run.
func appFailbackReady(ctx context.Context, tx pgx.Tx, installation, id, target uuid.UUID) (bool, error) {
	var specBytes, helloBytes []byte
	var writer uuid.UUID
	var stable bool
	err := tx.QueryRow(ctx, `SELECT a.spec,a.writer_node_id,
 a.tier=2 AND a.desired_lifecycle='running' AND a.spec->'replication'->>'failback'='auto'
 AND p.online_since+make_interval(secs=>(a.spec->'replication'->>'failback_stable_seconds')::double precision)<=clock_timestamp()
 AND p.last_seen_at>clock_timestamp()-interval '20 seconds' AND NOT p.recovery_required AND n.revoked_at IS NULL
 AND NOT EXISTS(SELECT 1 FROM operations WHERE installation_id=$1 AND app_id=$2 AND status IN ('pending','running','waiting','cancelling'))
 AND NOT EXISTS(SELECT 1 FROM operations WHERE installation_id=$1 AND app_id=$2 AND kind='app.move' AND status IN ('failed','cancelled') AND finished_at+make_interval(secs=>(a.spec->'replication'->>'failback_stable_seconds')::double precision)>clock_timestamp()),s.hello_bytes
 FROM apps a JOIN agent_presence p ON p.installation_id=a.installation_id AND p.node_id=$3
 JOIN nodes n ON n.installation_id=p.installation_id AND n.id=p.node_id
 JOIN agent_sessions s ON s.session_id=p.session_id
 WHERE a.installation_id=$1 AND a.id=$2 AND a.writer_epoch>0`, installation, id, target).Scan(&specBytes, &writer, &stable, &helloBytes)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, admissionError(err)
	}
	if !stable || writer == target {
		return false, nil
	}
	var spec app.AppSpec
	var hello agentv1.Hello
	if json.Unmarshal(specBytes, &spec) != nil || proto.Unmarshal(helloBytes, &hello) != nil {
		return false, operation.ErrCommandConflict
	}
	if !slices.Contains(hello.Capabilities, replication.Capability) || !slices.Contains(hello.Capabilities, "runtime.docker") {
		return false, nil
	}
	nodes, err := readPlacementNodes(ctx, tx, installation)
	if err != nil {
		return false, err
	}
	sourceRank, targetRank := len(spec.DevicePreference.Preferred), len(spec.DevicePreference.Preferred)
	for _, node := range nodes {
		if node.ID != target && node.ID != writer {
			continue
		}
		for rank, name := range spec.DevicePreference.Preferred {
			if name == node.ID.String() || (node.Policy != nil && name == node.Policy.Labels["synod.name"]) {
				if node.ID == target {
					targetRank = min(targetRank, rank)
				} else {
					sourceRank = min(sourceRank, rank)
				}
			}
		}
		if node.ID == target {
			if node.Policy == nil || node.Policy.Maintenance || node.Policy.Cordoned || node.Policy.ClientAddress == "" {
				return false, nil
			}
			for _, name := range spec.DevicePreference.Never {
				if name == node.ID.String() || name == node.Policy.Labels["synod.name"] {
					return false, nil
				}
			}
		}
	}
	return targetRank < sourceRank, nil
}

// AppDataRemovalReady waits for the current session to withdraw replication.
func (s *Store) AppDataRemovalReady(ctx context.Context, installation, id, node uuid.UUID) (bool, error) {
	var encoded []byte
	err := s.pool.QueryRow(ctx, `SELECT t.table_bytes FROM agent_replication t JOIN agent_presence p USING(installation_id,node_id,session_id)
 JOIN apps a ON a.installation_id=t.installation_id AND a.id=$2
 WHERE t.installation_id=$1 AND t.node_id=$3 AND a.desired_lifecycle='deleted' AND t.acknowledged_at IS NOT NULL AND t.failure_code='' AND NOT p.recovery_required`, installation, id, node).Scan(&encoded)
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
	return !slices.ContainsFunc(table.Apps, func(a *agentv1.AppReplication) bool { return a.AppId == id.String() }), nil
}

func (s *Store) IsAppMoveRollback(ctx context.Context, installation, operationID uuid.UUID) (bool, error) {
	var rollback bool
	if err := s.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM app_moves WHERE installation_id=$1 AND rollback_operation_id=$2 AND promoted_at IS NULL)`, installation, operationID).Scan(&rollback); err != nil {
		return false, admissionError(err)
	}
	return rollback, nil
}
