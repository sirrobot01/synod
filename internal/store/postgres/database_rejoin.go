package postgres

import (
	"context"
	"crypto/sha256"
	"errors"
	"math"
	"slices"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/sirrobot01/synod/internal/operation"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/proto"
)

// DatabaseRejoin records one standby's rebuild or restart.
type DatabaseRejoin struct {
	CopyID      uuid.UUID
	NodeID      uuid.UUID
	WriterEpoch uint64
	RemoveData  bool
}

func (s *Store) GetDatabaseRejoin(ctx context.Context, installation, id uuid.UUID) (DatabaseRejoin, error) {
	var rejoin DatabaseRejoin
	err := s.pool.QueryRow(ctx, `SELECT r.copy_id,c.node_id,r.writer_epoch,r.remove_data FROM app_rejoins r JOIN app_copies c ON c.id=r.copy_id WHERE r.installation_id=$1 AND r.operation_id=$2`, installation, id).Scan(&rejoin.CopyID, &rejoin.NodeID, &rejoin.WriterEpoch, &rejoin.RemoveData)
	if err != nil {
		return rejoin, admissionError(err)
	}
	return rejoin, nil
}

// AdmitDatabaseRejoin keeps the current writer available while one standby returns.
func (s *Store) AdmitDatabaseRejoin(ctx context.Context, installation, id uuid.UUID) (uuid.UUID, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return uuid.Nil, admissionError(err)
	}
	defer tx.Rollback(ctx)
	var revision, epoch int64
	var eligible bool
	if err := tx.QueryRow(ctx, `SELECT desired_revision,writer_epoch,tier=1 AND desired_lifecycle='running' AND COALESCE(writer_last_grant_at>clock_timestamp()-interval '20 seconds',false) FROM apps WHERE installation_id=$1 AND id=$2 FOR UPDATE`, installation, id).Scan(&revision, &epoch, &eligible); err != nil {
		return uuid.Nil, admissionError(err)
	}
	if !eligible || revision == math.MaxInt64 {
		return uuid.Nil, nil
	}
	var active bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM operations WHERE installation_id=$1 AND app_id=$2 AND status IN ('pending','running','waiting','cancelling'))`, installation, id).Scan(&active); err != nil {
		return uuid.Nil, admissionError(err)
	}
	if active {
		return uuid.Nil, nil
	}
	var copyID uuid.UUID
	var remove bool
	err = tx.QueryRow(ctx, `SELECT c.id,c.rebuild_required AND NOT c.rebuild_data_removed FROM app_copies c JOIN agent_presence p USING(installation_id,node_id)
 JOIN nodes n ON n.installation_id=c.installation_id AND n.id=c.node_id
 WHERE c.installation_id=$1 AND c.app_id=$2 AND c.desired AND c.role='standby'
 AND (c.rebuild_required OR (c.observed_state='stopped' AND c.observed_session_id=p.session_id AND c.observed_at>clock_timestamp()-interval '20 seconds'))
 AND p.last_seen_at>clock_timestamp()-interval '20 seconds' AND NOT p.recovery_required AND n.revoked_at IS NULL
 AND NOT EXISTS(SELECT 1 FROM app_rejoins r JOIN operations o ON o.id=r.operation_id WHERE r.copy_id=c.id AND o.finished_at>clock_timestamp()-interval '10 seconds' AND o.status IN ('failed','cancelled'))
 ORDER BY c.copy_index LIMIT 1`, installation, id).Scan(&copyID, &remove)
	if errors.Is(err, pgx.ErrNoRows) {
		return uuid.Nil, nil
	}
	if err != nil {
		return uuid.Nil, admissionError(err)
	}
	pending := uuid.New()
	hash := sha256.Sum256([]byte(pending.String()))
	if _, err := tx.Exec(ctx, `UPDATE apps SET desired_revision=desired_revision+1,updated_at=clock_timestamp() WHERE installation_id=$1 AND id=$2`, installation, id); err != nil {
		return uuid.Nil, admissionError(err)
	}
	if _, err := tx.Exec(ctx, `UPDATE app_copies SET desired_revision=$3,updated_at=clock_timestamp() WHERE installation_id=$1 AND app_id=$2 AND desired`, installation, id, revision+1); err != nil {
		return uuid.Nil, admissionError(err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO operations(id,installation_id,app_id,actor_id,api_operation,idempotency_key,request_hash,hash_version,kind,desired_revision,status,plan_version,execution_epoch,attempts,created_at,updated_at)
 VALUES($1,$2,$3,'controller','app.rejoin',$4,$5,1,'app.rejoin',$6,'pending',0,0,0,clock_timestamp(),clock_timestamp())`, pending, installation, id, pending.String(), hash[:], revision+1); err != nil {
		return uuid.Nil, admissionError(err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO app_rejoins(installation_id,app_id,operation_id,copy_id,writer_epoch,remove_data) VALUES($1,$2,$3,$4,$5,$6)`, installation, id, pending, copyID, epoch, remove); err != nil {
		return uuid.Nil, admissionError(err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO outbox(event_id,installation_id,app_id,operation_id,kind,schema_version,created_at) VALUES($1,$2,$3,$4,'operation.ready',1,clock_timestamp())`, uuid.New(), installation, id, pending); err != nil {
		return uuid.Nil, admissionError(err)
	}
	if err := s.commitMetadata(ctx, tx); err != nil {
		return uuid.Nil, err
	}
	return pending, nil
}

// PrepareDatabaseRejoinStep publishes the standby only after data removal is proved.
func (s *Store) PrepareDatabaseRejoinStep(ctx context.Context, claim operation.Claim, stepID uuid.UUID) (bool, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, admissionError(err)
	}
	defer tx.Rollback(ctx)
	var epoch uint64
	var revision int64
	var writer uuid.UUID
	var lifecycle string
	if err := tx.QueryRow(ctx, `SELECT writer_epoch,desired_revision,writer_node_id,desired_lifecycle FROM apps WHERE installation_id=$1 AND id=$2 FOR UPDATE`, claim.InstallationID, claim.AppID).Scan(&epoch, &revision, &writer, &lifecycle); err != nil {
		return false, admissionError(err)
	}
	_, status, encoded, err := lockClaim(ctx, tx, claim)
	if err != nil {
		return false, err
	}
	if status == operation.Cancelling || status == operation.Cancelled || status == operation.Failed || status == operation.Succeeded {
		return false, operation.ErrStaleOwner
	}
	var rejoin DatabaseRejoin
	var removed bool
	err = tx.QueryRow(ctx, `SELECT r.copy_id,c.node_id,r.writer_epoch,r.remove_data,c.rebuild_data_removed FROM app_rejoins r JOIN app_copies c ON c.id=r.copy_id WHERE r.operation_id=$1 AND c.desired AND c.role='standby'`, claim.OperationID).Scan(&rejoin.CopyID, &rejoin.NodeID, &rejoin.WriterEpoch, &rejoin.RemoveData, &removed)
	if err != nil {
		return false, admissionError(err)
	}
	if lifecycle != "running" || epoch != rejoin.WriterEpoch || writer == rejoin.NodeID {
		return false, operation.ErrCommandConflict
	}
	var plan agentv1.ExecutionPlan
	if proto.Unmarshal(encoded, &plan) != nil {
		return false, operation.ErrCommandConflict
	}
	expected := 2
	if rejoin.RemoveData {
		expected = 5
	}
	if len(plan.Steps) != expected {
		return false, operation.ErrCommandConflict
	}
	for _, step := range plan.Steps {
		if step.Command.DesiredRevision != revision || step.Command.Input.ResourceId != rejoin.CopyID.String() || step.Command.Epoch.NodeId != rejoin.NodeID.String() {
			return false, operation.ErrCommandConflict
		}
	}
	index := slices.IndexFunc(plan.Steps, func(step *agentv1.PlanStep) bool { return step.Command.StepId == stepID.String() })
	if index < 0 {
		return false, operation.ErrCommandConflict
	}
	if !rejoin.RemoveData {
		return true, nil
	}
	if plan.Steps[0].Command.Input.GetStopContainer() == nil || plan.Steps[1].Command.Input.GetRemoveContainer() == nil || plan.Steps[2].Command.Input.GetRemoveAppData().GetWriterEpoch() != epoch {
		return false, operation.ErrCommandConflict
	}
	if index >= 3 {
		if !removed {
			if err := checkPlanCompletions(ctx, tx, operation.Operation{ID: claim.OperationID, InstallationID: claim.InstallationID, AppID: claim.AppID}, &agentv1.ExecutionPlan{Steps: plan.Steps[:3]}); err != nil {
				return false, err
			}
			if _, err := tx.Exec(ctx, `UPDATE app_copies SET rebuild_data_removed=true WHERE id=$1 AND rebuild_required`, rejoin.CopyID); err != nil {
				return false, admissionError(err)
			}
			if _, err := tx.Exec(ctx, `DELETE FROM app_replicas WHERE installation_id=$1 AND app_id=$2 AND node_id=$3`, claim.InstallationID, claim.AppID, rejoin.NodeID); err != nil {
				return false, admissionError(err)
			}
			if err := s.commitMetadata(ctx, tx); err != nil {
				return false, err
			}
		}
		return true, nil
	}
	if removed {
		return false, operation.ErrCommandConflict
	}
	var tableBytes []byte
	err = tx.QueryRow(ctx, `SELECT t.table_bytes FROM agent_replication t JOIN agent_presence p USING(installation_id,node_id,session_id)
 WHERE t.installation_id=$1 AND t.node_id=$2 AND t.acknowledged_at IS NOT NULL AND t.failure_code='' AND NOT p.recovery_required
 AND p.last_seen_at>clock_timestamp()-interval '20 seconds' FOR SHARE OF p`, claim.InstallationID, rejoin.NodeID).Scan(&tableBytes)
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
	return !slices.ContainsFunc(table.Apps, func(a *agentv1.AppReplication) bool { return a.AppId == claim.AppID.String() }), nil
}

func completeDatabaseRejoin(ctx context.Context, tx pgx.Tx, claim operation.Claim, op operation.Operation, plan *agentv1.ExecutionPlan, target string) error {
	if target != "running" {
		return operation.ErrCommandConflict
	}
	var copyID, node uuid.UUID
	var removed, remove bool
	if err := tx.QueryRow(ctx, `SELECT c.id,c.node_id,c.rebuild_data_removed,r.remove_data FROM app_rejoins r JOIN app_copies c ON c.id=r.copy_id
 JOIN apps a ON a.installation_id=r.installation_id AND a.id=r.app_id
 WHERE r.operation_id=$1 AND r.writer_epoch=a.writer_epoch AND c.desired AND c.role='standby' AND c.node_id<>a.writer_node_id`, claim.OperationID).Scan(&copyID, &node, &removed, &remove); err != nil {
		return admissionError(err)
	}
	if remove && !removed {
		return operation.ErrPostconditionUnproved
	}
	last := plan.Steps[len(plan.Steps)-1]
	if last.Postcondition != agentv1.Postcondition_POSTCONDITION_CONTAINER_RUNNING || last.Command.Input.ResourceId != copyID.String() || last.Command.Epoch.NodeId != node.String() {
		return operation.ErrPostconditionUnproved
	}
	if _, err := tx.Exec(ctx, `UPDATE app_copies SET rebuild_required=false,rebuild_data_removed=false,health='unknown',observed_state='pending',observed_at=NULL,observed_session_id=NULL,updated_at=clock_timestamp() WHERE id=$1`, copyID); err != nil {
		return admissionError(err)
	}
	if _, err := tx.Exec(ctx, `UPDATE apps SET observed_revision=$3,updated_at=clock_timestamp() WHERE installation_id=$1 AND id=$2`, claim.InstallationID, claim.AppID, op.DesiredRevision); err != nil {
		return admissionError(err)
	}
	return nil
}

// DatabaseRebuildNodes names copies that must lose old data before they can start.
func (s *Store) DatabaseRebuildNodes(ctx context.Context, installation, id uuid.UUID) ([]uuid.UUID, error) {
	rows, err := s.pool.Query(ctx, `SELECT node_id FROM app_copies WHERE installation_id=$1 AND app_id=$2 AND desired AND rebuild_required AND NOT rebuild_data_removed`, installation, id)
	if err != nil {
		return nil, admissionError(err)
	}
	return pgx.CollectRows(rows, pgx.RowTo[uuid.UUID])
}
