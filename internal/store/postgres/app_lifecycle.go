package postgres

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"strings"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/sirrobot01/synod/internal/app"
	"github.com/sirrobot01/synod/internal/operation"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/proto"
)

const (
	MoveAppAPIOperation   = "app.move"
	StopAppAPIOperation   = "app.stop"
	StartAppAPIOperation  = "app.start"
	DeleteAppAPIOperation = "app.delete"
)

type AppActionRequest struct {
	Automatic        bool
	RollbackMoveID   uuid.UUID
	Identity         RequestIdentity
	RequestID        string
	AppID            uuid.UUID
	ExpectedRevision int64
	TargetNodeID     uuid.UUID
}

// AdmitAppAction changes desired state and records its work in one transaction.
func (s *Store) AdmitAppAction(ctx context.Context, request AppActionRequest) (app.Admission, error) {
	identity := request.Identity
	target := ""
	switch identity.APIOperation {
	case StopAppAPIOperation:
		target = "stopped"
	case StartAppAPIOperation, MoveAppAPIOperation:
		target = "running"
	case DeleteAppAPIOperation:
		target = "deleted"
	}
	if (identity.APIOperation == MoveAppAPIOperation) != (request.TargetNodeID != uuid.Nil) || target == "" || identity.InstallationID == uuid.Nil || request.AppID == uuid.Nil || request.ExpectedRevision < 1 ||
		len(identity.ActorID) < 1 || len(identity.ActorID) > 256 || !utf8.ValidString(identity.ActorID) || strings.ContainsRune(identity.ActorID, 0) ||
		!printableWithin(identity.IdempotencyKey, 128) || !printableWithin(request.RequestID, 128) {
		return app.Admission{}, &AdmissionError{Code: InvalidRequest}
	}
	body, err := json.Marshal(struct {
		Action   string
		AppID    uuid.UUID
		Revision int64
		Target   uuid.UUID
	}{identity.APIOperation, request.AppID, request.ExpectedRevision, request.TargetNodeID})
	if err != nil {
		return app.Admission{}, err
	}
	hash := sha256.Sum256(body)
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return app.Admission{}, admissionError(err)
	}
	defer tx.Rollback(ctx)
	// The retry record is inserted before revision checks. It also resolves concurrent retries.
	result := app.Admission{AppID: request.AppID, OperationID: uuid.New(), DesiredRevision: request.ExpectedRevision + 1}
	err = tx.QueryRow(ctx, `INSERT INTO app_admissions
 (api_operation,installation_id,actor_id,idempotency_key,request_id,request_hash,app_id,operation_id,desired_revision,accepted_at)
 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,transaction_timestamp()) ON CONFLICT DO NOTHING RETURNING accepted_at`,
		identity.APIOperation, identity.InstallationID, identity.ActorID, identity.IdempotencyKey, request.RequestID, hash[:], request.AppID, result.OperationID, result.DesiredRevision).Scan(&result.AcceptedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		var stored []byte
		err = tx.QueryRow(ctx, `SELECT app_id,operation_id,desired_revision,accepted_at,request_hash FROM app_admissions
 WHERE installation_id=$1 AND actor_id=$2 AND api_operation=$3 AND idempotency_key=$4`, identity.InstallationID, identity.ActorID, identity.APIOperation, identity.IdempotencyKey).
			Scan(&result.AppID, &result.OperationID, &result.DesiredRevision, &result.AcceptedAt, &stored)
		if err != nil {
			return app.Admission{}, admissionError(err)
		}
		if !bytes.Equal(stored, hash[:]) {
			return app.Admission{}, &AdmissionError{Code: IdempotencyConflict, OperationID: result.OperationID}
		}
		result.Replayed = true
		return result, nil
	}
	if err != nil {
		return app.Admission{}, admissionError(err)
	}
	var revision int64
	var current string
	var deleted bool
	if err := tx.QueryRow(ctx, `SELECT desired_revision,desired_lifecycle,deleted_at IS NOT NULL FROM apps WHERE installation_id=$1 AND id=$2 FOR UPDATE`, identity.InstallationID, request.AppID).Scan(&revision, &current, &deleted); err != nil {
		return app.Admission{}, admissionError(err)
	}
	if deleted || (current == "deleted" && target != "deleted") {
		return app.Admission{}, &AdmissionError{Code: NotFound}
	}
	if revision != request.ExpectedRevision {
		return app.Admission{}, &AdmissionError{Code: RevisionConflict}
	}
	// Plan installation and dispatch lock the operation. Hold that lock before proving absence.
	if _, err := tx.Exec(ctx, `SELECT id FROM operations WHERE installation_id=$1 AND app_id=$2 ORDER BY id FOR UPDATE`, identity.InstallationID, request.AppID); err != nil {
		return app.Admission{}, admissionError(err)
	}
	var unused, active bool
	if err := tx.QueryRow(ctx, `SELECT
 NOT EXISTS(SELECT 1 FROM operations WHERE installation_id=$1 AND app_id=$2 AND plan_version>0)
 AND NOT EXISTS(SELECT 1 FROM command_dispatches WHERE installation_id=$1 AND cluster_id=$2)
 AND NOT EXISTS(SELECT 1 FROM resource_allocations WHERE installation_id=$1 AND cluster_id=$2)
 AND NOT EXISTS(SELECT 1 FROM app_copies WHERE installation_id=$1 AND app_id=$2),
 EXISTS(SELECT 1 FROM operations WHERE installation_id=$1 AND app_id=$2 AND status IN ('pending','running','waiting','cancelling'))`, identity.InstallationID, request.AppID).Scan(&unused, &active); err != nil {
		return app.Admission{}, admissionError(err)
	}
	if active {
		if !unused || target == "running" {
			return app.Admission{}, &AdmissionError{Code: OperationConflict}
		}
		if _, err := tx.Exec(ctx, `UPDATE operations SET status='cancelled',claim_active=false,claim_expires_at=CASE WHEN owner_id IS NOT NULL THEN clock_timestamp() END,finished_at=clock_timestamp(),updated_at=clock_timestamp()
 WHERE installation_id=$1 AND app_id=$2 AND status IN ('pending','running','waiting','cancelling')`, identity.InstallationID, request.AppID); err != nil {
			return app.Admission{}, admissionError(err)
		}
	}
	if identity.APIOperation == MoveAppAPIOperation {
		var eligible bool
		if err := tx.QueryRow(ctx, `SELECT tier=2 AND desired_lifecycle='running' AND writer_epoch>0 AND writer_epoch<9223372036854775807 AND writer_node_id<>$3
          AND EXISTS(SELECT 1 FROM nodes WHERE installation_id=$1 AND id=$3 AND revoked_at IS NULL)
          FROM apps WHERE installation_id=$1 AND id=$2`, identity.InstallationID, request.AppID, request.TargetNodeID).Scan(&eligible); err != nil {
			return app.Admission{}, admissionError(err)
		}
		if !eligible {
			return app.Admission{}, &AdmissionError{Code: InvalidRequest}
		}
	}
	if request.Automatic {
		if identity.APIOperation != MoveAppAPIOperation {
			return app.Admission{}, &AdmissionError{Code: InvalidRequest}
		}
		ready, err := appFailbackReady(ctx, tx, identity.InstallationID, request.AppID, request.TargetNodeID)
		if err != nil {
			return app.Admission{}, err
		}
		if !ready {
			return app.Admission{}, &AdmissionError{Code: OperationConflict}
		}
	}
	if request.RollbackMoveID != uuid.Nil {
		var targetNode uuid.UUID
		err := tx.QueryRow(ctx, `SELECT m.target_node_id FROM app_moves m JOIN operations o ON o.id=m.operation_id JOIN apps a ON a.installation_id=m.installation_id AND a.id=m.app_id
       WHERE m.operation_id=$1 AND m.installation_id=$2 AND m.app_id=$3 AND m.promoted_at IS NULL AND o.status IN ('failed','cancelled')
       AND a.writer_node_id=m.source_node_id AND a.writer_epoch=m.writer_epoch AND a.desired_revision=o.desired_revision`, request.RollbackMoveID, identity.InstallationID, request.AppID).Scan(&targetNode)
		if err != nil {
			return app.Admission{}, admissionError(err)
		}
		if identity.APIOperation != StartAppAPIOperation {
			return app.Admission{}, &AdmissionError{Code: InvalidRequest}
		}
		if err := releaseAppCompute(ctx, tx, identity.InstallationID, request.AppID, targetNode); err != nil {
			return app.Admission{}, err
		}
	}
	complete := unused && target != "running"
	if _, err := tx.Exec(ctx, `UPDATE apps SET desired_revision=$3,desired_lifecycle=$4,updated_at=clock_timestamp(),
 observed_revision=CASE WHEN $5 THEN $3 ELSE observed_revision END,
 observed_lifecycle=CASE WHEN $5 THEN $4 ELSE 'unknown' END,
 deleted_at=CASE WHEN $5 AND $4='deleted' THEN clock_timestamp() ELSE NULL END
 WHERE installation_id=$1 AND id=$2`, identity.InstallationID, request.AppID, result.DesiredRevision, target, complete); err != nil {
		return app.Admission{}, admissionError(err)
	}
	// Keep source health during move preparation. A restart requires new observations.
	if _, err := tx.Exec(ctx, `UPDATE app_copies SET desired_revision=$3,health=CASE WHEN $4 THEN health ELSE 'unknown' END,observed_at=CASE WHEN $4 THEN observed_at END,observed_session_id=CASE WHEN $4 THEN observed_session_id END,updated_at=clock_timestamp()
 WHERE installation_id=$1 AND app_id=$2 AND desired`, identity.InstallationID, request.AppID, result.DesiredRevision, identity.APIOperation == MoveAppAPIOperation); err != nil {
		return app.Admission{}, admissionError(err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO operations
 (id,installation_id,cluster_id,actor_id,api_operation,idempotency_key,request_hash,hash_version,kind,desired_revision,status,plan_version,execution_epoch,attempts,created_at,updated_at,finished_at)
 VALUES ($1,$2,$3,$4,$5,$6,$7,1,$5,$8,CASE WHEN $9 THEN 'succeeded' ELSE 'pending' END,0,0,0,clock_timestamp(),clock_timestamp(),CASE WHEN $9 THEN clock_timestamp() END)`,
		result.OperationID, identity.InstallationID, request.AppID, identity.ActorID, identity.APIOperation, identity.IdempotencyKey, hash[:], result.DesiredRevision, complete); err != nil {
		return app.Admission{}, admissionError(err)
	}
	if identity.APIOperation == MoveAppAPIOperation {
		if _, err := tx.Exec(ctx, `INSERT INTO app_moves(installation_id,app_id,operation_id,source_node_id,target_node_id,writer_epoch)
        SELECT installation_id,id,$3,writer_node_id,$4,writer_epoch FROM apps WHERE installation_id=$1 AND id=$2`, identity.InstallationID, request.AppID, result.OperationID, request.TargetNodeID); err != nil {
			return app.Admission{}, admissionError(err)
		}
	}
	if request.RollbackMoveID != uuid.Nil {
		if _, err := tx.Exec(ctx, `UPDATE app_moves SET rollback_operation_id=$2 WHERE operation_id=$1`, request.RollbackMoveID, result.OperationID); err != nil {
			return app.Admission{}, admissionError(err)
		}
	}
	if !complete {
		if _, err := tx.Exec(ctx, `INSERT INTO outbox(event_id,installation_id,cluster_id,operation_id,kind,schema_version,created_at) VALUES($1,$2,$3,$4,'operation.ready',1,clock_timestamp())`, uuid.New(), identity.InstallationID, request.AppID, result.OperationID); err != nil {
			return app.Admission{}, admissionError(err)
		}
	}
	if err := s.commitMetadata(ctx, tx); err != nil {
		return app.Admission{}, err
	}
	return result, nil
}

// ReadAppDeployment returns the immutable container configuration used for this app.
func (s *Store) ReadAppDeployment(ctx context.Context, installation, id uuid.UUID) (*agentv1.ExecutionPlan, error) {
	var encoded []byte
	err := s.pool.QueryRow(ctx, `SELECT plan_bytes FROM operations WHERE installation_id=$1 AND app_id=$2 AND kind IN ('app.deploy','app.start') AND plan_version>0 ORDER BY desired_revision LIMIT 1`, installation, id).Scan(&encoded)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, admissionError(err)
	}
	var plan agentv1.ExecutionPlan
	if err := proto.Unmarshal(encoded, &plan); err != nil {
		return nil, err
	}
	return &plan, nil
}

var ErrAppRoutesPending = errors.New("app_routes_pending")

// appRoutesWithdrawn requires the current session to confirm that each ingress is gone.
func appRoutesWithdrawn(ctx context.Context, tx pgx.Tx, installation, id uuid.UUID) error {
	rows, err := tx.Query(ctx, `SELECT DISTINCT a.node_id FROM resource_allocations a JOIN allocation_ports p
 ON p.installation_id=a.installation_id AND p.allocation_id=a.id
 WHERE a.installation_id=$1 AND a.cluster_id=$2 AND a.released_at IS NULL AND p.name='ingress' ORDER BY a.node_id`, installation, id)
	if err != nil {
		return admissionError(err)
	}
	nodes, err := pgx.CollectRows(rows, pgx.RowTo[uuid.UUID])
	if err != nil {
		return admissionError(err)
	}
	for _, node := range nodes {
		// Route publication and acknowledgements lock presence before the route row.
		var session uuid.UUID
		err := tx.QueryRow(ctx, `SELECT session_id FROM agent_presence WHERE installation_id=$1 AND node_id=$2 FOR SHARE`, installation, node).Scan(&session)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrAppRoutesPending
		}
		if err != nil {
			return admissionError(err)
		}
		var encoded []byte
		err = tx.QueryRow(ctx, `SELECT table_bytes FROM agent_routes
 WHERE installation_id=$1 AND node_id=$2 AND session_id=$3 AND acknowledged_at IS NOT NULL AND failure_code='' FOR SHARE`, installation, node, session).Scan(&encoded)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrAppRoutesPending
		}
		if err != nil {
			return admissionError(err)
		}
		var table agentv1.RouteTable
		if err := proto.Unmarshal(encoded, &table); err != nil {
			return err
		}
		for _, route := range table.Routes {
			if route.AppId == id.String() {
				return ErrAppRoutesPending
			}
		}
	}
	return nil
}

// completeAppLifecycle runs after all command receipts pass validation.
func completeAppLifecycle(ctx context.Context, tx pgx.Tx, claim operation.Claim, op operation.Operation, plan *agentv1.ExecutionPlan, target string) error {
	postcondition := agentv1.Postcondition_POSTCONDITION_CONTAINER_RUNNING
	switch op.Kind {
	case "app.deploy", "app.start", "app.failover", "app.move":
		if target != "running" {
			return operation.ErrCommandConflict
		}
	case "app.stop":
		if target != "stopped" {
			return operation.ErrCommandConflict
		}
		postcondition = agentv1.Postcondition_POSTCONDITION_CONTAINER_STOPPED
	case "app.delete":
		if target != "deleted" {
			return operation.ErrCommandConflict
		}
		postcondition = agentv1.Postcondition_POSTCONDITION_CONTAINER_ABSENT
	default:
		return operation.ErrCommandConflict
	}
	rows, err := tx.Query(ctx, `SELECT id,node_id FROM app_copies WHERE installation_id=$1 AND app_id=$2 AND ($3 OR (desired AND role<>'standby')) ORDER BY id`, claim.InstallationID, claim.ClusterID, target == "deleted")
	if err != nil {
		return admissionError(err)
	}
	type copyIdentity struct{ ID, Node uuid.UUID }
	copies, err := pgx.CollectRows(rows, pgx.RowToStructByPos[copyIdentity])
	if err != nil {
		return admissionError(err)
	}
	if len(copies) == 0 {
		return operation.ErrPostconditionUnproved
	}
	for _, copy := range copies {
		var last *agentv1.PlanStep
		for _, step := range plan.Steps {
			if step.Command.Input.GetRemoveAppData() != nil {
				continue
			}
			if step.Command.Input.ResourceId != copy.ID.String() || step.Command.Epoch.NodeId != copy.Node.String() {
				continue
			}
			// Each copy's commands must form one ordered chain.
			if last != nil {
				ordered := false
				for _, dependency := range step.DependsOn {
					if dependency == last.Command.StepId {
						ordered = true
					}
				}
				if !ordered {
					return operation.ErrPostconditionUnproved
				}
			}
			last = step
		}
		if last == nil || last.Postcondition != postcondition {
			return operation.ErrPostconditionUnproved
		}
	}
	if target == "deleted" {
		var tier int
		if err := tx.QueryRow(ctx, `SELECT tier FROM apps WHERE installation_id=$1 AND id=$2`, claim.InstallationID, claim.ClusterID).Scan(&tier); err != nil {
			return admissionError(err)
		}
		if tier == 2 {
			for _, copy := range copies {
				proved := false
				for _, step := range plan.Steps {
					if step.Postcondition == agentv1.Postcondition_POSTCONDITION_APP_DATA_ABSENT && step.Command.Epoch.NodeId == copy.Node.String() {
						proved = true
					}
				}
				if !proved {
					return operation.ErrPostconditionUnproved
				}
			}
		}
	}
	if target != "running" {
		if err := appRoutesWithdrawn(ctx, tx, claim.InstallationID, claim.ClusterID); err != nil {
			return err
		}
	}
	if target == "running" {
		// An observation from before the start receipt is not current health.
		if _, err := tx.Exec(ctx, `UPDATE app_copies SET health='unknown',observed_state='pending',observed_at=NULL,observed_session_id=NULL,updated_at=clock_timestamp() WHERE installation_id=$1 AND app_id=$2 AND desired`, claim.InstallationID, claim.ClusterID); err != nil {
			return admissionError(err)
		}
	}
	if target == "stopped" {
		if _, err := tx.Exec(ctx, `UPDATE app_copies SET health='unknown',observed_state='stopped',observed_at=NULL,observed_session_id=NULL,updated_at=clock_timestamp() WHERE installation_id=$1 AND app_id=$2 AND desired`, claim.InstallationID, claim.ClusterID); err != nil {
			return admissionError(err)
		}
	}
	if target == "deleted" {
		if _, err := tx.Exec(ctx, `UPDATE allocation_ports SET released_at=clock_timestamp() WHERE installation_id=$1 AND cluster_id=$2 AND released_at IS NULL`, claim.InstallationID, claim.ClusterID); err != nil {
			return admissionError(err)
		}
		if _, err := tx.Exec(ctx, `UPDATE resource_allocations SET released_at=clock_timestamp() WHERE installation_id=$1 AND cluster_id=$2 AND released_at IS NULL`, claim.InstallationID, claim.ClusterID); err != nil {
			return admissionError(err)
		}
		if _, err := tx.Exec(ctx, `UPDATE app_copies SET desired=false,health='unknown',updated_at=clock_timestamp() WHERE installation_id=$1 AND app_id=$2`, claim.InstallationID, claim.ClusterID); err != nil {
			return admissionError(err)
		}
	}
	_, err = tx.Exec(ctx, `UPDATE apps SET observed_revision=$3,observed_lifecycle=$4,updated_at=clock_timestamp(),deleted_at=CASE WHEN $4='deleted' THEN clock_timestamp() ELSE NULL END WHERE installation_id=$1 AND id=$2`, claim.InstallationID, claim.ClusterID, op.DesiredRevision, target)
	if err != nil {
		return admissionError(err)
	}
	return nil
}
