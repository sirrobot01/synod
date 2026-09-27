package postgres

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/sirrobot01/synod/internal/cluster"
)

// The lifecycle actions this version admits.
const (
	StopClusterAPIOperation   = "clusters.stop.v1"
	StartClusterAPIOperation  = "clusters.start.v1"
	DeleteClusterAPIOperation = "clusters.delete.v1"
	ScaleClusterAPIOperation  = "clusters.scale.v1"
	RecoverSetupAPIOperation  = "clusters.recover_setup.v1"
)

// ClusterActionRequest asks for one lifecycle change to an existing cluster.
type ClusterActionRequest struct {
	Identity  RequestIdentity
	RequestID string
	ClusterID uuid.UUID
	// ExpectedRevision is the desired revision this action replaces. A caller that
	// has not read the cluster has not seen what it is changing.
	ExpectedRevision int64
	Confirmation     *cluster.ActionConfirmation
	// Replicas is how many replicas the cluster should run beside its primary. Only a
	// scale carries it. A nil value leaves the count where it is.
	Replicas *int
}

// AdmitClusterAction records one lifecycle change and the work that carries it out.
//
// It moves desired state and admits an operation in one transaction. The unique index
// on unfinished operations is what stops a second action starting while one is still
// running, so a stop and a delete cannot race.
//
// Nothing here touches a host. A successful return is an admission, not an effect.
func (s *Store) AdmitClusterAction(ctx context.Context, request ClusterActionRequest) (Admission, error) {
	lifecycle, kind, err := actionOf(request.Identity.APIOperation)
	if err != nil {
		return Admission{}, err
	}
	if err := validActionRequest(request); err != nil {
		return Admission{}, err
	}
	hash, err := actionHash(request)
	if err != nil {
		return Admission{}, err
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return Admission{}, admissionError(err)
	}
	defer tx.Rollback(ctx)
	if replay, err := replayedAction(ctx, tx, request.Identity, hash); err == nil {
		return replay, nil
	} else if !errors.Is(err, pgx.ErrNoRows) {
		return Admission{}, err
	}
	confirmed := request.Identity.APIOperation == DeleteClusterAPIOperation || request.Identity.APIOperation == RecoverSetupAPIOperation ||
		(request.Identity.APIOperation == ScaleClusterAPIOperation && request.Replicas != nil && *request.Replicas == 0)
	if confirmed {
		if request.Confirmation == nil || request.Confirmation.PlanToken == uuid.Nil || !request.Confirmation.Confirm || request.Confirmation.DataDisposition != "retain" {
			return Admission{}, &AdmissionError{Code: InvalidRequest}
		}
	} else if request.Confirmation != nil {
		return Admission{}, &AdmissionError{Code: InvalidRequest}
	}
	// Lock the cluster before reading its revision, so two actions cannot both read
	// the same one and both believe they are the next.
	var revision int64
	var current string
	var replicas int
	var deleted *string
	var engine string
	if err := tx.QueryRow(ctx, `SELECT desired_revision,desired_lifecycle,desired_replicas,deleted_at::text,engine FROM clusters
 WHERE installation_id=$1 AND id=$2 FOR UPDATE`,
		request.Identity.InstallationID, request.ClusterID).Scan(&revision, &current, &replicas, &deleted, &engine); err != nil {
		return Admission{}, admissionError(err)
	}
	operationID, err := uuid.NewRandom()
	if err != nil {
		return Admission{}, admissionError(err)
	}
	next := revision + 1
	result := Admission{ClusterID: request.ClusterID, OperationID: operationID, DesiredRevision: next}
	identity := request.Identity
	err = tx.QueryRow(ctx, `INSERT INTO idempotency_results
 (installation_id,actor_id,api_operation,idempotency_key,request_hash,hash_version,cluster_id,operation_id,desired_revision,accepted_at)
 VALUES ($1,$2,$3,$4,$5,1,$6,$7,$8,transaction_timestamp())
 ON CONFLICT DO NOTHING RETURNING accepted_at`,
		identity.InstallationID, identity.ActorID, identity.APIOperation, identity.IdempotencyKey,
		hash[:], request.ClusterID, operationID, next).Scan(&result.AcceptedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return replayedAction(ctx, tx, identity, hash)
	}
	if err != nil {
		return Admission{}, admissionError(err)
	}
	// The expected revision is checked after the replay, so a retry of an accepted
	// action still replays rather than failing on a revision it already moved.
	if deleted != nil || current == string(cluster.DesiredDeleted) {
		return Admission{}, &AdmissionError{Code: NotFound}
	}
	if request.ExpectedRevision != revision {
		return Admission{}, &AdmissionError{Code: RevisionConflict}
	}
	// Only Redis scales native replicas, and it runs one. A scale acts on a running cluster,
	// because scaling a stopped one would plan work for an engine that is not there.
	if request.Identity.APIOperation == ScaleClusterAPIOperation {
		if engine != string(cluster.Redis) || *request.Replicas > 1 {
			return Admission{}, &AdmissionError{Code: UnsupportedSpec}
		}
		if current != string(cluster.DesiredRunning) {
			return Admission{}, &AdmissionError{Code: OperationConflict}
		}
	}
	var active bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM operations WHERE installation_id=$1 AND cluster_id=$2 AND status IN ('pending','running','waiting','cancelling'))`, identity.InstallationID, request.ClusterID).Scan(&active); err != nil {
		return Admission{}, admissionError(err)
	}
	if active {
		return Admission{}, &AdmissionError{Code: OperationConflict}
	}
	var retryOf *uuid.UUID
	// A Redis scale to the count it already asks for retries the failed add or removal.
	if request.Identity.APIOperation == ScaleClusterAPIOperation && *request.Replicas == replicas {
		err := tx.QueryRow(ctx, `SELECT o.id FROM operations o
 JOIN clusters c ON c.installation_id=o.installation_id AND c.id=o.cluster_id
 WHERE o.installation_id=$1 AND o.cluster_id=$2 AND o.desired_revision=$3
 AND o.kind='cluster.scale' AND o.status IN ('failed','cancelled') AND c.engine='redis'
 AND ($4=1 OR EXISTS(SELECT 1 FROM cluster_members m WHERE m.installation_id=c.installation_id AND m.cluster_id=c.id AND m.role='replica' AND m.removed_at IS NULL))`,
			identity.InstallationID, request.ClusterID, revision, replicas).Scan(&retryOf)
		if errors.Is(err, pgx.ErrNoRows) {
			return Admission{}, &AdmissionError{Code: InvalidRequest}
		}
		if err != nil {
			return Admission{}, admissionError(err)
		}
	}
	var actionPlanToken *uuid.UUID
	if request.Confirmation != nil {
		actionPlanToken = &request.Confirmation.PlanToken
		action := "delete"
		if request.Identity.APIOperation == ScaleClusterAPIOperation {
			action = "scale_down"
		}
		if request.Identity.APIOperation == RecoverSetupAPIOperation {
			action = "recover_setup"
		}
		var planned []uuid.UUID
		var member, resource *uuid.UUID
		err := tx.QueryRow(ctx, `SELECT retained_volumes,member_id,resource_id,action FROM cluster_action_plans
 WHERE plan_token=$1 AND installation_id=$2 AND actor_id=$3 AND cluster_id=$4
 AND expected_revision=$5 AND action=$6 AND data_disposition='retain' AND expires_at>clock_timestamp()`,
			request.Confirmation.PlanToken, identity.InstallationID, identity.ActorID, request.ClusterID, revision, action).Scan(&planned, &member, &resource, &action)
		if errors.Is(err, pgx.ErrNoRows) {
			return Admission{}, &AdmissionError{Code: InvalidRequest}
		}
		if err != nil {
			return Admission{}, admissionError(err)
		}
		volumes, err := retainedVolumeIDs(ctx, tx, identity.InstallationID, request.ClusterID)
		if err != nil {
			return Admission{}, err
		}
		if action == "scale_down" {
			// The plan named one member. It must still be a live replica, so a review
			// of a membership that has since moved cannot authorize this removal.
			var targetResource uuid.UUID
			err := tx.QueryRow(ctx, `SELECT resource_id FROM cluster_members
 WHERE installation_id=$1 AND cluster_id=$2 AND member_id=$3 AND role='replica' AND removed_at IS NULL`,
				identity.InstallationID, request.ClusterID, member).Scan(&targetResource)
			if errors.Is(err, pgx.ErrNoRows) {
				return Admission{}, &AdmissionError{Code: OperationConflict}
			}
			if err != nil {
				return Admission{}, admissionError(err)
			}
			owned, err := memberVolumeIDs(ctx, tx, identity.InstallationID, request.ClusterID, targetResource)
			if err != nil {
				return Admission{}, err
			}
			if !slices.Equal(planned, owned) {
				return Admission{}, &AdmissionError{Code: OperationConflict}
			}
		} else if action == "recover_setup" {
			targetMember, targetResource, targetVolumes, err := setupRecoveryTarget(ctx, tx, identity.InstallationID, request.ClusterID)
			if err != nil {
				return Admission{}, err
			}
			if member == nil || resource == nil || *member != targetMember || *resource != targetResource || !slices.Equal(planned, targetVolumes) {
				return Admission{}, &AdmissionError{Code: OperationConflict}
			}
		} else if !slices.Equal(planned, volumes) {
			return Admission{}, &AdmissionError{Code: OperationConflict}
		}
	}
	var desired *string
	if lifecycle != "" {
		moved := string(lifecycle)
		desired = &moved
	}
	desiredReplicas := request.Replicas
	if _, err := tx.Exec(ctx, `UPDATE clusters SET desired_revision=$1,
 desired_lifecycle=COALESCE($2,desired_lifecycle),
 desired_replicas=COALESCE($3,desired_replicas),
 updated_at=transaction_timestamp()
 WHERE installation_id=$4 AND id=$5`, next, desired, desiredReplicas, identity.InstallationID, request.ClusterID); err != nil {
		return Admission{}, admissionError(err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO operations
 (id,installation_id,cluster_id,actor_id,api_operation,idempotency_key,request_hash,hash_version,kind,desired_revision,status,plan_version,execution_epoch,attempts,created_at,updated_at,retry_of,action_plan_token)
 VALUES ($1,$2,$3,$4,$5,$6,$7,1,$8,$9,'pending',0,0,0,transaction_timestamp(),transaction_timestamp(),$10,$11)`,
		operationID, identity.InstallationID, request.ClusterID, identity.ActorID,
		identity.APIOperation, identity.IdempotencyKey, hash[:], kind, next, retryOf, actionPlanToken); err != nil {
		return Admission{}, admissionError(err)
	}
	eventID, err := uuid.NewRandom()
	if err != nil {
		return Admission{}, admissionError(err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO outbox (event_id,installation_id,cluster_id,operation_id,kind,schema_version,created_at)
 VALUES ($1,$2,$3,$4,'operation.ready',1,transaction_timestamp())`,
		eventID, identity.InstallationID, request.ClusterID, operationID); err != nil {
		return Admission{}, admissionError(err)
	}
	auditID, err := uuid.NewRandom()
	if err != nil {
		return Admission{}, admissionError(err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO audit_events (id,installation_id,cluster_id,operation_id,actor_id,request_id,action,outcome,created_at)
 VALUES ($1,$2,$3,$4,$5,$6,$7,'accepted',transaction_timestamp())`,
		auditID, identity.InstallationID, request.ClusterID, operationID, identity.ActorID,
		request.RequestID, kind+".accepted"); err != nil {
		return Admission{}, admissionError(err)
	}
	if err := s.commitMetadata(ctx, tx); err != nil {
		return Admission{}, err
	}
	return result, nil
}

// replayedAction answers a repeat of an action that was already accepted.
func replayedAction(ctx context.Context, tx pgx.Tx, identity RequestIdentity, hash [32]byte) (Admission, error) {
	var result Admission
	var storedHash []byte
	var hashVersion int
	if err := tx.QueryRow(ctx, `SELECT cluster_id,operation_id,desired_revision,accepted_at,request_hash,hash_version
 FROM idempotency_results WHERE installation_id=$1 AND actor_id=$2 AND api_operation=$3 AND idempotency_key=$4`,
		identity.InstallationID, identity.ActorID, identity.APIOperation, identity.IdempotencyKey).
		Scan(&result.ClusterID, &result.OperationID, &result.DesiredRevision, &result.AcceptedAt, &storedHash, &hashVersion); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return Admission{}, err
		}
		return Admission{}, &AdmissionError{Code: DependencyUnavailable, Cause: err}
	}
	if hashVersion != 1 || !bytes.Equal(storedHash, hash[:]) {
		return Admission{}, &AdmissionError{Code: IdempotencyConflict, OperationID: result.OperationID}
	}
	result.Replayed = true
	return result, nil
}

// actionOf maps one API operation to the lifecycle it asks for and the work kind.
func actionOf(apiOperation string) (lifecycle cluster.DesiredLifecycle, kind string, err error) {
	switch apiOperation {
	case StopClusterAPIOperation:
		return cluster.DesiredStopped, "cluster.stop", nil
	case StartClusterAPIOperation:
		return cluster.DesiredRunning, "cluster.start", nil
	case DeleteClusterAPIOperation:
		return cluster.DesiredDeleted, "cluster.delete", nil
	case RecoverSetupAPIOperation:
		return cluster.DesiredRunning, "cluster.recover_setup", nil
	case ScaleClusterAPIOperation:
		// A scale changes the member count. It does not move the lifecycle, so it
		// names no lifecycle to write.
		return "", "cluster.scale", nil
	}
	return "", "", &AdmissionError{Code: InvalidRequest}
}

// actionHash fixes the replay identity of one action. It covers what was asked for,
// so a repeat that asks for something else is a conflict rather than a replay.
func actionHash(request ClusterActionRequest) ([32]byte, error) {
	body, err := json.Marshal(struct {
		APIOperation string                      `json:"api_operation"`
		ClusterID    uuid.UUID                   `json:"cluster_id"`
		Expected     int64                       `json:"expected_revision"`
		Confirmation *cluster.ActionConfirmation `json:"confirmation,omitempty"`
		Replicas     *int                        `json:"replicas,omitempty"`
	}{request.Identity.APIOperation, request.ClusterID, request.ExpectedRevision, request.Confirmation, request.Replicas})
	if err != nil {
		return [32]byte{}, &AdmissionError{Code: InvalidRequest, Cause: err}
	}
	return sha256.Sum256(body), nil
}

func validActionRequest(request ClusterActionRequest) error {
	identity := request.Identity
	if identity.InstallationID == uuid.Nil || request.ClusterID == uuid.Nil || request.ExpectedRevision <= 0 ||
		len(identity.ActorID) < 1 || len(identity.ActorID) > 256 || !utf8.ValidString(identity.ActorID) ||
		strings.ContainsRune(identity.ActorID, 0) ||
		!printableWithin(identity.IdempotencyKey, 128) || !printableWithin(request.RequestID, 128) {
		return &AdmissionError{Code: InvalidRequest}
	}
	// Only a scale carries a replica count. Redis runs none or one.
	if (identity.APIOperation == ScaleClusterAPIOperation) != (request.Replicas != nil) {
		return &AdmissionError{Code: InvalidRequest}
	}
	if request.Replicas != nil && (*request.Replicas < 0 || *request.Replicas > 1) {
		return &AdmissionError{Code: InvalidRequest}
	}
	return nil
}

// printableWithin accepts the visible ASCII the admission columns constrain.
func printableWithin(value string, limit int) bool {
	if len(value) < 1 || len(value) > limit {
		return false
	}
	return !strings.ContainsFunc(value, func(r rune) bool { return r < '!' || r > '~' })
}
