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
	StopClusterAPIOperation    = "clusters.stop.v1"
	StartClusterAPIOperation   = "clusters.start.v1"
	DeleteClusterAPIOperation  = "clusters.delete.v1"
	ScaleClusterAPIOperation   = "clusters.scale.v1"
	RecoverSetupAPIOperation   = "clusters.recover_setup.v1"
	RebuildReplicaAPIOperation = "clusters.rebuild_replica.v1"
	RebuildMemberAPIOperation  = "clusters.rebuild_member.v1"
	// RenewCertificatesAPIOperation renews the native certificates of a Patroni cluster.
	// The controller admits it on its own before expiry. An operator can also ask for it.
	RenewCertificatesAPIOperation = "clusters.renew_certificates.v1"
	// SwitchoverAPIOperation moves the Patroni leader to a named member.
	SwitchoverAPIOperation = "clusters.switchover.v1"
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
	// MemberID names the member that a switchover makes the leader. Only a switchover carries it.
	MemberID uuid.UUID
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
	confirmed := request.Identity.APIOperation == DeleteClusterAPIOperation || request.Identity.APIOperation == RecoverSetupAPIOperation || request.Identity.APIOperation == RebuildReplicaAPIOperation ||
		request.Identity.APIOperation == RebuildMemberAPIOperation ||
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
	var topology string
	if err := tx.QueryRow(ctx, `SELECT desired_revision,desired_lifecycle,desired_replicas,deleted_at::text,COALESCE(spec->'postgresql'->>'topology','') FROM clusters
 WHERE installation_id=$1 AND id=$2 FOR UPDATE`,
		request.Identity.InstallationID, request.ClusterID).Scan(&revision, &current, &replicas, &deleted, &topology); err != nil {
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
	// Patroni fixes its members and elects its own primary. Replica actions and setup
	// recovery are standalone workflows. A member rebuild is a Patroni workflow.
	patroniAction := request.Identity.APIOperation == StopClusterAPIOperation || request.Identity.APIOperation == StartClusterAPIOperation ||
		request.Identity.APIOperation == DeleteClusterAPIOperation || request.Identity.APIOperation == RebuildMemberAPIOperation ||
		request.Identity.APIOperation == RenewCertificatesAPIOperation || request.Identity.APIOperation == SwitchoverAPIOperation
	patroniOnly := request.Identity.APIOperation == RebuildMemberAPIOperation || request.Identity.APIOperation == RenewCertificatesAPIOperation ||
		request.Identity.APIOperation == SwitchoverAPIOperation
	if (topology == "patroni" && !patroniAction) || (topology != "patroni" && patroniOnly) {
		return Admission{}, &AdmissionError{Code: UnsupportedSpec}
	}
	// A residual Patroni member may hold incomplete storage. Stop and start would act on it
	// with the plan of a whole member, so it must be rebuilt first. Delete stays available.
	// A renewal recreates every member, so each must be active. A switchover proves every
	// member before and after the leader moves, so each must be active too.
	if request.Identity.APIOperation == RenewCertificatesAPIOperation || request.Identity.APIOperation == SwitchoverAPIOperation {
		var members, active int
		var named bool
		if err := tx.QueryRow(ctx, `SELECT count(*),count(*) FILTER (WHERE role='patroni' AND state='active'),COALESCE(bool_or(member_id=$3),false)
 FROM cluster_members WHERE installation_id=$1 AND cluster_id=$2 AND removed_at IS NULL`, identity.InstallationID, request.ClusterID, request.MemberID).Scan(&members, &active, &named); err != nil {
			return Admission{}, admissionError(err)
		}
		if members != 3 || active != 3 {
			return Admission{}, &AdmissionError{Code: OperationConflict}
		}
		if request.Identity.APIOperation == SwitchoverAPIOperation && !named {
			return Admission{}, &AdmissionError{Code: InvalidRequest}
		}
	}
	if topology == "patroni" && (request.Identity.APIOperation == StopClusterAPIOperation || request.Identity.APIOperation == StartClusterAPIOperation) {
		var residual bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM cluster_members WHERE installation_id=$1 AND cluster_id=$2 AND state='residual' AND removed_at IS NULL)`, identity.InstallationID, request.ClusterID).Scan(&residual); err != nil {
			return Admission{}, admissionError(err)
		}
		if residual {
			return Admission{}, &AdmissionError{Code: OperationConflict}
		}
	}
	if request.Identity.APIOperation == ScaleClusterAPIOperation || request.Identity.APIOperation == RebuildReplicaAPIOperation || request.Identity.APIOperation == RebuildMemberAPIOperation ||
		request.Identity.APIOperation == SwitchoverAPIOperation {
		// A scale acts on a running cluster. Scaling a stopped one would plan work for
		// an engine that is not there.
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
	if request.Identity.APIOperation == ScaleClusterAPIOperation && *request.Replicas == replicas {
		err := tx.QueryRow(ctx, `SELECT o.id FROM operations o
 JOIN clusters c ON c.installation_id=o.installation_id AND c.id=o.cluster_id
 WHERE o.installation_id=$1 AND o.cluster_id=$2 AND o.desired_revision=$3
 AND (o.kind='cluster.scale' OR ($4=1 AND o.kind='cluster.rebuild_replica')) AND o.status IN ('failed','cancelled') AND c.engine IN ('redis','postgres')
 AND ($4=1 OR EXISTS(SELECT 1 FROM cluster_members m WHERE m.installation_id=c.installation_id AND m.cluster_id=c.id AND m.role='replica' AND m.removed_at IS NULL))`,
			identity.InstallationID, request.ClusterID, revision, replicas).Scan(&retryOf)
		if errors.Is(err, pgx.ErrNoRows) {
			return Admission{}, &AdmissionError{Code: InvalidRequest}
		}
		if err != nil {
			return Admission{}, admissionError(err)
		}
	}
	if request.Identity.APIOperation == RebuildReplicaAPIOperation {
		var live int
		if err := tx.QueryRow(ctx, `SELECT count(*) FROM cluster_members WHERE installation_id=$1 AND cluster_id=$2 AND role='replica' AND removed_at IS NULL`, identity.InstallationID, request.ClusterID).Scan(&live); err != nil {
			return Admission{}, admissionError(err)
		}
		if live != 1 {
			return Admission{}, &AdmissionError{Code: OperationConflict}
		}
		err := tx.QueryRow(ctx, `SELECT id FROM operations WHERE installation_id=$1 AND cluster_id=$2 AND desired_revision=$3 AND kind='cluster.rebuild_replica' AND status IN ('failed','cancelled')`, identity.InstallationID, request.ClusterID, revision).Scan(&retryOf)
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
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
		if request.Identity.APIOperation == RebuildReplicaAPIOperation {
			action = "rebuild_replica"
		}
		if request.Identity.APIOperation == RebuildMemberAPIOperation {
			action = "rebuild_member"
		}
		var planned []uuid.UUID
		var member, resource *uuid.UUID
		err := tx.QueryRow(ctx, `SELECT retained_volumes,member_id,resource_id,action FROM cluster_action_plans
 WHERE plan_token=$1 AND installation_id=$2 AND actor_id=$3 AND cluster_id=$4
 AND expected_revision=$5 AND (action=$6 OR ($6='scale_down' AND action='scale_down_failed')) AND data_disposition='retain' AND expires_at>clock_timestamp()`,
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
		if action == "scale_down" || action == "scale_down_failed" || action == "rebuild_replica" {
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
			if action == "scale_down_failed" || action == "rebuild_replica" {
				var supported bool
				if err := tx.QueryRow(ctx, `SELECT engine='postgres' AND spec->'postgresql'->>'topology'='standalone' AND spec->'postgresql'->>'version'='17.11' FROM clusters WHERE installation_id=$1 AND id=$2`, identity.InstallationID, request.ClusterID).Scan(&supported); err != nil {
					return Admission{}, admissionError(err)
				}
				if !supported {
					return Admission{}, &AdmissionError{Code: UnsupportedSpec}
				}
				if resource == nil || *resource != targetResource {
					return Admission{}, &AdmissionError{Code: OperationConflict}
				}
			}
		} else if action == "rebuild_member" {
			// The reviewed member must still be a rebuild target with the same storage.
			if member == nil {
				return Admission{}, &AdmissionError{Code: OperationConflict}
			}
			targetResource, _, err := patroniRebuildTarget(ctx, tx, identity.InstallationID, request.ClusterID, *member)
			if refused, ok := errors.AsType[*AdmissionError](err); ok && refused.Code == NotFound {
				return Admission{}, &AdmissionError{Code: OperationConflict}
			}
			if err != nil {
				return Admission{}, err
			}
			owned, err := memberVolumeIDs(ctx, tx, identity.InstallationID, request.ClusterID, targetResource)
			if err != nil {
				return Admission{}, err
			}
			if resource == nil || *resource != targetResource || !slices.Equal(planned, owned) {
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
	var target *uuid.UUID
	if request.MemberID != uuid.Nil {
		target = &request.MemberID
	}
	var desired *string
	if lifecycle != "" {
		moved := string(lifecycle)
		desired = &moved
	}
	desiredReplicas := request.Replicas
	if request.Identity.APIOperation == RebuildReplicaAPIOperation {
		desiredReplicas = new(1)
	}
	if _, err := tx.Exec(ctx, `UPDATE clusters SET desired_revision=$1,
 desired_lifecycle=COALESCE($2,desired_lifecycle),
 desired_replicas=COALESCE($3,desired_replicas),
 updated_at=transaction_timestamp()
 WHERE installation_id=$4 AND id=$5`, next, desired, desiredReplicas, identity.InstallationID, request.ClusterID); err != nil {
		return Admission{}, admissionError(err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO operations
 (id,installation_id,cluster_id,actor_id,api_operation,idempotency_key,request_hash,hash_version,kind,desired_revision,status,plan_version,execution_epoch,attempts,created_at,updated_at,retry_of,action_plan_token,target_member_id)
 VALUES ($1,$2,$3,$4,$5,$6,$7,1,$8,$9,'pending',0,0,0,transaction_timestamp(),transaction_timestamp(),$10,$11,$12)`,
		operationID, identity.InstallationID, request.ClusterID, identity.ActorID,
		identity.APIOperation, identity.IdempotencyKey, hash[:], kind, next, retryOf, actionPlanToken, target); err != nil {
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
	case RebuildReplicaAPIOperation:
		return "", "cluster.rebuild_replica", nil
	case RebuildMemberAPIOperation:
		return "", "cluster.rebuild_member", nil
	case RenewCertificatesAPIOperation:
		return "", "cluster.renew_certificates", nil
	case SwitchoverAPIOperation:
		return "", "cluster.switchover", nil
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
		MemberID     uuid.UUID                   `json:"member_id,omitzero"`
	}{request.Identity.APIOperation, request.ClusterID, request.ExpectedRevision, request.Confirmation, request.Replicas, request.MemberID})
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
	// Only a scale carries a replica count, and this slice supports none or one.
	if (identity.APIOperation == ScaleClusterAPIOperation) != (request.Replicas != nil) {
		return &AdmissionError{Code: InvalidRequest}
	}
	if request.Replicas != nil && (*request.Replicas < 0 || *request.Replicas > 1) {
		return &AdmissionError{Code: InvalidRequest}
	}
	// Only a switchover names a member, and it must name one.
	if (identity.APIOperation == SwitchoverAPIOperation) != (request.MemberID != uuid.Nil) {
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
