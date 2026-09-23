package postgres

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/sirrobot01/synod/internal/operation"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/proto"
)

// ClaimRequest asks for durable ownership of one operation.
type ClaimRequest struct {
	ClaimID              uuid.UUID
	InstallationID       uuid.UUID
	OperationID          uuid.UUID
	RecoveryGeneration   uuid.UUID
	OwnerID              string
	TTL                  time.Duration
	CoordinationKey      string
	CoordinationLease    int64
	CoordinationRevision int64
}

// InitializeGeneration is idempotent. Ordinary claims cannot rotate a generation.
func (s *Store) InitializeGeneration(ctx context.Context, installationID, generation uuid.UUID) error {
	if installationID == uuid.Nil || generation == uuid.Nil {
		return &AdmissionError{Code: InvalidRequest}
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return admissionError(err)
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `INSERT INTO installation_generations VALUES ($1,$2) ON CONFLICT DO NOTHING`, installationID, generation); err != nil {
		return admissionError(err)
	}
	var current uuid.UUID
	if err := tx.QueryRow(ctx, `SELECT recovery_generation FROM installation_generations WHERE installation_id=$1`, installationID).Scan(&current); err != nil {
		return admissionError(err)
	}
	if current != generation {
		return operation.ErrStaleOwner
	}
	return s.commitMetadata(ctx, tx)
}

// AcquireClaim uses a caller-generated claim ID to resolve an uncertain commit.
// Verify the etcd proof before this call and before activation or dispatch.
func (s *Store) AcquireClaim(ctx context.Context, r ClaimRequest) (operation.Claim, error) {
	if r.ClaimID == uuid.Nil || r.InstallationID == uuid.Nil || r.OperationID == uuid.Nil || r.RecoveryGeneration == uuid.Nil || r.OwnerID == "" || len(r.OwnerID) > 256 || r.TTL < time.Second || r.TTL > 5*time.Minute || r.CoordinationKey == "" || r.CoordinationLease == 0 || r.CoordinationRevision < 1 {
		return operation.Claim{}, &AdmissionError{Code: InvalidRequest}
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return operation.Claim{}, admissionError(err)
	}
	defer tx.Rollback(ctx)
	var generation uuid.UUID
	if err := tx.QueryRow(ctx, `SELECT recovery_generation FROM installation_generations WHERE installation_id=$1 FOR SHARE`, r.InstallationID).Scan(&generation); err != nil {
		return operation.Claim{}, admissionError(err)
	}
	if generation != r.RecoveryGeneration {
		return operation.Claim{}, operation.ErrStaleOwner
	}
	var result operation.Claim
	// Lock the cluster separately so the join planner cannot choose operation-first
	// locking against admission or terminal lifecycle completion.
	if err := tx.QueryRow(ctx, `SELECT c.id FROM clusters c JOIN operations o
 ON o.installation_id=c.installation_id AND o.cluster_id=c.id
 WHERE o.id=$1 AND o.installation_id=$2 FOR UPDATE OF c`, r.OperationID, r.InstallationID).Scan(&result.ClusterID); err != nil {
		return operation.Claim{}, admissionError(err)
	}
	var oldClaim, oldGeneration *uuid.UUID
	var oldOwner *string
	var oldExpiry *time.Time
	var status operation.Status
	var plan []byte
	var oldKey *string
	var oldLease, oldRevision *int64
	err = tx.QueryRow(ctx, `SELECT c.id,c.execution_epoch,o.claim_id,o.owner_id,o.claim_expires_at,o.status,o.plan_bytes,o.claim_active,o.coordination_key,o.coordination_lease,o.coordination_revision,o.recovery_generation
		FROM operations o JOIN clusters c ON c.id=o.cluster_id AND c.installation_id=o.installation_id
		WHERE o.id=$1 AND o.installation_id=$2 FOR UPDATE OF o`, r.OperationID, r.InstallationID).
		Scan(&result.ClusterID, &result.ExecutionEpoch, &oldClaim, &oldOwner, &oldExpiry, &status, &plan, &result.Active, &oldKey, &oldLease, &oldRevision, &oldGeneration)
	if err != nil {
		return operation.Claim{}, admissionError(err)
	}
	if status == operation.Succeeded || status == operation.Failed || status == operation.Cancelled {
		return operation.Claim{}, operation.ErrStaleOwner
	}
	var now time.Time
	if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&now); err != nil {
		return operation.Claim{}, admissionError(err)
	}
	result.ID = r.ClaimID
	result.InstallationID = r.InstallationID
	result.OperationID = r.OperationID
	result.OwnerID = r.OwnerID
	result.RecoveryGeneration = generation
	if oldClaim != nil && *oldClaim == r.ClaimID {
		if oldGeneration == nil || *oldGeneration != generation || oldOwner == nil || *oldOwner != r.OwnerID || oldExpiry == nil || !oldExpiry.After(now) || oldKey == nil || *oldKey != r.CoordinationKey || oldLease == nil || *oldLease != r.CoordinationLease || oldRevision == nil || *oldRevision != r.CoordinationRevision {
			return operation.Claim{}, operation.ErrStaleOwner
		}
		result.ExpiresAt = *oldExpiry
	} else {
		if oldExpiry != nil && oldExpiry.After(now) {
			return operation.Claim{}, operation.ErrClaimConflict
		}
		result.ExecutionEpoch++
		result.Active = false
		result.ExpiresAt = now.Add(r.TTL)
		if _, err := tx.Exec(ctx, `INSERT INTO execution_claims (claim_id,installation_id,cluster_id,operation_id,execution_epoch,recovery_generation,owner_id,coordination_key,coordination_lease,coordination_revision)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`, r.ClaimID, r.InstallationID, result.ClusterID, r.OperationID, result.ExecutionEpoch, generation, r.OwnerID, r.CoordinationKey, r.CoordinationLease, r.CoordinationRevision); err != nil {
			if pgErr, ok := errors.AsType[*pgconn.PgError](err); ok && pgErr.Code == "23505" && pgErr.ConstraintName == "execution_claims_pkey" {
				return operation.Claim{}, operation.ErrStaleOwner
			}
			return operation.Claim{}, admissionError(err)
		}
		if _, err := tx.Exec(ctx, `UPDATE clusters SET execution_epoch=$1 WHERE id=$2 AND installation_id=$3`, result.ExecutionEpoch, result.ClusterID, r.InstallationID); err != nil {
			return operation.Claim{}, admissionError(err)
		}
		if _, err := tx.Exec(ctx, `UPDATE operations SET claim_id=$1,owner_id=$2,execution_epoch=$3,claim_expires_at=$4,recovery_generation=$5,claim_active=false,
			coordination_key=$6,coordination_lease=$7,coordination_revision=$8,updated_at=clock_timestamp() WHERE id=$9`,
			r.ClaimID, r.OwnerID, result.ExecutionEpoch, result.ExpiresAt, generation, r.CoordinationKey, r.CoordinationLease, r.CoordinationRevision, r.OperationID); err != nil {
			return operation.Claim{}, admissionError(err)
		}
		if _, err := tx.Exec(ctx, `INSERT INTO claim_barriers (claim_id,installation_id,cluster_id,operation_id,node_id,execution_epoch,recovery_generation)
			SELECT DISTINCT $1::uuid,installation_id,cluster_id,$2::uuid,node_id,$3::bigint,$4::uuid FROM command_dispatches WHERE installation_id=$5 AND cluster_id=$6`,
			r.ClaimID, r.OperationID, result.ExecutionEpoch, generation, r.InstallationID, result.ClusterID); err != nil {
			return operation.Claim{}, admissionError(err)
		}
		if plan != nil {
			var persisted agentv1.ExecutionPlan
			if err := proto.Unmarshal(plan, &persisted); err != nil {
				return operation.Claim{}, admissionError(err)
			}
			for _, step := range persisted.Steps {
				if step.Command == nil || step.Command.Epoch == nil {
					return operation.Claim{}, admissionError(fmt.Errorf("persisted plan has no target"))
				}
				if _, err := tx.Exec(ctx, `INSERT INTO claim_barriers (claim_id,installation_id,cluster_id,operation_id,node_id,execution_epoch,recovery_generation) VALUES ($1,$2,$3,$4,$5,$6,$7) ON CONFLICT DO NOTHING`, result.ID, result.InstallationID, result.ClusterID, result.OperationID, step.Command.Epoch.NodeId, result.ExecutionEpoch, generation); err != nil {
					return operation.Claim{}, admissionError(err)
				}
			}
		}
	}
	rows, err := tx.Query(ctx, `SELECT node_id FROM claim_barriers WHERE claim_id=$1 ORDER BY node_id`, r.ClaimID)
	if err != nil {
		return operation.Claim{}, admissionError(err)
	}
	for rows.Next() {
		var node uuid.UUID
		if err := rows.Scan(&node); err != nil {
			rows.Close()
			return operation.Claim{}, admissionError(err)
		}
		result.AffectedNodes = append(result.AffectedNodes, node)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return operation.Claim{}, admissionError(err)
	}
	if err := s.commitMetadata(ctx, tx); err != nil {
		return operation.Claim{}, err
	}
	return result, nil
}

// lockClaim checks time after the row lock. A lock wait must not extend authority.
func lockClaim(ctx context.Context, tx pgx.Tx, claim operation.Claim) (bool, operation.Status, []byte, error) {
	var currentGeneration uuid.UUID
	if err := tx.QueryRow(ctx, `SELECT recovery_generation FROM installation_generations WHERE installation_id=$1 FOR SHARE`, claim.InstallationID).Scan(&currentGeneration); err != nil {
		return false, "", nil, admissionError(err)
	}
	if currentGeneration != claim.RecoveryGeneration {
		return false, "", nil, operation.ErrStaleOwner
	}
	var id, generation uuid.UUID
	var owner string
	var epoch int64
	var expires time.Time
	var active bool
	var status operation.Status
	var plan []byte
	err := tx.QueryRow(ctx, `SELECT claim_id,recovery_generation,owner_id,execution_epoch,claim_expires_at,claim_active,status,plan_bytes
		FROM operations WHERE id=$1 AND installation_id=$2 AND cluster_id=$3 AND claim_id IS NOT NULL FOR UPDATE`, claim.OperationID, claim.InstallationID, claim.ClusterID).
		Scan(&id, &generation, &owner, &epoch, &expires, &active, &status, &plan)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, "", nil, operation.ErrStaleOwner
	}
	if err != nil {
		return false, "", nil, admissionError(err)
	}
	var now time.Time
	if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&now); err != nil {
		return false, "", nil, admissionError(err)
	}
	if id != claim.ID || generation != claim.RecoveryGeneration || owner != claim.OwnerID || epoch != claim.ExecutionEpoch || !expires.After(now) {
		return false, "", nil, operation.ErrStaleOwner
	}
	return active, status, plan, nil
}

func (s *Store) commitMetadata(ctx context.Context, tx pgx.Tx) error {
	var err error
	if s.commitTx != nil {
		err = s.commitTx(ctx, tx)
	} else {
		err = tx.Commit(ctx)
	}
	if err == nil {
		return nil
	}
	if errors.Is(err, pgx.ErrTxCommitRollback) {
		return admissionError(err)
	}
	if pgErr, ok := errors.AsType[*pgconn.PgError](err); ok && (strings.HasPrefix(pgErr.Code, "23") || pgErr.Code == "40001" || pgErr.Code == "40P01") {
		return admissionError(err)
	}
	return &AdmissionError{Code: OutcomeUnknown, Cause: err}
}

// RenewClaim extends a claim that is still valid and returns the new expiry.
// It never revives an expired claim.
func (s *Store) RenewClaim(ctx context.Context, claim operation.Claim, ttl time.Duration) (time.Time, error) {
	if ttl < time.Second || ttl > 5*time.Minute {
		return time.Time{}, &AdmissionError{Code: InvalidRequest}
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return time.Time{}, admissionError(err)
	}
	defer tx.Rollback(ctx)
	_, status, _, err := lockClaim(ctx, tx, claim)
	if err != nil {
		return time.Time{}, err
	}
	if status == operation.Succeeded || status == operation.Failed || status == operation.Cancelled {
		return time.Time{}, operation.ErrStaleOwner
	}
	var expiry time.Time
	if err := tx.QueryRow(ctx, `UPDATE operations SET claim_expires_at=clock_timestamp()+$1*interval '1 microsecond',updated_at=clock_timestamp() WHERE id=$2 RETURNING claim_expires_at`, ttl.Microseconds(), claim.OperationID).Scan(&expiry); err != nil {
		return time.Time{}, admissionError(err)
	}
	if err := s.commitMetadata(ctx, tx); err != nil {
		return time.Time{}, err
	}
	return expiry, nil
}

// InstallPlan fixes typed commands and step IDs before activation.
func (s *Store) InstallPlan(ctx context.Context, claim operation.Claim, plan *agentv1.ExecutionPlan) error {
	encoded, err := validateInstalledPlan(claim, plan)
	if err != nil {
		return err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return admissionError(err)
	}
	defer tx.Rollback(ctx)
	changed, err := installPlanTx(ctx, tx, claim, plan, encoded)
	if err != nil || !changed {
		return err
	}
	return s.commitMetadata(ctx, tx)
}

func validateInstalledPlan(claim operation.Claim, plan *agentv1.ExecutionPlan) ([]byte, error) {
	if plan == nil || plan.Version != 1 || len(plan.Steps) < 1 || len(plan.Steps) > 128 {
		return nil, &AdmissionError{Code: InvalidRequest}
	}
	stepIDs := make([]string, 0, len(plan.Steps))
	commandIDs := make([]string, 0, len(plan.Steps))
	for _, step := range plan.Steps {
		if step == nil || step.Command == nil || step.MaxAttempts < 1 || step.MaxAttempts > 10 || step.CancellationBoundary < agentv1.CancellationBoundary_CANCELLATION_BOUNDARY_BEFORE_EXECUTION || step.CancellationBoundary > agentv1.CancellationBoundary_CANCELLATION_BOUNDARY_SAFE_STOP || step.Postcondition < agentv1.Postcondition_POSTCONDITION_VOLUME_EXISTS || step.Postcondition > agentv1.Postcondition_POSTCONDITION_POSTGRES_REPLICA_FAILED {
			return nil, &AdmissionError{Code: InvalidRequest}
		}
		command := step.Command
		if err := operation.ValidateCommand(command); err != nil {
			return nil, err
		}
		if command.Epoch.InstallationId != claim.InstallationID.String() || command.Epoch.ClusterId != claim.ClusterID.String() || command.Epoch.RecoveryGeneration != claim.RecoveryGeneration.String() || command.Epoch.ExecutionEpoch != claim.ExecutionEpoch || command.OperationId != claim.OperationID.String() || command.PlanVersion != plan.Version || slices.Contains(stepIDs, command.StepId) || slices.Contains(commandIDs, command.CommandId) {
			return nil, operation.ErrCommandConflict
		}
		for _, dependency := range step.DependsOn {
			if !slices.Contains(stepIDs, dependency) {
				return nil, &AdmissionError{Code: InvalidRequest}
			}
		}
		stepIDs = append(stepIDs, command.StepId)
		commandIDs = append(commandIDs, command.CommandId)
	}
	encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(plan)
	if err != nil {
		return nil, &AdmissionError{Code: InvalidRequest, Cause: err}
	}
	return encoded, nil
}

func installPlanTx(ctx context.Context, tx pgx.Tx, claim operation.Claim, plan *agentv1.ExecutionPlan, encoded []byte) (bool, error) {
	active, _, existing, err := lockClaim(ctx, tx, claim)
	if err != nil {
		return false, err
	}
	if existing != nil {
		if !bytes.Equal(existing, encoded) {
			return false, operation.ErrCommandConflict
		}
		return false, nil
	}
	if active {
		return false, operation.ErrCommandConflict
	}
	var revision int64
	if err := tx.QueryRow(ctx, `SELECT desired_revision FROM operations WHERE id=$1`, claim.OperationID).Scan(&revision); err != nil {
		return false, admissionError(err)
	}
	resources := make([]string, 0, len(plan.Steps))
	for _, step := range plan.Steps {
		if step.Command.DesiredRevision != revision {
			return false, operation.ErrCommandConflict
		}
		resources = append(resources, step.Command.Input.ResourceId)
		if _, err := tx.Exec(ctx, `INSERT INTO claim_barriers (claim_id,installation_id,cluster_id,operation_id,node_id,execution_epoch,recovery_generation) VALUES ($1,$2,$3,$4,$5,$6,$7) ON CONFLICT DO NOTHING`, claim.ID, claim.InstallationID, claim.ClusterID, claim.OperationID, step.Command.Epoch.NodeId, claim.ExecutionEpoch, claim.RecoveryGeneration); err != nil {
			return false, admissionError(err)
		}
	}
	if _, err := tx.Exec(ctx, `UPDATE operations SET plan_version=$1,plan_bytes=$2,updated_at=clock_timestamp() WHERE id=$3`, plan.Version, encoded, claim.OperationID); err != nil {
		return false, admissionError(err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO operation_allocation_bindings
 (installation_id,cluster_id,operation_id,allocation_id)
 SELECT a.installation_id,a.cluster_id,o.id,a.id FROM resource_allocations a
 JOIN operations o ON o.installation_id=a.installation_id AND o.cluster_id=a.cluster_id
 WHERE o.id=$1 AND o.retry_of IS NOT NULL AND a.released_at IS NULL
 AND a.resource_id=ANY($2::uuid[]) ON CONFLICT DO NOTHING`, claim.OperationID, resources); err != nil {
		return false, admissionError(err)
	}
	return true, nil
}

// RecordBarrier preserves agent evidence. It never activates a claim by itself.
func (s *Store) RecordBarrier(ctx context.Context, claim operation.Claim, report *agentv1.BarrierResult) error {
	if report == nil || report.Epoch == nil {
		return &AdmissionError{Code: InvalidRequest}
	}
	epoch := report.Epoch
	if epoch.InstallationId != claim.InstallationID.String() || epoch.ClusterId != claim.ClusterID.String() || epoch.RecoveryGeneration != claim.RecoveryGeneration.String() || epoch.ExecutionEpoch != claim.ExecutionEpoch {
		return operation.ErrStaleOwner
	}
	nodeID, err := uuid.Parse(epoch.NodeId)
	if err != nil || nodeID == uuid.Nil {
		return &AdmissionError{Code: InvalidRequest}
	}
	encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(report)
	if err != nil {
		return &AdmissionError{Code: InvalidRequest, Cause: err}
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return admissionError(err)
	}
	defer tx.Rollback(ctx)
	if err := recordBarrier(ctx, tx, claim, nodeID, uuid.Nil, encoded); err != nil {
		return err
	}
	return s.commitMetadata(ctx, tx)
}

func recordBarrier(ctx context.Context, tx pgx.Tx, claim operation.Claim, nodeID, sessionID uuid.UUID, encoded []byte) error {
	active, _, _, err := lockClaim(ctx, tx, claim)
	if err != nil {
		return err
	}
	if active {
		return operation.ErrCommandConflict
	}
	var existing []byte
	var reconciled *time.Time
	var priorSession *uuid.UUID
	if err := tx.QueryRow(ctx, `SELECT report_bytes,reconciled_at,session_id FROM claim_barriers WHERE claim_id=$1 AND node_id=$2 FOR UPDATE`, claim.ID, nodeID).Scan(&existing, &reconciled, &priorSession); err != nil {
		return admissionError(err)
	}
	if priorSession != nil && *priorSession != sessionID {
		return ErrStaleSession
	}
	if bytes.Equal(existing, encoded) && (sessionID == uuid.Nil || priorSession != nil) {
		return nil
	}
	if reconciled != nil {
		return operation.ErrCommandConflict
	}
	var prior, next agentv1.BarrierResult
	if proto.Unmarshal(existing, &prior) != nil || proto.Unmarshal(encoded, &next) != nil {
		return &AdmissionError{Code: DependencyUnavailable}
	}
	for _, effect := range prior.Effects {
		if effect == nil {
			return &AdmissionError{Code: DependencyUnavailable}
		}
		if !effect.Active && !effect.OutcomeUnknown {
			continue
		}
		if !slices.ContainsFunc(next.Effects, func(candidate *agentv1.Effect) bool {
			return candidate != nil && candidate.CommandId == effect.CommandId && candidate.ResourceId == effect.ResourceId && candidate.NativeOperationId == effect.NativeOperationId
		}) {
			return operation.ErrBarrierPending
		}
	}
	var storedSession *uuid.UUID
	if sessionID != uuid.Nil {
		storedSession = &sessionID
	}
	if _, err := tx.Exec(ctx, `UPDATE claim_barriers SET report_bytes=$1,session_id=$4 WHERE claim_id=$2 AND node_id=$3`, encoded, claim.ID, nodeID, storedSession); err != nil {
		return admissionError(err)
	}
	return nil
}

// ReconcileBarrier records a controller decision after it reconciles stored effects.
// summary must describe the checked residual state without secrets or raw tool output.
func (s *Store) ReconcileBarrier(ctx context.Context, claim operation.Claim, nodeID uuid.UUID, reportHash [32]byte, summary string) error {
	if nodeID == uuid.Nil || summary == "" || len(summary) > 1024 {
		return &AdmissionError{Code: InvalidRequest}
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return admissionError(err)
	}
	defer tx.Rollback(ctx)
	active, _, _, err := lockClaim(ctx, tx, claim)
	if err != nil {
		return err
	}
	if active {
		return operation.ErrCommandConflict
	}
	var encoded []byte
	if err := tx.QueryRow(ctx, `SELECT report_bytes FROM claim_barriers WHERE claim_id=$1 AND node_id=$2 FOR UPDATE`, claim.ID, nodeID).Scan(&encoded); err != nil {
		return admissionError(err)
	}
	if encoded == nil || sha256.Sum256(encoded) != reportHash {
		return operation.ErrBarrierPending
	}
	var report agentv1.BarrierResult
	if err := proto.Unmarshal(encoded, &report); err != nil {
		return admissionError(err)
	}
	if err := operation.BarrierDrained(&report); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `UPDATE claim_barriers SET reconciled_at=clock_timestamp(),reconciliation_summary=$1 WHERE claim_id=$2 AND node_id=$3`, summary, claim.ID, nodeID); err != nil {
		return admissionError(err)
	}
	return s.commitMetadata(ctx, tx)
}

// ActivateClaim requires every planned and previously dispatched node barrier.
// The controller must verify its etcd lease immediately before this call.
func (s *Store) ActivateClaim(ctx context.Context, claim operation.Claim) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return admissionError(err)
	}
	defer tx.Rollback(ctx)
	active, status, plan, err := lockClaim(ctx, tx, claim)
	if err != nil {
		return err
	}
	if active {
		if status == operation.Running {
			return nil
		}
		return operation.ErrBarrierPending
	}
	if plan == nil || (status != operation.Pending && status != operation.Running && status != operation.Waiting) {
		return operation.ErrBarrierPending
	}
	var pending bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM claim_barriers WHERE claim_id=$1 AND reconciled_at IS NULL)`, claim.ID).Scan(&pending); err != nil {
		return admissionError(err)
	}
	if pending {
		return operation.ErrBarrierPending
	}
	if _, err := tx.Exec(ctx, `UPDATE operations SET claim_active=true,status='running',updated_at=clock_timestamp() WHERE id=$1`, claim.OperationID); err != nil {
		return admissionError(err)
	}
	return s.commitMetadata(ctx, tx)
}

// ReadPlan returns a typed copy of the immutable plan under the current claim.
func (s *Store) ReadPlan(ctx context.Context, claim operation.Claim) (*agentv1.ExecutionPlan, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, admissionError(err)
	}
	defer tx.Rollback(ctx)
	_, _, encoded, err := lockClaim(ctx, tx, claim)
	if err != nil {
		return nil, err
	}
	if encoded == nil {
		return nil, operation.ErrBarrierPending
	}
	var plan agentv1.ExecutionPlan
	if err := proto.Unmarshal(encoded, &plan); err != nil {
		return nil, admissionError(err)
	}
	return &plan, nil
}

// RecordDispatch stores the authenticated target before any command leaves the controller.
// Revalidate both PostgreSQL and etcd ownership before sending the recorded command.
func (s *Store) RecordDispatch(ctx context.Context, claim operation.Claim, command *agentv1.Command) error {
	if err := operation.ValidateCommand(command); err != nil {
		return err
	}
	if command.Epoch.InstallationId != claim.InstallationID.String() || command.Epoch.ClusterId != claim.ClusterID.String() || command.Epoch.RecoveryGeneration != claim.RecoveryGeneration.String() || command.Epoch.ExecutionEpoch != claim.ExecutionEpoch || command.OperationId != claim.OperationID.String() {
		return operation.ErrStaleOwner
	}
	encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(command)
	if err != nil {
		return &AdmissionError{Code: InvalidRequest, Cause: err}
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return admissionError(err)
	}
	defer tx.Rollback(ctx)
	active, status, planBytes, err := lockClaim(ctx, tx, claim)
	if err != nil {
		return err
	}
	if !active || status != operation.Running || planBytes == nil {
		return operation.ErrBarrierPending
	}
	var plan agentv1.ExecutionPlan
	if err := proto.Unmarshal(planBytes, &plan); err != nil {
		return admissionError(err)
	}
	matched := false
	for _, step := range plan.Steps {
		if step.Command != nil && step.Command.CommandId == command.CommandId && bytes.Equal(step.Command.InputHash, command.InputHash) {
			matched = true
			break
		}
	}
	if !matched {
		return operation.ErrCommandConflict
	}
	var ready bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM claim_barriers WHERE claim_id=$1 AND node_id=$2 AND reconciled_at IS NOT NULL)`, claim.ID, command.Epoch.NodeId).Scan(&ready); err != nil {
		return admissionError(err)
	}
	if !ready {
		return operation.ErrBarrierPending
	}
	if _, err := tx.Exec(ctx, `INSERT INTO command_identities (command_id,input_hash) VALUES ($1,$2) ON CONFLICT DO NOTHING`, command.CommandId, command.InputHash); err != nil {
		return admissionError(err)
	}
	var hash []byte
	if err := tx.QueryRow(ctx, `SELECT input_hash FROM command_identities WHERE command_id=$1`, command.CommandId).Scan(&hash); err != nil {
		return admissionError(err)
	}
	if !bytes.Equal(hash, command.InputHash) {
		return operation.ErrCommandConflict
	}
	if _, err := tx.Exec(ctx, `INSERT INTO command_dispatches (command_id,execution_epoch,installation_id,cluster_id,operation_id,recovery_generation,node_id,step_id,input_hash,command_bytes)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) ON CONFLICT DO NOTHING`, command.CommandId, claim.ExecutionEpoch, claim.InstallationID, claim.ClusterID, claim.OperationID, claim.RecoveryGeneration, command.Epoch.NodeId, command.StepId, command.InputHash, encoded); err != nil {
		return admissionError(err)
	}
	return s.commitMetadata(ctx, tx)
}

// ValidateClaim checks current PostgreSQL authority without extending the claim.
func (s *Store) ValidateClaim(ctx context.Context, claim operation.Claim) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return admissionError(err)
	}
	defer tx.Rollback(ctx)
	active, status, _, err := lockClaim(ctx, tx, claim)
	if err != nil {
		return err
	}
	if !active || status != operation.Running {
		return operation.ErrBarrierPending
	}
	return nil
}

// ProgressUpdate is one operation status change from the current owner.
type ProgressUpdate struct {
	Sequence       int64
	Status         operation.Status
	Reason         string
	UnknownEffects bool
	FailureCode    string
}

// UpdateProgress keeps unknown effects waiting and rejects a stale owner or sequence.
func (s *Store) UpdateProgress(ctx context.Context, claim operation.Claim, update ProgressUpdate) error {
	if update.Sequence < 1 || update.Reason == "" || len(update.Reason) > 1024 || (update.UnknownEffects && update.Status != operation.Waiting) {
		return &AdmissionError{Code: InvalidRequest}
	}
	if len(update.FailureCode) > 128 || strings.ContainsFunc(update.FailureCode, func(r rune) bool { return !(r >= 'a' && r <= 'z' || r >= '0' && r <= '9' || r == '_') }) || (update.FailureCode != "" && update.Status != operation.Failed) {
		return &AdmissionError{Code: InvalidRequest}
	}
	if update.Status == operation.Failed && update.FailureCode == "" {
		update.FailureCode = "operation_failed"
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return admissionError(err)
	}
	defer tx.Rollback(ctx)
	if update.Status == operation.Succeeded || update.Status == operation.Failed || update.Status == operation.Cancelled {
		// Admission locks the cluster before its operation. Keep that order while
		// finalizing lifecycle metadata, after the installation generation lock.
		var generation uuid.UUID
		if err := tx.QueryRow(ctx, `SELECT recovery_generation FROM installation_generations WHERE installation_id=$1 FOR SHARE`, claim.InstallationID).Scan(&generation); err != nil {
			return admissionError(err)
		}
		if generation != claim.RecoveryGeneration {
			return operation.ErrStaleOwner
		}
		if _, err := tx.Exec(ctx, `SELECT 1 FROM clusters WHERE installation_id=$1 AND id=$2 FOR UPDATE`, claim.InstallationID, claim.ClusterID); err != nil {
			return admissionError(err)
		}
	}
	active, current, plan, err := lockClaim(ctx, tx, claim)
	if err != nil {
		return err
	}
	var prior operation.Status
	var reason string
	var unknown bool
	var code string
	// A failed operation is terminal, so its failure code cannot change after this row.
	err = tx.QueryRow(ctx, `SELECT p.state,p.reason,p.unknown_effects,
		CASE WHEN p.state='failed' THEN COALESCE(o.failure_code,'') ELSE '' END
		FROM operation_progress p JOIN operations o ON o.id=p.operation_id
		WHERE p.operation_id=$1 AND p.execution_epoch=$2 AND p.sequence=$3`, claim.OperationID, claim.ExecutionEpoch, update.Sequence).Scan(&prior, &reason, &unknown, &code)
	if err == nil {
		if prior == update.Status && reason == update.Reason && unknown == update.UnknownEffects && code == update.FailureCode {
			return nil
		}
		return operation.ErrCommandConflict
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return admissionError(err)
	}
	if !active && (update.Status != operation.Waiting || current == operation.Succeeded || current == operation.Failed || current == operation.Cancelled) {
		return operation.ErrBarrierPending
	}
	allowed := false
	switch current {
	case operation.Pending:
		allowed = !active && update.Status == operation.Waiting
	case operation.Running:
		allowed = update.Status == operation.Running || update.Status == operation.Waiting || update.Status == operation.Cancelling || update.Status == operation.Succeeded || update.Status == operation.Failed
	case operation.Waiting:
		allowed = update.Status == operation.Waiting || update.Status == operation.Running || update.Status == operation.Cancelling || update.Status == operation.Failed
	case operation.Cancelling:
		allowed = update.Status == operation.Cancelling || update.Status == operation.Cancelled || update.Status == operation.Succeeded || update.Status == operation.Failed
	}
	if !allowed {
		return &AdmissionError{Code: InvalidRequest}
	}
	var last int64
	if err := tx.QueryRow(ctx, `SELECT COALESCE(max(sequence),0) FROM operation_progress WHERE operation_id=$1 AND execution_epoch=$2`, claim.OperationID, claim.ExecutionEpoch).Scan(&last); err != nil {
		return admissionError(err)
	}
	if update.Sequence <= last {
		return operation.ErrCommandConflict
	}
	if update.Status == operation.Succeeded {
		if err := completeLifecycle(ctx, tx, claim, plan); err != nil {
			return err
		}
		if _, _, _, err := lockClaim(ctx, tx, claim); err != nil {
			return err
		}
	}
	terminal := update.Status == operation.Succeeded || update.Status == operation.Failed || update.Status == operation.Cancelled
	if update.Status == operation.Failed || update.Status == operation.Cancelled {
		if err := settleReplicaRebuild(ctx, tx, claim, plan, false); err != nil {
			return err
		}
		if _, _, _, err := lockClaim(ctx, tx, claim); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `UPDATE cluster_members SET state='residual',updated_at=clock_timestamp()
 WHERE installation_id=$1 AND cluster_id=$2 AND role='replica' AND state='pending' AND removed_at IS NULL
 AND EXISTS (SELECT 1 FROM operations WHERE id=$3 AND kind IN ('cluster.scale','cluster.rebuild_replica'))`,
			claim.InstallationID, claim.ClusterID, claim.OperationID); err != nil {
			return admissionError(err)
		}
	}
	if _, err := tx.Exec(ctx, `INSERT INTO operation_progress (operation_id,execution_epoch,sequence,state,reason,unknown_effects) VALUES ($1,$2,$3,$4,$5,$6)`, claim.OperationID, claim.ExecutionEpoch, update.Sequence, update.Status, update.Reason, update.UnknownEffects); err != nil {
		return admissionError(err)
	}
	if _, err := tx.Exec(ctx, `UPDATE operations SET status=$1,updated_at=clock_timestamp(),finished_at=CASE WHEN $2 THEN clock_timestamp() ELSE NULL END,
		claim_active=CASE WHEN $2 THEN false ELSE claim_active END,failure_code=CASE WHEN $1='waiting' AND $3 THEN 'outcome_unknown' WHEN $1='failed' THEN $5 ELSE NULL END WHERE id=$4`, update.Status, terminal, update.UnknownEffects, claim.OperationID, update.FailureCode); err != nil {
		return admissionError(err)
	}
	return s.commitMetadata(ctx, tx)
}
