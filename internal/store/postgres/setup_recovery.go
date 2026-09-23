package postgres

import (
	"context"
	"errors"
	"slices"
	"strings"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/sirrobot01/synod/internal/cluster"
)

// PreviewSetupRecovery binds operator intent without claiming current native failure.
func (s *Store) PreviewSetupRecovery(ctx context.Context, installationID, clusterID uuid.UUID, actorID string) (cluster.ActionPlan, error) {
	if installationID == uuid.Nil || clusterID == uuid.Nil || len(actorID) < 1 || len(actorID) > 256 || !utf8.ValidString(actorID) || strings.ContainsRune(actorID, 0) {
		return cluster.ActionPlan{}, &AdmissionError{Code: InvalidRequest}
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return cluster.ActionPlan{}, admissionError(err)
	}
	defer tx.Rollback(ctx)
	plan := cluster.ActionPlan{PlanToken: uuid.New(), ClusterID: clusterID, Action: "recover_setup", DataDisposition: "retain", Downtime: "Recovery requires confirmed failed setup. It preserves data and credentials, then restarts the existing member. Access requires final application readiness."}
	var deleted bool
	if err := tx.QueryRow(ctx, `SELECT name,desired_revision,(deleted_at IS NOT NULL OR desired_lifecycle='deleted') FROM clusters WHERE installation_id=$1 AND id=$2 FOR UPDATE`, installationID, clusterID).Scan(&plan.ClusterName, &plan.ExpectedRevision, &deleted); err != nil {
		return cluster.ActionPlan{}, admissionError(err)
	}
	if deleted {
		return cluster.ActionPlan{}, &AdmissionError{Code: NotFound}
	}
	var active bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM operations WHERE installation_id=$1 AND cluster_id=$2 AND status IN ('pending','running','waiting','cancelling'))`, installationID, clusterID).Scan(&active); err != nil {
		return cluster.ActionPlan{}, admissionError(err)
	}
	if active {
		return cluster.ActionPlan{}, &AdmissionError{Code: OperationConflict}
	}
	plan.MemberID, plan.ResourceID, plan.RetainedVolumes, err = setupRecoveryTarget(ctx, tx, installationID, clusterID)
	if err != nil {
		return cluster.ActionPlan{}, err
	}
	if err := tx.QueryRow(ctx, `INSERT INTO cluster_action_plans
 (plan_token,installation_id,actor_id,cluster_id,cluster_name,expected_revision,action,data_disposition,retained_volumes,downtime,member_id,resource_id,created_at,expires_at)
 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,statement_timestamp(),statement_timestamp()+interval '5 minutes') RETURNING expires_at`, plan.PlanToken, installationID, actorID, clusterID, plan.ClusterName, plan.ExpectedRevision, plan.Action, plan.DataDisposition, plan.RetainedVolumes, plan.Downtime, plan.MemberID, plan.ResourceID).Scan(&plan.ExpiresAt); err != nil {
		return cluster.ActionPlan{}, admissionError(err)
	}
	if err := s.commitMetadata(ctx, tx); err != nil {
		return cluster.ActionPlan{}, err
	}
	return plan, nil
}

func setupRecoveryTarget(ctx context.Context, tx pgx.Tx, installationID, clusterID uuid.UUID) (uuid.UUID, uuid.UUID, []uuid.UUID, error) {
	var engine, topology, lifecycle string
	var replicas int
	if err := tx.QueryRow(ctx, `SELECT engine,COALESCE(spec->'postgresql'->>'topology',''),desired_lifecycle,desired_replicas FROM clusters WHERE installation_id=$1 AND id=$2`, installationID, clusterID).Scan(&engine, &topology, &lifecycle, &replicas); err != nil {
		return uuid.Nil, uuid.Nil, nil, admissionError(err)
	}
	if engine != "postgres" || topology != "standalone" || replicas != 0 {
		return uuid.Nil, uuid.Nil, nil, &AdmissionError{Code: UnsupportedSpec}
	}
	if lifecycle != "running" {
		return uuid.Nil, uuid.Nil, nil, &AdmissionError{Code: OperationConflict}
	}
	var member, resource uuid.UUID
	err := tx.QueryRow(ctx, `SELECT m.member_id,m.resource_id FROM cluster_members m
 JOIN resource_allocations a ON a.installation_id=m.installation_id AND a.cluster_id=m.cluster_id AND a.resource_id=m.resource_id
 WHERE m.installation_id=$1 AND m.cluster_id=$2 AND m.role='primary' AND m.removed_at IS NULL AND a.released_at IS NULL
 AND (SELECT count(*) FROM cluster_members WHERE installation_id=$1 AND cluster_id=$2 AND removed_at IS NULL)=1
 AND (SELECT count(*) FROM resource_allocations WHERE installation_id=$1 AND cluster_id=$2 AND released_at IS NULL)=1
 FOR UPDATE OF m,a`, installationID, clusterID).Scan(&member, &resource)
	if errors.Is(err, pgx.ErrNoRows) {
		return uuid.Nil, uuid.Nil, nil, &AdmissionError{Code: OperationConflict}
	}
	if err != nil {
		return uuid.Nil, uuid.Nil, nil, admissionError(err)
	}
	volumes, err := retainedVolumeIDs(ctx, tx, installationID, clusterID)
	if err != nil {
		return uuid.Nil, uuid.Nil, nil, err
	}
	owned, err := memberVolumeIDs(ctx, tx, installationID, clusterID, resource)
	if err != nil {
		return uuid.Nil, uuid.Nil, nil, err
	}
	if len(volumes) != 1 || !slices.Equal(volumes, owned) {
		return uuid.Nil, uuid.Nil, nil, &AdmissionError{Code: OperationConflict}
	}
	return member, resource, volumes, nil
}
