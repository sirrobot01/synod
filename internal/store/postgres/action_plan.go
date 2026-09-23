package postgres

import (
	"context"
	"errors"
	"strings"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/sirrobot01/synod/internal/cluster"
)

// PreviewClusterDelete records an actor-bound plan without changing the cluster.
func (s *Store) PreviewClusterDelete(ctx context.Context, installationID, clusterID uuid.UUID, actorID string) (cluster.ActionPlan, error) {
	if installationID == uuid.Nil || clusterID == uuid.Nil || len(actorID) < 1 || len(actorID) > 256 || !utf8.ValidString(actorID) || strings.ContainsRune(actorID, 0) {
		return cluster.ActionPlan{}, &AdmissionError{Code: InvalidRequest}
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return cluster.ActionPlan{}, admissionError(err)
	}
	defer tx.Rollback(ctx)
	plan := cluster.ActionPlan{PlanToken: uuid.New(), ClusterID: clusterID, Action: "delete", DataDisposition: "retain", Downtime: "Database access stops. Data volumes remain retained."}
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
	plan.RetainedVolumes, err = retainedVolumeIDs(ctx, tx, installationID, clusterID)
	if err != nil {
		return cluster.ActionPlan{}, err
	}
	if err := tx.QueryRow(ctx, `INSERT INTO cluster_action_plans
 (plan_token,installation_id,actor_id,cluster_id,cluster_name,expected_revision,action,data_disposition,retained_volumes,downtime,member_id,created_at,expires_at)
 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,NULL,statement_timestamp(),statement_timestamp()+interval '5 minutes') RETURNING expires_at`,
		plan.PlanToken, installationID, actorID, clusterID, plan.ClusterName, plan.ExpectedRevision, plan.Action, plan.DataDisposition, plan.RetainedVolumes, plan.Downtime).Scan(&plan.ExpiresAt); err != nil {
		return cluster.ActionPlan{}, admissionError(err)
	}
	if err := s.commitMetadata(ctx, tx); err != nil {
		return cluster.ActionPlan{}, err
	}
	return plan, nil
}

// retainedVolumeIDs reads the storage covered by a retained-data deletion.
func retainedVolumeIDs(ctx context.Context, tx pgx.Tx, installationID, clusterID uuid.UUID) ([]uuid.UUID, error) {
	rows, err := tx.Query(ctx, `SELECT resource_id,retention FROM volume_identities WHERE installation_id=$1 AND cluster_id=$2 AND retention<>'purged' ORDER BY resource_id FOR SHARE`, installationID, clusterID)
	if err != nil {
		return nil, admissionError(err)
	}
	defer rows.Close()
	volumes := make([]uuid.UUID, 0)
	for rows.Next() {
		var id uuid.UUID
		var retention string
		if err := rows.Scan(&id, &retention); err != nil {
			return nil, admissionError(err)
		}
		if retention != string(Retained) {
			return nil, &AdmissionError{Code: OperationConflict}
		}
		volumes = append(volumes, id)
		if len(volumes) > maxVolumeIdentities {
			return nil, ErrSnapshotTooLarge
		}
	}
	if err := rows.Err(); err != nil {
		return nil, admissionError(err)
	}
	return volumes, nil
}

// PreviewClusterScaleDown shows the member a removal would take away.
//
// A removal is destructive, so specification 33 requires showing it before it runs. The
// plan names the exact member, and the confirmation must carry the same token, so a
// review of one member can never authorize removing another.
func (s *Store) PreviewClusterScaleDown(ctx context.Context, installationID, clusterID uuid.UUID, actorID string) (cluster.ActionPlan, error) {
	return s.previewReplicaChange(ctx, installationID, clusterID, actorID, "scale_down")
}

// PreviewFailedReplicaRemoval requires native failure proof before removal can run.
func (s *Store) PreviewFailedReplicaRemoval(ctx context.Context, installationID, clusterID uuid.UUID, actorID string) (cluster.ActionPlan, error) {
	return s.previewReplicaChange(ctx, installationID, clusterID, actorID, "scale_down_failed")
}

// PreviewReplicaRebuild binds a failed replica and its retained data before replacement.
func (s *Store) PreviewReplicaRebuild(ctx context.Context, installationID, clusterID uuid.UUID, actorID string) (cluster.ActionPlan, error) {
	return s.previewReplicaChange(ctx, installationID, clusterID, actorID, "rebuild_replica")
}

func (s *Store) previewReplicaChange(ctx context.Context, installationID, clusterID uuid.UUID, actorID string, action string) (cluster.ActionPlan, error) {
	if installationID == uuid.Nil || clusterID == uuid.Nil || len(actorID) < 1 || len(actorID) > 256 || !utf8.ValidString(actorID) || strings.ContainsRune(actorID, 0) {
		return cluster.ActionPlan{}, &AdmissionError{Code: InvalidRequest}
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return cluster.ActionPlan{}, admissionError(err)
	}
	defer tx.Rollback(ctx)
	plan := cluster.ActionPlan{PlanToken: uuid.New(), ClusterID: clusterID, Action: "scale_down", DataDisposition: "retain",
		Downtime: "The replica stops serving. The primary keeps serving and the replica's data volume remains retained."}
	var deleted bool
	var lifecycle, engine, topology, version, replicationPolicy string
	if err := tx.QueryRow(ctx, `SELECT name,desired_revision,desired_lifecycle,(deleted_at IS NOT NULL OR desired_lifecycle='deleted'),engine,COALESCE(spec->'postgresql'->>'topology',''),COALESCE(spec->'postgresql'->>'version',''),COALESCE(spec->'postgresql'->>'replication_policy','')
 FROM clusters WHERE installation_id=$1 AND id=$2 FOR UPDATE`, installationID, clusterID).
		Scan(&plan.ClusterName, &plan.ExpectedRevision, &lifecycle, &deleted, &engine, &topology, &version, &replicationPolicy); err != nil {
		return cluster.ActionPlan{}, admissionError(err)
	}
	if deleted {
		return cluster.ActionPlan{}, &AdmissionError{Code: NotFound}
	}
	if lifecycle != string(cluster.DesiredRunning) {
		return cluster.ActionPlan{}, &AdmissionError{Code: OperationConflict}
	}
	if action != "scale_down" {
		if engine != "postgres" || topology != "standalone" || version != "17.11" {
			return cluster.ActionPlan{}, &AdmissionError{Code: UnsupportedSpec}
		}
		plan.Action = action
		plan.Downtime = "Remove the replica only after native failure is proved. Keep its data volume. A later replacement uses a new volume. The primary keeps serving."
	}
	if action == "rebuild_replica" {
		var replicas int
		if err := tx.QueryRow(ctx, `SELECT count(*) FROM cluster_members WHERE installation_id=$1 AND cluster_id=$2 AND role='replica' AND removed_at IS NULL`, installationID, clusterID).Scan(&replicas); err != nil {
			return cluster.ActionPlan{}, admissionError(err)
		}
		if replicas != 1 {
			return cluster.ActionPlan{}, &AdmissionError{Code: OperationConflict}
		}
		plan.Downtime = "Prove native replica failure, remove its container, and create a fresh copy from the primary. Keep the old volume. Reserve extra capacity for the new member and volume. The primary keeps serving reads."
	}
	if replicationPolicy == "synchronous" {
		plan.Downtime += " The primary keeps serving reads. Commits wait while no synchronous replica is connected."
	}
	var active bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM operations WHERE installation_id=$1 AND cluster_id=$2 AND status IN ('pending','running','waiting','cancelling'))`, installationID, clusterID).Scan(&active); err != nil {
		return cluster.ActionPlan{}, admissionError(err)
	}
	if active {
		return cluster.ActionPlan{}, &AdmissionError{Code: OperationConflict}
	}
	// The oldest replica is the one a removal takes. A cluster keeps its primary, so
	// the primary is never a candidate.
	var resource uuid.UUID
	err = tx.QueryRow(ctx, `SELECT member_id,resource_id FROM cluster_members
 WHERE installation_id=$1 AND cluster_id=$2 AND role='replica' AND removed_at IS NULL
 ORDER BY created_at, member_id LIMIT 1 FOR UPDATE`, installationID, clusterID).Scan(&plan.MemberID, &resource)
	if errors.Is(err, pgx.ErrNoRows) {
		return cluster.ActionPlan{}, &AdmissionError{Code: InvalidRequest}
	}
	if err != nil {
		return cluster.ActionPlan{}, admissionError(err)
	}
	plan.RetainedVolumes, err = memberVolumeIDs(ctx, tx, installationID, clusterID, resource)
	if err != nil {
		return cluster.ActionPlan{}, err
	}
	var plannedResource *uuid.UUID
	if action != "scale_down" {
		plan.ResourceID = resource
		plannedResource = &resource
	}
	if err := tx.QueryRow(ctx, `INSERT INTO cluster_action_plans
 (plan_token,installation_id,actor_id,cluster_id,cluster_name,expected_revision,action,data_disposition,retained_volumes,downtime,member_id,resource_id,created_at,expires_at)
 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,statement_timestamp(),statement_timestamp()+interval '5 minutes') RETURNING expires_at`,
		plan.PlanToken, installationID, actorID, clusterID, plan.ClusterName, plan.ExpectedRevision, plan.Action,
		plan.DataDisposition, plan.RetainedVolumes, plan.Downtime, plan.MemberID, plannedResource).Scan(&plan.ExpiresAt); err != nil {
		return cluster.ActionPlan{}, admissionError(err)
	}
	if err := s.commitMetadata(ctx, tx); err != nil {
		return cluster.ActionPlan{}, err
	}
	return plan, nil
}

// memberVolumeIDs reads the storage one member's reservation owns.
func memberVolumeIDs(ctx context.Context, tx pgx.Tx, installationID, clusterID, resource uuid.UUID) ([]uuid.UUID, error) {
	rows, err := tx.Query(ctx, `SELECT v.resource_id FROM volume_identities v
 JOIN allocation_resources r ON r.installation_id=v.installation_id AND r.resource_id=v.resource_id
 JOIN resource_allocations a ON a.installation_id=r.installation_id AND a.id=r.allocation_id
 WHERE v.installation_id=$1 AND v.cluster_id=$2 AND v.retention<>'purged' AND a.resource_id=$3
 ORDER BY v.resource_id FOR SHARE OF v`, installationID, clusterID, resource)
	if err != nil {
		return nil, admissionError(err)
	}
	defer rows.Close()
	found := []uuid.UUID{}
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			return nil, admissionError(err)
		}
		found = append(found, id)
	}
	if rows.Err() != nil {
		return nil, admissionError(rows.Err())
	}
	return found, nil
}
