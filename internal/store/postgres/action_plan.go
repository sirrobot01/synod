package postgres

import (
	"context"
	"errors"
	"strings"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/sirrobot01/synod/internal/cluster"
	"github.com/sirrobot01/synod/internal/operation"
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
	var lifecycle, engine string
	if err := tx.QueryRow(ctx, `SELECT name,desired_revision,desired_lifecycle,(deleted_at IS NOT NULL OR desired_lifecycle='deleted'),engine
 FROM clusters WHERE installation_id=$1 AND id=$2 FOR UPDATE`, installationID, clusterID).
		Scan(&plan.ClusterName, &plan.ExpectedRevision, &lifecycle, &deleted, &engine); err != nil {
		return cluster.ActionPlan{}, admissionError(err)
	}
	if deleted {
		return cluster.ActionPlan{}, &AdmissionError{Code: NotFound}
	}
	if lifecycle != string(cluster.DesiredRunning) {
		return cluster.ActionPlan{}, &AdmissionError{Code: OperationConflict}
	}
	// Only Redis runs native replicas.
	if engine != "redis" {
		return cluster.ActionPlan{}, &AdmissionError{Code: UnsupportedSpec}
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
	if err := tx.QueryRow(ctx, `INSERT INTO cluster_action_plans
 (plan_token,installation_id,actor_id,cluster_id,cluster_name,expected_revision,action,data_disposition,retained_volumes,downtime,member_id,resource_id,created_at,expires_at)
 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,statement_timestamp(),statement_timestamp()+interval '5 minutes') RETURNING expires_at`,
		plan.PlanToken, installationID, actorID, clusterID, plan.ClusterName, plan.ExpectedRevision, plan.Action,
		plan.DataDisposition, plan.RetainedVolumes, plan.Downtime, plan.MemberID, nil).Scan(&plan.ExpiresAt); err != nil {
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

// PreviewMemberRemoval binds one named Patroni member and its retained data before a scale
// removes it. Patroni elects its primary, so the operator names the member. The plan proves
// the member is a replica before it removes anything.
func (s *Store) PreviewMemberRemoval(ctx context.Context, installationID, clusterID, memberID uuid.UUID, actorID string) (cluster.ActionPlan, error) {
	if installationID == uuid.Nil || clusterID == uuid.Nil || memberID == uuid.Nil || len(actorID) < 1 || len(actorID) > 256 || !utf8.ValidString(actorID) || strings.ContainsRune(actorID, 0) {
		return cluster.ActionPlan{}, &AdmissionError{Code: InvalidRequest}
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return cluster.ActionPlan{}, admissionError(err)
	}
	defer tx.Rollback(ctx)
	plan := cluster.ActionPlan{PlanToken: uuid.New(), ClusterID: clusterID, Action: "scale_down", DataDisposition: "retain", MemberID: memberID,
		Downtime: "The member proves it is a replica, stops, and leaves the cluster. The other members keep serving. Its data volume remains retained."}
	var deleted bool
	var lifecycle, topology string
	if err := tx.QueryRow(ctx, `SELECT name,desired_revision,desired_lifecycle,(deleted_at IS NOT NULL OR desired_lifecycle='deleted'),COALESCE(spec->'postgresql'->>'topology','')
 FROM clusters WHERE installation_id=$1 AND id=$2 FOR UPDATE`, installationID, clusterID).
		Scan(&plan.ClusterName, &plan.ExpectedRevision, &lifecycle, &deleted, &topology); err != nil {
		return cluster.ActionPlan{}, admissionError(err)
	}
	if deleted {
		return cluster.ActionPlan{}, &AdmissionError{Code: NotFound}
	}
	if topology != "patroni" {
		return cluster.ActionPlan{}, &AdmissionError{Code: UnsupportedSpec}
	}
	if lifecycle != string(cluster.DesiredRunning) {
		return cluster.ActionPlan{}, &AdmissionError{Code: OperationConflict}
	}
	var active bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM operations WHERE installation_id=$1 AND cluster_id=$2 AND status IN ('pending','running','waiting','cancelling'))`, installationID, clusterID).Scan(&active); err != nil {
		return cluster.ActionPlan{}, admissionError(err)
	}
	if active {
		return cluster.ActionPlan{}, &AdmissionError{Code: OperationConflict}
	}
	resource, err := patroniRemovalTarget(ctx, tx, installationID, clusterID, memberID)
	if err != nil {
		return cluster.ActionPlan{}, err
	}
	if plan.RetainedVolumes, err = memberVolumeIDs(ctx, tx, installationID, clusterID, resource); err != nil {
		return cluster.ActionPlan{}, err
	}
	if err := tx.QueryRow(ctx, `INSERT INTO cluster_action_plans
 (plan_token,installation_id,actor_id,cluster_id,cluster_name,expected_revision,action,data_disposition,retained_volumes,downtime,member_id,resource_id,created_at,expires_at)
 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,statement_timestamp(),statement_timestamp()+interval '5 minutes') RETURNING expires_at`,
		plan.PlanToken, installationID, actorID, clusterID, plan.ClusterName, plan.ExpectedRevision, plan.Action,
		plan.DataDisposition, plan.RetainedVolumes, plan.Downtime, plan.MemberID, nil).Scan(&plan.ExpiresAt); err != nil {
		return cluster.ActionPlan{}, admissionError(err)
	}
	if err := s.commitMetadata(ctx, tx); err != nil {
		return cluster.ActionPlan{}, err
	}
	return plan, nil
}

// patroniRemovalTarget answers the resource of a Patroni member that a removal may take: an
// active member of a cluster above the minimum size whose members are all active. When the
// member's node holds an etcd service, that service moves to a kept member's node that holds
// none, or an unfinished move resumes. Without either, the member cannot leave.
func patroniRemovalTarget(ctx context.Context, tx pgx.Tx, installationID, clusterID, memberID uuid.UUID) (uuid.UUID, error) {
	var members, active int
	var resource *uuid.UUID
	if err := tx.QueryRow(ctx, `SELECT count(*),count(*) FILTER (WHERE role='patroni' AND state='active'),
 (SELECT resource_id FROM cluster_members WHERE installation_id=$1 AND cluster_id=$2 AND member_id=$3 AND role='patroni' AND state='active' AND removed_at IS NULL)
 FROM cluster_members WHERE installation_id=$1 AND cluster_id=$2 AND removed_at IS NULL`, installationID, clusterID, memberID).Scan(&members, &active, &resource); err != nil {
		return uuid.Nil, admissionError(err)
	}
	if resource == nil {
		return uuid.Nil, &AdmissionError{Code: NotFound}
	}
	if members <= operation.MinPatroniMembers || active != members {
		return uuid.Nil, &AdmissionError{Code: OperationConflict}
	}
	// live counts the etcd services. onTarget reports one on the member's node. free counts the
	// kept members' nodes that hold none.
	var live, free int
	var onTarget bool
	if err := tx.QueryRow(ctx, `WITH etcd AS (SELECT a.node_id FROM etcd_members e
 JOIN resource_allocations a ON a.installation_id=e.installation_id AND a.cluster_id=e.cluster_id AND a.resource_id=e.resource_id AND a.released_at IS NULL
 WHERE e.installation_id=$1 AND e.cluster_id=$2),
 kept AS (SELECT a.node_id FROM cluster_members m
 JOIN resource_allocations a ON a.installation_id=m.installation_id AND a.cluster_id=m.cluster_id AND a.resource_id=m.resource_id AND a.released_at IS NULL
 WHERE m.installation_id=$1 AND m.cluster_id=$2 AND m.removed_at IS NULL AND m.resource_id<>$3)
 SELECT (SELECT count(*) FROM etcd),
 (SELECT count(*) FROM kept WHERE node_id NOT IN (SELECT node_id FROM etcd)),
 EXISTS(SELECT 1 FROM etcd JOIN resource_allocations t ON t.node_id=etcd.node_id
 WHERE t.installation_id=$1 AND t.cluster_id=$2 AND t.resource_id=$3 AND t.released_at IS NULL)`, installationID, clusterID, *resource).Scan(&live, &free, &onTarget); err != nil {
		return uuid.Nil, admissionError(err)
	}
	if onTarget && live <= 3 && free == 0 {
		return uuid.Nil, &AdmissionError{Code: OperationConflict}
	}
	return *resource, nil
}

// PreviewMemberRebuild binds one named Patroni member and its retained data before replacement.
//
// Patroni elects its own primary, so the operator names the failed member. The plan
// proves failure natively before it removes anything.
func (s *Store) PreviewMemberRebuild(ctx context.Context, installationID, clusterID, memberID uuid.UUID, actorID string) (cluster.ActionPlan, error) {
	if installationID == uuid.Nil || clusterID == uuid.Nil || memberID == uuid.Nil || len(actorID) < 1 || len(actorID) > 256 || !utf8.ValidString(actorID) || strings.ContainsRune(actorID, 0) {
		return cluster.ActionPlan{}, &AdmissionError{Code: InvalidRequest}
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return cluster.ActionPlan{}, admissionError(err)
	}
	defer tx.Rollback(ctx)
	plan := cluster.ActionPlan{PlanToken: uuid.New(), ClusterID: clusterID, Action: "rebuild_member", DataDisposition: "retain", MemberID: memberID,
		Downtime: "Prove that the member does not serve, remove its container, and create a fresh member on the same node. The other two members keep serving. Keep the old volume. Reserve extra capacity on that node for the new member and volume."}
	var deleted bool
	var lifecycle, topology string
	if err := tx.QueryRow(ctx, `SELECT name,desired_revision,desired_lifecycle,(deleted_at IS NOT NULL OR desired_lifecycle='deleted'),COALESCE(spec->'postgresql'->>'topology','')
 FROM clusters WHERE installation_id=$1 AND id=$2 FOR UPDATE`, installationID, clusterID).
		Scan(&plan.ClusterName, &plan.ExpectedRevision, &lifecycle, &deleted, &topology); err != nil {
		return cluster.ActionPlan{}, admissionError(err)
	}
	if deleted {
		return cluster.ActionPlan{}, &AdmissionError{Code: NotFound}
	}
	if topology != "patroni" {
		return cluster.ActionPlan{}, &AdmissionError{Code: UnsupportedSpec}
	}
	if lifecycle != string(cluster.DesiredRunning) {
		return cluster.ActionPlan{}, &AdmissionError{Code: OperationConflict}
	}
	var active bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM operations WHERE installation_id=$1 AND cluster_id=$2 AND status IN ('pending','running','waiting','cancelling'))`, installationID, clusterID).Scan(&active); err != nil {
		return cluster.ActionPlan{}, admissionError(err)
	}
	if active {
		return cluster.ActionPlan{}, &AdmissionError{Code: OperationConflict}
	}
	var residual bool
	plan.ResourceID, residual, err = patroniRebuildTarget(ctx, tx, installationID, clusterID, memberID)
	if err != nil {
		return cluster.ActionPlan{}, err
	}
	if residual {
		history, err := patroniStartHistory(ctx, tx, installationID, clusterID, plan.ResourceID)
		if err != nil {
			return cluster.ActionPlan{}, err
		}
		plan.Downtime = "Resume the member left by a failed rebuild with its own storage. The two active members keep serving."
		if !history.Resume() {
			plan.Downtime = "Stop the member left by a failed rebuild, prove that its storage failed, and remove its container. Create a fresh member on the same node. The two active members keep serving. Keep the old volume. Reserve extra capacity on that node."
		}
	}
	plan.RetainedVolumes, err = memberVolumeIDs(ctx, tx, installationID, clusterID, plan.ResourceID)
	if err != nil {
		return cluster.ActionPlan{}, err
	}
	if err := tx.QueryRow(ctx, `INSERT INTO cluster_action_plans
 (plan_token,installation_id,actor_id,cluster_id,cluster_name,expected_revision,action,data_disposition,retained_volumes,downtime,member_id,resource_id,created_at,expires_at)
 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,statement_timestamp(),statement_timestamp()+interval '5 minutes') RETURNING expires_at`,
		plan.PlanToken, installationID, actorID, clusterID, plan.ClusterName, plan.ExpectedRevision, plan.Action,
		plan.DataDisposition, plan.RetainedVolumes, plan.Downtime, plan.MemberID, plan.ResourceID).Scan(&plan.ExpiresAt); err != nil {
		return cluster.ActionPlan{}, admissionError(err)
	}
	if err := s.commitMetadata(ctx, tx); err != nil {
		return cluster.ActionPlan{}, err
	}
	return plan, nil
}

// patroniRebuildTarget returns the named member's resource and whether it is residual.
//
// It accepts three active Patroni members with an active target, or two active members and
// one residual target that a failed member rebuild created. Other memberships hold work
// that a rebuild must not hide.
func patroniRebuildTarget(ctx context.Context, tx pgx.Tx, installationID, clusterID, memberID uuid.UUID) (uuid.UUID, bool, error) {
	rows, err := tx.Query(ctx, `SELECT m.member_id,m.resource_id,m.role,m.state,COALESCE(o.kind,'') FROM cluster_members m
 LEFT JOIN operations o ON o.installation_id=m.installation_id AND o.id=m.created_operation_id
 WHERE m.installation_id=$1 AND m.cluster_id=$2 AND m.removed_at IS NULL ORDER BY m.member_id FOR UPDATE OF m`, installationID, clusterID)
	if err != nil {
		return uuid.Nil, false, admissionError(err)
	}
	defer rows.Close()
	var target uuid.UUID
	var targetState string
	active, residual := 0, 0
	for rows.Next() {
		var member, resource uuid.UUID
		var role, state, kind string
		if err := rows.Scan(&member, &resource, &role, &state, &kind); err != nil {
			return uuid.Nil, false, admissionError(err)
		}
		switch {
		case role == "patroni" && state == "active":
			active++
		case role == "patroni" && state == "residual" && kind == "cluster.rebuild_member":
			residual++
		default:
			return uuid.Nil, false, &AdmissionError{Code: OperationConflict}
		}
		if member == memberID {
			target, targetState = resource, state
		}
	}
	if err := rows.Err(); err != nil {
		return uuid.Nil, false, admissionError(err)
	}
	if active+residual < operation.MinPatroniMembers || active+residual > operation.MaxPatroniMembers || residual > 1 {
		return uuid.Nil, false, &AdmissionError{Code: OperationConflict}
	}
	if target == uuid.Nil {
		return uuid.Nil, false, &AdmissionError{Code: NotFound}
	}
	// With a residual member, only that member can be rebuilt.
	if residual == 1 && targetState != "residual" {
		return uuid.Nil, false, &AdmissionError{Code: OperationConflict}
	}
	return target, residual == 1, nil
}
