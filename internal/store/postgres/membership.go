package postgres

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/sirrobot01/synod/internal/operation"
	"github.com/sirrobot01/synod/internal/placement"
)

// Member is one named instance of a cluster.
//
// The identity outlives the reservation. A delete releases capacity and ports, and a
// scale down sets RemovedAt, but the row stays. A later add therefore cannot reuse a
// name whose data a node may still hold.
type Member struct {
	MemberID           uuid.UUID
	ResourceID         uuid.UUID
	Role               string
	State              string
	CreatedByOperation uuid.UUID
	CreatedRevision    int64
	RemovedAt          *time.Time
}

// ReadClusterMembers lists the members a cluster still has, oldest first.
func (s *Store) ReadClusterMembers(ctx context.Context, installationID, clusterID uuid.UUID) ([]Member, error) {
	if installationID == uuid.Nil || clusterID == uuid.Nil {
		return nil, &AdmissionError{Code: InvalidRequest}
	}
	rows, err := s.pool.Query(ctx, `SELECT member_id,resource_id,role,state,created_operation_id,created_revision,removed_at
 FROM cluster_members WHERE installation_id=$1 AND cluster_id=$2 AND removed_at IS NULL
 ORDER BY created_at, member_id`, installationID, clusterID)
	if err != nil {
		return nil, admissionError(err)
	}
	defer rows.Close()
	var members []Member
	for rows.Next() {
		var member Member
		if err := rows.Scan(&member.MemberID, &member.ResourceID, &member.Role, &member.State,
			&member.CreatedByOperation, &member.CreatedRevision, &member.RemovedAt); err != nil {
			return nil, admissionError(err)
		}
		members = append(members, member)
	}
	if rows.Err() != nil {
		return nil, admissionError(rows.Err())
	}
	return members, nil
}

// recordMembers names one member for every instance the placement reserved.
//
// It runs inside the placement transaction, so a member and the capacity it runs on
// commit together. A placement that rolls back leaves no member behind.
//
// The resource identifies the member. A later operation can place the same resource
// again, for example to grow its budget, and that is the same member with a new
// reservation. Naming it again would give one cluster a second primary.
func recordMembers(ctx context.Context, tx pgx.Tx, claim operation.Claim, request placement.Request) error {
	for _, resource := range request.Resources {
		if resource.Kind != "instance" {
			continue
		}
		if _, err := tx.Exec(ctx, `INSERT INTO cluster_members
 (installation_id,cluster_id,member_id,role,resource_id,state,created_operation_id,created_revision)
 VALUES ($1,$2,$3,$4,$5,'pending',$6,$7)
 ON CONFLICT (installation_id,resource_id) DO NOTHING`,
			claim.InstallationID, claim.ClusterID, uuid.New(), resource.Role, resource.ID,
			claim.OperationID, request.DesiredRevision); err != nil {
			return admissionError(err)
		}
	}
	return nil
}

// ReadReplicaActionTarget answers the member and proof mode one admitted replica removal or rebuild named.
//
// The operation carries the reviewed plan, so the planner removes the member the
// operator saw rather than choosing one again when the plan runs.
func (s *Store) ReadReplicaActionTarget(ctx context.Context, installationID, operationID uuid.UUID) (uuid.UUID, bool, error) {
	var member uuid.UUID
	var failed bool
	err := s.pool.QueryRow(ctx, `SELECT p.member_id,p.action IN ('scale_down_failed','rebuild_replica') FROM cluster_action_plans p
 JOIN operations o ON o.installation_id=p.installation_id AND o.cluster_id=p.cluster_id
 AND o.actor_id=p.actor_id AND o.action_plan_token=p.plan_token
 AND o.desired_revision=p.expected_revision+1
 WHERE p.installation_id=$1 AND o.id=$2 AND p.action IN ('scale_down','scale_down_failed','rebuild_replica')`, installationID, operationID).Scan(&member, &failed)
	if err != nil {
		return uuid.Nil, false, admissionError(err)
	}
	return member, failed, nil
}
