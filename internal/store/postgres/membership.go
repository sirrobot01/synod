package postgres

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/sirrobot01/synod/internal/operation"
	"github.com/sirrobot01/synod/internal/placement"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/proto"
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
	// NodeID and Address name where the member's live reservation runs and the client address
	// its node declares. They are empty when the member has no live reservation.
	NodeID  uuid.UUID
	Address string
}

// ReadClusterMembers lists the members a cluster still has, oldest first.
func (s *Store) ReadClusterMembers(ctx context.Context, installationID, clusterID uuid.UUID) ([]Member, error) {
	if installationID == uuid.Nil || clusterID == uuid.Nil {
		return nil, &AdmissionError{Code: InvalidRequest}
	}
	rows, err := s.pool.Query(ctx, `SELECT m.member_id,m.resource_id,m.role,m.state,m.created_operation_id,m.created_revision,m.removed_at,
 COALESCE(a.node_id,'00000000-0000-0000-0000-000000000000'::uuid),COALESCE(convert_from(p.policy_bytes,'UTF8')::jsonb->>'client_address','')
 FROM cluster_members m
 LEFT JOIN resource_allocations a ON a.installation_id=m.installation_id AND a.cluster_id=m.cluster_id AND a.resource_id=m.resource_id AND a.released_at IS NULL
 LEFT JOIN node_placement_policies p ON p.installation_id=a.installation_id AND p.node_id=a.node_id
 WHERE m.installation_id=$1 AND m.cluster_id=$2 AND m.removed_at IS NULL
 ORDER BY m.created_at, m.member_id`, installationID, clusterID)
	if err != nil {
		return nil, admissionError(err)
	}
	defer rows.Close()
	var members []Member
	for rows.Next() {
		var member Member
		if err := rows.Scan(&member.MemberID, &member.ResourceID, &member.Role, &member.State,
			&member.CreatedByOperation, &member.CreatedRevision, &member.RemovedAt, &member.NodeID, &member.Address); err != nil {
			return nil, admissionError(err)
		}
		members = append(members, member)
	}
	if rows.Err() != nil {
		return nil, admissionError(rows.Err())
	}
	return members, nil
}

// recordMembers names one member for every instance the placement reserved, and records
// every etcd service beside a Patroni member.
//
// It runs inside the placement transaction, so a member and the capacity it runs on
// commit together. A placement that rolls back leaves no member behind.
//
// The resource identifies the member. A later operation can place the same resource
// again, for example to grow its budget, and that is the same member with a new
// reservation. Naming it again would give one cluster a second primary.
func recordMembers(ctx context.Context, tx pgx.Tx, claim operation.Claim, request placement.Request) error {
	for _, resource := range request.Resources {
		if resource.Role == "etcd" {
			if _, err := tx.Exec(ctx, `INSERT INTO etcd_members (installation_id,cluster_id,resource_id,created_operation_id)
 VALUES ($1,$2,$3,$4) ON CONFLICT (installation_id,resource_id) DO NOTHING`,
				claim.InstallationID, claim.ClusterID, resource.ID, claim.OperationID); err != nil {
				return admissionError(err)
			}
			continue
		}
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

// EtcdMember is one etcd service of a Patroni cluster and the node its live reservation holds.
type EtcdMember struct {
	ResourceID uuid.UUID
	NodeID     uuid.UUID
}

// ReadEtcdMembers lists the etcd members that still hold a reservation, oldest first.
func (s *Store) ReadEtcdMembers(ctx context.Context, installationID, clusterID uuid.UUID) ([]EtcdMember, error) {
	if installationID == uuid.Nil || clusterID == uuid.Nil {
		return nil, &AdmissionError{Code: InvalidRequest}
	}
	rows, err := s.pool.Query(ctx, `SELECT e.resource_id,a.node_id FROM etcd_members e
 JOIN resource_allocations a ON a.installation_id=e.installation_id AND a.cluster_id=e.cluster_id AND a.resource_id=e.resource_id AND a.released_at IS NULL
 JOIN operations o ON o.installation_id=e.installation_id AND o.id=e.created_operation_id
 WHERE e.installation_id=$1 AND e.cluster_id=$2 ORDER BY o.created_at, e.resource_id`, installationID, clusterID)
	if err != nil {
		return nil, admissionError(err)
	}
	defer rows.Close()
	var members []EtcdMember
	for rows.Next() {
		var member EtcdMember
		if err := rows.Scan(&member.ResourceID, &member.NodeID); err != nil {
			return nil, admissionError(err)
		}
		members = append(members, member)
	}
	if rows.Err() != nil {
		return nil, admissionError(rows.Err())
	}
	return members, nil
}

// ReadEtcdContainer returns the container configuration of one etcd member from the plan
// that created it. A renewal changes only the certificate version, which it reads itself.
func (s *Store) ReadEtcdContainer(ctx context.Context, installationID, clusterID, resourceID uuid.UUID) (*agentv1.EnsureContainer, error) {
	if installationID == uuid.Nil || clusterID == uuid.Nil || resourceID == uuid.Nil {
		return nil, &AdmissionError{Code: InvalidRequest}
	}
	var planBytes []byte
	if err := s.pool.QueryRow(ctx, `SELECT o.plan_bytes FROM etcd_members e
 JOIN operations o ON o.installation_id=e.installation_id AND o.cluster_id=e.cluster_id AND o.id=e.created_operation_id
 WHERE e.installation_id=$1 AND e.cluster_id=$2 AND e.resource_id=$3`, installationID, clusterID, resourceID).Scan(&planBytes); err != nil {
		return nil, admissionError(err)
	}
	var plan agentv1.ExecutionPlan
	if proto.Unmarshal(planBytes, &plan) != nil {
		return nil, &AdmissionError{Code: DependencyUnavailable}
	}
	for _, step := range plan.Steps {
		if container := step.Command.GetInput().GetEnsureContainer(); container.GetEtcd() != nil && step.Command.Input.ResourceId == resourceID.String() {
			return proto.Clone(container).(*agentv1.EnsureContainer), nil
		}
	}
	return nil, &AdmissionError{Code: NotFound}
}

// ReadReplicaActionTarget answers the member and the action of the reviewed plan that one
// admitted replica removal or member rebuild named.
//
// The operation carries the reviewed plan, so the planner removes the member the
// operator saw rather than choosing one again when the plan runs.
func (s *Store) ReadReplicaActionTarget(ctx context.Context, installationID, operationID uuid.UUID) (uuid.UUID, string, error) {
	var member uuid.UUID
	var action string
	err := s.pool.QueryRow(ctx, `SELECT p.member_id,p.action FROM cluster_action_plans p
 JOIN operations o ON o.installation_id=p.installation_id AND o.cluster_id=p.cluster_id
 AND o.actor_id=p.actor_id AND o.action_plan_token=p.plan_token
 AND o.desired_revision=p.expected_revision+1
 WHERE p.installation_id=$1 AND o.id=$2 AND p.action IN ('scale_down','rebuild_member')`, installationID, operationID).Scan(&member, &action)
	if err != nil {
		return uuid.Nil, "", admissionError(err)
	}
	return member, action, nil
}

// ReadMemberContainer returns the current container configuration of one live member. It is
// in the plan that created the member, or in the plan of the renewal that last recreated it.
// A Patroni start must repeat that exact configuration.
func (s *Store) ReadMemberContainer(ctx context.Context, installationID, clusterID, resourceID uuid.UUID) (*agentv1.EnsureContainer, error) {
	if installationID == uuid.Nil || clusterID == uuid.Nil || resourceID == uuid.Nil {
		return nil, &AdmissionError{Code: InvalidRequest}
	}
	var planBytes []byte
	if err := s.pool.QueryRow(ctx, `SELECT o.plan_bytes FROM cluster_members m
 JOIN operations o ON o.installation_id=m.installation_id AND o.cluster_id=m.cluster_id AND o.id=COALESCE(m.container_operation_id,m.created_operation_id)
 WHERE m.installation_id=$1 AND m.cluster_id=$2 AND m.resource_id=$3 AND m.removed_at IS NULL`,
		installationID, clusterID, resourceID).Scan(&planBytes); err != nil {
		return nil, admissionError(err)
	}
	var plan agentv1.ExecutionPlan
	if proto.Unmarshal(planBytes, &plan) != nil {
		return nil, &AdmissionError{Code: DependencyUnavailable}
	}
	for _, step := range plan.Steps {
		container := step.Command.GetInput().GetEnsureContainer()
		if recreate := step.Command.GetInput().GetRecreatePatroniMember(); recreate != nil {
			container = recreate.Next
		}
		if container != nil && step.Command.Input.ResourceId == resourceID.String() {
			return proto.Clone(container).(*agentv1.EnsureContainer), nil
		}
	}
	return nil, &AdmissionError{Code: NotFound}
}
