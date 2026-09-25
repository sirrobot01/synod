package postgres

import (
	"context"

	"github.com/google/uuid"

	"github.com/jackc/pgx/v5"
	"github.com/sirrobot01/synod/internal/operation"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
)

// checkPlanCredentials proves that the transaction holds each exact authorized secret reference.
// A member credential serves the member and every resource its live reservation owns on the
// same node, such as the member's pooler and router.
func checkPlanCredentials(ctx context.Context, tx pgx.Tx, claim operation.Claim, plan *agentv1.ExecutionPlan) error {
	for _, step := range plan.Steps {
		for _, reference := range step.Command.Input.Secrets {
			var found bool
			if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM cluster_credentials c
 WHERE c.installation_id=$1 AND c.cluster_id=$2 AND c.credential_id=$3 AND c.version=$4 AND c.purpose=$5
 AND (c.member_id IS NULL OR EXISTS(SELECT 1 FROM resource_allocations a
 JOIN allocation_resources r ON r.installation_id=a.installation_id AND r.allocation_id=a.id
 WHERE a.installation_id=c.installation_id AND a.cluster_id=c.cluster_id AND a.resource_id=c.member_id
 AND a.node_id=$7 AND a.released_at IS NULL AND r.resource_id=$6)))`, claim.InstallationID, claim.ClusterID,
				reference.CredentialId, reference.Version, reference.Purpose, step.Command.Input.ResourceId, step.Command.Epoch.NodeId).Scan(&found); err != nil {
				return admissionError(err)
			}
			if !found {
				return ErrCredentialConflict
			}
		}
	}
	return nil
}

// MemberOwnsResource reports whether a member's live reservation owns a resource on a node.
// A member credential may serve that resource.
func (s *Store) MemberOwnsResource(ctx context.Context, installationID, clusterID, member, resource, node uuid.UUID) (bool, error) {
	if installationID == uuid.Nil || clusterID == uuid.Nil || member == uuid.Nil || resource == uuid.Nil || node == uuid.Nil {
		return false, &AdmissionError{Code: InvalidRequest}
	}
	var owned bool
	if err := s.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM resource_allocations a
 JOIN allocation_resources r ON r.installation_id=a.installation_id AND r.allocation_id=a.id
 WHERE a.installation_id=$1 AND a.cluster_id=$2 AND a.resource_id=$3 AND a.node_id=$5 AND a.released_at IS NULL AND r.resource_id=$4)`,
		installationID, clusterID, member, resource, node).Scan(&owned); err != nil {
		return false, admissionError(err)
	}
	return owned, nil
}
