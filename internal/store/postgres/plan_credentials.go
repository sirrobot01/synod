package postgres

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/sirrobot01/synod/internal/operation"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
)

// checkPlanCredentials proves that the transaction holds each exact authorized secret reference.
func checkPlanCredentials(ctx context.Context, tx pgx.Tx, claim operation.Claim, plan *agentv1.ExecutionPlan) error {
	for _, step := range plan.Steps {
		for _, reference := range step.Command.Input.Secrets {
			var found bool
			if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM cluster_credentials c
 WHERE c.installation_id=$1 AND c.cluster_id=$2 AND c.credential_id=$3 AND c.version=$4 AND c.purpose=$5
 AND (c.member_id IS NULL OR (c.member_id=$6 AND EXISTS(SELECT 1 FROM resource_allocations a
 WHERE a.installation_id=c.installation_id AND a.cluster_id=c.cluster_id AND a.resource_id=c.member_id
 AND a.node_id=$7 AND a.released_at IS NULL))))`, claim.InstallationID, claim.ClusterID,
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
