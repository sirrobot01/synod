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
			if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM cluster_credentials
 WHERE installation_id=$1 AND cluster_id=$2 AND credential_id=$3 AND version=$4 AND purpose=$5)`,
				claim.InstallationID, claim.ClusterID, reference.CredentialId, reference.Version, reference.Purpose).Scan(&found); err != nil {
				return admissionError(err)
			}
			if !found {
				return ErrCredentialConflict
			}
		}
	}
	return nil
}
