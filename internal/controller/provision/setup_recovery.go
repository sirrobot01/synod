package provision

import (
	"context"

	"github.com/sirrobot01/synod/internal/cluster"
	"github.com/sirrobot01/synod/internal/operation"
	"github.com/sirrobot01/synod/internal/store/postgres"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
)

func (p *Planner) recoverSetupSteps(ctx context.Context, claim operation.Claim, op operation.Operation, found cluster.Cluster, members []member) ([]*agentv1.PlanStep, error) {
	if found.Spec.Engine != cluster.PostgreSQL || found.Spec.PostgreSQL == nil || found.Spec.PostgreSQL.Topology != "standalone" || found.DesiredReplicas != 0 || len(members) != 1 || members[0].Role != "primary" {
		return nil, ErrUnplannable
	}
	admin, err := p.config.Store.ReadClusterCredential(ctx, claim.InstallationID, claim.ClusterID, postgres.SuperuserCredential)
	if err != nil {
		return nil, err
	}
	app, err := p.config.Store.ReadClusterCredential(ctx, claim.InstallationID, claim.ClusterID, postgres.ApplicationCredential)
	if err != nil {
		return nil, err
	}
	if admin.RoleName != "synod_admin" || app.RoleName != "synod_app" {
		return nil, ErrUnplannable
	}
	each := members[0]
	recovery, err := p.step(claim, epochFor(claim, each.NodeID), op.DesiredRevision, &agentv1.CommandInput{
		ResourceId: each.ResourceID.String(),
		Secrets:    []*agentv1.SecretReference{{CredentialId: admin.CredentialID.String(), Version: admin.Version, Purpose: admin.Purpose}, {CredentialId: app.CredentialID.String(), Version: app.Version, Purpose: app.Purpose}},
		Payload:    &agentv1.CommandInput_RecoverPostgresSetup{RecoverPostgresSetup: &agentv1.RecoverPostgresSetup{Postgres: &agentv1.PostgresContainerConfig{Port: uint32(each.Port), SuperuserCredentialId: admin.CredentialID.String(), SuperuserCredentialVersion: admin.Version, ApplicationCredentialId: app.CredentialID.String(), ApplicationCredentialVersion: app.Version}}},
	}, agentv1.Postcondition_POSTCONDITION_POSTGRES_SETUP_RECOVERED, nil, agentv1.CancellationBoundary_CANCELLATION_BOUNDARY_BEFORE_EXECUTION)
	if err != nil {
		return nil, err
	}
	stops, err := p.stopSteps(claim, op, members, false)
	if err != nil {
		return nil, err
	}
	starts, err := p.startSteps(ctx, claim, op, found, members)
	if err != nil {
		return nil, err
	}
	stops[0].DependsOn = []string{recovery.Command.StepId}
	starts[0].DependsOn = []string{stops[0].Command.StepId}
	return append([]*agentv1.PlanStep{recovery, stops[0]}, starts...), nil
}
