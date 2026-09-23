package provision

import (
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/operation"
	"github.com/sirrobot01/synod/internal/store/postgres"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/proto"
)

func TestSetupRecoveryPreservesIdentityAndRequiresCompleteProof(t *testing.T) {
	f := provisioned(t)
	before, err := f.store.ReadClusterMembers(t.Context(), f.claim.InstallationID, f.claim.ClusterID)
	if err != nil {
		t.Fatal(err)
	}
	volume := volumeOf(t, f)
	app, err := f.store.ReadClusterCredential(t.Context(), f.claim.InstallationID, f.claim.ClusterID, postgres.ApplicationCredential)
	if err != nil {
		t.Fatal(err)
	}
	plan := f.planFor(t, postgres.RecoverSetupAPIOperation)
	op, err := f.store.GetOperation(t.Context(), f.claim.InstallationID, f.claim.OperationID)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Steps) != 4 || plan.Steps[0].Command.Input.GetRecoverPostgresSetup() == nil || plan.Steps[1].Command.Input.GetStopContainer() == nil || plan.Steps[2].Command.Input.GetStartContainer() == nil || plan.Steps[3].Command.Input.GetCheckReadiness() == nil {
		t.Fatal("recovery omitted a required step")
	}
	for _, step := range plan.Steps {
		if step.Command.Input.ResourceId != before[0].ResourceID.String() {
			t.Fatal("recovery replaced the instance")
		}
	}
	config := plan.Steps[0].Command.Input.GetRecoverPostgresSetup().Postgres
	if config.ApplicationCredentialId != app.CredentialID.String() || config.ApplicationCredentialVersion != app.Version {
		t.Fatal("recovery replaced the application credential")
	}
	after, err := f.store.ReadClusterMembers(t.Context(), f.claim.InstallationID, f.claim.ClusterID)
	if err != nil || len(after) != 1 || after[0].MemberID != before[0].MemberID || volumeOf(t, f) != volume {
		t.Fatalf("recovery changed ownership: %+v %v", after, err)
	}
	for name, alter := range map[string]func(*agentv1.ExecutionPlan){
		"missing recovery":    func(p *agentv1.ExecutionPlan) { p.Steps = p.Steps[1:]; p.Steps[0].DependsOn = nil },
		"missing readiness":   func(p *agentv1.ExecutionPlan) { p.Steps = p.Steps[:3] },
		"unsequenced restart": func(p *agentv1.ExecutionPlan) { p.Steps[2].DependsOn = nil },
		"initialization": func(p *agentv1.ExecutionPlan) {
			p.Steps[2].Command.Input.GetStartContainer().AllowInitialization = true
		},
		"different resource":   func(p *agentv1.ExecutionPlan) { p.Steps[3].Command.Input.ResourceId = uuid.NewString() },
		"different node":       func(p *agentv1.ExecutionPlan) { p.Steps[3].Command.Epoch.NodeId = uuid.NewString() },
		"different credential": func(p *agentv1.ExecutionPlan) { p.Steps[3].Command.Input.Secrets[0].CredentialId = uuid.NewString() },
		"replica readiness":    func(p *agentv1.ExecutionPlan) { p.Steps[3].Command.Input.GetCheckReadiness().PostgresRole = "replica" },
		"wrong database":       func(p *agentv1.ExecutionPlan) { p.Steps[3].Command.Input.GetCheckReadiness().Database = "postgres" },
	} {
		t.Run(name, func(t *testing.T) {
			changed := proto.Clone(plan).(*agentv1.ExecutionPlan)
			alter(changed)
			for _, step := range changed.Steps {
				hash, err := operation.CommandHash(step.Command)
				if err != nil {
					t.Fatal(err)
				}
				step.Command.InputHash = hash[:]
			}
			if err := operation.ValidatePlan(changed, op); !errors.Is(err, operation.ErrUnsupportedPlan) {
				t.Fatalf("unsafe recovery plan accepted: %v", err)
			}
		})
	}
}
