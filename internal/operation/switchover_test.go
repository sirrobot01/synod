package operation

import (
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/pki"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/proto"
)

func switchoverCommand() *agentv1.Command {
	command := commandFixture()
	command.Input.Secrets = []*agentv1.SecretReference{
		{CredentialId: uuid.NewString(), Version: 1, Purpose: "patroni_rest"},
		{CredentialId: uuid.NewString(), Version: 2, Purpose: "patroni_member_tls"},
	}
	command.Input.Payload = &agentv1.CommandInput_SwitchoverPatroni{SwitchoverPatroni: &agentv1.SwitchoverPatroni{Address: "10.0.0.2", RestPort: 8008}}
	return command
}

func TestSwitchoverValidation(t *testing.T) {
	if _, err := CommandHash(switchoverCommand()); err != nil {
		t.Fatal("valid switchover refused", err)
	}
	for name, change := range map[string]func(*agentv1.Command, *agentv1.SwitchoverPatroni){
		"loopback address":     func(_ *agentv1.Command, p *agentv1.SwitchoverPatroni) { p.Address = "127.0.0.1" },
		"noncanonical address": func(_ *agentv1.Command, p *agentv1.SwitchoverPatroni) { p.Address = "010.0.0.2" },
		"zero port":            func(_ *agentv1.Command, p *agentv1.SwitchoverPatroni) { p.RestPort = 0 },
		"wide port":            func(_ *agentv1.Command, p *agentv1.SwitchoverPatroni) { p.RestPort = 70000 },
		"no certificate":       func(c *agentv1.Command, _ *agentv1.SwitchoverPatroni) { c.Input.Secrets = c.Input.Secrets[:1] },
		"extra credential": func(c *agentv1.Command, _ *agentv1.SwitchoverPatroni) {
			c.Input.Secrets = append(c.Input.Secrets, &agentv1.SecretReference{CredentialId: uuid.NewString(), Version: 1, Purpose: "superuser"})
		},
		"repeated purpose": func(c *agentv1.Command, _ *agentv1.SwitchoverPatroni) {
			c.Input.Secrets[1].Purpose = "patroni_rest"
		},
	} {
		command := switchoverCommand()
		change(command, command.Input.GetSwitchoverPatroni())
		if _, err := CommandHash(command); err == nil {
			t.Errorf("%s: switchover accepted", name)
		}
	}
	step := &agentv1.PlanStep{Command: switchoverCommand(), Postcondition: agentv1.Postcondition_POSTCONDITION_PATRONI_LEADER}
	effect := &agentv1.Effect{CommandId: step.Command.CommandId, ResourceId: step.Command.Input.ResourceId, NativeOperationId: "patroni-switchover:" + step.Command.CommandId, State: "patroni_leader"}
	if err := CheckStepEffects(step, []*agentv1.Effect{effect}); err != nil {
		t.Fatal("leader effect did not prove the switchover", err)
	}
	effect.State = "ready"
	if err := CheckStepEffects(step, []*agentv1.Effect{effect}); err == nil {
		t.Fatal("another state proved the switchover")
	}
}

// switchoverPlan builds a valid switchover plan for three members. Member 1 is the candidate.
func switchoverPlan(t *testing.T) (*agentv1.ExecutionPlan, Operation, func(*agentv1.PlanStep)) {
	t.Helper()
	root, _, err := pki.CreateCA(uuid.New(), time.Now(), time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	base := commandFixture()
	op := Operation{ID: uuid.MustParse(base.OperationId), InstallationID: uuid.MustParse(base.Epoch.InstallationId), ClusterID: uuid.MustParse(base.Epoch.ClusterId), DesiredRevision: 1, Kind: "cluster.switchover"}
	var nodes, resources [3]string
	for index := range 3 {
		nodes[index], resources[index] = uuid.NewString(), uuid.NewString()
	}
	seal := func(step *agentv1.PlanStep) {
		hash, err := CommandHash(step.Command)
		if err != nil {
			t.Fatal(err)
		}
		step.Command.InputHash = hash[:]
	}
	build := func(member int, payload *agentv1.CommandInput, postcondition agentv1.Postcondition, after ...string) *agentv1.PlanStep {
		command := proto.Clone(base).(*agentv1.Command)
		command.StepId, command.CommandId = uuid.NewString(), uuid.NewString()
		command.Epoch.NodeId = nodes[member]
		payload.ResourceId, payload.PayloadVersion, payload.Timeout = resources[member], 1, base.Input.Timeout
		command.Input = payload
		step := &agentv1.PlanStep{Command: command, MaxAttempts: 1, CancellationBoundary: agentv1.CancellationBoundary_CANCELLATION_BOUNDARY_BEFORE_EXECUTION, Postcondition: postcondition, DependsOn: after}
		seal(step)
		return step
	}
	ready := func(member int, role string, after ...string) *agentv1.PlanStep {
		return build(member, &agentv1.CommandInput{
			Secrets: []*agentv1.SecretReference{{CredentialId: uuid.NewString(), Version: 1, Purpose: "application"}},
			Payload: &agentv1.CommandInput_CheckReadiness{CheckReadiness: &agentv1.CheckReadiness{Engine: "postgres", PostgresRole: role, Address: "127.0.0.2", Port: 5432, Database: "app", Role: "synod_app", PostgresTls: &agentv1.PostgresTLS{RootCaPem: root, ServerName: "10.0.0.2"}}},
		}, agentv1.Postcondition_POSTCONDITION_ENGINE_READY, after...)
	}
	plan := &agentv1.ExecutionPlan{Version: 1}
	for member := range 3 {
		plan.Steps = append(plan.Steps, ready(member, "any"))
	}
	switched := switchoverCommand().Input
	plan.Steps = append(plan.Steps, build(1, switched, agentv1.Postcondition_POSTCONDITION_PATRONI_LEADER, plan.Steps[0].Command.StepId, plan.Steps[1].Command.StepId, plan.Steps[2].Command.StepId))
	for _, check := range []struct {
		member int
		role   string
	}{{1, "primary"}, {0, "replica"}, {2, "replica"}} {
		plan.Steps = append(plan.Steps, ready(check.member, check.role, plan.Steps[len(plan.Steps)-1].Command.StepId))
	}
	return plan, op, seal
}

func TestSwitchoverPlanShape(t *testing.T) {
	plan, op, _ := switchoverPlan(t)
	if err := ValidatePlan(plan, op); err != nil {
		t.Fatal("valid switchover plan refused", err)
	}
	other := op
	other.Kind = "cluster.start"
	if err := ValidatePlan(plan, other); !errors.Is(err, ErrUnsupportedPlan) {
		t.Fatal("a switchover step was accepted in another kind", err)
	}
	for name, change := range map[string]func(*agentv1.ExecutionPlan){
		"missing member check": func(p *agentv1.ExecutionPlan) {
			p.Steps[3].DependsOn = p.Steps[3].DependsOn[:2]
		},
		"candidate checked as a replica": func(p *agentv1.ExecutionPlan) {
			p.Steps[4].Command.Input.GetCheckReadiness().PostgresRole = "replica"
			p.Steps[5].Command.Input.GetCheckReadiness().PostgresRole = "primary"
		},
		"primary check on another node": func(p *agentv1.ExecutionPlan) {
			p.Steps[4].Command.Epoch.NodeId = p.Steps[0].Command.Epoch.NodeId
		},
		"replica checked twice": func(p *agentv1.ExecutionPlan) {
			p.Steps[6].Command.Input.ResourceId = p.Steps[5].Command.Input.ResourceId
			p.Steps[6].Command.Epoch.NodeId = p.Steps[5].Command.Epoch.NodeId
		},
		"parallel final checks": func(p *agentv1.ExecutionPlan) {
			p.Steps[6].DependsOn = []string{p.Steps[4].Command.StepId}
		},
		"no final replica check": func(p *agentv1.ExecutionPlan) { p.Steps = p.Steps[:6] },
		"a container change after it": func(p *agentv1.ExecutionPlan) {
			stop := proto.Clone(p.Steps[6]).(*agentv1.PlanStep)
			stop.Command.StepId, stop.Command.CommandId, stop.Command.Input.ResourceId = uuid.NewString(), uuid.NewString(), uuid.NewString()
			stop.Command.Input.Secrets = nil
			stop.Command.Input.Payload = &agentv1.CommandInput_StartContainer{StartContainer: &agentv1.StartContainer{}}
			stop.Postcondition = agentv1.Postcondition_POSTCONDITION_CONTAINER_RUNNING
			stop.DependsOn = []string{p.Steps[6].Command.StepId}
			p.Steps = append(p.Steps, stop)
		},
	} {
		t.Run(name, func(t *testing.T) {
			bad, op, seal := switchoverPlan(t)
			change(bad)
			for _, step := range bad.Steps {
				seal(step)
			}
			if err := ValidatePlan(bad, op); !errors.Is(err, ErrUnsupportedPlan) {
				t.Fatalf("invalid switchover plan accepted: %v", err)
			}
		})
	}
}
