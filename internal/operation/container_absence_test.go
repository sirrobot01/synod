package operation

import (
	"testing"

	"github.com/google/uuid"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/proto"
)

func TestContainerAbsenceRequiresReadOnlyInputAndNativeProof(t *testing.T) {
	command := commandFixture()
	command.Input.Payload = &agentv1.CommandInput_VerifyContainerAbsent{VerifyContainerAbsent: &agentv1.VerifyContainerAbsent{}}
	hash, err := CommandHash(command)
	if err != nil {
		t.Fatal(err)
	}
	command.InputHash = hash[:]
	step := &agentv1.PlanStep{Command: command, MaxAttempts: 1, CancellationBoundary: agentv1.CancellationBoundary_CANCELLATION_BOUNDARY_BEFORE_EXECUTION, Postcondition: agentv1.Postcondition_POSTCONDITION_CONTAINER_ABSENT}
	op := Operation{ID: uuid.MustParse(command.OperationId), InstallationID: uuid.MustParse(command.Epoch.InstallationId), ClusterID: uuid.MustParse(command.Epoch.ClusterId), DesiredRevision: command.DesiredRevision}
	if err := ValidatePlan(&agentv1.ExecutionPlan{Version: 1, Steps: []*agentv1.PlanStep{step}}, op); err != nil {
		t.Fatal(err)
	}
	for _, change := range []string{"nil", "secret", "mutation"} {
		changed := proto.Clone(command).(*agentv1.Command)
		switch change {
		case "nil":
			changed.Input.Payload = &agentv1.CommandInput_VerifyContainerAbsent{}
		case "secret":
			changed.Input.Secrets = []*agentv1.SecretReference{{CredentialId: uuid.NewString(), Version: 1, Purpose: "superuser"}}
		case "mutation":
			changed.Input.Payload = &agentv1.CommandInput_RemoveContainer{RemoveContainer: &agentv1.RemoveContainer{}}
		}
		if ValidateCommand(changed) == nil {
			t.Fatalf("accepted %s", change)
		}
	}
	for _, state := range []string{"absent", "present", "exited", "unknown"} {
		effect := &agentv1.Effect{CommandId: command.CommandId, ResourceId: command.Input.ResourceId, NativeOperationId: "docker-command:" + command.CommandId, State: state}
		if err := CheckStepEffects(step, []*agentv1.Effect{effect}); (err == nil) != (state == "absent") {
			t.Fatalf("state %s: %v", state, err)
		}
		effect.OutcomeUnknown = true
		if CheckStepEffects(step, []*agentv1.Effect{effect}) == nil {
			t.Fatal("unknown absence passed")
		}
	}
	// A read-only absence command does not waive ordering for a later mutation.
	remove := proto.Clone(command).(*agentv1.Command)
	remove.CommandId, remove.StepId = uuid.NewString(), uuid.NewString()
	remove.Input.Payload = &agentv1.CommandInput_RemoveContainer{RemoveContainer: &agentv1.RemoveContainer{}}
	hash, err = CommandHash(remove)
	if err != nil {
		t.Fatal(err)
	}
	remove.InputHash = hash[:]
	mutated := proto.Clone(step).(*agentv1.PlanStep)
	mutated.Command = remove
	mutated.DependsOn = []string{command.StepId}
	if ValidatePlan(&agentv1.ExecutionPlan{Version: 1, Steps: []*agentv1.PlanStep{step, mutated}}, op) == nil {
		t.Fatal("absence bypassed stop-before-remove")
	}
}
