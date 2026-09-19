package worker

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"slices"

	"github.com/sirrobot01/synod/internal/operation"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/proto"
)

var ErrUnsupportedPlan = errors.New("unsupported_execution_plan")
var ErrPostconditionUnproved = errors.New("postcondition_unproved")

func validatePlan(plan *agentv1.ExecutionPlan, op operation.Operation) error {
	if plan == nil || plan.Version != 1 || len(plan.Steps) == 0 || len(plan.Steps) > 128 {
		return ErrUnsupportedPlan
	}
	steps, commands := make(map[string]bool), make(map[string]bool)
	// stopped maps a resource to the step that stopped it, so a removal can be
	// required to depend on it rather than merely follow it in the list.
	stopped := make(map[string]string)
	for _, step := range plan.Steps {
		if step == nil || step.Command == nil || step.MaxAttempts < 1 || step.MaxAttempts > 10 || step.CancellationBoundary < agentv1.CancellationBoundary_CANCELLATION_BOUNDARY_BEFORE_EXECUTION || step.CancellationBoundary > agentv1.CancellationBoundary_CANCELLATION_BOUNDARY_SAFE_STOP {
			return ErrUnsupportedPlan
		}
		command := step.Command
		if operation.ValidateCommand(command) != nil || command.Epoch.InstallationId != op.InstallationID.String() || command.Epoch.ClusterId != op.ClusterID.String() || command.OperationId != op.ID.String() || command.DesiredRevision != op.DesiredRevision || command.PlanVersion != plan.Version || len(command.Input.Secrets) != 0 || commands[command.CommandId] || steps[command.StepId] {
			return ErrUnsupportedPlan
		}
		var expected agentv1.Postcondition
		switch command.Input.Payload.(type) {
		case *agentv1.CommandInput_EnsureVolume:
			expected = agentv1.Postcondition_POSTCONDITION_VOLUME_EXISTS
		case *agentv1.CommandInput_EnsureNetwork:
			expected = agentv1.Postcondition_POSTCONDITION_NETWORK_EXISTS
		case *agentv1.CommandInput_EnsureContainer:
			expected = agentv1.Postcondition_POSTCONDITION_CONTAINER_EXISTS
		case *agentv1.CommandInput_StartContainer:
			expected = agentv1.Postcondition_POSTCONDITION_CONTAINER_RUNNING
		case *agentv1.CommandInput_StopContainer:
			expected = agentv1.Postcondition_POSTCONDITION_CONTAINER_STOPPED
			stopped[command.Input.ResourceId] = command.StepId
		case *agentv1.CommandInput_RemoveContainer:
			expected = agentv1.Postcondition_POSTCONDITION_CONTAINER_ABSENT
			// Specification 33 requires stopping active work before runtime objects
			// are removed. A plan that removes a container it never stopped tears a
			// running engine out from under its clients, so the ordering is a
			// property of the plan rather than a hope about dispatch order.
			stop, ordered := stopped[command.Input.ResourceId]
			if !ordered || !slices.Contains(step.DependsOn, stop) {
				return ErrUnsupportedPlan
			}
			// A removal is destructive, so it can only be cancelled before it starts.
			if step.CancellationBoundary != agentv1.CancellationBoundary_CANCELLATION_BOUNDARY_BEFORE_EXECUTION {
				return ErrUnsupportedPlan
			}
		case *agentv1.CommandInput_InspectContainer:
			expected = agentv1.Postcondition_POSTCONDITION_CONTAINER_INSPECTED
		default:
			return ErrUnsupportedPlan
		}
		if step.Postcondition != expected {
			return ErrUnsupportedPlan
		}
		for _, dependency := range step.DependsOn {
			if !steps[dependency] {
				return ErrUnsupportedPlan
			}
		}
		steps[command.StepId], commands[command.CommandId] = true, true
	}
	return nil
}

func checkResult(step *agentv1.PlanStep, result *agentv1.TerminalResult) error {
	command := step.Command
	if result == nil || result.Epoch == nil || result.CommandId != command.CommandId || !bytes.Equal(result.InputHash, command.InputHash) || result.Epoch.InstallationId != command.Epoch.InstallationId || result.Epoch.NodeId != command.Epoch.NodeId || result.Epoch.ClusterId != command.Epoch.ClusterId || result.State != "succeeded" || result.FailureCode != "" {
		return ErrPostconditionUnproved
	}
	return checkEffects(step, result.Effects)
}

func checkEffects(step *agentv1.PlanStep, effects []*agentv1.Effect) error {
	command := step.Command
	var observed *agentv1.Effect
	for _, effect := range effects {
		if effect == nil || effect.Active || effect.OutcomeUnknown || effect.CommandId != command.CommandId || effect.ResourceId != command.Input.ResourceId {
			return ErrPostconditionUnproved
		}
		if effect.NativeOperationId == "docker-command:"+command.CommandId {
			if observed != nil {
				return ErrPostconditionUnproved
			}
			observed = effect
		}
	}
	if observed == nil {
		return ErrPostconditionUnproved
	}
	matched := false
	switch step.Postcondition {
	case agentv1.Postcondition_POSTCONDITION_VOLUME_EXISTS, agentv1.Postcondition_POSTCONDITION_NETWORK_EXISTS:
		matched = observed.State == "present"
	case agentv1.Postcondition_POSTCONDITION_CONTAINER_EXISTS, agentv1.Postcondition_POSTCONDITION_CONTAINER_INSPECTED:
		matched = slices.Contains([]string{"created", "restarting", "running", "removing", "paused", "exited", "dead"}, observed.State)
	case agentv1.Postcondition_POSTCONDITION_CONTAINER_RUNNING:
		matched = observed.State == "running"
	case agentv1.Postcondition_POSTCONDITION_CONTAINER_STOPPED:
		matched = slices.Contains([]string{"created", "exited", "dead"}, observed.State)
	case agentv1.Postcondition_POSTCONDITION_CONTAINER_ABSENT:
		matched = observed.State == "absent"
	}
	if !matched {
		return ErrPostconditionUnproved
	}
	return nil
}

// recoveryFrontier retains configuration and the latest lifecycle obligation.
// An intentional later removal supersedes earlier existence and running checks.
func recoveryFrontier(plan *agentv1.ExecutionPlan, completed map[string]bool) ([]*agentv1.PlanStep, [32]byte, error) {
	type resource struct{ node, id string }
	type obligations struct{ configuration, lifecycle, inspection *agentv1.PlanStep }
	resources := make(map[resource]obligations)
	for _, step := range plan.Steps {
		if !completed[step.Command.StepId] {
			continue
		}
		key := resource{step.Command.Epoch.NodeId, step.Command.Input.ResourceId}
		current := resources[key]
		switch step.Postcondition {
		case agentv1.Postcondition_POSTCONDITION_VOLUME_EXISTS, agentv1.Postcondition_POSTCONDITION_NETWORK_EXISTS, agentv1.Postcondition_POSTCONDITION_CONTAINER_EXISTS:
			current.configuration = step
			if current.lifecycle != nil && current.lifecycle.Postcondition == agentv1.Postcondition_POSTCONDITION_CONTAINER_ABSENT {
				current.lifecycle = nil
			}
		case agentv1.Postcondition_POSTCONDITION_CONTAINER_RUNNING, agentv1.Postcondition_POSTCONDITION_CONTAINER_STOPPED:
			current.lifecycle = step
		case agentv1.Postcondition_POSTCONDITION_CONTAINER_ABSENT:
			current = obligations{lifecycle: step}
		case agentv1.Postcondition_POSTCONDITION_CONTAINER_INSPECTED:
			current.inspection = step
		}
		resources[key] = current
	}
	var frontier []*agentv1.PlanStep
	for _, step := range plan.Steps {
		current := resources[resource{step.Command.Epoch.NodeId, step.Command.Input.ResourceId}]
		if current.configuration == step || current.lifecycle == step || (current.configuration == nil && current.lifecycle == nil && current.inspection == step) {
			frontier = append(frontier, step)
		}
	}
	encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(&agentv1.ExecutionPlan{Version: plan.Version, Steps: frontier})
	if err != nil {
		return nil, [32]byte{}, err
	}
	return frontier, sha256.Sum256(encoded), nil
}
