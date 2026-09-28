package worker

import (
	"crypto/sha256"

	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/proto"
)

// recoveryFrontier retains configuration and the latest lifecycle obligation.
// An intentional later removal supersedes earlier existence and running checks.
func recoveryFrontier(plan *agentv1.ExecutionPlan, completed map[string]bool) ([]*agentv1.PlanStep, [32]byte, error) {
	type resource struct{ node, id string }
	type obligations struct{ configuration, lifecycle *agentv1.PlanStep }
	resources := make(map[resource]obligations)
	for _, step := range plan.Steps {
		if !completed[step.Command.StepId] {
			continue
		}
		key := resource{step.Command.Epoch.NodeId, step.Command.Input.ResourceId}
		current := resources[key]
		switch step.Postcondition {
		case agentv1.Postcondition_POSTCONDITION_CONTAINER_EXISTS:
			current.configuration = step
			if current.lifecycle != nil && current.lifecycle.Postcondition == agentv1.Postcondition_POSTCONDITION_CONTAINER_ABSENT {
				current.lifecycle = nil
			}
		case agentv1.Postcondition_POSTCONDITION_CONTAINER_RUNNING, agentv1.Postcondition_POSTCONDITION_CONTAINER_STOPPED:
			current.lifecycle = step
		case agentv1.Postcondition_POSTCONDITION_CONTAINER_ABSENT:
			current = obligations{lifecycle: step}
		}
		resources[key] = current
	}
	var frontier []*agentv1.PlanStep
	for _, step := range plan.Steps {
		current := resources[resource{step.Command.Epoch.NodeId, step.Command.Input.ResourceId}]
		if current.configuration == step || current.lifecycle == step {
			frontier = append(frontier, step)
		}
	}
	encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(&agentv1.ExecutionPlan{Version: plan.Version, Steps: frontier})
	if err != nil {
		return nil, [32]byte{}, err
	}
	return frontier, sha256.Sum256(encoded), nil
}
