package operation

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"maps"
	"math"
	"slices"

	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
)

var ErrUnsupportedPlan = errors.New("unsupported_execution_plan")
var ErrPostconditionUnproved = errors.New("postcondition_unproved")

// ValidatePlan checks immutable identities, typed postconditions, and step dependencies.
func ValidatePlan(plan *agentv1.ExecutionPlan, op Operation) error {
	if plan == nil || plan.Version != 1 || len(plan.Steps) == 0 || len(plan.Steps) > 128 {
		return ErrUnsupportedPlan
	}
	steps, commands := make(map[string]bool), make(map[string]bool)
	// stopped maps a resource to the step that stopped it, so a removal can be
	// required to depend on it rather than merely follow it in the list.
	type resource struct{ node, id string }
	stopped := make(map[resource]string)
	for _, step := range plan.Steps {
		if step == nil || step.Command == nil || step.MaxAttempts < 1 || step.MaxAttempts > 10 || step.CancellationBoundary < agentv1.CancellationBoundary_CANCELLATION_BOUNDARY_BEFORE_EXECUTION || step.CancellationBoundary > agentv1.CancellationBoundary_CANCELLATION_BOUNDARY_SAFE_STOP {
			return ErrUnsupportedPlan
		}
		command := step.Command
		if ValidateCommand(command) != nil || command.Epoch.InstallationId != op.InstallationID.String() || command.Epoch.ClusterId != op.ClusterID.String() || command.OperationId != op.ID.String() || command.DesiredRevision != op.DesiredRevision || command.PlanVersion != plan.Version || commands[command.CommandId] || steps[command.StepId] {
			return ErrUnsupportedPlan
		}
		// A plan carries credential references and never a credential. The reference
		// shape is checked by operation.ValidateCommand; this checks that the purpose
		// is one this installation knows how to resolve.
		for _, reference := range command.Input.Secrets {
			if !knownPurpose(reference.Purpose) {
				return ErrUnsupportedPlan
			}
		}
		var expected agentv1.Postcondition
		switch command.Input.Payload.(type) {
		case *agentv1.CommandInput_EnsureVolume:
			expected = agentv1.Postcondition_POSTCONDITION_VOLUME_EXISTS
		case *agentv1.CommandInput_EnsureNetwork:
			expected = agentv1.Postcondition_POSTCONDITION_NETWORK_EXISTS
		case *agentv1.CommandInput_EnsureContainer:
			expected = agentv1.Postcondition_POSTCONDITION_CONTAINER_EXISTS
		case *agentv1.CommandInput_RecoverPostgresSetup:
			expected = agentv1.Postcondition_POSTCONDITION_POSTGRES_SETUP_RECOVERED
			if step.CancellationBoundary != agentv1.CancellationBoundary_CANCELLATION_BOUNDARY_BEFORE_EXECUTION {
				return ErrUnsupportedPlan
			}
		case *agentv1.CommandInput_FinalAppSnapshot:
			expected = agentv1.Postcondition_POSTCONDITION_APP_SNAPSHOT_TRANSFERRED
			stop, ok := stopped[resource{command.Epoch.NodeId, command.Input.ResourceId}]
			if !ok || !slices.Contains(step.DependsOn, stop) || step.CancellationBoundary != agentv1.CancellationBoundary_CANCELLATION_BOUNDARY_BEFORE_EXECUTION {
				return ErrUnsupportedPlan
			}
		case *agentv1.CommandInput_RemoveAppData:
			expected = agentv1.Postcondition_POSTCONDITION_APP_DATA_ABSENT
			if step.CancellationBoundary != agentv1.CancellationBoundary_CANCELLATION_BOUNDARY_BEFORE_EXECUTION {
				return ErrUnsupportedPlan
			}
		case *agentv1.CommandInput_StartContainer:
			expected = agentv1.Postcondition_POSTCONDITION_CONTAINER_RUNNING
		case *agentv1.CommandInput_StopContainer:
			expected = agentv1.Postcondition_POSTCONDITION_CONTAINER_STOPPED
			stopped[resource{command.Epoch.NodeId, command.Input.ResourceId}] = command.StepId
		case *agentv1.CommandInput_RemoveContainer:
			expected = agentv1.Postcondition_POSTCONDITION_CONTAINER_ABSENT
			// Specification 33 requires stopping active work before runtime objects
			// are removed. A plan that removes a container it never stopped tears a
			// running engine out from under its clients, so the ordering is a
			// property of the plan rather than a hope about dispatch order.
			stop, ordered := stopped[resource{command.Epoch.NodeId, command.Input.ResourceId}]
			if !ordered || !slices.Contains(step.DependsOn, stop) {
				return ErrUnsupportedPlan
			}
			// A removal is destructive, so it can only be cancelled before it starts.
			if step.CancellationBoundary != agentv1.CancellationBoundary_CANCELLATION_BOUNDARY_BEFORE_EXECUTION {
				return ErrUnsupportedPlan
			}
		case *agentv1.CommandInput_VerifyContainerAbsent:
			expected = agentv1.Postcondition_POSTCONDITION_CONTAINER_ABSENT
			if step.CancellationBoundary != agentv1.CancellationBoundary_CANCELLATION_BOUNDARY_BEFORE_EXECUTION {
				return ErrUnsupportedPlan
			}
		case *agentv1.CommandInput_InspectContainer:
			expected = agentv1.Postcondition_POSTCONDITION_CONTAINER_INSPECTED
		case *agentv1.CommandInput_CheckReadiness:
			expected = agentv1.Postcondition_POSTCONDITION_ENGINE_READY
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
	if err := serialOnEachNode(plan); err != nil {
		return err
	}
	if op.Kind == "cluster.recover_setup" {
		if len(plan.Steps) != 4 {
			return ErrUnsupportedPlan
		}
		recover := plan.Steps[0].Command.Input.GetRecoverPostgresSetup()
		stop := plan.Steps[1].Command.Input.GetStopContainer()
		start := plan.Steps[2].Command.Input.GetStartContainer()
		ready := plan.Steps[3].Command.Input.GetCheckReadiness()
		if recover == nil || stop == nil || start == nil || start.AllowInitialization || ready == nil || ready.Engine != "postgres" || ready.PostgresRole != "primary" || ready.Database != "app" || ready.Role != "synod_app" || ready.Port != recover.Postgres.Port || ready.RedisReplication != nil {
			return ErrUnsupportedPlan
		}
		first := plan.Steps[0].Command
		for index, step := range plan.Steps {
			if step.Command.Epoch.NodeId != first.Epoch.NodeId || step.Command.Input.ResourceId != first.Input.ResourceId {
				return ErrUnsupportedPlan
			}
			if index == 0 {
				if len(step.DependsOn) != 0 {
					return ErrUnsupportedPlan
				}
			} else if !slices.Equal(step.DependsOn, []string{plan.Steps[index-1].Command.StepId}) {
				return ErrUnsupportedPlan
			}
		}
		references := plan.Steps[3].Command.Input.Secrets
		if len(references) != 1 || references[0].Purpose != "application" || references[0].CredentialId != recover.Postgres.ApplicationCredentialId || references[0].Version != recover.Postgres.ApplicationCredentialVersion {
			return ErrUnsupportedPlan
		}
	}
	return nil
}

// CheckStepResult verifies a stored receipt against its immutable plan step.
func CheckStepResult(step *agentv1.PlanStep, result *agentv1.TerminalResult) error {
	if step == nil || step.Command == nil || step.Command.Epoch == nil || step.Command.Input == nil {
		return ErrPostconditionUnproved
	}
	command := step.Command
	if result == nil || result.Epoch == nil || result.CommandId != command.CommandId || !bytes.Equal(result.InputHash, command.InputHash) || result.Epoch.InstallationId != command.Epoch.InstallationId || result.Epoch.NodeId != command.Epoch.NodeId || result.Epoch.ClusterId != command.Epoch.ClusterId || result.State != "succeeded" || result.FailureCode != "" {
		return ErrPostconditionUnproved
	}
	return CheckStepEffects(step, result.Effects)
}

// CheckStepEffects verifies quiescent native effects against one postcondition.
func CheckStepEffects(step *agentv1.PlanStep, effects []*agentv1.Effect) error {
	if step == nil || step.Command == nil || step.Command.Input == nil {
		return ErrPostconditionUnproved
	}
	command := step.Command
	var observed *agentv1.Effect
	for _, effect := range effects {
		if effect == nil || effect.Active || effect.OutcomeUnknown || effect.CommandId != command.CommandId || effect.ResourceId != command.Input.ResourceId {
			return ErrPostconditionUnproved
		}
		// A readiness check is an observation over the network rather than a
		// container action, so it names itself differently. Both are one effect
		// answering for one command.
		if effect.NativeOperationId == "docker-command:"+command.CommandId ||
			effect.NativeOperationId == "readiness:"+command.CommandId || effect.NativeOperationId == "app-data:"+command.CommandId {
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
		// A missing container runs nothing, so absence also proves a stop.
		matched = slices.Contains([]string{"created", "exited", "dead", "absent"}, observed.State)
	case agentv1.Postcondition_POSTCONDITION_CONTAINER_ABSENT:
		matched = observed.State == "absent"
	case agentv1.Postcondition_POSTCONDITION_POSTGRES_SETUP_RECOVERED:
		matched = observed.State == "setup_recovered"
	case agentv1.Postcondition_POSTCONDITION_APP_SNAPSHOT_TRANSFERRED:
		input, receipt := command.Input.GetFinalAppSnapshot(), observed.AppSnapshot
		matched = observed.State == "snapshot_transferred" && input != nil && receipt != nil && receipt.WriterEpoch == input.WriterEpoch && receipt.TargetNodeId == input.TargetNodeId && receipt.Sequence > 0 && receipt.Sequence <= math.MaxInt64 && observed.NativeOperationId == "app-data:"+command.CommandId && len(receipt.ManifestHash) == sha256.Size
	case agentv1.Postcondition_POSTCONDITION_APP_DATA_ABSENT:
		matched = observed.State == "app_data_absent" && observed.NativeOperationId == "app-data:"+command.CommandId && command.Input.GetRemoveAppData() != nil
	case agentv1.Postcondition_POSTCONDITION_ENGINE_READY:
		matched = observed.State == "ready"
	}
	if !matched {
		return ErrPostconditionUnproved
	}
	return nil
}

// knownPurpose reports a SQL credential purpose accepted by existing plans.
func knownPurpose(purpose string) bool {
	switch purpose {
	case "superuser", "application", "replication":
		return true
	}
	return false
}

// serialOnEachNode requires a dependency path between every two steps on one node. An agent admits
// one queued or running command of a cluster at a time and refuses another as agent_busy, and each
// refusal spends a delivery attempt. Two steps on one node that could be ready together would race.
// Dependencies always name earlier steps, so ancestors are complete when a step is reached.
func serialOnEachNode(plan *agentv1.ExecutionPlan) error {
	ancestors := make(map[string]map[string]bool, len(plan.Steps))
	last := map[string]string{}
	for _, step := range plan.Steps {
		prior := map[string]bool{}
		for _, dependency := range step.DependsOn {
			prior[dependency] = true
			maps.Copy(prior, ancestors[dependency])
		}
		id, node := step.Command.StepId, step.Command.Epoch.NodeId
		// Every earlier step on this node is an ancestor of the node's last step, so checking that
		// one step is enough.
		if previous, seen := last[node]; seen && !prior[previous] {
			return ErrUnsupportedPlan
		}
		ancestors[id], last[node] = prior, id
	}
	return nil
}
