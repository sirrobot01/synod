package operation

import (
	"bytes"
	"errors"
	"slices"

	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/proto"
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
	prepared := make(map[resource]string)
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
				prepared := command.Input.GetEnsureContainer()
				if prepared == nil || prepared.Postgres == nil || prepared.Postgres.Patroni == nil {
					return ErrUnsupportedPlan
				}
				switch reference.Purpose {
				case "patroni_rewind", "patroni_dcs", "patroni_rest", "patroni_member_tls", "patroni_dcs_tls":
					continue
				}
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
		case *agentv1.CommandInput_PreparePostgresReplica:
			expected = agentv1.Postcondition_POSTCONDITION_POSTGRES_REPLICA_PREPARED
			prepared[resource{command.Epoch.NodeId, command.Input.ResourceId}] = command.StepId
			if step.CancellationBoundary != agentv1.CancellationBoundary_CANCELLATION_BOUNDARY_BEFORE_EXECUTION {
				return ErrUnsupportedPlan
			}
		case *agentv1.CommandInput_ClonePostgresReplica:
			expected = agentv1.Postcondition_POSTCONDITION_POSTGRES_REPLICA_CLONED
			prepare, exists := prepared[resource{command.Epoch.NodeId, command.Input.ResourceId}]
			if !exists || !slices.Contains(step.DependsOn, prepare) || step.CancellationBoundary != agentv1.CancellationBoundary_CANCELLATION_BOUNDARY_BEFORE_EXECUTION {
				return ErrUnsupportedPlan
			}
		case *agentv1.CommandInput_VerifyPostgresReplicaClone:
			expected = agentv1.Postcondition_POSTCONDITION_POSTGRES_REPLICA_CLONED
			if step.CancellationBoundary != agentv1.CancellationBoundary_CANCELLATION_BOUNDARY_BEFORE_EXECUTION {
				return ErrUnsupportedPlan
			}
		case *agentv1.CommandInput_VerifyPostgresReplicaFailure:
			expected = agentv1.Postcondition_POSTCONDITION_POSTGRES_REPLICA_FAILED
			if step.CancellationBoundary != agentv1.CancellationBoundary_CANCELLATION_BOUNDARY_BEFORE_EXECUTION {
				return ErrUnsupportedPlan
			}
		case *agentv1.CommandInput_StartContainer:
			expected = agentv1.Postcondition_POSTCONDITION_CONTAINER_RUNNING
		case *agentv1.CommandInput_StartPatroniMember:
			expected = agentv1.Postcondition_POSTCONDITION_CONTAINER_RUNNING
			if step.CancellationBoundary != agentv1.CancellationBoundary_CANCELLATION_BOUNDARY_BEFORE_EXECUTION {
				return ErrUnsupportedPlan
			}
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
	if op.Kind == "cluster.rebuild_replica" {
		if len(plan.Steps) != 12 {
			return ErrUnsupportedPlan
		}
		old, source, container, clone := plan.Steps[0].Command, plan.Steps[3].Command, plan.Steps[5].Command, plan.Steps[7].Command
		failure, primary, created, copied := old.Input.GetVerifyPostgresReplicaFailure(), source.Input.GetCheckReadiness(), container.Input.GetEnsureContainer(), clone.Input.GetClonePostgresReplica()
		if failure == nil || plan.Steps[1].Command.Input.GetStopContainer() == nil || plan.Steps[2].Command.Input.GetRemoveContainer() == nil || primary == nil || primary.PostgresRole != "primary" || primary.PostgresReplication == nil || created == nil || created.Postgres == nil || copied == nil || plan.Steps[4].Command.Input.GetEnsureVolume() == nil || plan.Steps[6].Command.Input.GetPreparePostgresReplica() == nil || plan.Steps[8].Command.Input.GetStopContainer() == nil || plan.Steps[9].Command.Input.GetStartContainer() == nil || plan.Steps[9].Command.Input.GetStartContainer().AllowInitialization {
			return ErrUnsupportedPlan
		}
		native, app := plan.Steps[10].Command.Input.GetCheckReadiness(), plan.Steps[11].Command.Input.GetCheckReadiness()
		if native == nil || native.PostgresRole != "replica" || native.PostgresReplication == nil || app == nil || app.PostgresRole != "replica" || app.Database != "app" || app.Role != "synod_app" || native.Port != copied.Postgres.Port || app.Port != copied.Postgres.Port || primary.Port != copied.Postgres.ReplicaSource.Port || !proto.Equal(created.Postgres, copied.Postgres) || !proto.Equal(native.PostgresReplication.Source, copied.Postgres.ReplicaSource) || copied.Postgres.ReplicaSource.ResourceId != source.Input.ResourceId || failure.Postgres.ReplicaSource.ResourceId != source.Input.ResourceId || created.VolumeId != plan.Steps[4].Command.Input.ResourceId || old.Input.ResourceId == container.Input.ResourceId || source.Input.ResourceId == container.Input.ResourceId {
			return ErrUnsupportedPlan
		}
		for i, step := range plan.Steps {
			expected := container
			if i < 3 {
				expected = old
			} else if i == 3 {
				expected = source
			}
			resource := expected.Input.ResourceId
			if i == 4 {
				resource = created.VolumeId
			}
			if step.Command.Epoch.NodeId != expected.Epoch.NodeId || step.Command.Input.ResourceId != resource {
				return ErrUnsupportedPlan
			}
			if i == 0 {
				if len(step.DependsOn) != 0 {
					return ErrUnsupportedPlan
				}
			} else if !slices.Equal(step.DependsOn, []string{plan.Steps[i-1].Command.StepId}) {
				return ErrUnsupportedPlan
			}
		}
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
			effect.NativeOperationId == "readiness:"+command.CommandId {
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
	case agentv1.Postcondition_POSTCONDITION_POSTGRES_SETUP_RECOVERED:
		matched = observed.State == "setup_recovered"
	case agentv1.Postcondition_POSTCONDITION_POSTGRES_REPLICA_PREPARED:
		matched = observed.State == "replica_prepared"
	case agentv1.Postcondition_POSTCONDITION_POSTGRES_REPLICA_CLONED:
		matched = observed.State == "replica_cloned"
	case agentv1.Postcondition_POSTCONDITION_POSTGRES_REPLICA_FAILED:
		matched = observed.State == "replica_refused"
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
