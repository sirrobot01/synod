package operation

import (
	"bytes"
	"errors"
	"maps"
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
				if recreate := command.Input.GetRecreatePatroniMember(); recreate != nil {
					prepared = recreate.Next
				}
				// A route container presents the member certificate.
				if prepared.GetRoute() != nil && reference.Purpose == "patroni_member_tls" {
					continue
				}
				// A switchover signs in to the member REST API. ValidateCommand fixes its purposes.
				if command.Input.GetSwitchoverPatroni() != nil {
					continue
				}
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
		case *agentv1.CommandInput_VerifyPatroniMemberNotServing:
			expected = agentv1.Postcondition_POSTCONDITION_PATRONI_MEMBER_NOT_SERVING
			if step.CancellationBoundary != agentv1.CancellationBoundary_CANCELLATION_BOUNDARY_BEFORE_EXECUTION {
				return ErrUnsupportedPlan
			}
		case *agentv1.CommandInput_VerifyPatroniMemberFailed:
			expected = agentv1.Postcondition_POSTCONDITION_PATRONI_MEMBER_FAILED
			if step.CancellationBoundary != agentv1.CancellationBoundary_CANCELLATION_BOUNDARY_BEFORE_EXECUTION {
				return ErrUnsupportedPlan
			}
		case *agentv1.CommandInput_RecreatePatroniMember:
			expected = agentv1.Postcondition_POSTCONDITION_PATRONI_MEMBER_RECREATED
			if step.CancellationBoundary != agentv1.CancellationBoundary_CANCELLATION_BOUNDARY_BEFORE_EXECUTION {
				return ErrUnsupportedPlan
			}
		case *agentv1.CommandInput_SwitchoverPatroni:
			expected = agentv1.Postcondition_POSTCONDITION_PATRONI_LEADER
			if op.Kind != "cluster.switchover" || step.CancellationBoundary != agentv1.CancellationBoundary_CANCELLATION_BOUNDARY_BEFORE_EXECUTION {
				return ErrUnsupportedPlan
			}
		case *agentv1.CommandInput_VerifyPatroniStartFailure:
			expected = agentv1.Postcondition_POSTCONDITION_PATRONI_START_FAILED
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
	if err := serialOnEachNode(plan); err != nil {
		return err
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
	if op.Kind == "cluster.renew_certificates" {
		if err := validRenewal(plan); err != nil {
			return err
		}
	}
	if op.Kind == "cluster.rebuild_member" {
		if err := validMemberRebuild(plan); err != nil {
			return err
		}
	}
	if op.Kind == "cluster.switchover" {
		if err := validSwitchover(plan); err != nil {
			return err
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
			effect.NativeOperationId == "readiness:"+command.CommandId ||
			effect.NativeOperationId == "patroni-switchover:"+command.CommandId {
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
	case agentv1.Postcondition_POSTCONDITION_POSTGRES_REPLICA_PREPARED:
		matched = observed.State == "replica_prepared"
	case agentv1.Postcondition_POSTCONDITION_POSTGRES_REPLICA_CLONED:
		matched = observed.State == "replica_cloned"
	case agentv1.Postcondition_POSTCONDITION_POSTGRES_REPLICA_FAILED:
		matched = observed.State == "replica_refused"
	case agentv1.Postcondition_POSTCONDITION_PATRONI_MEMBER_NOT_SERVING:
		matched = observed.State == "patroni_member_not_serving"
	case agentv1.Postcondition_POSTCONDITION_PATRONI_MEMBER_FAILED:
		matched = observed.State == "patroni_member_storage_failed"
	case agentv1.Postcondition_POSTCONDITION_PATRONI_MEMBER_RECREATED:
		matched = observed.State == "patroni_member_recreated"
	case agentv1.Postcondition_POSTCONDITION_PATRONI_LEADER:
		matched = observed.State == "patroni_leader"
	case agentv1.Postcondition_POSTCONDITION_PATRONI_START_FAILED:
		matched = slices.Contains([]string{"patroni_storage_not_started", "patroni_storage_attempt_failed", "patroni_bootstrap_cancelled"}, observed.State)
	case agentv1.Postcondition_POSTCONDITION_ENGINE_READY:
		// A role-any check reports the role it observed, so completion can count writers.
		if command.Input.GetCheckReadiness().GetPostgresRole() == "any" {
			matched = observed.State == "ready_primary" || observed.State == "ready_replica"
		} else {
			matched = observed.State == "ready"
		}
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

// validMemberRebuild fixes the order of a Patroni member rebuild. It has three shapes.
//
// An active rebuild has 10 steps: two peer checks, not-serving proof, stop, storage proof, removal,
// and a fresh member. A residual replace has 9 steps: two peer checks, stop, one failure proof,
// removal, and a fresh member. A residual resume has 6 steps: two peer checks and the residual
// member's own volume, container, start, and check.
// The two other members prove readiness first. A removal needs a stop and a proof.
// A fresh member runs on the same node and address with new storage.
func validMemberRebuild(full *agentv1.ExecutionPlan) error {
	count := MemberRebuildPrefix(full)
	if count == 0 {
		return ErrUnsupportedPlan
	}
	if err := validRouteTail(full.Steps[:count], full.Steps[count:]); err != nil {
		return err
	}
	plan := &agentv1.ExecutionPlan{Version: full.Version, Steps: full.Steps[:count]}
	if count != 10 && count != 9 && count != 6 {
		return ErrUnsupportedPlan
	}
	input := func(i int) *agentv1.CommandInput { return plan.Steps[i].Command.Input }
	// fresh is the index of the member volume step. old is the index of the first target step.
	fresh, old := count-4, 2
	if first, second := input(0).GetCheckReadiness(), input(1).GetCheckReadiness(); first == nil || second == nil || first.PostgresRole != "any" || second.PostgresRole != "any" {
		return ErrUnsupportedPlan
	}
	container, start, final := input(fresh+1).GetEnsureContainer(), input(fresh+2).GetStartPatroniMember(), input(fresh+3).GetCheckReadiness()
	if input(fresh).GetEnsureVolume() == nil || container == nil || container.GetPostgres().GetPatroni() == nil || start == nil || final == nil ||
		final.PostgresRole != "any" || final.Port != container.Postgres.Port || !proto.Equal(start.Container, container) ||
		container.VolumeId != input(fresh).ResourceId {
		return ErrUnsupportedPlan
	}
	target, member := plan.Steps[old].Command, plan.Steps[fresh+1].Command
	var stored *agentv1.EnsureContainer
	switch count {
	case 10:
		notServing, failed := input(2).GetVerifyPatroniMemberNotServing(), input(4).GetVerifyPatroniMemberFailed()
		if notServing == nil || failed == nil || input(3).GetStopContainer() == nil || input(5).GetRemoveContainer() == nil || !proto.Equal(failed.Container, notServing.Container) {
			return ErrUnsupportedPlan
		}
		stored = notServing.Container
	case 9:
		if input(2).GetStopContainer() == nil || input(4).GetRemoveContainer() == nil {
			return ErrUnsupportedPlan
		}
		if proof := input(3).GetVerifyPatroniStartFailure(); proof != nil {
			stored = proof.Container
		} else if proof := input(3).GetVerifyPatroniMemberFailed(); proof != nil {
			stored = proof.Container
		} else {
			return ErrUnsupportedPlan
		}
	case 6:
		// A resume keeps the member, its storage, and its configuration.
		target, stored = member, container
	}
	if count != 6 && (!start.AllowInitialization || target.Input.ResourceId == member.Input.ResourceId) ||
		stored.GetPostgres().GetPatroni().GetAddress() != container.Postgres.Patroni.Address || target.Epoch.NodeId != member.Epoch.NodeId {
		return ErrUnsupportedPlan
	}
	peers := map[string]bool{}
	for i, each := range plan.Steps {
		command := each.Command
		want := []string{}
		switch {
		case i < 2:
			if command.Epoch.NodeId == target.Epoch.NodeId || command.Input.ResourceId == target.Input.ResourceId || peers[command.Input.ResourceId] {
				return ErrUnsupportedPlan
			}
			peers[command.Input.ResourceId] = true
			want = nil
		case i == 2:
			want = []string{plan.Steps[0].Command.StepId, plan.Steps[1].Command.StepId}
		case i == fresh-1:
			// The removal waits for the stop and the proof that precede it.
			want = []string{plan.Steps[i-2].Command.StepId, plan.Steps[i-1].Command.StepId}
		default:
			want = []string{plan.Steps[i-1].Command.StepId}
		}
		if !slices.Equal(each.DependsOn, want) {
			return ErrUnsupportedPlan
		}
		switch {
		case i < 2:
		case i < fresh:
			if command.Epoch.NodeId != target.Epoch.NodeId || command.Input.ResourceId != target.Input.ResourceId {
				return ErrUnsupportedPlan
			}
		case i == fresh:
			if command.Epoch.NodeId != member.Epoch.NodeId {
				return ErrUnsupportedPlan
			}
		default:
			if command.Epoch.NodeId != member.Epoch.NodeId || command.Input.ResourceId != member.Input.ResourceId || peers[command.Input.ResourceId] {
				return ErrUnsupportedPlan
			}
		}
	}
	return nil
}

// validRenewal fixes the order of a Patroni certificate renewal. A stopped cluster recreates
// its three members in one chain. A running cluster renews one member at a time: the two
// other members prove readiness, then the member stops, is recreated, restarts without
// initialization, and proves readiness. A chain of three final checks, one per member, follows.
func validRenewal(full *agentv1.ExecutionPlan) error {
	count := RenewalPrefix(full)
	if count == 0 {
		return ErrUnsupportedPlan
	}
	if err := validRouteTail(full.Steps[:count], full.Steps[count:]); err != nil {
		return err
	}
	plan := &agentv1.ExecutionPlan{Version: full.Version, Steps: full.Steps[:count]}
	nodes := map[string]string{}
	// place binds each resource to one node across the plan.
	place := func(command *agentv1.Command) bool {
		node, seen := nodes[command.Input.ResourceId]
		nodes[command.Input.ResourceId] = command.Epoch.NodeId
		return !seen || node == command.Epoch.NodeId
	}
	if len(plan.Steps) == 3 {
		for index, step := range plan.Steps {
			var want []string
			if index > 0 {
				want = []string{plan.Steps[index-1].Command.StepId}
			}
			if _, repeated := nodes[step.Command.Input.ResourceId]; repeated || step.Command.Input.GetRecreatePatroniMember() == nil || !slices.Equal(step.DependsOn, want) {
				return ErrUnsupportedPlan
			}
			place(step.Command)
		}
		return nil
	}
	if len(plan.Steps) != 21 {
		return ErrUnsupportedPlan
	}
	id := func(i int) string { return plan.Steps[i].Command.StepId }
	resource := func(i int) string { return plan.Steps[i].Command.Input.ResourceId }
	anyReady := func(i int) *agentv1.CheckReadiness {
		check := plan.Steps[i].Command.Input.GetCheckReadiness()
		if check == nil || check.PostgresRole != "any" {
			return nil
		}
		return check
	}
	var members []string
	for block := range 3 {
		base := 6 * block
		member := resource(base + 2)
		recreate := plan.Steps[base+3].Command.Input.GetRecreatePatroniMember()
		start := plan.Steps[base+4].Command.Input.GetStartPatroniMember()
		ready := anyReady(base + 5)
		if anyReady(base) == nil || anyReady(base+1) == nil || plan.Steps[base+2].Command.Input.GetStopContainer() == nil ||
			recreate == nil || start == nil || start.AllowInitialization || !proto.Equal(start.Container, recreate.Next) ||
			ready == nil || ready.Port != recreate.Next.Postgres.Port || slices.Contains(members, member) {
			return ErrUnsupportedPlan
		}
		members = append(members, member)
		first := []string(nil)
		if block > 0 {
			first = []string{id(base - 1)}
		}
		want := [][]string{first, first, {id(base), id(base + 1)}, {id(base + 2)}, {id(base + 3)}, {id(base + 4)}}
		for offset := range 6 {
			step := plan.Steps[base+offset]
			if !slices.Equal(step.DependsOn, want[offset]) || !place(step.Command) {
				return ErrUnsupportedPlan
			}
			if offset >= 2 && resource(base+offset) != member {
				return ErrUnsupportedPlan
			}
		}
		if resource(base) == member || resource(base+1) == member || resource(base) == resource(base+1) {
			return ErrUnsupportedPlan
		}
	}
	// Every peer check names one of the renewed members.
	for block := range 3 {
		if !slices.Contains(members, resource(6*block)) || !slices.Contains(members, resource(6*block+1)) {
			return ErrUnsupportedPlan
		}
	}
	var final []string
	for i := 18; i < 21; i++ {
		if anyReady(i) == nil || !slices.Equal(plan.Steps[i].DependsOn, []string{id(i - 1)}) || !place(plan.Steps[i].Command) || !slices.Contains(members, resource(i)) || slices.Contains(final, resource(i)) {
			return ErrUnsupportedPlan
		}
		final = append(final, resource(i))
	}
	return nil
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

// switchoverPrefix is the length of a switchover's member steps. A routed cluster's router
// checks follow them.
const switchoverPrefix = 7

// validSwitchover checks a switchover plan. Three role-any checks prove every member. The
// switchover on one member follows all three. Then a chain proves that member as the primary
// and each other member as a replica. Every step stays on its member's node.
func validSwitchover(plan *agentv1.ExecutionPlan) error {
	if len(plan.Steps) < switchoverPrefix {
		return ErrUnsupportedPlan
	}
	if err := validRouteTail(plan.Steps[:switchoverPrefix], plan.Steps[switchoverPrefix:]); err != nil {
		return err
	}
	for _, step := range plan.Steps[switchoverPrefix:] {
		if step.Command.Input.GetCheckReadiness() == nil {
			return ErrUnsupportedPlan
		}
	}
	nodes := map[string]string{}
	var ready []string
	for _, step := range plan.Steps[:3] {
		command := step.Command
		if command.Input.GetCheckReadiness().GetPostgresRole() != "any" || len(step.DependsOn) != 0 {
			return ErrUnsupportedPlan
		}
		if _, repeated := nodes[command.Input.ResourceId]; repeated {
			return ErrUnsupportedPlan
		}
		nodes[command.Input.ResourceId] = command.Epoch.NodeId
		ready = append(ready, command.StepId)
	}
	switched := plan.Steps[3]
	if switched.Command.Input.GetSwitchoverPatroni() == nil || !slices.Equal(switched.DependsOn, ready) {
		return ErrUnsupportedPlan
	}
	candidate := switched.Command.Input.ResourceId
	checked := map[string]bool{}
	for index, step := range plan.Steps[3:switchoverPrefix] {
		command := step.Command
		if nodes[command.Input.ResourceId] != command.Epoch.NodeId || checked[command.Input.ResourceId] {
			return ErrUnsupportedPlan
		}
		if index == 0 {
			continue
		}
		checked[command.Input.ResourceId] = true
		want := "replica"
		if index == 1 {
			want = "primary"
		}
		check := command.Input.GetCheckReadiness()
		if check == nil || check.PostgresRole != want || (command.Input.ResourceId == candidate) != (index == 1) ||
			!slices.Equal(step.DependsOn, []string{plan.Steps[3+index-1].Command.StepId}) {
			return ErrUnsupportedPlan
		}
	}
	return nil
}

// MemberRebuildPrefix is the length of a member rebuild's member steps: 10 for an active
// rebuild, 9 for a residual replace, 6 for a residual resume, or 0 for no rebuild shape.
// A routed cluster's route steps follow them.
func MemberRebuildPrefix(plan *agentv1.ExecutionPlan) int {
	if len(plan.GetSteps()) < 6 {
		return 0
	}
	switch plan.Steps[2].Command.GetInput().GetPayload().(type) {
	case *agentv1.CommandInput_VerifyPatroniMemberNotServing:
		if len(plan.Steps) >= 10 {
			return 10
		}
	case *agentv1.CommandInput_StopContainer:
		if len(plan.Steps) >= 9 {
			return 9
		}
	case *agentv1.CommandInput_EnsureVolume:
		return 6
	}
	return 0
}

// RenewalPrefix is the length of a renewal's member steps: 21 for a running cluster, 3 for a
// stopped one, or 0 for no renewal shape. A routed cluster's route steps follow them.
func RenewalPrefix(plan *agentv1.ExecutionPlan) int {
	switch {
	case len(plan.GetSteps()) >= 21 && plan.Steps[0].Command.GetInput().GetCheckReadiness() != nil:
		return 21
	case len(plan.GetSteps()) >= 3 && plan.Steps[0].Command.GetInput().GetRecreatePatroniMember() != nil:
		return 3
	}
	return 0
}

// validRouteTail checks the route steps after a Patroni plan's member steps. They form one
// chain that starts after the last member step. They touch only pooler and router containers,
// never a member resource, and their checks prove a primary or a replica route.
func validRouteTail(members, tail []*agentv1.PlanStep) error {
	if len(tail) == 0 {
		return nil
	}
	touched := map[string]bool{}
	for _, step := range members {
		touched[step.Command.Input.ResourceId] = true
		if container := step.Command.Input.GetEnsureContainer(); container != nil {
			touched[container.VolumeId] = true
		}
		if recreate := step.Command.Input.GetRecreatePatroniMember(); recreate != nil {
			touched[recreate.Next.VolumeId] = true
		}
	}
	previous := members[len(members)-1].Command.StepId
	for _, step := range tail {
		input := step.Command.Input
		allowed := false
		switch payload := input.Payload.(type) {
		case *agentv1.CommandInput_EnsureContainer:
			allowed = payload.EnsureContainer.GetRoute() != nil
		case *agentv1.CommandInput_StartContainer:
			allowed = !payload.StartContainer.AllowInitialization
		case *agentv1.CommandInput_StopContainer, *agentv1.CommandInput_RemoveContainer:
			allowed = true
		case *agentv1.CommandInput_CheckReadiness:
			allowed = payload.CheckReadiness.PostgresRole == "primary" || payload.CheckReadiness.PostgresRole == "replica"
		}
		if !allowed || touched[input.ResourceId] || !slices.Equal(step.DependsOn, []string{previous}) {
			return ErrUnsupportedPlan
		}
		previous = step.Command.StepId
	}
	return nil
}
