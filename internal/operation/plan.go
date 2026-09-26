package operation

import (
	"bytes"
	"errors"
	"maps"
	"net"
	"slices"
	"strconv"

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
				// An etcd container presents the member certificate.
				if prepared.GetEtcd() != nil && reference.Purpose == "patroni_member_tls" {
					continue
				}
				// A switchover signs in to the member REST API, and an etcd membership change to the
				// local etcd member. ValidateCommand fixes their purposes.
				if command.Input.GetSwitchoverPatroni() != nil || command.Input.GetEtcdMembership() != nil {
					continue
				}
				if prepared == nil || prepared.Postgres == nil || prepared.Postgres.Patroni == nil {
					return ErrUnsupportedPlan
				}
				switch reference.Purpose {
				case "patroni_rewind", "patroni_rest", "patroni_member_tls", "patroni_dcs_tls":
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
		case *agentv1.CommandInput_EtcdMembership:
			expected = agentv1.Postcondition_POSTCONDITION_ETCD_MEMBERSHIP
			if op.Kind != "cluster.scale" || step.CancellationBoundary != agentv1.CancellationBoundary_CANCELLATION_BOUNDARY_BEFORE_EXECUTION {
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
	// A Patroni add recreates and starts Patroni members. A Patroni removal opens with a check
	// of every member it keeps. A Redis scale does neither.
	if op.Kind == "cluster.scale" && slices.ContainsFunc(plan.Steps, func(step *agentv1.PlanStep) bool {
		return step.Command.Input.GetRecreatePatroniMember() != nil || step.Command.Input.GetStartPatroniMember() != nil
	}) {
		if err := validPatroniScale(plan); err != nil {
			return err
		}
	} else if op.Kind == "cluster.scale" && LeadingChecks(plan.Steps) > 0 {
		if err := validPatroniRemoval(plan); err != nil {
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
			effect.NativeOperationId == "patroni-switchover:"+command.CommandId ||
			effect.NativeOperationId == "etcd-membership:"+command.CommandId {
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
	case agentv1.Postcondition_POSTCONDITION_PATRONI_MEMBER_NOT_SERVING:
		matched = observed.State == "patroni_member_not_serving"
	case agentv1.Postcondition_POSTCONDITION_PATRONI_MEMBER_FAILED:
		matched = observed.State == "patroni_member_storage_failed"
	case agentv1.Postcondition_POSTCONDITION_PATRONI_MEMBER_RECREATED:
		matched = observed.State == "patroni_member_recreated"
	case agentv1.Postcondition_POSTCONDITION_PATRONI_LEADER:
		matched = observed.State == "patroni_leader"
	case agentv1.Postcondition_POSTCONDITION_ETCD_MEMBERSHIP:
		matched = observed.State == "etcd_"+command.Input.GetEtcdMembership().GetAction()
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

// validMemberRebuild fixes the order of a Patroni member rebuild. Every other member proves
// readiness first, one check each with no dependency. Then the plan has one of three shapes.
//
// An active rebuild has eight more steps: not-serving proof, stop, storage proof, removal, and a
// fresh member. A residual replace has seven: stop, one failure proof, removal, and a fresh
// member. A residual resume has four: the residual member's own volume, container, start, and
// check. A removal needs a stop and a proof. A fresh member runs on the same node and address
// with new storage.
func validMemberRebuild(full *agentv1.ExecutionPlan) error {
	count := MemberRebuildPrefix(full)
	if count == 0 {
		return ErrUnsupportedPlan
	}
	if err := validRouteTail(full.Steps[:count], full.Steps[count:]); err != nil {
		return err
	}
	plan := &agentv1.ExecutionPlan{Version: full.Version, Steps: full.Steps[:count]}
	input := func(i int) *agentv1.CommandInput { return plan.Steps[i].Command.Input }
	// peers is the number of peer checks and the index of the first target step. fresh is the
	// index of the member volume step.
	peers := LeadingChecks(plan.Steps)
	fresh := count - 4
	container, start, final := input(fresh+1).GetEnsureContainer(), input(fresh+2).GetStartPatroniMember(), input(fresh+3).GetCheckReadiness()
	if input(fresh).GetEnsureVolume() == nil || container == nil || container.GetPostgres().GetPatroni() == nil || start == nil || final == nil ||
		final.PostgresRole != "any" || final.Port != container.Postgres.Port || !proto.Equal(start.Container, container) ||
		container.VolumeId != input(fresh).ResourceId {
		return ErrUnsupportedPlan
	}
	target, member := plan.Steps[peers].Command, plan.Steps[fresh+1].Command
	var stored *agentv1.EnsureContainer
	switch count - peers {
	case 8:
		notServing, failed := input(peers).GetVerifyPatroniMemberNotServing(), input(peers+2).GetVerifyPatroniMemberFailed()
		if notServing == nil || failed == nil || input(peers+1).GetStopContainer() == nil || input(peers+3).GetRemoveContainer() == nil || !proto.Equal(failed.Container, notServing.Container) {
			return ErrUnsupportedPlan
		}
		stored = notServing.Container
	case 7:
		if input(peers).GetStopContainer() == nil || input(peers+2).GetRemoveContainer() == nil {
			return ErrUnsupportedPlan
		}
		if proof := input(peers + 1).GetVerifyPatroniStartFailure(); proof != nil {
			stored = proof.Container
		} else if proof := input(peers + 1).GetVerifyPatroniMemberFailed(); proof != nil {
			stored = proof.Container
		} else {
			return ErrUnsupportedPlan
		}
	case 4:
		// A resume keeps the member, its storage, and its configuration.
		target, stored = member, container
	default:
		return ErrUnsupportedPlan
	}
	if count-peers != 4 && (!start.AllowInitialization || target.Input.ResourceId == member.Input.ResourceId) ||
		stored.GetPostgres().GetPatroni().GetAddress() != container.Postgres.Patroni.Address || target.Epoch.NodeId != member.Epoch.NodeId {
		return ErrUnsupportedPlan
	}
	checked := map[string]bool{}
	var checks []string
	for i, each := range plan.Steps {
		command := each.Command
		var want []string
		switch {
		case i < peers:
			if command.Epoch.NodeId == target.Epoch.NodeId || command.Input.ResourceId == target.Input.ResourceId || checked[command.Input.ResourceId] {
				return ErrUnsupportedPlan
			}
			checked[command.Input.ResourceId] = true
			checks = append(checks, command.StepId)
		case i == peers:
			want = checks
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
		case i < peers:
		case i < fresh:
			if command.Epoch.NodeId != target.Epoch.NodeId || command.Input.ResourceId != target.Input.ResourceId {
				return ErrUnsupportedPlan
			}
		case i == fresh:
			if command.Epoch.NodeId != member.Epoch.NodeId {
				return ErrUnsupportedPlan
			}
		default:
			if command.Epoch.NodeId != member.Epoch.NodeId || command.Input.ResourceId != member.Input.ResourceId || checked[command.Input.ResourceId] {
				return ErrUnsupportedPlan
			}
		}
	}
	return nil
}

// validRenewal fixes the order of a Patroni certificate renewal. A stopped cluster recreates
// its members in one chain. A running cluster renews one member at a time, then a chain of
// final checks proves every member.
func validRenewal(full *agentv1.ExecutionPlan) error {
	count := RenewalPrefix(full)
	if count == 0 {
		return ErrUnsupportedPlan
	}
	if err := validRouteTail(full.Steps[:count], full.Steps[count:]); err != nil {
		return err
	}
	steps := full.Steps[:count]
	nodes := map[string]string{}
	if steps[0].Command.Input.GetRecreatePatroniMember() != nil {
		for index, step := range steps {
			var want []string
			if index > 0 {
				want = []string{steps[index-1].Command.StepId}
			}
			if _, repeated := nodes[step.Command.Input.ResourceId]; repeated || step.Command.Input.GetRecreatePatroniMember() == nil || !slices.Equal(step.DependsOn, want) {
				return ErrUnsupportedPlan
			}
			place(nodes, step.Command)
		}
		return nil
	}
	members := LeadingChecks(steps) + 1
	blocks := members * (members + 3)
	renewed, err := validRecreateBlocks(steps[:blocks], members, nodes)
	if err != nil {
		return err
	}
	return validFinalChecks(steps[blocks:], renewed, steps[blocks-1].Command.StepId, nodes)
}

// validPatroniScale fixes the order of a Patroni member add. Each current member is recreated
// in turn, as a renewal does, so that it admits the new member. Then the new member creates
// its volume and container, starts with initialization allowed, and proves readiness on a node
// no member uses. A chain of final checks proves every member.
func validPatroniScale(full *agentv1.ExecutionPlan) error {
	count := PatroniScalePrefix(full)
	if count == 0 {
		return ErrUnsupportedPlan
	}
	if err := validRouteTail(full.Steps[:count], full.Steps[count:]); err != nil {
		return err
	}
	steps := full.Steps[:count]
	members := LeadingChecks(steps) + 1
	blocks := members * (members + 3)
	nodes := map[string]string{}
	current, err := validRecreateBlocks(steps[:blocks], members, nodes)
	if err != nil {
		return err
	}
	input := func(i int) *agentv1.CommandInput { return steps[blocks+i].Command.Input }
	container, start, ready := input(1).GetEnsureContainer(), input(2).GetStartPatroniMember(), input(3).GetCheckReadiness()
	if input(0).GetEnsureVolume() == nil || container.GetPostgres().GetPatroni() == nil || container.VolumeId != input(0).ResourceId ||
		start == nil || !start.AllowInitialization || !proto.Equal(start.Container, container) ||
		ready == nil || ready.PostgresRole != "any" || ready.Port != container.Postgres.Port {
		return ErrUnsupportedPlan
	}
	added, node := input(1).ResourceId, steps[blocks].Command.Epoch.NodeId
	if slices.Contains(current, added) || slices.Contains(slices.Collect(maps.Values(nodes)), node) {
		return ErrUnsupportedPlan
	}
	for i := range 4 {
		step := steps[blocks+i]
		if !slices.Equal(step.DependsOn, []string{steps[blocks+i-1].Command.StepId}) || step.Command.Epoch.NodeId != node || i > 0 && step.Command.Input.ResourceId != added {
			return ErrUnsupportedPlan
		}
	}
	place(nodes, steps[blocks+1].Command)
	return validFinalChecks(steps[blocks+4:], append(current, added), steps[blocks+3].Command.StepId, nodes)
}

// validPatroniRemoval fixes the order of a Patroni member removal. Every kept member proves
// readiness. Then the removed member proves it is a replica, so the leader is never removed.
// When an etcd service shares its node, that service moves first: the new service's volume and
// container on a kept member's node, a learner added through an established service, the start,
// the promotion, the removal of the old service from the cluster, and its stop and removal.
// Then the member stops and its container is removed. A chain of final checks proves the kept
// members.
func validPatroniRemoval(full *agentv1.ExecutionPlan) error {
	count := PatroniRemovalPrefix(full)
	if count == 0 {
		return ErrUnsupportedPlan
	}
	if err := validRouteTail(full.Steps[:count], full.Steps[count:]); err != nil {
		return err
	}
	steps := full.Steps[:count]
	kept := LeadingChecks(steps)
	nodes := map[string]string{}
	var members, checks []string
	for _, step := range steps[:kept] {
		resource := step.Command.Input.ResourceId
		if len(step.DependsOn) != 0 || slices.Contains(members, resource) || !place(nodes, step.Command) {
			return ErrUnsupportedPlan
		}
		members, checks = append(members, resource), append(checks, step.Command.StepId)
	}
	check := steps[kept]
	removed, node := check.Command.Input.ResourceId, check.Command.Epoch.NodeId
	if check.Command.Input.GetCheckReadiness().GetPostgresRole() != "replica" || slices.Contains(members, removed) || !slices.Equal(check.DependsOn, checks) || !place(nodes, check.Command) {
		return ErrUnsupportedPlan
	}
	for i := kept + 1; i < count-kept; i++ {
		if !slices.Equal(steps[i].DependsOn, []string{steps[i-1].Command.StepId}) {
			return ErrUnsupportedPlan
		}
	}
	tail := steps[kept+1 : count-kept]
	if len(tail) == etcdMoveSteps+2 {
		if err := validEtcdMove(tail[:etcdMoveSteps], slices.Collect(maps.Values(nodes)), node); err != nil {
			return err
		}
		tail = tail[etcdMoveSteps:]
	}
	stop, remove := tail[0], tail[1]
	if stop.Command.Input.GetStopContainer() == nil || remove.Command.Input.GetRemoveContainer() == nil {
		return ErrUnsupportedPlan
	}
	for _, step := range tail {
		if step.Command.Input.ResourceId != removed || !place(nodes, step.Command) {
			return ErrUnsupportedPlan
		}
	}
	return validFinalChecks(steps[count-kept:], members, remove.Command.StepId, nodes)
}

// etcdMoveSteps is the length of an etcd move inside a member removal.
const etcdMoveSteps = 8

// validEtcdMove checks the move of the etcd service on source, the removed member's node, to a
// kept member's node. Its membership changes run on a third node, through an established
// service. The new service joins as a learner of the existing cluster and votes only once in
// sync, so quorum never depends on it before it runs.
func validEtcdMove(steps []*agentv1.PlanStep, kept []string, source string) error {
	input := func(i int) *agentv1.CommandInput { return steps[i].Command.Input }
	node := func(i int) string { return steps[i].Command.Epoch.NodeId }
	container := input(1).GetEnsureContainer()
	etcd := container.GetEtcd()
	added, promoted, left := input(2).GetEtcdMembership(), input(4).GetEtcdMembership(), input(5).GetEtcdMembership()
	if input(0).GetEnsureVolume() == nil || etcd == nil || container.VolumeId != input(0).ResourceId || etcd.InitialClusterState != "existing" ||
		added.GetAction() != "add_learner" || input(3).GetStartContainer() == nil || promoted.GetAction() != "promote" || left.GetAction() != "remove" ||
		input(6).GetStopContainer() == nil || input(7).GetRemoveContainer() == nil {
		return ErrUnsupportedPlan
	}
	fresh, peer, old := input(1).ResourceId, input(2).ResourceId, input(6).ResourceId
	target, through := node(0), node(2)
	if added.Name != etcd.Name || promoted.Name != etcd.Name || left.Name == etcd.Name ||
		added.PeerUrl != "https://"+net.JoinHostPort(etcd.Address, strconv.Itoa(int(etcd.PeerPort))) ||
		input(3).ResourceId != fresh || input(5).ResourceId != peer || input(4).ResourceId != peer || input(7).ResourceId != old ||
		fresh == peer || old == peer || old == fresh ||
		!slices.Contains(kept, target) || target == source || through == source || through == target ||
		node(1) != target || node(3) != target || node(4) != through || node(5) != through || node(6) != source || node(7) != source {
		return ErrUnsupportedPlan
	}
	return nil
}

// EtcdMoveSource names the etcd service that a member removal moved off the member's node, or
// is empty for a removal that moved none.
func EtcdMoveSource(plan *agentv1.ExecutionPlan) string {
	count := PatroniRemovalPrefix(plan)
	kept := LeadingChecks(plan.GetSteps())
	if count == 0 || count-2*kept-3 != etcdMoveSteps {
		return ""
	}
	return plan.Steps[kept+1+6].Command.Input.ResourceId
}

// validRecreateBlocks checks one block for each member. The other members prove readiness,
// then the member stops, is recreated, restarts without initialization, and proves readiness.
// It returns the members in block order.
func validRecreateBlocks(steps []*agentv1.PlanStep, members int, nodes map[string]string) ([]string, error) {
	size := members + 3
	if len(steps) != members*size {
		return nil, ErrUnsupportedPlan
	}
	id := func(i int) string { return steps[i].Command.StepId }
	resource := func(i int) string { return steps[i].Command.Input.ResourceId }
	var recreated, peers []string
	for block := range members {
		base := size * block
		stop := base + members - 1
		member := resource(stop)
		recreate := steps[stop+1].Command.Input.GetRecreatePatroniMember()
		start := steps[stop+2].Command.Input.GetStartPatroniMember()
		ready := steps[stop+3].Command.Input.GetCheckReadiness()
		if steps[stop].Command.Input.GetStopContainer() == nil || recreate == nil || start == nil || start.AllowInitialization || !proto.Equal(start.Container, recreate.Next) ||
			ready.GetPostgresRole() != "any" || ready.Port != recreate.Next.Postgres.Port || slices.Contains(recreated, member) {
			return nil, ErrUnsupportedPlan
		}
		recreated = append(recreated, member)
		var first, checks []string
		if block > 0 {
			first = []string{id(base - 1)}
		}
		for i := base; i < stop; i++ {
			if steps[i].Command.Input.GetCheckReadiness().GetPostgresRole() != "any" || resource(i) == member ||
				slices.ContainsFunc(steps[base:i], func(other *agentv1.PlanStep) bool { return other.Command.Input.ResourceId == resource(i) }) ||
				!slices.Equal(steps[i].DependsOn, first) {
				return nil, ErrUnsupportedPlan
			}
			checks, peers = append(checks, id(i)), append(peers, resource(i))
		}
		if !slices.Equal(steps[stop].DependsOn, checks) {
			return nil, ErrUnsupportedPlan
		}
		for i := stop + 1; i <= stop+3; i++ {
			if !slices.Equal(steps[i].DependsOn, []string{id(i - 1)}) || resource(i) != member {
				return nil, ErrUnsupportedPlan
			}
		}
		for i := base; i <= stop+3; i++ {
			if !place(nodes, steps[i].Command) {
				return nil, ErrUnsupportedPlan
			}
		}
	}
	// Every peer check names one of the recreated members.
	for _, peer := range peers {
		if !slices.Contains(recreated, peer) {
			return nil, ErrUnsupportedPlan
		}
	}
	return recreated, nil
}

// validFinalChecks checks a chain of role-any readiness checks after previous, one for each member.
func validFinalChecks(steps []*agentv1.PlanStep, members []string, previous string, nodes map[string]string) error {
	if len(steps) != len(members) {
		return ErrUnsupportedPlan
	}
	var final []string
	for _, step := range steps {
		resource := step.Command.Input.ResourceId
		if step.Command.Input.GetCheckReadiness().GetPostgresRole() != "any" || !slices.Equal(step.DependsOn, []string{previous}) ||
			!place(nodes, step.Command) || !slices.Contains(members, resource) || slices.Contains(final, resource) {
			return ErrUnsupportedPlan
		}
		final, previous = append(final, resource), step.Command.StepId
	}
	return nil
}

// place binds each resource to one node across a plan.
func place(nodes map[string]string, command *agentv1.Command) bool {
	node, seen := nodes[command.Input.ResourceId]
	nodes[command.Input.ResourceId] = command.Epoch.NodeId
	return !seen || node == command.Epoch.NodeId
}

// leadingChecks counts the role-any readiness checks that open a plan. A Patroni plan opens
// with one check for each member it relies on.
func LeadingChecks(steps []*agentv1.PlanStep) int {
	count := 0
	for count < len(steps) && steps[count].Command.GetInput().GetCheckReadiness().GetPostgresRole() == "any" {
		count++
	}
	return count
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

// MinPatroniMembers and MaxPatroniMembers bound the members of a Patroni cluster. A new
// cluster starts with the minimum.
const (
	MinPatroniMembers = 3
	MaxPatroniMembers = 5
)

// SwitchoverPrefix is the length of a switchover's member steps: a check of every member, the
// switchover, and a check of every member again. A routed cluster's router checks follow them.
func SwitchoverPrefix(plan *agentv1.ExecutionPlan) int {
	members := LeadingChecks(plan.GetSteps())
	if members < MinPatroniMembers || members > MaxPatroniMembers || len(plan.Steps) < 2*members+1 {
		return 0
	}
	return 2*members + 1
}

// validSwitchover checks a switchover plan. Role-any checks prove every member. The switchover
// on one member follows all of them. Then a chain proves that member as the primary and each
// other member as a replica. Every step stays on its member's node.
func validSwitchover(plan *agentv1.ExecutionPlan) error {
	prefix := SwitchoverPrefix(plan)
	if prefix == 0 {
		return ErrUnsupportedPlan
	}
	if err := validRouteTail(plan.Steps[:prefix], plan.Steps[prefix:]); err != nil {
		return err
	}
	for _, step := range plan.Steps[prefix:] {
		if step.Command.Input.GetCheckReadiness() == nil {
			return ErrUnsupportedPlan
		}
	}
	members := prefix / 2
	nodes := map[string]string{}
	var ready []string
	for _, step := range plan.Steps[:members] {
		command := step.Command
		if len(step.DependsOn) != 0 {
			return ErrUnsupportedPlan
		}
		if _, repeated := nodes[command.Input.ResourceId]; repeated {
			return ErrUnsupportedPlan
		}
		nodes[command.Input.ResourceId] = command.Epoch.NodeId
		ready = append(ready, command.StepId)
	}
	switched := plan.Steps[members]
	if switched.Command.Input.GetSwitchoverPatroni() == nil || !slices.Equal(switched.DependsOn, ready) {
		return ErrUnsupportedPlan
	}
	candidate := switched.Command.Input.ResourceId
	checked := map[string]bool{}
	for index, step := range plan.Steps[members:prefix] {
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
			!slices.Equal(step.DependsOn, []string{plan.Steps[members+index-1].Command.StepId}) {
			return ErrUnsupportedPlan
		}
	}
	return nil
}

// MemberRebuildPrefix is the length of a member rebuild's member steps, or 0 for no rebuild
// shape. After a check of every other member, an active rebuild has 8 more steps, a residual
// replace 7, and a residual resume 4. A routed cluster's route steps follow them.
func MemberRebuildPrefix(plan *agentv1.ExecutionPlan) int {
	peers := LeadingChecks(plan.GetSteps())
	if peers+1 < MinPatroniMembers || peers+1 > MaxPatroniMembers || len(plan.Steps) <= peers {
		return 0
	}
	size := 0
	switch plan.Steps[peers].Command.GetInput().GetPayload().(type) {
	case *agentv1.CommandInput_VerifyPatroniMemberNotServing:
		size = peers + 8
	case *agentv1.CommandInput_StopContainer:
		size = peers + 7
	case *agentv1.CommandInput_EnsureVolume:
		size = peers + 4
	}
	if size == 0 || len(plan.Steps) < size {
		return 0
	}
	return size
}

// RenewalPrefix is the length of a renewal's member steps, or 0 for no renewal shape. A stopped
// cluster recreates each member once. A running cluster has one block of members+3 steps for
// each member and a final check of each member. A routed cluster's route steps follow them.
func RenewalPrefix(plan *agentv1.ExecutionPlan) int {
	steps := plan.GetSteps()
	recreated := 0
	for recreated < len(steps) && steps[recreated].Command.GetInput().GetRecreatePatroniMember() != nil {
		recreated++
	}
	if recreated > 0 {
		if recreated < MinPatroniMembers || recreated > MaxPatroniMembers {
			return 0
		}
		return recreated
	}
	members := LeadingChecks(steps) + 1
	size := members*(members+3) + members
	if members < MinPatroniMembers || members > MaxPatroniMembers || len(steps) < size {
		return 0
	}
	return size
}

// PatroniRemovalPrefix is the length of a Patroni member removal's member steps, or 0 for no
// removal shape: a check of each kept member, the removed member's replica check, an optional
// etcd move, the member's stop and removal, and a final check of each kept member. The route
// steps follow them.
func PatroniRemovalPrefix(plan *agentv1.ExecutionPlan) int {
	kept := LeadingChecks(plan.GetSteps())
	size := 2*kept + 3
	if kept < MinPatroniMembers || kept >= MaxPatroniMembers || len(plan.Steps) < size ||
		plan.Steps[kept].Command.GetInput().GetCheckReadiness().GetPostgresRole() != "replica" {
		return 0
	}
	// An etcd move opens with the new service's volume.
	if plan.Steps[kept+1].Command.GetInput().GetEnsureVolume() != nil {
		size += etcdMoveSteps
	}
	if len(plan.Steps) < size {
		return 0
	}
	return size
}

// PatroniScalePrefix is the length of a Patroni member add's member steps, or 0 for no add
// shape: a renewal block for each current member, the new member's four steps, and a final
// check of every member. The route steps follow them.
func PatroniScalePrefix(plan *agentv1.ExecutionPlan) int {
	members := LeadingChecks(plan.GetSteps()) + 1
	size := members*(members+3) + 4 + members + 1
	if members < MinPatroniMembers || members >= MaxPatroniMembers || len(plan.Steps) < size ||
		plan.Steps[members-1].Command.GetInput().GetStopContainer() == nil {
		return 0
	}
	return size
}

// validRouteTail checks the steps after a Patroni plan's member steps. They form one chain
// that starts after the last member step. They touch only pooler, router, and etcd containers,
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
			allowed = payload.EnsureContainer.GetRoute() != nil || payload.EnsureContainer.GetEtcd() != nil
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
