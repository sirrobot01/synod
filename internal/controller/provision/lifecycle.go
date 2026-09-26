package provision

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/cluster"
	"github.com/sirrobot01/synod/internal/operation"
	"github.com/sirrobot01/synod/internal/store/postgres"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/types/known/durationpb"
)

// The operation kinds this planner produces plans for.
const (
	createKind        = "cluster.create"
	stopKind          = "cluster.stop"
	startKind         = "cluster.start"
	deleteKind        = "cluster.delete"
	scaleKind         = "cluster.scale"
	recoverSetupKind  = "cluster.recover_setup"
	rebuildMemberKind = "cluster.rebuild_member"
	renewKind         = "cluster.renew_certificates"
	switchoverKind    = "cluster.switchover"
)

var (
	// ErrUnplannable reports an operation kind nothing here builds a plan for.
	ErrUnplannable = errors.New("unplannable_operation")
	// ErrNoWorkload reports a lifecycle action on a cluster that holds nothing. An
	// action on an absent workload is a mistake, not an empty plan.
	ErrNoWorkload = errors.New("no_workload_to_act_on")
)

// Plan produces and installs the plan for one claimed operation.
func (p *Planner) Plan(ctx context.Context, claim operation.Claim) error {
	op, err := p.config.Store.GetOperation(ctx, claim.InstallationID, claim.OperationID)
	if err != nil {
		return err
	}
	switch op.Kind {
	case createKind:
		_, err := p.provision(ctx, claim)
		return err
	case rebuildMemberKind:
		return p.rebuildMember(ctx, claim, op)
	case renewKind:
		return p.renewCertificates(ctx, claim, op)
	case switchoverKind:
		return p.switchover(ctx, claim, op)
	case scaleKind:
		return p.scale(ctx, claim, op)
	}
	return p.lifecycle(ctx, claim, op)
}

// lifecycle plans a change to a cluster that already exists.
//
// The resources come from the reservations the cluster holds, not from a fresh
// placement: a lifecycle action acts on what is there, and specification 33 states
// that a failed reservation never justifies relocating existing data.
func (p *Planner) lifecycle(ctx context.Context, claim operation.Claim, op operation.Operation) error {
	found, err := p.config.Store.GetCluster(ctx, claim.InstallationID, claim.ClusterID)
	if err != nil {
		return err
	}
	running, err := p.members(ctx, claim)
	if err != nil {
		return err
	}
	var steps []*agentv1.PlanStep
	// Patroni members stop together. Stopping the leader alone makes a replica promote
	// only to be stopped next.
	together := found.Spec.PostgreSQL != nil && found.Spec.PostgreSQL.Topology == "patroni"
	switch op.Kind {
	case stopKind:
		steps, err = p.stopSteps(claim, op, running, together)
	case startKind:
		if together {
			steps, err = p.patroniStartSteps(ctx, claim, op, running)
		} else {
			steps, err = p.startSteps(ctx, claim, op, found, running)
		}
	case recoverSetupKind:
		steps, err = p.recoverSetupSteps(ctx, claim, op, found, running)
	case deleteKind:
		steps, err = p.deleteSteps(claim, op, running, together)
	default:
		return fmt.Errorf("%w: %s", ErrUnplannable, op.Kind)
	}
	if err != nil {
		return err
	}
	// Routes, then etcd services, stop and leave with their members. Patroni keeps its DCS
	// until its members are down.
	if together && (op.Kind == stopKind || op.Kind == deleteKind) {
		// Each node's steps follow that node's last step so far.
		last := func() map[uuid.UUID]string {
			found := map[uuid.UUID]string{}
			for _, step := range steps {
				found[uuid.MustParse(step.Command.Epoch.NodeId)] = step.Command.StepId
			}
			return found
		}
		if routed(running) {
			routes, err := p.routeStopSteps(claim, op, running, op.Kind == deleteKind, last())
			if err != nil {
				return err
			}
			steps = append(steps, routes...)
		}
		etcd, err := p.etcdStopSteps(ctx, claim, op, op.Kind == deleteKind, last())
		if err != nil {
			return err
		}
		steps = append(steps, etcd...)
	}
	plan := &agentv1.ExecutionPlan{Version: 1, Steps: steps}
	if err := operation.ValidatePlan(plan, op); err != nil {
		return err
	}
	return p.config.Store.InstallPlan(ctx, claim, plan)
}

// member is one instance the cluster runs, where it runs, and the host port its
// reservation holds. A restart must dial the port the cluster was placed on, not
// whatever the range would choose today.
type member struct {
	MemberID   uuid.UUID
	Role       string
	State      string
	NodeID     uuid.UUID
	ResourceID uuid.UUID
	Port       uint16
	// Ports holds the named ports of the reservation, such as a Patroni member's routes.
	Ports map[string]uint16
}

// members answers what the cluster runs. The recorded membership names each instance
// and its role. The reservation says where that instance runs and what it holds.
//
// A recorded member with no reservation is a broken cluster, not an empty plan, so it
// refuses rather than planning around the gap.
func (p *Planner) members(ctx context.Context, claim operation.Claim) ([]member, error) {
	recorded, err := p.config.Store.ReadClusterMembers(ctx, claim.InstallationID, claim.ClusterID)
	if err != nil {
		return nil, err
	}
	holds, err := p.config.Store.ReadPlacementHolds(ctx, claim.InstallationID)
	if err != nil {
		return nil, err
	}
	reserved := make(map[uuid.UUID]postgres.Hold, len(holds))
	for _, hold := range holds {
		if hold.ClusterID == claim.ClusterID {
			reserved[hold.ResourceID] = hold
		}
	}
	running := make([]member, 0, len(recorded))
	for _, each := range recorded {
		hold, live := reserved[each.ResourceID]
		if !live {
			return nil, fmt.Errorf("%w: member %s holds no reservation", ErrNoWorkload, each.MemberID)
		}
		instance := member{MemberID: each.MemberID, Role: each.Role, State: each.State, NodeID: hold.NodeID, ResourceID: each.ResourceID, Ports: map[string]uint16{}}
		for _, port := range hold.Ports {
			if port.Transport == "tcp" && instance.Port == 0 {
				instance.Port = port.Number
			}
			if port.Name != "" {
				instance.Ports[port.Name] = port.Number
			}
		}
		running = append(running, instance)
	}
	if len(running) == 0 {
		return nil, fmt.Errorf("%w: the cluster records no member", ErrNoWorkload)
	}
	return running, nil
}

// epochFor names the node one member's commands run on. Members can sit on different
// nodes, so the epoch belongs to the member rather than to the operation.
func epochFor(claim operation.Claim, nodeID uuid.UUID) *agentv1.Epoch {
	return &agentv1.Epoch{
		InstallationId: claim.InstallationID.String(), NodeId: nodeID.String(),
		ClusterId: claim.ClusterID.String(), RecoveryGeneration: claim.RecoveryGeneration.String(),
		ExecutionEpoch: claim.ExecutionEpoch,
	}
}

// stopSteps stops every member and leaves their data alone. Together stops them with
// no ordering between members.
func (p *Planner) stopSteps(claim operation.Claim, op operation.Operation, running []member, together bool) ([]*agentv1.PlanStep, error) {
	steps := make([]*agentv1.PlanStep, 0, len(running))
	for _, each := range running {
		var dependsOn []string
		if len(steps) > 0 && !together {
			dependsOn = []string{steps[len(steps)-1].Command.StepId}
		}
		epoch := epochFor(claim, each.NodeID)
		stop, err := p.step(claim, epoch, op.DesiredRevision, &agentv1.CommandInput{
			ResourceId: each.ResourceID.String(),
			Payload: &agentv1.CommandInput_StopContainer{StopContainer: &agentv1.StopContainer{
				GracePeriod: durationpb.New(p.config.StopGrace),
			}},
		}, agentv1.Postcondition_POSTCONDITION_CONTAINER_STOPPED, dependsOn,
			// A stop is safe to abandon until it starts. Once the engine is shutting
			// down, stopping halfway is worse than finishing.
			agentv1.CancellationBoundary_CANCELLATION_BOUNDARY_SAFE_STOP)
		if err != nil {
			return nil, err
		}
		steps = append(steps, stop)
	}
	return steps, nil
}

// startSteps starts every member and proves each answers before the operation succeeds.
func (p *Planner) startSteps(ctx context.Context, claim operation.Claim, op operation.Operation, found cluster.Cluster, running []member) ([]*agentv1.PlanStep, error) {
	purpose := postgres.ApplicationCredential
	database := defaultDatabase
	var upstream *agentv1.RedisUpstream
	if found.Spec.Engine == cluster.PostgreSQL && (len(running) != 1 || running[0].Role != "primary") {
		return nil, fmt.Errorf("%w: standalone PostgreSQL runs one primary", ErrUnplannable)
	}
	if found.Spec.Engine == cluster.Redis {
		purpose = postgres.SuperuserCredential
		database = "0"
		slices.SortStableFunc(running, func(a, b member) int { return cmp.Compare(a.Role, b.Role) })
		for _, each := range running {
			if each.Role == "primary" && len(running) > 1 {
				var err error
				upstream, err = p.peer(ctx, claim, each)
				if err != nil {
					return nil, err
				}
			}
		}
	}
	credential, err := p.config.Store.ReadClusterCredential(ctx, claim.InstallationID, claim.ClusterID, purpose)
	if err != nil {
		return nil, err
	}
	steps := make([]*agentv1.PlanStep, 0, 2*len(running))
	for _, each := range running {
		check := &agentv1.CheckReadiness{
			Address: p.config.EndpointAddress, Port: uint32(each.Port),
			Database: database, Role: credential.RoleName, Engine: string(found.Spec.Engine),
		}
		if found.Spec.Engine == cluster.PostgreSQL {
			check.PostgresRole = each.Role
		}
		if found.Spec.Engine == cluster.Redis {
			check.RedisReplication = &agentv1.RedisReplicationCheck{ExpectedRole: each.Role}
			if each.Role == "replica" {
				if upstream == nil {
					return nil, fmt.Errorf("%w: replica %s has no primary", ErrNoWorkload, each.MemberID)
				}
				check.RedisReplication.Upstream = upstream
			}
		}
		epoch := epochFor(claim, each.NodeID)
		var dependsOn []string
		if len(steps) > 0 {
			// Prove the primary before starting its replica. Concurrent checks also
			// compete for the agent's cluster admission slot.
			dependsOn = []string{steps[len(steps)-1].Command.StepId}
		}
		start, err := p.step(claim, epoch, op.DesiredRevision, &agentv1.CommandInput{
			ResourceId: each.ResourceID.String(),
			Payload:    &agentv1.CommandInput_StartContainer{StartContainer: &agentv1.StartContainer{}},
		}, agentv1.Postcondition_POSTCONDITION_CONTAINER_RUNNING, dependsOn,
			agentv1.CancellationBoundary_CANCELLATION_BOUNDARY_BEFORE_EXECUTION)
		if err != nil {
			return nil, err
		}
		steps = append(steps, start)
		ready, err := p.step(claim, epoch, op.DesiredRevision, &agentv1.CommandInput{
			ResourceId: each.ResourceID.String(),
			Secrets: []*agentv1.SecretReference{{
				CredentialId: credential.CredentialID.String(), Version: credential.Version, Purpose: credential.Purpose,
			}},
			Payload: &agentv1.CommandInput_CheckReadiness{CheckReadiness: check},
		}, agentv1.Postcondition_POSTCONDITION_ENGINE_READY, []string{steps[len(steps)-1].Command.StepId},
			agentv1.CancellationBoundary_CANCELLATION_BOUNDARY_BEFORE_EXECUTION)
		if err != nil {
			return nil, err
		}
		steps = append(steps, ready)
	}
	return steps, nil
}

// deleteSteps stop the engine and remove its container. The volume is not touched:
// specification 33 retains data on a delete, and purge is a separate operation.
// Together removes members with no ordering between them.
func (p *Planner) deleteSteps(claim operation.Claim, op operation.Operation, running []member, together bool) ([]*agentv1.PlanStep, error) {
	steps := make([]*agentv1.PlanStep, 0, 2*len(running))
	for _, each := range running {
		epoch := epochFor(claim, each.NodeID)
		var dependsOn []string
		if len(steps) > 0 && !together {
			dependsOn = []string{steps[len(steps)-1].Command.StepId}
		}
		stop, err := p.step(claim, epoch, op.DesiredRevision, &agentv1.CommandInput{
			ResourceId: each.ResourceID.String(),
			Payload: &agentv1.CommandInput_StopContainer{StopContainer: &agentv1.StopContainer{
				GracePeriod: durationpb.New(p.config.StopGrace),
			}},
		}, agentv1.Postcondition_POSTCONDITION_CONTAINER_STOPPED, dependsOn,
			agentv1.CancellationBoundary_CANCELLATION_BOUNDARY_SAFE_STOP)
		if err != nil {
			return nil, err
		}
		// The removal declares the stop it depends on, which operation.ValidatePlan requires: a
		// container removed without being stopped is torn out from under its clients.
		remove, err := p.step(claim, epoch, op.DesiredRevision, &agentv1.CommandInput{
			ResourceId: each.ResourceID.String(),
			Payload:    &agentv1.CommandInput_RemoveContainer{RemoveContainer: &agentv1.RemoveContainer{}},
		}, agentv1.Postcondition_POSTCONDITION_CONTAINER_ABSENT, []string{stop.Command.StepId},
			agentv1.CancellationBoundary_CANCELLATION_BOUNDARY_BEFORE_EXECUTION)
		if err != nil {
			return nil, err
		}
		steps = append(steps, stop, remove)
	}
	return steps, nil
}

// scale plans the member change the cluster's desired replica count asks for.
func (p *Planner) scale(ctx context.Context, claim operation.Claim, op operation.Operation) error {
	found, err := p.config.Store.GetCluster(ctx, claim.InstallationID, claim.ClusterID)
	if err != nil {
		return err
	}
	if found.Spec.PostgreSQL != nil && found.Spec.PostgreSQL.Topology == "patroni" {
		running, err := p.members(ctx, claim)
		if err != nil {
			return err
		}
		if found.DesiredReplicas+1 < len(running) {
			return p.removePatroniMember(ctx, claim, op, found, running)
		}
		return p.addPatroniMember(ctx, claim, op, found)
	}
	// Only Redis runs native replicas. PostgreSQL replicas need the patroni topology.
	if found.Spec.Engine != cluster.Redis {
		return fmt.Errorf("%w: %s does not scale", ErrUnplannable, found.Spec.Engine)
	}
	// A replica copies the primary's dataset. Without persistence a restart loses it
	// on both members, so replication would hand back an empty cluster.
	if found.Spec.Engine == cluster.Redis && found.Spec.Redis.Persistence == "none" {
		return fmt.Errorf("%w: a replicated cluster needs rdb, aof, or rdb+aof", ErrUnplannable)
	}
	running, err := p.members(ctx, claim)
	if err != nil {
		return err
	}
	var primary member
	replicas := 0
	for _, each := range running {
		if each.Role == "primary" {
			if primary.MemberID != uuid.Nil {
				return fmt.Errorf("%w: more than one primary", ErrUnplannable)
			}
			primary = each
			continue
		}
		if each.Role != "replica" {
			return fmt.Errorf("%w: unknown member role %q", ErrUnplannable, each.Role)
		}
		replicas++
	}
	if primary.MemberID == uuid.Nil {
		return fmt.Errorf("%w: the cluster has no primary to replicate", ErrNoWorkload)
	}
	if op.RetryOf != nil && found.DesiredReplicas > 0 {
		if planned, err := p.retryReplica(ctx, claim, op, running); err != nil || planned {
			return err
		}
	}
	switch {
	case found.DesiredReplicas > replicas:
		upstream, err := p.peer(ctx, claim, primary)
		if err != nil {
			return err
		}
		return p.addReplica(ctx, claim, found, primary, upstream, nil)
	case found.DesiredReplicas < replicas:
		return p.removeReplica(ctx, claim, op, running)
	}
	return fmt.Errorf("%w: the cluster already runs %d replicas", ErrUnplannable, replicas)
}

// peer answers the address a replica must dial to reach the primary.
//
// It comes from the primary node's recorded canonical address. The controller's own
// probe address is where the controller reaches the engine, which is not where another
// node reaches it, and a name would let DNS move the peer after the plan is sealed.
func (p *Planner) peer(ctx context.Context, claim operation.Claim, primary member) (*agentv1.RedisUpstream, error) {
	policy, err := p.config.Store.ReadNodePlacementPolicy(ctx, claim.InstallationID, primary.NodeID)
	if err != nil {
		return nil, err
	}
	if policy.ClientAddress == "" {
		return nil, fmt.Errorf("%w: node %s records no canonical address", ErrUnplannable, primary.NodeID)
	}
	return &agentv1.RedisUpstream{Address: policy.ClientAddress, Port: uint32(primary.Port)}, nil
}

// removeReplica stops the replica the reviewed plan named and removes its container.
//
// The volume is left alone. Specification 33 retains data on a removal, and an operator
// who wants the storage gone asks for a purge, which is a separate decision.
func (p *Planner) removeReplica(ctx context.Context, claim operation.Claim, op operation.Operation, running []member) error {
	planned, action, err := p.config.Store.ReadReplicaActionTarget(ctx, claim.InstallationID, claim.OperationID)
	if err != nil {
		return err
	}
	if action != "scale_down" {
		return fmt.Errorf("%w: operation reviewed %s, not a replica removal", ErrUnplannable, action)
	}
	var target, primary member
	for _, each := range running {
		if each.Role == "primary" {
			if primary.MemberID != uuid.Nil {
				return fmt.Errorf("%w: more than one primary", ErrUnplannable)
			}
			primary = each
		}
		if each.MemberID == planned {
			target = each
		}
	}
	if target.MemberID == uuid.Nil {
		return fmt.Errorf("%w: member %s is not running", ErrNoWorkload, planned)
	}
	if target.Role != "replica" {
		return fmt.Errorf("%w: member %s is the %s", ErrUnplannable, planned, target.Role)
	}
	if primary.MemberID == uuid.Nil {
		return fmt.Errorf("%w: replica %s has no primary", ErrNoWorkload, planned)
	}
	if op.RetryOf != nil {
		if planned, err := p.retryReplicaRemoval(ctx, claim, op, target); err != nil || planned {
			return err
		}
	}
	steps, err := p.replicaRemovalSteps(ctx, claim, op, target, primary)
	if err != nil {
		return err
	}
	return p.config.Store.InstallPlan(ctx, claim, &agentv1.ExecutionPlan{Version: 1, Steps: steps})
}

// replicaRemovalSteps proves the target is the replica of the primary, then stops and removes it.
func (p *Planner) replicaRemovalSteps(ctx context.Context, claim operation.Claim, op operation.Operation, target, primary member) ([]*agentv1.PlanStep, error) {
	epoch := epochFor(claim, target.NodeID)
	upstream, err := p.peer(ctx, claim, primary)
	if err != nil {
		return nil, err
	}
	admin, err := p.config.Store.ReadClusterCredential(ctx, claim.InstallationID, claim.ClusterID, postgres.SuperuserCredential)
	if err != nil {
		return nil, err
	}
	check, err := p.step(claim, epoch, op.DesiredRevision, &agentv1.CommandInput{
		ResourceId: target.ResourceID.String(),
		Secrets:    []*agentv1.SecretReference{{CredentialId: admin.CredentialID.String(), Version: admin.Version, Purpose: admin.Purpose}},
		Payload: &agentv1.CommandInput_CheckReadiness{CheckReadiness: &agentv1.CheckReadiness{
			Address: p.config.EndpointAddress, Port: uint32(target.Port), Database: "0", Role: admin.RoleName,
			Engine: string(cluster.Redis), RedisReplication: replicationCheck(upstream),
		}},
	}, agentv1.Postcondition_POSTCONDITION_ENGINE_READY, nil,
		agentv1.CancellationBoundary_CANCELLATION_BOUNDARY_BEFORE_EXECUTION)
	if err != nil {
		return nil, err
	}

	stop, err := p.step(claim, epoch, op.DesiredRevision, &agentv1.CommandInput{
		ResourceId: target.ResourceID.String(),
		Payload: &agentv1.CommandInput_StopContainer{StopContainer: &agentv1.StopContainer{
			GracePeriod: durationpb.New(p.config.StopGrace),
		}},
	}, agentv1.Postcondition_POSTCONDITION_CONTAINER_STOPPED, []string{check.Command.StepId},
		agentv1.CancellationBoundary_CANCELLATION_BOUNDARY_SAFE_STOP)
	if err != nil {
		return nil, err
	}
	remove, err := p.step(claim, epoch, op.DesiredRevision, &agentv1.CommandInput{
		ResourceId: target.ResourceID.String(),
		Payload:    &agentv1.CommandInput_RemoveContainer{RemoveContainer: &agentv1.RemoveContainer{}},
	}, agentv1.Postcondition_POSTCONDITION_CONTAINER_ABSENT, []string{stop.Command.StepId},
		agentv1.CancellationBoundary_CANCELLATION_BOUNDARY_BEFORE_EXECUTION)
	if err != nil {
		return nil, err
	}
	return []*agentv1.PlanStep{check, stop, remove}, nil
}
