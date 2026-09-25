package provision

import (
	"fmt"
	"net/netip"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/cluster"
	enginepostgres "github.com/sirrobot01/synod/internal/engine/postgres"
	"github.com/sirrobot01/synod/internal/operation"
	"github.com/sirrobot01/synod/internal/store/postgres"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/types/known/durationpb"
)

// Route containers are small. The member reservation carries their capacity.
const (
	routeMemoryBytes = 64 << 20
	routeCPUMillis   = 250
)

// routeID names the pooler or router container of one member. It is derived from the
// member resource, so later plans find it without a separate record.
func routeID(member uuid.UUID, kind string) uuid.UUID {
	return uuid.NewSHA1(member, []byte("synod-route-"+kind))
}

// routeMember holds what route configuration needs from one member.
type routeMember struct {
	node, resource                         uuid.UUID
	address                                netip.Addr
	database, rest, pooler, writer, reader uint16
	certificate                            *agentv1.SecretReference
}

// routeMemberFrom reads a member's route inputs from its stored configuration and named ports.
func routeMemberFrom(each member, container *agentv1.EnsureContainer) (routeMember, error) {
	patroni := container.GetPostgres().GetPatroni()
	address, err := netip.ParseAddr(patroni.GetAddress())
	if err != nil {
		return routeMember{}, fmt.Errorf("%w: member %s stores no Patroni address", ErrUnplannable, each.MemberID)
	}
	route := routeMember{node: each.NodeID, resource: each.ResourceID, address: address,
		database: uint16(container.Postgres.Port), rest: uint16(patroni.RestPort),
		pooler: each.Ports["pooler"], writer: each.Ports["writer"], reader: each.Ports["reader"]}
	for _, reference := range patroni.Credentials {
		if reference.Purpose == postgres.PatroniMemberTLSCredential {
			route.certificate = reference
		}
	}
	if route.certificate == nil || route.pooler == 0 || route.writer == 0 || route.reader == 0 {
		return routeMember{}, fmt.Errorf("%w: member %s has no route ports or certificate", ErrUnplannable, each.MemberID)
	}
	return route, nil
}

// routed reports whether the cluster's members reserve route ports.
func routed(running []member) bool {
	for _, each := range running {
		if each.Ports["writer"] == 0 {
			return false
		}
	}
	return len(running) > 0
}

// routeConfigs derives the pooler and router of one member. The router forwards to the
// pooler of every member, and each pooler serves only its own member.
func (p *Planner) routeConfigs(members []routeMember, index int, application postgres.Credential) (pooler, router *agentv1.EnsureContainer) {
	own := members[index]
	var backends []*agentv1.PostgresRouteBackend
	for _, each := range members {
		backends = append(backends, &agentv1.PostgresRouteBackend{Address: each.address.String(), PoolerPort: uint32(each.pooler), RestPort: uint32(each.rest)})
	}
	base := func(route *agentv1.PostgresRouteConfig) *agentv1.EnsureContainer {
		return &agentv1.EnsureContainer{Engine: string(cluster.PostgreSQL), ImageDigest: p.config.Patroni.Image, NetworkMode: agentv1.NetworkMode_NETWORK_MODE_HOST,
			MemoryBytes: routeMemoryBytes, CpuMillis: routeCPUMillis, Route: route}
	}
	applicationRef := &agentv1.SecretReference{CredentialId: application.CredentialID.String(), Version: application.Version, Purpose: application.Purpose}
	pooler = base(&agentv1.PostgresRouteConfig{
		Pooler:      &agentv1.PostgresPoolerConfig{Address: own.address.String(), Port: uint32(own.pooler), DatabasePort: uint32(own.database), RestPort: uint32(own.rest)},
		Credentials: []*agentv1.SecretReference{applicationRef, own.certificate},
	})
	router = base(&agentv1.PostgresRouteConfig{
		Router:      &agentv1.PostgresRouterConfig{Address: own.address.String(), WriterPort: uint32(own.writer), ReaderPort: uint32(own.reader), Backends: backends},
		Credentials: []*agentv1.SecretReference{own.certificate},
	})
	return pooler, router
}

// routeDeploySteps creates and starts every member's pooler and router after the given steps.
func (p *Planner) routeDeploySteps(claim operation.Claim, op operation.Operation, members []routeMember, application postgres.Credential, after []string) ([]*agentv1.PlanStep, []string, error) {
	var steps []*agentv1.PlanStep
	var started []string
	for index, each := range members {
		pooler, router := p.routeConfigs(members, index, application)
		epoch := epochFor(claim, each.node)
		previous := after
		for _, route := range []struct {
			kind      string
			container *agentv1.EnsureContainer
		}{{"pooler", pooler}, {"router", router}} {
			id := routeID(each.resource, route.kind).String()
			ensure, err := p.step(claim, epoch, op.DesiredRevision, &agentv1.CommandInput{
				ResourceId: id, Secrets: route.container.Route.Credentials,
				Payload: &agentv1.CommandInput_EnsureContainer{EnsureContainer: route.container},
			}, agentv1.Postcondition_POSTCONDITION_CONTAINER_EXISTS, previous, beforeExecution)
			if err != nil {
				return nil, nil, err
			}
			start, err := p.step(claim, epoch, op.DesiredRevision, &agentv1.CommandInput{
				ResourceId: id, Payload: &agentv1.CommandInput_StartContainer{StartContainer: &agentv1.StartContainer{}},
			}, agentv1.Postcondition_POSTCONDITION_CONTAINER_RUNNING, []string{ensure.Command.StepId}, beforeExecution)
			if err != nil {
				return nil, nil, err
			}
			steps = append(steps, ensure, start)
			previous = []string{start.Command.StepId}
		}
		started = append(started, previous...)
	}
	return steps, started, nil
}

// routeCheckSteps proves every router's writer route reaches the primary and its reader
// route reaches a replica. Each check dials the router's loopback listener, as agent
// readiness requires, and verifies the node address, which every member leaf names.
// The reader check follows the writer check, because an agent runs one command of a
// cluster at a time.
func (p *Planner) routeCheckSteps(claim operation.Claim, op operation.Operation, members []routeMember, application postgres.Credential, after []string) ([]*agentv1.PlanStep, error) {
	var steps []*agentv1.PlanStep
	for _, each := range members {
		previous := after
		for _, route := range []struct {
			port uint16
			role string
		}{{each.writer, "primary"}, {each.reader, "replica"}} {
			check, err := p.step(claim, epochFor(claim, each.node), op.DesiredRevision, &agentv1.CommandInput{
				ResourceId: routeID(each.resource, "router").String(),
				Secrets:    []*agentv1.SecretReference{{CredentialId: application.CredentialID.String(), Version: application.Version, Purpose: application.Purpose}},
				Payload: &agentv1.CommandInput_CheckReadiness{CheckReadiness: &agentv1.CheckReadiness{
					Address: enginepostgres.ReadinessLoopback(each.address).Addr().String(), Port: uint32(route.port), Database: defaultDatabase, Role: application.RoleName,
					Engine: string(cluster.PostgreSQL), PostgresRole: route.role,
					PostgresTls: &agentv1.PostgresTLS{RootCaPem: p.config.Patroni.CA.CertificatePEM(), ServerName: each.address.String()},
				}},
			}, agentv1.Postcondition_POSTCONDITION_ENGINE_READY, previous, beforeExecution)
			if err != nil {
				return nil, err
			}
			steps, previous = append(steps, check), []string{check.Command.StepId}
		}
	}
	return steps, nil
}

// routeStopSteps stops every member's router and pooler, and removes them when asked.
// A missing route container counts as stopped and absent. The steps of each node form one
// chain after that node's step in after, because an agent runs one command of a cluster at a time.
func (p *Planner) routeStopSteps(claim operation.Claim, op operation.Operation, running []member, remove bool, after map[uuid.UUID]string) ([]*agentv1.PlanStep, error) {
	var steps []*agentv1.PlanStep
	for _, each := range running {
		var previous []string
		if last, ok := after[each.NodeID]; ok {
			previous = []string{last}
		}
		for _, kind := range []string{"router", "pooler"} {
			id := routeID(each.ResourceID, kind).String()
			epoch := epochFor(claim, each.NodeID)
			stop, err := p.step(claim, epoch, op.DesiredRevision, &agentv1.CommandInput{
				ResourceId: id, Payload: &agentv1.CommandInput_StopContainer{StopContainer: &agentv1.StopContainer{GracePeriod: durationpb.New(p.config.StopGrace)}},
			}, agentv1.Postcondition_POSTCONDITION_CONTAINER_STOPPED, previous, agentv1.CancellationBoundary_CANCELLATION_BOUNDARY_SAFE_STOP)
			if err != nil {
				return nil, err
			}
			steps, previous = append(steps, stop), []string{stop.Command.StepId}
			if remove {
				removed, err := p.step(claim, epoch, op.DesiredRevision, &agentv1.CommandInput{
					ResourceId: id, Payload: &agentv1.CommandInput_RemoveContainer{RemoveContainer: &agentv1.RemoveContainer{}},
				}, agentv1.Postcondition_POSTCONDITION_CONTAINER_ABSENT, []string{stop.Command.StepId}, beforeExecution)
				if err != nil {
					return nil, err
				}
				steps, previous = append(steps, removed), []string{removed.Command.StepId}
			}
		}
	}
	return steps, nil
}

// routeReplace stops, removes, creates, and optionally starts one route container. A missing
// container counts as stopped and absent, so a replace also creates a route that never existed.
func (p *Planner) routeReplace(claim operation.Claim, op operation.Operation, node, id uuid.UUID, container *agentv1.EnsureContainer, start bool) ([]*agentv1.PlanStep, error) {
	type action struct {
		input         *agentv1.CommandInput
		postcondition agentv1.Postcondition
		boundary      agentv1.CancellationBoundary
	}
	actions := []action{
		{&agentv1.CommandInput{ResourceId: id.String(), Payload: &agentv1.CommandInput_StopContainer{StopContainer: &agentv1.StopContainer{GracePeriod: durationpb.New(p.config.StopGrace)}}},
			agentv1.Postcondition_POSTCONDITION_CONTAINER_STOPPED, agentv1.CancellationBoundary_CANCELLATION_BOUNDARY_SAFE_STOP},
		{&agentv1.CommandInput{ResourceId: id.String(), Payload: &agentv1.CommandInput_RemoveContainer{RemoveContainer: &agentv1.RemoveContainer{}}},
			agentv1.Postcondition_POSTCONDITION_CONTAINER_ABSENT, beforeExecution},
	}
	if container != nil {
		actions = append(actions, action{&agentv1.CommandInput{ResourceId: id.String(), Secrets: container.Route.Credentials, Payload: &agentv1.CommandInput_EnsureContainer{EnsureContainer: container}},
			agentv1.Postcondition_POSTCONDITION_CONTAINER_EXISTS, beforeExecution})
		if start {
			actions = append(actions, action{&agentv1.CommandInput{ResourceId: id.String(), Payload: &agentv1.CommandInput_StartContainer{StartContainer: &agentv1.StartContainer{}}},
				agentv1.Postcondition_POSTCONDITION_CONTAINER_RUNNING, beforeExecution})
		}
	}
	epoch := epochFor(claim, node)
	var steps []*agentv1.PlanStep
	for _, each := range actions {
		step, err := p.step(claim, epoch, op.DesiredRevision, each.input, each.postcondition, nil, each.boundary)
		if err != nil {
			return nil, err
		}
		steps = append(steps, step)
	}
	return steps, nil
}

// chainAfter makes steps one sequence that starts after a step. Dependencies sit outside the
// command hash, so they can be set after the steps are built.
func chainAfter(previous string, steps []*agentv1.PlanStep) []*agentv1.PlanStep {
	for _, step := range steps {
		step.DependsOn = []string{previous}
		previous = step.Command.StepId
	}
	return steps
}

// rebuildRouteTail removes the routes of removed members, replaces the pooler and router of
// the fresh or resumed member, and replaces every other router, because its backends changed.
// Then it checks the writer and reader routes of every router.
func (p *Planner) rebuildRouteTail(claim operation.Claim, op operation.Operation, removed []member, members []routeMember, fresh uuid.UUID, application postgres.Credential, previous string) ([]*agentv1.PlanStep, error) {
	var steps []*agentv1.PlanStep
	for _, each := range removed {
		for _, kind := range []string{"router", "pooler"} {
			gone, err := p.routeReplace(claim, op, each.NodeID, routeID(each.ResourceID, kind), nil, false)
			if err != nil {
				return nil, err
			}
			steps = append(steps, gone...)
		}
	}
	for index, each := range members {
		pooler, router := p.routeConfigs(members, index, application)
		if each.resource == fresh {
			replaced, err := p.routeReplace(claim, op, each.node, routeID(each.resource, "pooler"), pooler, true)
			if err != nil {
				return nil, err
			}
			steps = append(steps, replaced...)
		}
		replaced, err := p.routeReplace(claim, op, each.node, routeID(each.resource, "router"), router, true)
		if err != nil {
			return nil, err
		}
		steps = append(steps, replaced...)
	}
	checks, err := p.routeCheckSteps(claim, op, members, application, nil)
	if err != nil {
		return nil, err
	}
	return chainAfter(previous, append(steps, checks...)), nil
}
