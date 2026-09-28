// Package provision turns admitted apps into durable execution plans.
package provision

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/app"
	"github.com/sirrobot01/synod/internal/operation"
	"github.com/sirrobot01/synod/internal/placement"
	imageinspect "github.com/sirrobot01/synod/internal/runtime/image"
	"github.com/sirrobot01/synod/internal/secret"
	"github.com/sirrobot01/synod/internal/store/postgres"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/types/known/durationpb"
)

var (
	// ErrNoImage reports an engine version with no pinned image. Specification 36 forbids
	// a moving latest tag, so it is refused rather than resolved at run time.
	ErrNoImage = errors.New("no_pinned_image")
	// ErrUnplannable reports an operation kind nothing here builds a plan for.
	ErrUnplannable = errors.New("unplannable_operation")
	// ErrNoWorkload reports a lifecycle action on an app that holds nothing. An
	// action on an absent workload is a mistake, not an empty plan.
	ErrNoWorkload = errors.New("no_workload_to_act_on")
)

// Decider refreshes capacity evidence and plans on what nodes actually answered.
// *capacity.Driver satisfies it.
type Decider interface {
	Decide(ctx context.Context, request placement.Request) (placement.Decision, placement.Snapshot, error)
}

// Config binds one planner.
type Config struct {
	// ResolveImage supplies verified image facts. A nil value uses registry inspection.
	ResolveImage func(context.Context, app.ServiceSpec) (imageinspect.Resolution, error)
	Store        *postgres.Store
	Decider      Decider
	// Keyring seals tier 1 credentials. Only tier 1 plans need it.
	Keyring *secret.Keyring
	// DatabaseImages pins the multi-architecture engine images of tier 1 apps.
	DatabaseImages []DatabaseImage
	// Budget is the resource amount one tier 1 copy reserves.
	Budget placement.Vector
	// StepTimeout bounds one command on a node.
	StepTimeout time.Duration
	// StopGrace is how long a container is given to shut down cleanly before the
	// runtime stops waiting. A database killed part-way leaves recovery work.
	StopGrace time.Duration
	// EndpointPorts is the span of host ports placement chooses a copy port from.
	EndpointPorts placement.PortRange
}

// Planner produces and installs execution plans.
type Planner struct{ config Config }

// New checks the planner inputs.
func New(config Config) (*Planner, error) {
	if config.Store == nil || config.Decider == nil {
		return nil, errors.New("provision planner requires a store and a decider")
	}
	if config.ResolveImage == nil {
		config.ResolveImage = (imageinspect.Inspector{}).Resolve
	}
	if config.StepTimeout == 0 {
		config.StepTimeout = 5 * time.Minute
	}
	if config.StepTimeout < time.Second || config.StepTimeout > time.Hour {
		return nil, errors.New("invalid provision step timeout")
	}
	if config.StopGrace == 0 {
		config.StopGrace = 30 * time.Second
	}
	if config.StopGrace < time.Second || config.StopGrace > 10*time.Minute {
		return nil, errors.New("invalid provision stop grace period")
	}
	if config.EndpointPorts.First == 0 || config.EndpointPorts.First > config.EndpointPorts.Last {
		return nil, errors.New("provision planner requires the port range it publishes on")
	}
	if !config.Budget.Valid() || config.Budget.CPUMillis == 0 || config.Budget.MemoryBytes == 0 || config.Budget.DiskBytes == 0 {
		return nil, errors.New("provision planner requires a complete budget")
	}
	return &Planner{config: config}, nil
}

// Plan produces and installs the plan for one claimed operation.
func (p *Planner) Plan(ctx context.Context, claim operation.Claim) error {
	op, err := p.config.Store.GetOperation(ctx, claim.InstallationID, claim.OperationID)
	if err != nil {
		return err
	}
	if !strings.HasPrefix(op.Kind, "app.") {
		return fmt.Errorf("%w: %s", ErrUnplannable, op.Kind)
	}
	err = p.appLifecycle(ctx, claim, op)
	if err != nil && ctx.Err() == nil {
		code := "app_planning_pending"
		if failure, ok := errors.AsType[*imageinspect.Error](err); ok {
			code = string(failure.Code)
		}
		if errors.Is(err, placement.ErrUnsatisfied) {
			code = "app_capacity_unavailable"
		}
		if noteErr := p.config.Store.RecordAppPlanningFailure(ctx, claim, code); noteErr != nil {
			return errors.Join(err, noteErr)
		}
	}
	return err
}

// member is one copy and the node it runs on.
type member struct {
	NodeID     uuid.UUID
	ResourceID uuid.UUID
}

// epochFor names the node one copy's commands run on. Copies can sit on different
// nodes, so the epoch belongs to the copy rather than to the operation.
func epochFor(claim operation.Claim, nodeID uuid.UUID) *agentv1.Epoch {
	return &agentv1.Epoch{
		InstallationId: claim.InstallationID.String(), NodeId: nodeID.String(),
		AppId: claim.AppID.String(), RecoveryGeneration: claim.RecoveryGeneration.String(),
		ExecutionEpoch: claim.ExecutionEpoch,
	}
}

// stopSteps stops every copy in turn and leaves their data alone.
func (p *Planner) stopSteps(claim operation.Claim, op operation.Operation, running []member) ([]*agentv1.PlanStep, error) {
	steps := make([]*agentv1.PlanStep, 0, len(running))
	for _, each := range running {
		var dependsOn []string
		if len(steps) > 0 {
			dependsOn = []string{steps[len(steps)-1].Command.StepId}
		}
		stop, err := p.step(claim, epochFor(claim, each.NodeID), op.DesiredRevision, &agentv1.CommandInput{
			ResourceId: each.ResourceID.String(),
			Payload: &agentv1.CommandInput_StopContainer{StopContainer: &agentv1.StopContainer{
				GracePeriod: durationpb.New(p.config.StopGrace),
			}},
		}, agentv1.Postcondition_POSTCONDITION_CONTAINER_STOPPED, dependsOn,
			// A stop is safe to abandon until it starts. Once the container is shutting
			// down, stopping halfway is worse than finishing.
			agentv1.CancellationBoundary_CANCELLATION_BOUNDARY_SAFE_STOP)
		if err != nil {
			return nil, err
		}
		steps = append(steps, stop)
	}
	return steps, nil
}

// deleteSteps stop each container and remove it. Data removal is a separate step.
func (p *Planner) deleteSteps(claim operation.Claim, op operation.Operation, running []member) ([]*agentv1.PlanStep, error) {
	steps := make([]*agentv1.PlanStep, 0, 2*len(running))
	for _, each := range running {
		epoch := epochFor(claim, each.NodeID)
		var dependsOn []string
		if len(steps) > 0 {
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
		}, agentv1.Postcondition_POSTCONDITION_CONTAINER_ABSENT, []string{stop.Command.StepId}, beforeExecution)
		if err != nil {
			return nil, err
		}
		steps = append(steps, stop, remove)
	}
	return steps, nil
}

// step builds one plan step and seals its input hash.
func (p *Planner) step(claim operation.Claim, epoch *agentv1.Epoch, revision int64,
	input *agentv1.CommandInput, postcondition agentv1.Postcondition, dependsOn []string,
	boundary agentv1.CancellationBoundary) (*agentv1.PlanStep, error) {
	input.PayloadVersion = operation.PayloadVersion
	input.Timeout = durationpb.New(p.config.StepTimeout)
	if input.GetSwitchDatabase() != nil {
		input.Timeout = durationpb.New(min(p.config.StepTimeout, time.Minute))
	}
	command := &agentv1.Command{
		Epoch: epoch, OperationId: claim.OperationID.String(),
		StepId: uuid.NewString(), CommandId: uuid.NewString(),
		DesiredRevision: revision, PlanVersion: 1, Input: input,
	}
	hash, err := operation.CommandHash(command)
	if err != nil {
		return nil, err
	}
	command.InputHash = hash[:]
	return &agentv1.PlanStep{
		Command: command, DependsOn: dependsOn, MaxAttempts: 3,
		CancellationBoundary: boundary, Postcondition: postcondition,
	}, nil
}

// beforeExecution is the boundary for a step that creates or starts something.
// Nothing there is destructive, so nothing needs a later one.
const beforeExecution = agentv1.CancellationBoundary_CANCELLATION_BOUNDARY_BEFORE_EXECUTION

// superuserRole is the Synod-only superuser of a tier 1 engine.
const superuserRole = "synod_admin"
