package provision

import (
	"context"
	"fmt"

	"github.com/sirrobot01/synod/internal/operation"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/proto"
)

func (p *Planner) appLifecycle(ctx context.Context, claim operation.Claim, op operation.Operation) error {
	if op.Kind == "app.deploy" {
		return p.deployApp(ctx, claim)
	}
	copies, err := p.config.Store.GetAppCopies(ctx, claim.InstallationID, claim.ClusterID)
	if err != nil {
		return err
	}
	if len(copies) == 0 {
		if op.Kind == "app.start" {
			return p.deployApp(ctx, claim)
		}
		return ErrNoWorkload
	}
	var deployment *agentv1.ExecutionPlan
	if op.Kind == "app.start" {
		deployment, err = p.config.Store.ReadAppDeployment(ctx, claim.InstallationID, claim.ClusterID)
		if err != nil {
			return err
		}
		if deployment == nil {
			return ErrNoWorkload
		}
	}
	var steps []*agentv1.PlanStep
	for _, copy := range copies {
		each := member{NodeID: copy.NodeID, ResourceID: copy.ID, Port: copy.HostPort}
		var next []*agentv1.PlanStep
		switch op.Kind {
		case "app.stop":
			next, err = p.stopSteps(claim, op, []member{each})
		case "app.delete":
			next, err = p.deleteSteps(claim, op, []member{each})
		case "app.start":
			var input *agentv1.CommandInput
			for _, step := range deployment.Steps {
				if step.Command.Input.ResourceId == copy.ID.String() && step.Command.Epoch.NodeId == copy.NodeID.String() && step.Command.Input.GetEnsureContainer() != nil {
					input = proto.Clone(step.Command.Input).(*agentv1.CommandInput)
				}
			}
			if input == nil || input.GetEnsureContainer().GetGeneric() == nil {
				return fmt.Errorf("%w: copy %s has no stored configuration", ErrNoWorkload, copy.ID)
			}
			ensure, e := p.step(claim, epochFor(claim, copy.NodeID), op.DesiredRevision, input, agentv1.Postcondition_POSTCONDITION_CONTAINER_EXISTS, nil, agentv1.CancellationBoundary_CANCELLATION_BOUNDARY_BEFORE_EXECUTION)
			if e != nil {
				return e
			}
			start, e := p.step(claim, epochFor(claim, copy.NodeID), op.DesiredRevision, &agentv1.CommandInput{ResourceId: copy.ID.String(), Payload: &agentv1.CommandInput_StartContainer{StartContainer: &agentv1.StartContainer{}}}, agentv1.Postcondition_POSTCONDITION_CONTAINER_RUNNING, []string{ensure.Command.StepId}, agentv1.CancellationBoundary_CANCELLATION_BOUNDARY_BEFORE_EXECUTION)
			if e != nil {
				return e
			}
			next = []*agentv1.PlanStep{ensure, start}
		default:
			return ErrUnplannable
		}
		if err != nil {
			return err
		}
		if len(steps) > 0 {
			next[0].DependsOn = []string{steps[len(steps)-1].Command.StepId}
		}
		steps = append(steps, next...)
	}
	plan := &agentv1.ExecutionPlan{Version: 1, Steps: steps}
	if err := operation.ValidatePlan(plan, op); err != nil {
		return err
	}
	return p.config.Store.InstallPlan(ctx, claim, plan)
}
