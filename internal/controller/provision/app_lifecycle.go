package provision

import (
	"context"
	"fmt"

	"github.com/sirrobot01/synod/internal/app"
	"github.com/sirrobot01/synod/internal/operation"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/proto"
)

func (p *Planner) appLifecycle(ctx context.Context, claim operation.Claim, op operation.Operation) error {
	if op.Kind == "app.move" {
		return p.moveApp(ctx, claim, op)
	}
	if op.Kind == "app.failover" {
		return p.failoverApp(ctx, claim, op)
	}
	if op.Kind == "app.deploy" {
		return p.deployApp(ctx, claim)
	}
	copies, err := p.config.Store.GetAppCopies(ctx, claim.InstallationID, claim.ClusterID)
	if op.Kind == "app.delete" {
		copies, err = p.config.Store.GetAllAppCopies(ctx, claim.InstallationID, claim.ClusterID)
	}
	if err != nil {
		return err
	}
	if len(copies) == 0 {
		if op.Kind == "app.start" {
			return p.deployApp(ctx, claim)
		}
		return ErrNoWorkload
	}
	found, err := p.config.Store.GetApp(ctx, claim.InstallationID, claim.ClusterID)
	if err != nil {
		return err
	}
	var deployment *agentv1.ExecutionPlan
	var rollback bool
	if op.Kind == "app.start" {
		rollback, err = p.config.Store.IsAppMoveRollback(ctx, claim.InstallationID, claim.OperationID)
		if err != nil {
			return err
		}
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
		if copy.Role == "standby" && op.Kind != "app.delete" {
			continue
		}
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
			if input == nil && found.Tier == app.TierSingleWriter {
				for _, step := range deployment.Steps {
					if step.Command.Input.GetEnsureContainer().GetGeneric() == nil {
						continue
					}
					input = proto.Clone(step.Command.Input).(*agentv1.CommandInput)
					input.ResourceId = copy.ID.String()
					input.GetEnsureContainer().Generic.CopyIndex = uint32(copy.CopyIndex)
					input.GetEnsureContainer().Ports[0].HostPort = uint32(copy.HostPort)
					break
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
			if rollback {
				// The first stop may have failed. Close its port before the ensure check.
				stopped, err := p.stopSteps(claim, op, []member{each})
				if err != nil {
					return err
				}
				ensure.DependsOn = []string{stopped[0].Command.StepId}
				next = append(stopped, ensure, start)
			} else {
				next = []*agentv1.PlanStep{ensure, start}
			}
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
	if op.Kind == "app.delete" && found.Tier == app.TierSingleWriter {
		writer, err := p.config.Store.GetAppWriter(ctx, claim.InstallationID, claim.ClusterID)
		if err != nil {
			return err
		}
		nodes := map[string]bool{}
		for _, copy := range copies {
			if nodes[copy.NodeID.String()] {
				continue
			}
			nodes[copy.NodeID.String()] = true
			cleanup, err := p.step(claim, epochFor(claim, copy.NodeID), op.DesiredRevision, &agentv1.CommandInput{ResourceId: copy.ID.String(), Payload: &agentv1.CommandInput_RemoveAppData{RemoveAppData: &agentv1.RemoveAppData{WriterEpoch: writer.Epoch}}}, agentv1.Postcondition_POSTCONDITION_APP_DATA_ABSENT, []string{steps[len(steps)-1].Command.StepId}, beforeExecution)
			if err != nil {
				return err
			}
			steps = append(steps, cleanup)
		}
	}
	plan := &agentv1.ExecutionPlan{Version: 1, Steps: steps}
	if err := operation.ValidatePlan(plan, op); err != nil {
		return err
	}
	return p.config.Store.InstallPlan(ctx, claim, plan)
}
