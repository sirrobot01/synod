package provision

import (
	"context"

	"github.com/sirrobot01/synod/internal/operation"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/proto"
)

func (p *Planner) rejoinDatabase(ctx context.Context, claim operation.Claim, op operation.Operation) error {
	rejoin, err := p.config.Store.GetDatabaseRejoin(ctx, claim.InstallationID, claim.OperationID)
	if err != nil {
		return err
	}
	deployment, err := p.config.Store.ReadAppDeployment(ctx, claim.InstallationID, claim.AppID)
	if err != nil {
		return err
	}
	if deployment == nil {
		return ErrNoWorkload
	}
	var input *agentv1.CommandInput
	for _, step := range deployment.Steps {
		if step.Command.Input.ResourceId == rejoin.CopyID.String() && step.Command.Epoch.NodeId == rejoin.NodeID.String() && step.Command.Input.GetEnsureContainer() != nil {
			input = proto.Clone(step.Command.Input).(*agentv1.CommandInput)
			break
		}
	}
	if input == nil {
		return ErrNoWorkload
	}
	var steps []*agentv1.PlanStep
	var depends []string
	if rejoin.RemoveData {
		steps, err = p.deleteSteps(claim, op, []member{{NodeID: rejoin.NodeID, ResourceID: rejoin.CopyID}})
		if err != nil {
			return err
		}
		remove, err := p.step(claim, epochFor(claim, rejoin.NodeID), op.DesiredRevision, &agentv1.CommandInput{ResourceId: rejoin.CopyID.String(), Payload: &agentv1.CommandInput_RemoveAppData{RemoveAppData: &agentv1.RemoveAppData{WriterEpoch: rejoin.WriterEpoch}}}, agentv1.Postcondition_POSTCONDITION_APP_DATA_ABSENT, []string{steps[len(steps)-1].Command.StepId}, beforeExecution)
		if err != nil {
			return err
		}
		steps = append(steps, remove)
		depends = []string{remove.Command.StepId}
	}
	ensure, err := p.step(claim, epochFor(claim, rejoin.NodeID), op.DesiredRevision, input, agentv1.Postcondition_POSTCONDITION_CONTAINER_EXISTS, depends, beforeExecution)
	if err != nil {
		return err
	}
	start, err := p.step(claim, epochFor(claim, rejoin.NodeID), op.DesiredRevision, &agentv1.CommandInput{ResourceId: rejoin.CopyID.String(), Payload: &agentv1.CommandInput_StartContainer{StartContainer: &agentv1.StartContainer{}}}, agentv1.Postcondition_POSTCONDITION_CONTAINER_RUNNING, []string{ensure.Command.StepId}, beforeExecution)
	if err != nil {
		return err
	}
	steps = append(steps, ensure, start)
	plan := &agentv1.ExecutionPlan{Version: 1, Steps: steps}
	if err := operation.ValidatePlan(plan, op); err != nil {
		return err
	}
	return p.config.Store.InstallPlan(ctx, claim, plan)
}
