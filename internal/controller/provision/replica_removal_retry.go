package provision

import (
	"context"

	"github.com/sirrobot01/synod/internal/operation"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
)

// retryReplicaRemoval verifies absence after a prior removal proved deletion.
func (p *Planner) retryReplicaRemoval(ctx context.Context, claim operation.Claim, op operation.Operation, target member) (bool, error) {
	plan, dispatched, err := p.config.Store.ReadScaleRetryPlan(ctx, claim)
	if err != nil || plan == nil {
		return false, err
	}
	last := plan.Steps[len(plan.Steps)-1]
	for _, step := range plan.Steps {
		if step.Command.Epoch.NodeId != target.NodeID.String() || step.Command.Input.ResourceId != target.ResourceID.String() {
			return false, operation.ErrCommandConflict
		}
	}
	proved := len(plan.Steps) == 1 && last.Command.Input.GetVerifyContainerAbsent() != nil
	if !proved {
		if len(plan.Steps) != 3 || last.Command.Input.GetRemoveContainer() == nil {
			return false, operation.ErrUnsupportedPlan
		}
		for _, evidence := range dispatched {
			if evidence.Command.CommandId == last.Command.CommandId && operation.CheckStepResult(last, evidence.Result) == nil {
				proved = true
			}
		}
	}
	if !proved {
		return false, nil
	}
	step, err := p.step(claim, epochFor(claim, target.NodeID), op.DesiredRevision, &agentv1.CommandInput{
		ResourceId: target.ResourceID.String(),
		Payload:    &agentv1.CommandInput_VerifyContainerAbsent{VerifyContainerAbsent: &agentv1.VerifyContainerAbsent{}},
	}, agentv1.Postcondition_POSTCONDITION_CONTAINER_ABSENT, nil, agentv1.CancellationBoundary_CANCELLATION_BOUNDARY_BEFORE_EXECUTION)
	if err != nil {
		return false, err
	}
	return true, p.config.Store.InstallPlan(ctx, claim, &agentv1.ExecutionPlan{Version: 1, Steps: []*agentv1.PlanStep{step}})
}
