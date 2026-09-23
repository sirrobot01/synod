package provision

import (
	"context"
	"fmt"
	"slices"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/operation"
	"github.com/sirrobot01/synod/internal/store/postgres"
)

// retryReplica preserves the failed add's inputs and binds its commands to the new operation.
func (p *Planner) retryReplica(ctx context.Context, claim operation.Claim, op operation.Operation, running []member) (bool, error) {
	plan, dispatched, err := p.config.Store.ReadScaleRetryPlan(ctx, claim)
	if err != nil || plan == nil {
		return false, err
	}
	var replica member
	for _, each := range running {
		if each.Role == "replica" {
			if replica.MemberID != uuid.Nil {
				return false, fmt.Errorf("%w: more than one residual replica", ErrUnplannable)
			}
			replica = each
		}
	}
	if replica.MemberID == uuid.Nil || (replica.State != "pending" && replica.State != "residual") {
		return false, fmt.Errorf("%w: no residual replica to recover", ErrUnplannable)
	}
	if len(plan.Steps) != 4 || plan.Steps[0].Command.Input.GetEnsureVolume() == nil ||
		plan.Steps[1].Command.Input.GetEnsureContainer() == nil ||
		plan.Steps[2].Command.Input.GetStartContainer() == nil ||
		plan.Steps[3].Command.Input.GetCheckReadiness() == nil {
		return false, operation.ErrUnsupportedPlan
	}
	container := plan.Steps[1].Command.Input.GetEnsureContainer()
	check := plan.Steps[3].Command.Input.GetCheckReadiness()
	if container.Redis == nil || container.Redis.Upstream == nil || check.RedisReplication == nil ||
		check.RedisReplication.ExpectedRole != "replica" || container.Engine != "redis" {
		return false, operation.ErrUnsupportedPlan
	}
	steps := make(map[string]string, len(plan.Steps))
	for i, step := range plan.Steps {
		command := step.Command
		if command.Epoch.NodeId != replica.NodeID.String() || (i > 0 && command.Input.ResourceId != replica.ResourceID.String()) {
			return false, operation.ErrCommandConflict
		}
		if start := command.Input.GetStartContainer(); start != nil && slices.ContainsFunc(dispatched, func(evidence postgres.DispatchEvidence) bool {
			return evidence.Command.CommandId == command.CommandId
		}) {
			start.AllowInitialization = false
		}
		for i, dependency := range step.DependsOn {
			step.DependsOn[i] = steps[dependency]
		}
		newStep := uuid.NewString()
		steps[command.StepId] = newStep
		command.StepId, command.CommandId = newStep, uuid.NewString()
		command.OperationId = claim.OperationID.String()
		command.Epoch = epochFor(claim, replica.NodeID)
		command.DesiredRevision = op.DesiredRevision
		hash, err := operation.CommandHash(command)
		if err != nil {
			return false, err
		}
		command.InputHash = hash[:]
	}
	return true, p.config.Store.InstallPlan(ctx, claim, plan)
}
