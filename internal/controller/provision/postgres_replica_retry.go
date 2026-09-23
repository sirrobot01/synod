package provision

import (
	"context"
	"fmt"
	"slices"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/operation"
	"github.com/sirrobot01/synod/internal/store/postgres"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/proto"
)

// retryPostgresReplica preserves the allocation and verifies any previously dispatched copy.
func (p *Planner) retryPostgresReplica(ctx context.Context, claim operation.Claim, op operation.Operation, primary member, running []member) (bool, error) {
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
		return false, fmt.Errorf("%w: no residual PostgreSQL replica to recover", ErrUnplannable)
	}
	// A failed rebuild retires the proved old member in its terminal transaction.
	// The remaining live member must match the retained replacement plan below.
	if len(plan.Steps) == 12 && plan.Steps[0].Command.Input.GetVerifyPostgresReplicaFailure() != nil && plan.Steps[2].Command.Input.GetRemoveContainer() != nil {
		plan.Steps = plan.Steps[3:]
	}
	var config *agentv1.PostgresContainerConfig
	switch len(plan.Steps) {
	case 9:
		volume := plan.Steps[1].Command.Input.GetEnsureVolume()
		container := plan.Steps[2].Command.Input.GetEnsureContainer()
		clone := plan.Steps[4].Command.Input.GetClonePostgresReplica()
		if volume == nil || container == nil || container.Engine != "postgres" || container.Postgres == nil ||
			plan.Steps[3].Command.Input.GetPreparePostgresReplica() == nil || clone == nil ||
			container.VolumeId != plan.Steps[1].Command.Input.ResourceId || !proto.Equal(container.Postgres, clone.Postgres) {
			return false, operation.ErrUnsupportedPlan
		}
		config = clone.Postgres
		for i, step := range plan.Steps[1:5] {
			if step.Command.Epoch.NodeId != replica.NodeID.String() || (i > 0 && step.Command.Input.ResourceId != replica.ResourceID.String()) {
				return false, operation.ErrCommandConflict
			}
		}
		// Only these agent guards prove that runtime execution never started.
		if slices.ContainsFunc(dispatched, func(evidence postgres.DispatchEvidence) bool {
			if evidence.Command.CommandId != plan.Steps[4].Command.CommandId {
				return false
			}
			result := evidence.Result
			return result == nil || result.State != "failed" || len(result.Effects) != 0 ||
				(result.FailureCode != "capacity_unavailable" && result.FailureCode != "secret_unavailable")
		}) {
			verify := proto.Clone(plan.Steps[4]).(*agentv1.PlanStep)
			verify.Command.Input.Secrets = nil
			verify.Command.Input.Payload = &agentv1.CommandInput_VerifyPostgresReplicaClone{
				VerifyPostgresReplicaClone: &agentv1.VerifyPostgresReplicaClone{Postgres: config, CloneCommandId: verify.Command.CommandId},
			}
			plan.Steps = append([]*agentv1.PlanStep{plan.Steps[0], verify}, plan.Steps[5:]...)
		}
	case 6:
		verify := plan.Steps[1].Command.Input.GetVerifyPostgresReplicaClone()
		if verify == nil {
			return false, operation.ErrUnsupportedPlan
		}
		config = verify.Postgres
	default:
		return false, operation.ErrUnsupportedPlan
	}
	last := len(plan.Steps) - 1
	source := plan.Steps[0].Command.Input.GetCheckReadiness()
	stop := plan.Steps[last-3].Command.Input.GetStopContainer()
	start := plan.Steps[last-2].Command.Input.GetStartContainer()
	native := plan.Steps[last-1].Command.Input.GetCheckReadiness()
	app := plan.Steps[last].Command.Input.GetCheckReadiness()
	if config == nil || config.ReplicaSource == nil || config.Port != uint32(replica.Port) ||
		config.ReplicaSource.ResourceId != primary.ResourceID.String() || config.ReplicaSource.Port != uint32(primary.Port) ||
		source == nil || source.Engine != "postgres" || source.PostgresRole != "primary" || source.PostgresReplication == nil ||
		stop == nil || start == nil || start.AllowInitialization ||
		native == nil || native.Engine != "postgres" || native.PostgresRole != "replica" || native.PostgresReplication == nil ||
		!proto.Equal(native.PostgresReplication.Source, config.ReplicaSource) ||
		app == nil || app.Engine != "postgres" || app.PostgresRole != "replica" {
		return false, operation.ErrUnsupportedPlan
	}
	for i, step := range plan.Steps {
		command := step.Command
		node, resource := replica.NodeID, replica.ResourceID.String()
		if i == 0 {
			node, resource = primary.NodeID, primary.ResourceID.String()
		} else if len(plan.Steps) == 9 && i == 1 {
			resource = plan.Steps[2].Command.Input.GetEnsureContainer().VolumeId
		}
		if command.Epoch.NodeId != node.String() || command.Input.ResourceId != resource {
			return false, operation.ErrCommandConflict
		}
		command.StepId, command.CommandId = uuid.NewString(), uuid.NewString()
		command.OperationId, command.DesiredRevision = claim.OperationID.String(), op.DesiredRevision
		command.Epoch = epochFor(claim, node)
		step.DependsOn = nil
		if i > 0 {
			step.DependsOn = []string{plan.Steps[i-1].Command.StepId}
		}
		hash, err := operation.CommandHash(command)
		if err != nil {
			return false, err
		}
		command.InputHash = hash[:]
	}
	return true, p.config.Store.InstallPlan(ctx, claim, plan)
}
