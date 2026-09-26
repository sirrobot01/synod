package provision

import (
	"context"
	"errors"
	"fmt"
	"net/netip"

	"github.com/sirrobot01/synod/internal/cluster"
	"github.com/sirrobot01/synod/internal/operation"
	"github.com/sirrobot01/synod/internal/store/postgres"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
)

// switchover makes the named member the Patroni leader.
//
// A plan is fixed before it runs, and Patroni elects its own leader, so the plan cannot know
// the leader in advance. The switchover step succeeds at once when the named member already
// leads. Every member proves readiness first, and afterwards the named member proves it
// writes and the others prove they replicate. A routed cluster then checks every router.
func (p *Planner) switchover(ctx context.Context, claim operation.Claim, op operation.Operation) error {
	if p.config.Patroni == nil {
		return errors.New("Patroni HA is not configured on this controller")
	}
	if op.TargetMemberID == nil {
		return ErrUnplannable
	}
	found, err := p.config.Store.GetCluster(ctx, claim.InstallationID, claim.ClusterID)
	if err != nil {
		return err
	}
	if found.Spec.Engine != cluster.PostgreSQL || found.Spec.PostgreSQL == nil || found.Spec.PostgreSQL.Topology != "patroni" {
		return ErrUnplannable
	}
	running, err := p.members(ctx, claim)
	if err != nil {
		return err
	}
	if len(running) < operation.MinPatroniMembers || len(running) > operation.MaxPatroniMembers {
		return ErrUnplannable
	}
	candidate := -1
	for index, each := range running {
		if each.Role != "patroni" || each.State != "active" {
			return fmt.Errorf("%w: member %s is %s %s", ErrUnplannable, each.MemberID, each.State, each.Role)
		}
		if each.MemberID == *op.TargetMemberID {
			candidate = index
		}
	}
	if candidate < 0 {
		return fmt.Errorf("%w: member %s is not in the cluster", ErrUnplannable, *op.TargetMemberID)
	}
	application, err := p.config.Store.ReadClusterCredential(ctx, claim.InstallationID, claim.ClusterID, postgres.ApplicationCredential)
	if err != nil {
		return err
	}
	stored := make([]*agentv1.EnsureContainer, len(running))
	var steps []*agentv1.PlanStep
	var ready []string
	for index, each := range running {
		if stored[index], err = p.config.Store.ReadMemberContainer(ctx, claim.InstallationID, claim.ClusterID, each.ResourceID); err != nil {
			return err
		}
		check, err := p.patroniReadiness(claim, op, application, each, stored[index], "any", nil)
		if err != nil {
			return err
		}
		steps, ready = append(steps, check), append(ready, check.Command.StepId)
	}
	target, container := running[candidate], stored[candidate]
	patroni := container.GetPostgres().GetPatroni()
	address, err := netip.ParseAddr(patroni.GetAddress())
	if err != nil {
		return fmt.Errorf("%w: member %s stores no Patroni address", ErrUnplannable, target.MemberID)
	}
	var secrets []*agentv1.SecretReference
	for _, reference := range patroni.Credentials {
		if reference.Purpose == postgres.PatroniRESTCredential || reference.Purpose == postgres.PatroniMemberTLSCredential {
			secrets = append(secrets, reference)
		}
	}
	switched, err := p.step(claim, epochFor(claim, target.NodeID), op.DesiredRevision, &agentv1.CommandInput{
		ResourceId: target.ResourceID.String(), Secrets: secrets,
		Payload: &agentv1.CommandInput_SwitchoverPatroni{SwitchoverPatroni: &agentv1.SwitchoverPatroni{Address: address.String(), RestPort: patroni.RestPort}},
	}, agentv1.Postcondition_POSTCONDITION_PATRONI_LEADER, ready, beforeExecution)
	if err != nil {
		return err
	}
	steps = append(steps, switched)
	// The new leader proves it writes, then each other member proves it replicates.
	previous := switched.Command.StepId
	order := []int{candidate}
	for index := range running {
		if index != candidate {
			order = append(order, index)
		}
	}
	for _, index := range order {
		role := "replica"
		if index == candidate {
			role = "primary"
		}
		check, err := p.patroniReadiness(claim, op, application, running[index], stored[index], role, []string{previous})
		if err != nil {
			return err
		}
		steps, previous = append(steps, check), check.Command.StepId
	}
	if routed(running) {
		routes := make([]routeMember, len(running))
		for index, each := range running {
			if routes[index], err = routeMemberFrom(each, stored[index]); err != nil {
				return err
			}
		}
		checks, err := p.routeCheckSteps(claim, op, routes, application, nil)
		if err != nil {
			return err
		}
		steps = append(steps, chainAfter(previous, checks)...)
	}
	plan := &agentv1.ExecutionPlan{Version: 1, Steps: steps}
	if err := operation.ValidatePlan(plan, op); err != nil {
		return err
	}
	return p.config.Store.InstallPlan(ctx, claim, plan)
}
