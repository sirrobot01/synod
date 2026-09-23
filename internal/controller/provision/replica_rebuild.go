package provision

import (
	"context"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/cluster"
	"github.com/sirrobot01/synod/internal/operation"
)

func (p *Planner) rebuildReplica(ctx context.Context, claim operation.Claim, op operation.Operation) error {
	found, err := p.config.Store.GetCluster(ctx, claim.InstallationID, claim.ClusterID)
	if err != nil {
		return err
	}
	if found.Spec.Engine != cluster.PostgreSQL || found.Spec.PostgreSQL == nil || found.Spec.PostgreSQL.Version != "17.11" || found.Spec.PostgreSQL.Topology != "standalone" || found.DesiredReplicas != 1 {
		return ErrUnplannable
	}
	running, err := p.members(ctx, claim)
	if err != nil {
		return err
	}
	if len(running) != 2 {
		return ErrUnplannable
	}
	var primary, target member
	for _, each := range running {
		switch each.Role {
		case "primary":
			primary = each
		case "replica":
			target = each
		default:
			return ErrUnplannable
		}
	}
	if primary.MemberID == uuid.Nil || target.MemberID == uuid.Nil {
		return ErrUnplannable
	}
	reviewed, failed, err := p.config.Store.ReadReplicaActionTarget(ctx, claim.InstallationID, claim.OperationID)
	if err != nil {
		return err
	}
	if !failed || reviewed != target.MemberID {
		return operation.ErrCommandConflict
	}
	prefix, err := p.replicaRemovalSteps(ctx, claim, op, found, target, primary, true)
	if err != nil {
		return err
	}
	upstream, err := p.peer(ctx, claim, primary)
	if err != nil {
		return err
	}
	return p.addReplica(ctx, claim, found, primary, upstream, prefix)
}
