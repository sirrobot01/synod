package drain

import (
	"bytes"
	"context"
	"errors"
	"slices"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/cluster"
	"github.com/sirrobot01/synod/internal/operation"
	"github.com/sirrobot01/synod/internal/placement"
	"github.com/sirrobot01/synod/internal/store/postgres"
)

// ErrNodeUnknown reports a node this installation does not have.
var ErrNodeUnknown = errors.New("node_unknown")

// Store is the durable boundary this package reads. *postgres.Store satisfies it.
//
// Every call is a read. Nothing here can change what the node holds, which is the
// point: a drain report is evidence for an operator's decision, not the decision.
type Store interface {
	ReadPlacementSnapshot(ctx context.Context, installationID uuid.UUID) (placement.Snapshot, error)
	ReadPlacementHolds(ctx context.Context, installationID uuid.UUID) ([]postgres.Hold, error)
	ReadNodeVolumeIdentities(ctx context.Context, installationID, nodeID uuid.UUID) ([]postgres.VolumeIdentity, error)
	GetCluster(ctx context.Context, installationID, clusterID uuid.UUID) (cluster.Cluster, error)
}

// Read gathers what one node holds, so Decide can answer what it needs.
//
// Reservations and storage are read separately on purpose. A node can hold retained
// storage for a cluster that has no reservation left on it, and that storage is
// exactly what stops the node being declared free.
func Read(ctx context.Context, store Store, installationID, nodeID uuid.UUID) (State, error) {
	if store == nil || installationID == uuid.Nil || nodeID == uuid.Nil {
		return State{}, ErrNodeUnknown
	}
	snapshot, err := store.ReadPlacementSnapshot(ctx, installationID)
	if err != nil {
		return State{}, err
	}
	index := slices.IndexFunc(snapshot.Nodes, func(node placement.Node) bool { return node.ID == nodeID })
	if index < 0 {
		return State{}, ErrNodeUnknown
	}
	state := State{InstallationID: installationID, NodeID: nodeID}
	if policy := snapshot.Nodes[index].Policy; policy != nil {
		state.Cordoned = policy.Cordoned
	}
	holds, err := store.ReadPlacementHolds(ctx, installationID)
	if err != nil {
		return State{}, err
	}
	identities, err := store.ReadNodeVolumeIdentities(ctx, installationID, nodeID)
	if err != nil {
		return State{}, err
	}
	workloads := make(map[uuid.UUID]*Workload)
	for _, hold := range holds {
		if hold.NodeID != nodeID {
			continue
		}
		found := workloads[hold.ClusterID]
		if found == nil {
			found = &Workload{ClusterID: hold.ClusterID}
			workloads[hold.ClusterID] = found
		}
		found.Resources = append(found.Resources, hold.ResourceID)
		if unfinished(hold.Status) {
			found.InFlight = true
		}
	}
	for _, identity := range identities {
		found := workloads[identity.ClusterID]
		if found == nil {
			found = &Workload{ClusterID: identity.ClusterID}
			workloads[identity.ClusterID] = found
		}
		found.Volumes = append(found.Volumes, Volume{
			ResourceID:           identity.ResourceID,
			Retention:            identity.Retention,
			CapacityRequestBytes: identity.CapacityRequestBytes,
		})
	}
	for clusterID, workload := range workloads {
		found, err := store.GetCluster(ctx, installationID, clusterID)
		if err != nil {
			return State{}, err
		}
		workload.Spec, workload.DesiredLifecycle = found.Spec, found.DesiredLifecycle
		slices.SortFunc(workload.Resources, func(a, b uuid.UUID) int { return compare(a, b) })
		slices.SortFunc(workload.Volumes, func(a, b Volume) int { return compare(a.ResourceID, b.ResourceID) })
		state.Workloads = append(state.Workloads, *workload)
	}
	return state, nil
}

// unfinished reports an operation that has not stopped. Acting on a cluster while one
// is running races it.
func unfinished(status operation.Status) bool {
	switch status {
	case operation.Pending, operation.Running, operation.Waiting, operation.Cancelling:
		return true
	}
	return false
}

func compare(a, b uuid.UUID) int { return bytes.Compare(a[:], b[:]) }
