package journal

import (
	"google.golang.org/protobuf/encoding/protowire"
	"maps"
	"math"

	"github.com/sirrobot01/synod/internal/operation"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/proto"
)

// Prepare closes admission durably. It does not stop an already running native task.
func (j *Journal) Prepare(epoch *agentv1.Epoch) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if err := j.ready(); err != nil {
		return err
	}
	if err := j.checkTarget(epoch); err != nil {
		return err
	}
	previous, exists := j.state.Clusters[epoch.ClusterId]
	if exists {
		if epoch.ExecutionEpoch < previous.Prepared.ExecutionEpoch {
			return operation.ErrStaleOwner
		}
		if epoch.ExecutionEpoch == previous.Prepared.ExecutionEpoch {
			return nil
		}
	}
	prepared := clusterState{Prepared: proto.Clone(epoch).(*agentv1.Epoch), InventoryLimit: previous.InventoryLimit, ReservedBytes: previous.ReservedBytes}
	if !exists {
		prepared.InventoryLimit = j.options.MaxInventoryBytes
		prepared.ReservedBytes = inventoryReservation(j.options.MaxInventoryBytes)
	}
	if len(previous.Barrier) != 0 {
		var retained agentv1.BarrierResult
		if proto.Unmarshal(previous.Barrier, &retained) != nil {
			return ErrRecoveryRequired
		}
		retained.Epoch = proto.Clone(epoch).(*agentv1.Epoch)
		retained.AdmissionsClosed = true
		retained.Drained = false
		var err error
		prepared.Barrier, err = proto.Marshal(&retained)
		if err != nil {
			return err
		}
	}
	return j.persist(nil, map[string]clusterState{epoch.ClusterId: prepared})
}

func (j *Journal) barrier(epoch *agentv1.Epoch) (*agentv1.BarrierResult, error) {
	if err := j.checkTarget(epoch); err != nil {
		return nil, err
	}
	cluster, exists := j.state.Clusters[epoch.ClusterId]
	if !exists || cluster.Active || !proto.Equal(epoch, cluster.Prepared) {
		return nil, operation.ErrBarrierPending
	}
	result := &agentv1.BarrierResult{Epoch: proto.Clone(epoch).(*agentv1.Epoch), AdmissionsClosed: true, Drained: true}
	if len(cluster.Barrier) != 0 {
		var previous agentv1.BarrierResult
		if proto.Unmarshal(cluster.Barrier, &previous) != nil {
			return nil, ErrRecoveryRequired
		}
		result.Effects = previous.Effects
		result.Drained = previous.Drained
		result.FailureCode = previous.FailureCode
	}
	size := proto.Size(result)
	for record := range maps.Values(j.state.Commands) {
		if record.Epoch.ClusterId != epoch.ClusterId {
			continue
		}
		if record.State == Running {
			result.Drained = false
			effect := &agentv1.Effect{CommandId: record.ID, ResourceId: record.ResourceID, Active: true, OutcomeUnknown: true, State: "runtime_inspection_required"}
			size += protowire.SizeTag(4) + protowire.SizeBytes(proto.Size(effect))
			if size > cluster.InventoryLimit {
				return nil, j.blockExecution(ErrBusy)
			}
			result.Effects = append(result.Effects, effect)
		}
		for _, effect := range record.Effects {
			if !effect.Active && !effect.OutcomeUnknown {
				continue
			}
			size += protowire.SizeTag(4) + protowire.SizeBytes(proto.Size(effect))
			if size > cluster.InventoryLimit {
				return nil, j.blockExecution(ErrBusy)
			}
			result.Effects = append(result.Effects, proto.Clone(effect).(*agentv1.Effect))
			if effect.Active || effect.OutcomeUnknown {
				result.Drained = false
			}
		}
	}
	return result, nil
}

// Barrier combines durable runtime observations with incomplete command state.
func (j *Journal) Barrier(epoch *agentv1.Epoch) (*agentv1.BarrierResult, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if err := j.ready(); err != nil {
		return nil, err
	}
	return j.barrier(epoch)
}

// RecordBarrier stores inventory evidence. The caller must inspect native tasks,
// including tasks that do not have a command record after process interruption.
func (j *Journal) RecordBarrier(result *agentv1.BarrierResult) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if err := j.ready(); err != nil {
		return err
	}
	if result == nil {
		return operation.ErrBarrierPending
	}
	if err := j.checkTarget(result.Epoch); err != nil {
		return err
	}
	cluster, exists := j.state.Clusters[result.Epoch.ClusterId]
	if !exists || cluster.Active || !proto.Equal(cluster.Prepared, result.Epoch) || !result.AdmissionsClosed {
		return operation.ErrBarrierPending
	}
	for _, effect := range result.Effects {
		if effect == nil {
			return operation.ErrBarrierPending
		}
	}
	if proto.Size(result) > cluster.InventoryLimit {
		return j.blockExecution(ErrBusy)
	}
	encoded, err := proto.Marshal(result)
	if err != nil {
		return err
	}
	// Keep room for the largest later epoch number when Prepare retains inventory.
	future := proto.Clone(result).(*agentv1.BarrierResult)
	future.Epoch.ExecutionEpoch = math.MaxInt64
	if proto.Size(future) > cluster.InventoryLimit {
		return j.blockExecution(ErrBusy)
	}
	// Previously observed native work must be accounted for, even when it was unjournaled.
	if len(cluster.Barrier) != 0 {
		var previous agentv1.BarrierResult
		if proto.Unmarshal(cluster.Barrier, &previous) != nil {
			return ErrRecoveryRequired
		}
		for _, old := range previous.Effects {
			if !old.Active && !old.OutcomeUnknown {
				continue
			}
			found := false
			for _, effect := range result.Effects {
				if effect.CommandId == old.CommandId && effect.ResourceId == old.ResourceId && effect.NativeOperationId == old.NativeOperationId {
					found = true
					break
				}
			}
			if !found {
				return operation.ErrBarrierPending
			}
		}
	}
	if result.Drained {
		for record := range maps.Values(j.state.Commands) {
			if record.Epoch.ClusterId != result.Epoch.ClusterId {
				continue
			}
			if record.State == Running {
				return operation.ErrBarrierPending
			}
			for _, effect := range record.Effects {
				if effect.Active || effect.OutcomeUnknown {
					return operation.ErrBarrierPending
				}
			}
		}
		if operation.BarrierDrained(result) != nil {
			return operation.ErrBarrierPending
		}
	}
	cluster.Barrier = encoded
	cluster.Reconciled = false
	return j.persist(nil, map[string]clusterState{result.Epoch.ClusterId: cluster})
}

// Activate requires an authenticated controller confirmation that all reported
// residual effects were reconciled. The journal does not authenticate the caller.
func (j *Journal) Activate(epoch *agentv1.Epoch, reconciled bool) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if err := j.ready(); err != nil {
		return err
	}
	if j.state.ExecutionBlocked {
		return ErrRecoveryRequired
	}
	if err := j.checkTarget(epoch); err != nil {
		return err
	}
	cluster, exists := j.state.Clusters[epoch.ClusterId]
	if !exists || !proto.Equal(epoch, cluster.Prepared) {
		return operation.ErrStaleOwner
	}
	if cluster.Active && cluster.Reconciled {
		return nil
	}
	if !reconciled || len(cluster.Barrier) == 0 {
		return operation.ErrBarrierPending
	}
	result, err := j.barrier(epoch)
	if err != nil {
		return err
	}
	if err := operation.BarrierDrained(result); err != nil {
		return err
	}
	cluster.Active = true
	cluster.Reconciled = true
	return j.persist(nil, map[string]clusterState{epoch.ClusterId: cluster})
}
