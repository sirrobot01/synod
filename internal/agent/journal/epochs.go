package journal

import (
	"maps"
	"math"
	"slices"

	"google.golang.org/protobuf/encoding/protowire"

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
	previous, exists := j.state.Apps[epoch.AppId]
	if exists {
		if epoch.ExecutionEpoch < previous.Prepared.ExecutionEpoch {
			return operation.ErrStaleOwner
		}
		if epoch.ExecutionEpoch == previous.Prepared.ExecutionEpoch {
			return nil
		}
	}
	prepared := appState{Prepared: proto.Clone(epoch).(*agentv1.Epoch), InventoryLimit: previous.InventoryLimit, ReservedBytes: previous.ReservedBytes}
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
	return j.persist(nil, map[string]appState{epoch.AppId: prepared})
}

func (j *Journal) barrier(epoch *agentv1.Epoch) (*agentv1.BarrierResult, error) {
	if err := j.checkTarget(epoch); err != nil {
		return nil, err
	}
	app, exists := j.state.Apps[epoch.AppId]
	if !exists || app.Active || !proto.Equal(epoch, app.Prepared) {
		return nil, operation.ErrBarrierPending
	}
	result := &agentv1.BarrierResult{Epoch: proto.Clone(epoch).(*agentv1.Epoch), AdmissionsClosed: true, Drained: true}
	if len(app.Barrier) != 0 {
		var previous agentv1.BarrierResult
		if proto.Unmarshal(app.Barrier, &previous) != nil {
			return nil, ErrRecoveryRequired
		}
		result.Effects = previous.Effects
		result.Drained = previous.Drained
		result.FailureCode = previous.FailureCode
	}
	size := proto.Size(result)
	for record := range maps.Values(j.state.Commands) {
		if record.Epoch.AppId != epoch.AppId {
			continue
		}
		current := make([]*agentv1.Effect, 0, len(record.Effects)+1)
		if record.State == Running {
			current = append(current, &agentv1.Effect{CommandId: record.ID, ResourceId: record.ResourceID, Active: true, OutcomeUnknown: true, State: "runtime_inspection_required"})
		}
		current = append(current, record.Effects...)
		for _, effect := range current {
			// A retained effect with the same identity is older evidence. The current
			// record replaces it, so repeated epochs never report one effect twice.
			index := slices.IndexFunc(result.Effects, func(retained *agentv1.Effect) bool {
				return retained.CommandId == effect.CommandId && retained.ResourceId == effect.ResourceId && retained.NativeOperationId == effect.NativeOperationId
			})
			if index < 0 && !effect.Active && !effect.OutcomeUnknown {
				continue
			}
			size += protowire.SizeTag(4) + protowire.SizeBytes(proto.Size(effect))
			if index >= 0 {
				size -= protowire.SizeTag(4) + protowire.SizeBytes(proto.Size(result.Effects[index]))
			}
			if size > app.InventoryLimit {
				return nil, j.blockExecution(ErrBusy)
			}
			if index >= 0 {
				result.Effects[index] = proto.Clone(effect).(*agentv1.Effect)
			} else {
				result.Effects = append(result.Effects, proto.Clone(effect).(*agentv1.Effect))
			}
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
	app, exists := j.state.Apps[result.Epoch.AppId]
	if !exists || app.Active || !proto.Equal(app.Prepared, result.Epoch) || !result.AdmissionsClosed {
		return operation.ErrBarrierPending
	}
	for _, effect := range result.Effects {
		if effect == nil {
			return operation.ErrBarrierPending
		}
	}
	if proto.Size(result) > app.InventoryLimit {
		return j.blockExecution(ErrBusy)
	}
	encoded, err := proto.Marshal(result)
	if err != nil {
		return err
	}
	// Keep room for the largest later epoch number when Prepare retains inventory.
	future := proto.Clone(result).(*agentv1.BarrierResult)
	future.Epoch.ExecutionEpoch = math.MaxInt64
	if proto.Size(future) > app.InventoryLimit {
		return j.blockExecution(ErrBusy)
	}
	// Previously observed native work must be accounted for, even when it was unjournaled.
	if len(app.Barrier) != 0 {
		var previous agentv1.BarrierResult
		if proto.Unmarshal(app.Barrier, &previous) != nil {
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
			if record.Epoch.AppId != result.Epoch.AppId {
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
	app.Barrier = encoded
	app.Reconciled = false
	return j.persist(nil, map[string]appState{result.Epoch.AppId: app})
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
	app, exists := j.state.Apps[epoch.AppId]
	if !exists || !proto.Equal(epoch, app.Prepared) {
		return operation.ErrStaleOwner
	}
	if app.Active && app.Reconciled {
		return nil
	}
	if !reconciled || len(app.Barrier) == 0 {
		return operation.ErrBarrierPending
	}
	result, err := j.barrier(epoch)
	if err != nil {
		return err
	}
	if err := operation.BarrierDrained(result); err != nil {
		return err
	}
	app.Active = true
	app.Reconciled = true
	return j.persist(nil, map[string]appState{epoch.AppId: app})
}
