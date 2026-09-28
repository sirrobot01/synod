package journal

import (
	"fmt"
	"maps"
	"slices"

	"github.com/sirrobot01/synod/internal/operation"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/proto"
)

// CloseAdmission closes the exact persisted epoch without lowering its authority.
// Already-closed calls preserve new drain evidence. Session freshness is a caller gate.
func (j *Journal) CloseAdmission(epoch *agentv1.Epoch) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if err := j.ready(); err != nil {
		return err
	}
	if err := j.checkTarget(epoch); err != nil {
		return err
	}
	app, exists := j.state.Apps[epoch.AppId]
	if !exists || !proto.Equal(app.Prepared, epoch) {
		return operation.ErrStaleOwner
	}
	if !app.Active {
		return nil
	}
	app.Active, app.Reconciled = false, false
	if len(app.Barrier) != 0 {
		var report agentv1.BarrierResult
		if err := proto.Unmarshal(app.Barrier, &report); err != nil {
			return fmt.Errorf("decode the saved barrier of app %s: %w: %w", epoch.AppId, ErrRecoveryRequired, err)
		}
		report.AdmissionsClosed, report.Drained = true, false
		encoded, err := proto.Marshal(&report)
		if err != nil {
			return err
		}
		app.Barrier = encoded
	}
	return j.persist(nil, map[string]appState{epoch.AppId: app})
}

// RecoveryRecords includes acknowledged results with current unresolved effects.
// The detached records use command-ID order and the journal's existing storage bounds.
func (j *Journal) RecoveryRecords() ([]Record, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if err := j.ready(); err != nil {
		return nil, err
	}
	var records []Record
	for _, id := range slices.Sorted(maps.Keys(j.state.Commands)) {
		r := j.state.Commands[id]
		if r.State != Running && (r.State != Terminal || !slices.ContainsFunc(r.Effects, func(effect *agentv1.Effect) bool { return effect.Active || effect.OutcomeUnknown })) {
			continue
		}
		view, err := r.view()
		if err != nil {
			return nil, err
		}
		records = append(records, view)
	}
	return records, nil
}
