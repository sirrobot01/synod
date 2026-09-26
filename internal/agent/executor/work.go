package executor

import (
	"context"
	"errors"
	"slices"
	"strings"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/agent/journal"
	"github.com/sirrobot01/synod/internal/operation"
	"github.com/sirrobot01/synod/internal/runtime"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/proto"
)

func (e *Executor) execute(ctx context.Context, task job) {
	e.mu.Lock()
	if ctx.Err() != nil || e.failure != nil {
		e.mu.Unlock()
		return
	}
	record, err := e.journal.Get(task.command)
	if err != nil {
		e.mu.Unlock()
		e.fail(err)
		return
	}
	if record.State != journal.Queued && record.State != journal.Running &&
		(record.State != journal.Terminal || !slices.ContainsFunc(record.Effects, func(effect *agentv1.Effect) bool { return effect.Active || effect.OutcomeUnknown })) {
		e.mu.Unlock()
		return
	}
	start := record.State == journal.Queued
	if start {
		current := e.session
		if current == nil || current != task.session || current.ctx.Err() != nil || current.gates[task.cluster] != task.gate || task.gate == nil || !task.gate.active {
			e.mu.Unlock()
			return
		}
		record, err = e.journal.Start(record.CommandID)
	}
	e.mu.Unlock()
	if err != nil {
		if !errors.Is(err, operation.ErrStaleOwner) && !errors.Is(err, journal.ErrBusy) && !errors.Is(err, journal.ErrAlreadyCompleted) {
			e.fail(err)
		}
		return
	}
	if start {
		// The host is rechecked immediately before the effect, because a
		// reservation taken at planning time does not stop another process from
		// filling the disk or taking the port. A refusal happens before the
		// runtime is called, so it leaves no native object behind.
		if err := e.precheck(record.Command); err != nil {
			e.recordOutcome(record, runtime.Result{}, err)
			return
		}
		// The credential is resolved from session memory immediately before the
		// effect. A command whose secret this session was never sent is refused
		// rather than run against an engine with no credential.
		secrets, err := e.resolve(record.Command)
		if err != nil {
			e.recordOutcome(record, runtime.Result{}, err)
			return
		}
		// A readiness check is an observation over the network, not a container
		// action, so it does not go to the runtime at all.
		if record.Command.Input.GetCheckReadiness() != nil {
			result, err := e.readiness(ctx, record.Command, secrets)
			e.recordOutcome(record, result, err)
			return
		}
		if record.Command.Input.GetSwitchoverPatroni() != nil {
			result, err := e.switchover(ctx, record.Command, secrets)
			e.recordOutcome(record, result, err)
			return
		}
		if record.Command.Input.GetEtcdMembership() != nil {
			result, err := e.etcdMembership(ctx, record.Command, secrets)
			e.recordOutcome(record, result, err)
			return
		}
		result, err := e.runtime.Execute(ctx, record.Command, secrets)
		e.recordOutcome(record, result, err)
		return
	}
	e.recoverRecord(ctx, record)
}

func (e *Executor) recoverRecord(ctx context.Context, record journal.Record) {
	if record.Command.Input.GetSwitchoverPatroni() != nil {
		e.recordOutcome(record, runtime.Result{}, errSwitchoverInterrupted)
		return
	}
	if record.Command.Input.GetEtcdMembership() != nil {
		e.recordOutcome(record, runtime.Result{}, errEtcdMembershipInterrupted)
		return
	}
	read, cancel := context.WithTimeout(context.WithoutCancel(ctx), e.options.InspectionTimeout)
	defer cancel()
	result, err := e.runtime.Recover(read, record.Command, record.Effects)
	e.recordOutcome(record, result, err)
}

func (e *Executor) recordOutcome(record journal.Record, result runtime.Result, runtimeErr error) {
	if len(result.Effects) != 0 {
		if err := e.journal.Observe(record.CommandID, result.Effects); err != nil {
			e.fail(err)
			return
		}
	}
	if record.State == journal.Terminal {
		return
	}
	if errors.Is(runtimeErr, runtime.ErrOutcomeUnknown) || slices.ContainsFunc(result.Effects, func(effect *agentv1.Effect) bool { return effect.Active || effect.OutcomeUnknown }) {
		return
	}
	state, code := "succeeded", ""
	if runtimeErr != nil {
		state, code = "failed", failureCode(runtimeErr)
	}
	if err := e.journal.Complete(&agentv1.TerminalResult{Epoch: record.Epoch, CommandId: record.CommandID, InputHash: record.InputHash, Sequence: 1, State: state, FailureCode: code, Effects: result.Effects}); err != nil {
		e.fail(err)
	}
}

// inspectBarrier runs under the cluster slot, without holding the session gate lock.
func (e *Executor) inspectBarrier(ctx context.Context, task job) {
	e.mu.Lock()
	valid := e.session == task.session && task.session.ctx.Err() == nil && task.session.gates[task.cluster] == task.gate && !task.gate.active && e.failure == nil
	e.mu.Unlock()
	if !valid || ctx.Err() != nil {
		return
	}
	records, err := e.journal.RecoveryRecords()
	if err != nil {
		e.fail(err)
		return
	}
	for _, record := range records {
		if ctx.Err() != nil || task.session.ctx.Err() != nil {
			return
		}
		if record.Epoch.ClusterId == task.cluster {
			e.recoverRecord(ctx, record)
		}
	}
	report, err := e.journal.Barrier(task.gate.epoch)
	if err != nil {
		if !errors.Is(err, operation.ErrBarrierPending) && !errors.Is(err, operation.ErrStaleOwner) {
			e.fail(err)
		}
		return
	}
	type effectKey struct{ command, resource, native string }
	effects := make(map[effectKey]*agentv1.Effect)
	var prior []*agentv1.Effect
	for _, effect := range report.Effects {
		if effect.CommandId == "" {
			prior = append(prior, effect)
			continue
		}
		current := proto.Clone(effect).(*agentv1.Effect)
		record, err := e.journal.Get(effect.CommandId)
		if err == nil && record.Epoch.ClusterId == task.cluster && record.Command != nil && record.Command.Input.ResourceId == effect.ResourceId {
			if effect.NativeOperationId == "" && record.State == journal.Terminal {
				current.Active, current.OutcomeUnknown, current.State = false, false, "command_terminal"
			} else if index := slices.IndexFunc(record.Effects, func(candidate *agentv1.Effect) bool { return candidate.NativeOperationId == effect.NativeOperationId }); index >= 0 {
				current = record.Effects[index]
			}
		}
		effects[effectKey{current.CommandId, current.ResourceId, current.NativeOperationId}] = current
	}
	read, cancel := context.WithTimeout(ctx, e.options.InspectionTimeout)
	inventory, inventoryErr := e.runtime.InspectCluster(read, uuid.MustParse(task.cluster), prior)
	cancel()
	for _, effect := range inventory.Effects {
		effects[effectKey{effect.CommandId, effect.ResourceId, effect.NativeOperationId}] = effect
	}
	report.Effects = report.Effects[:0]
	report.Drained, report.FailureCode = inventoryErr == nil, ""
	if inventoryErr != nil {
		report.FailureCode = "runtime_inspection_failed"
	}
	for _, effect := range effects {
		report.Effects = append(report.Effects, effect)
		if effect.Active || effect.OutcomeUnknown {
			report.Drained = false
		}
	}
	// Keep wire and durable report order stable for replay and change detection.
	slices.SortFunc(report.Effects, func(a, b *agentv1.Effect) int {
		return strings.Compare(a.CommandId+"/"+a.ResourceId+"/"+a.NativeOperationId, b.CommandId+"/"+b.ResourceId+"/"+b.NativeOperationId)
	})
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.session != task.session || task.session.ctx.Err() != nil || task.session.gates[task.cluster] != task.gate || task.gate.active || e.failure != nil {
		return
	}
	if err := e.journal.RecordBarrier(report); err != nil {
		e.failure = err
		return
	}
	if !proto.Equal(task.gate.report, report) {
		task.gate.report = report
		task.gate.unsent = true
	}
	task.gate.ready = report.Drained
}
