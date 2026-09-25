package docker

import (
	"context"
	"errors"
	"time"

	"github.com/moby/moby/client"
	"github.com/sirrobot01/synod/internal/operation"
	"github.com/sirrobot01/synod/internal/runtime"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
)

// verifyPatroniMember proves that a started member is not serving, or that its stopped
// storage cannot restart. It uses Docker API reads only and changes nothing.
func (r *Runtime) verifyPatroniMember(ctx context.Context, command *agentv1.Command, result runtime.Result) (runtime.Result, error) {
	stored := operation.PatroniMemberContainer(command.Input)
	inspected, err := r.api.ContainerInspect(ctx, "synod-instance-"+command.Input.ResourceId, client.ContainerInspectOptions{})
	if err != nil {
		return result, &runtime.Error{Kind: runtime.ErrOutcomeUnknown, Cause: err}
	}
	item := inspected.Container
	if err := r.checkContainer(ctx, command, item, stored); err != nil {
		return result, err
	}
	result = r.containerResult(command, item)
	if item.State == nil || len(item.ExecIDs) != 0 {
		result.Effects[0].Active, result.Effects[0].OutcomeUnknown = true, true
		return result, runtime.ErrOutcomeUnknown
	}
	started, err := time.Parse(time.RFC3339Nano, item.State.StartedAt)
	if err != nil {
		return result, runtime.ErrOutcomeUnknown
	}
	// A member that never started has no serving history. The start-failure proof covers it.
	if started.IsZero() {
		return result, runtime.ErrConfigurationConflict
	}
	if err := r.inspectPatroniSource(ctx, item); err != nil {
		result.Effects[0].State = "patroni_configuration_unknown"
		result.Effects[0].Active, result.Effects[0].OutcomeUnknown = true, true
		return result, &runtime.Error{Kind: runtime.ErrOutcomeUnknown, Cause: err}
	}
	state := "patroni_member_not_serving"
	if command.Input.GetVerifyPatroniMemberFailed() != nil {
		if item.State.Running || item.State.Restarting {
			result.Effects[0].Active, result.Effects[0].OutcomeUnknown = true, true
			return result, runtime.ErrOutcomeUnknown
		}
		// The restart check that a start runs. Storage it refuses cannot serve again.
		err := r.checkPatroniStartStorage(ctx, command, item, stored, false)
		switch {
		case err == nil:
			return result, runtime.ErrConfigurationConflict
		case errors.Is(err, runtime.ErrStorageLost), errors.Is(err, runtime.ErrConfigurationConflict):
			state = "patroni_member_storage_failed"
		default:
			return result, err
		}
	} else if phase, err := postgresPhase(item); err == nil && phase == "patroni_ready" && item.State.Running && !item.State.Restarting {
		// A member with current startup proof is serving.
		return result, runtime.ErrConfigurationConflict
	}
	current, err := r.api.ContainerInspect(ctx, item.ID, client.ContainerInspectOptions{})
	if err != nil {
		return result, &runtime.Error{Kind: runtime.ErrOutcomeUnknown, Cause: err}
	}
	now := current.Container
	if now.ID != item.ID || now.State == nil || len(now.ExecIDs) != 0 || now.State.StartedAt != item.State.StartedAt ||
		(state == "patroni_member_storage_failed" && (now.State.Running || now.State.Restarting || now.State.FinishedAt != item.State.FinishedAt || now.RestartCount != item.RestartCount)) {
		result.Effects[0].Active, result.Effects[0].OutcomeUnknown = true, true
		return result, runtime.ErrOutcomeUnknown
	}
	result.Effects[0].State, result.Effects[0].Active, result.Effects[0].OutcomeUnknown = state, false, false
	return result, nil
}
