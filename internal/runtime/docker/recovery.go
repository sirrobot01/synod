package docker

import (
	"context"
	"encoding/hex"
	"slices"
	"strings"

	"github.com/containerd/errdefs"
	"github.com/moby/moby/client"
	"github.com/sirrobot01/synod/internal/runtime"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/proto"
)

// Recover inspects the command and every retained native effect without mutation.
// Pass the journal's complete prior effect list. Never omit an unresolved identity.
func (r *Runtime) Recover(ctx context.Context, command *agentv1.Command, prior []*agentv1.Effect) (runtime.Result, error) {
	if err := r.validate(command); err != nil {
		return runtime.Result{}, err
	}
	for _, effect := range prior {
		if effect == nil || effect.CommandId != command.CommandId || effect.ResourceId != command.Input.ResourceId {
			return runtime.Result{}, runtime.ErrConfigurationConflict
		}
	}
	result, inspectErr := r.inspectCommand(ctx, command)
	for _, old := range prior {
		if slices.ContainsFunc(result.Effects, func(effect *agentv1.Effect) bool { return effect.NativeOperationId == old.NativeOperationId }) {
			continue
		}
		effect := proto.Clone(old).(*agentv1.Effect)
		result.Effects = append(result.Effects, effect)
		if !effect.Active && !effect.OutcomeUnknown {
			continue
		}
		if err := r.inspectRetainedExec(ctx, command.Epoch.AppId, effect); err != nil && inspectErr == nil {
			inspectErr = err
		}
	}
	return result, inspectErr
}

// inspectRetainedExec mutates only a detached effect after scoped native inspection.
func (r *Runtime) inspectRetainedExec(ctx context.Context, appID string, effect *agentv1.Effect) error {
	effect.Active, effect.OutcomeUnknown = true, true
	effect.State = "native_inspection_required"
	identity, ok := strings.CutPrefix(effect.NativeOperationId, "docker-exec:")
	containerID, execID, split := strings.Cut(identity, ":")
	containerBytes, containerErr := hex.DecodeString(containerID)
	execBytes, execErr := hex.DecodeString(execID)
	if !ok || !split || containerErr != nil || execErr != nil || len(containerBytes) != 32 || len(execBytes) != 32 || identity != strings.ToLower(identity) {
		return runtime.ErrOutcomeUnknown
	}
	item, err := r.api.ContainerInspect(ctx, containerID, client.ContainerInspectOptions{})
	if errdefs.IsNotFound(err) {
		effect.Active, effect.OutcomeUnknown, effect.State = false, false, "native_container_absent"
		return nil
	}
	if err != nil || item.Container.Config == nil || item.Container.State == nil {
		return runtime.ErrOutcomeUnknown
	}
	owned, err := r.inventoryIdentity(item.Container.Config.Labels, "container", strings.TrimPrefix(item.Container.Name, "/"), "synod-instance-")
	if err != nil || owned.AppID.String() != appID || owned.ResourceID.String() != effect.ResourceId {
		return runtime.ErrOwnershipConflict
	}
	if slices.Contains(item.Container.ExecIDs, execID) {
		effect.OutcomeUnknown, effect.State = false, "native_exec_active"
		return nil
	}
	native, err := r.api.ExecInspect(ctx, execID, client.ExecInspectOptions{})
	switch {
	case errdefs.IsNotFound(err) && !item.Container.State.Running && !item.Container.State.Restarting:
		effect.Active, effect.OutcomeUnknown, effect.State = false, false, "native_container_stopped"
	case err == nil && native.ContainerID == containerID && native.ID == execID:
		effect.Active, effect.OutcomeUnknown, effect.State = native.Running, false, "native_exec_exited"
		if native.Running {
			effect.State = "native_exec_active"
		}
	default:
		return runtime.ErrOutcomeUnknown
	}
	return nil
}
