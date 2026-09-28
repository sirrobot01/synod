package docker

import (
	"cmp"
	"context"
	"errors"
	"slices"
	"strings"

	"github.com/containerd/errdefs"
	"github.com/google/uuid"
	"github.com/moby/moby/client"
	"github.com/sirrobot01/synod/internal/runtime"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/proto"
)

// InspectApp reports owned inventory and reconciles prior unjournaled effects.
// Command effects belong to Recover. Inventory effects have an empty command ID.
func (r *Runtime) InspectApp(ctx context.Context, appID uuid.UUID, prior []*agentv1.Effect) (runtime.AppInspection, error) {
	if appID == uuid.Nil {
		return runtime.AppInspection{}, runtime.ErrOwnershipConflict
	}
	result := runtime.AppInspection{}
	for _, old := range prior {
		if old == nil || old.CommandId != "" {
			return result, runtime.ErrOwnershipConflict
		}
		id, err := uuid.Parse(old.ResourceId)
		if err != nil || id == uuid.Nil || id.String() != old.ResourceId {
			return result, runtime.ErrOwnershipConflict
		}
		result.Effects = append(result.Effects, proto.Clone(old).(*agentv1.Effect))
	}
	inventory, err := r.Inventory(ctx)
	if err != nil {
		return result, err
	}
	var inspectionErr error
	type effectKey struct{ resource, native string }
	observed := make(map[effectKey]*agentv1.Effect)
	for _, resource := range inventory {
		if resource.AppID != appID {
			continue
		}
		result.Resources = append(result.Resources, resource)
		if resource.Kind != "container" {
			continue
		}
		effects, err := r.inspectInventoryContainer(ctx, appID, resource.ResourceID, resource.NativeID)
		inspectionErr = errors.Join(inspectionErr, err)
		for _, effect := range effects {
			observed[effectKey{effect.ResourceId, effect.NativeOperationId}] = effect
		}
	}
	for _, old := range prior {
		if effect, ok := observed[effectKey{old.ResourceId, old.NativeOperationId}]; ok && effect.ResourceId == old.ResourceId {
			continue
		}
		if old.NativeOperationId == "docker-inventory:"+old.ResourceId {
			effects, err := r.inspectInventoryContainer(ctx, appID, uuid.MustParse(old.ResourceId), "synod-instance-"+old.ResourceId)
			inspectionErr = errors.Join(inspectionErr, err)
			for _, effect := range effects {
				observed[effectKey{effect.ResourceId, effect.NativeOperationId}] = effect
			}
			continue
		}
		effect := proto.Clone(old).(*agentv1.Effect)
		if effect.Active || effect.OutcomeUnknown {
			inspectionErr = errors.Join(inspectionErr, r.inspectRetainedExec(ctx, appID.String(), effect))
		}
		observed[effectKey{effect.ResourceId, effect.NativeOperationId}] = effect
	}
	result.Effects = result.Effects[:0]
	for _, effect := range observed {
		result.Effects = append(result.Effects, effect)
	}
	slices.SortFunc(result.Effects, func(a, b *agentv1.Effect) int {
		return cmp.Or(cmp.Compare(a.ResourceId, b.ResourceId), cmp.Compare(a.NativeOperationId, b.NativeOperationId))
	})
	return result, inspectionErr
}

func (r *Runtime) inspectInventoryContainer(ctx context.Context, appID, resourceID uuid.UUID, reference string) ([]*agentv1.Effect, error) {
	effect := &agentv1.Effect{ResourceId: resourceID.String(), NativeOperationId: "docker-inventory:" + resourceID.String(), Active: true, OutcomeUnknown: true, State: "inventory_unknown"}
	effects := []*agentv1.Effect{effect}
	item, err := r.api.ContainerInspect(ctx, reference, client.ContainerInspectOptions{})
	if errdefs.IsNotFound(err) {
		effect.Active, effect.OutcomeUnknown, effect.State = false, false, "absent"
		return effects, nil
	}
	if err != nil {
		return effects, &runtime.Error{Kind: runtime.ErrOutcomeUnknown, Cause: err}
	}
	if item.Container.Config == nil {
		return effects, runtime.ErrOutcomeUnknown
	}
	identity, err := r.inventoryIdentity(item.Container.Config.Labels, "container", strings.TrimPrefix(item.Container.Name, "/"), "synod-instance-")
	if err != nil || identity.AppID != appID || identity.ResourceID != resourceID {
		return effects, runtime.ErrOwnershipConflict
	}
	if item.Container.State == nil {
		return effects, runtime.ErrOutcomeUnknown
	}
	effect.Active, effect.OutcomeUnknown, effect.State = false, false, string(item.Container.State.Status)
	for _, id := range item.Container.ExecIDs {
		effects = append(effects, &agentv1.Effect{ResourceId: resourceID.String(), NativeOperationId: "docker-exec:" + item.Container.ID + ":" + id, Active: true, State: "native_exec_active"})
	}
	return effects, nil
}
