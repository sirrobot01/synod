package docker

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"io"

	"github.com/containerd/errdefs"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/client"
	"github.com/sirrobot01/synod/internal/runtime"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/proto"
)

// patroniRecords holds the storage records that bind a member container to its volume.
const patroniRecords = "/var/lib/synod-patroni"

// recreatePatroni replaces a stopped member container with one that holds the next protected
// files. The old container is renamed first, so each step can be found again after a crash:
// rename, create, copy the storage records, and remove the old container. The volume stays.
// Any error after the first mutation reports outcome unknown.
func (r *Runtime) recreatePatroni(ctx context.Context, command *agentv1.Command, secrets runtime.Secrets) (runtime.Result, error) {
	p := command.Input.GetRecreatePatroniMember()
	id := command.Input.ResourceId
	canonical, err := r.optionalContainer(ctx, "synod-instance-"+id)
	if err != nil {
		return runtime.Result{}, err
	}
	retired, err := r.optionalContainer(ctx, "synod-retired-"+id)
	if err != nil {
		return runtime.Result{}, err
	}
	// Once a retired container exists, the member is between two containers. Every error from
	// then on reports outcome unknown, because none of them leaves the member unchanged.
	unknown := func(cause error) (runtime.Result, error) {
		return runtime.Result{Effects: []*agentv1.Effect{{CommandId: command.CommandId, ResourceId: id, NativeOperationId: "docker-command:" + command.CommandId, State: "unknown", Active: true, OutcomeUnknown: true}}},
			&runtime.Error{Kind: runtime.ErrOutcomeUnknown, Cause: cause}
	}
	holdsNext := false
	if canonical != nil {
		stored, err := storedConfig(*canonical)
		if err != nil {
			return runtime.Result{}, err
		}
		switch {
		case proto.Equal(stored, p.Next):
			if err := r.checkContainer(ctx, command, *canonical, p.Next); err != nil {
				return runtime.Result{}, err
			}
			holdsNext = true
		case proto.Equal(stored, p.Previous) && retired == nil:
			if err := r.checkContainer(ctx, command, *canonical, p.Previous); err != nil {
				return runtime.Result{}, err
			}
			if canonical.State == nil || canonical.State.Running || canonical.State.Restarting || len(canonical.ExecIDs) != 0 {
				return runtime.Result{}, runtime.ErrConfigurationConflict
			}
			if err := r.inspectPatroniSource(ctx, *canonical); err != nil {
				return runtime.Result{}, err
			}
			// Only storage that can restart is carried to a new container.
			if err := r.checkPatroniStartStorage(ctx, command, *canonical, p.Previous, false); err != nil {
				return runtime.Result{}, err
			}
			if _, err := r.api.ContainerRename(ctx, canonical.ID, client.ContainerRenameOptions{NewName: "synod-retired-" + id}); err != nil {
				return unknown(err)
			}
			if retired, err = r.optionalContainer(ctx, "synod-retired-"+id); err != nil || retired == nil {
				return unknown(err)
			}
		default:
			return runtime.Result{}, runtime.ErrConfigurationConflict
		}
	}
	if !holdsNext {
		if retired == nil {
			return runtime.Result{}, runtime.ErrConfigurationConflict
		}
		if err := r.checkRetired(command, *retired, p.Previous); err != nil {
			return unknown(err)
		}
		if err := r.createPatroni(ctx, command, p.Next, secrets); err != nil {
			// A lost reply is settled by the protected files the new container holds.
			created, inspectErr := r.optionalContainer(ctx, "synod-instance-"+id)
			if inspectErr != nil || created == nil || r.inspectPatroniSource(ctx, *created) != nil {
				return unknown(errors.Join(err, inspectErr))
			}
		}
	}
	if retired != nil {
		if err := r.checkRetired(command, *retired, p.Previous); err != nil {
			return unknown(err)
		}
		created, err := r.optionalContainer(ctx, "synod-instance-"+id)
		if err != nil || created == nil {
			return unknown(err)
		}
		copied, err := r.api.CopyFromContainer(ctx, retired.ID, client.CopyFromContainerOptions{SourcePath: patroniRecords})
		if err != nil {
			return unknown(err)
		}
		records, err := io.ReadAll(io.LimitReader(copied.Content, (1<<20)+1))
		copied.Content.Close()
		if err != nil || len(records) > 1<<20 || !copied.Stat.Mode.IsDir() || copied.Stat.LinkTarget != "" {
			return unknown(errors.Join(err, errors.New("Patroni storage records are not a bounded directory")))
		}
		if _, err := r.api.CopyToContainer(ctx, created.ID, client.CopyToContainerOptions{DestinationPath: "/var/lib", Content: bytes.NewReader(records), CopyUIDGID: true}); err != nil {
			return unknown(err)
		}
		// The records must describe the same volume and completed storage before the old
		// container, which also holds them, is removed.
		if err := r.checkPatroniStartStorage(ctx, command, *created, p.Next, false); err != nil {
			return unknown(err)
		}
		if _, err := r.api.ContainerRemove(ctx, retired.ID, client.ContainerRemoveOptions{Force: false, RemoveVolumes: false}); err != nil {
			return unknown(err)
		}
	}
	inspectCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), r.options.InspectionTimeout)
	defer cancel()
	return r.Recover(inspectCtx, command, nil)
}

// inspectRecreate proves a completed recreate: the canonical container holds next with its
// protected files and restartable storage records, and no retired container remains.
func (r *Runtime) inspectRecreate(ctx context.Context, command *agentv1.Command, result runtime.Result) (runtime.Result, error) {
	p := command.Input.GetRecreatePatroniMember()
	id := command.Input.ResourceId
	retired, err := r.optionalContainer(ctx, "synod-retired-"+id)
	if err != nil {
		return result, err
	}
	canonical, err := r.optionalContainer(ctx, "synod-instance-"+id)
	if err != nil {
		return result, err
	}
	if retired != nil || canonical == nil {
		return result, runtime.ErrOutcomeUnknown
	}
	if stored, err := storedConfig(*canonical); err != nil || !proto.Equal(stored, p.Next) {
		return result, runtime.ErrOutcomeUnknown
	}
	if err := r.checkContainer(ctx, command, *canonical, p.Next); err != nil {
		return result, err
	}
	result = r.containerResult(command, *canonical)
	if err := r.inspectPatroniSource(ctx, *canonical); err != nil {
		result.Effects[0].Active, result.Effects[0].OutcomeUnknown = true, true
		return result, &runtime.Error{Kind: runtime.ErrOutcomeUnknown, Cause: err}
	}
	if err := r.checkPatroniStartStorage(ctx, command, *canonical, p.Next, false); err != nil {
		result.Effects[0].Active, result.Effects[0].OutcomeUnknown = true, true
		return result, &runtime.Error{Kind: runtime.ErrOutcomeUnknown, Cause: err}
	}
	if result.Effects[0].Active || result.Effects[0].OutcomeUnknown {
		return result, runtime.ErrOutcomeUnknown
	}
	result.Effects[0].State = "patroni_member_recreated"
	return result, nil
}

// optionalContainer inspects one container by name. A missing container is nil.
func (r *Runtime) optionalContainer(ctx context.Context, name string) (*container.InspectResponse, error) {
	inspected, err := r.api.ContainerInspect(ctx, name, client.ContainerInspectOptions{})
	if errdefs.IsNotFound(err) {
		return nil, nil
	}
	if err != nil {
		return nil, &runtime.Error{Kind: runtime.ErrOutcomeUnknown, Cause: err}
	}
	return &inspected.Container, nil
}

// checkRetired proves that the renamed container is this member's stopped previous container.
func (r *Runtime) checkRetired(command *agentv1.Command, item container.InspectResponse, previous *agentv1.EnsureContainer) error {
	if item.Config == nil || item.State == nil || item.State.Running || item.State.Restarting || len(item.ExecIDs) != 0 {
		return runtime.ErrConfigurationConflict
	}
	if err := r.checkLabels(command, item.Config.Labels, "container", command.Input.ResourceId); err != nil {
		return err
	}
	stored, err := storedConfig(item)
	if err != nil || !proto.Equal(stored, previous) {
		return runtime.ErrConfigurationConflict
	}
	return nil
}

// storedConfig decodes the configuration label and checks its hash label.
func storedConfig(item container.InspectResponse) (*agentv1.EnsureContainer, error) {
	if item.Config == nil {
		return nil, runtime.ErrConfigurationConflict
	}
	encoded, err := base64.RawStdEncoding.DecodeString(item.Config.Labels[labelConfig])
	if err != nil {
		return nil, runtime.ErrConfigurationConflict
	}
	hash := sha256.Sum256(encoded)
	if item.Config.Labels[labelHash] != hex.EncodeToString(hash[:]) {
		return nil, runtime.ErrConfigurationConflict
	}
	var stored agentv1.EnsureContainer
	if proto.Unmarshal(encoded, &stored) != nil {
		return nil, runtime.ErrConfigurationConflict
	}
	return &stored, nil
}
