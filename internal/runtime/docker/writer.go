package docker

import (
	"context"
	"encoding/base64"
	"strings"

	"github.com/containerd/errdefs"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/client"
	"github.com/sirrobot01/synod/internal/runtime"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/proto"
)

// AppContainers reads owned tier 2 containers without running health probes.
func (r *Runtime) AppContainers(ctx context.Context) ([]runtime.AppContainer, error) {
	filters := client.Filters{}.Add("label", labelInstallation+"="+r.options.InstallationID.String(), labelNode+"="+r.options.NodeID.String(), "synod.tier=2")
	listed, err := r.api.ContainerList(ctx, client.ContainerListOptions{All: true, Filters: filters})
	if err != nil {
		return nil, err
	}
	var apps []runtime.AppContainer
	for _, item := range listed.Items {
		if len(item.Names) != 1 {
			return nil, runtime.ErrOwnershipConflict
		}
		identity, err := r.inventoryIdentity(item.Labels, "container", strings.TrimPrefix(item.Names[0], "/"), "synod-instance-")
		if err != nil {
			return nil, err
		}
		detail, err := r.api.ContainerInspect(ctx, item.ID, client.ContainerInspectOptions{})
		if errdefs.IsNotFound(err) {
			continue
		}
		if err != nil {
			return nil, err
		}
		c := detail.Container
		if c.Config == nil || c.State == nil {
			return nil, runtime.ErrOutcomeUnknown
		}
		data, err := base64.RawStdEncoding.DecodeString(c.Config.Labels[labelConfig])
		if err != nil {
			return nil, runtime.ErrConfigurationConflict
		}
		var config agentv1.EnsureContainer
		if err := proto.Unmarshal(data, &config); err != nil || config.Generic == nil {
			return nil, runtime.ErrConfigurationConflict
		}
		apps = append(apps, runtime.AppContainer{AppID: identity.ClusterID.String(), ResourceID: identity.ResourceID.String(), PID: c.State.Pid, Running: c.State.Running || c.State.Restarting || c.State.Paused, Mounts: config.Generic.Mounts})
	}
	return apps, nil
}

// FenceApp disables restart and kills an owned writer when its lease ends.
func (r *Runtime) FenceApp(ctx context.Context, app runtime.AppContainer) error {
	detail, err := r.api.ContainerInspect(ctx, "synod-instance-"+app.ResourceID, client.ContainerInspectOptions{})
	if errdefs.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}
	c := detail.Container
	if c.Config == nil || c.State == nil {
		return runtime.ErrOutcomeUnknown
	}
	identity, err := r.inventoryIdentity(c.Config.Labels, "container", strings.TrimPrefix(c.Name, "/"), "synod-instance-")
	if err != nil || identity.ClusterID.String() != app.AppID || c.Config.Labels["synod.tier"] != "2" {
		return runtime.ErrOwnershipConflict
	}
	if _, err := r.api.ContainerUpdate(ctx, c.ID, client.ContainerUpdateOptions{RestartPolicy: &container.RestartPolicy{Name: container.RestartPolicyDisabled}}); err != nil {
		return err
	}
	if !c.State.Running && !c.State.Restarting && !c.State.Paused {
		return nil
	}
	_, err = r.api.ContainerKill(ctx, c.ID, client.ContainerKillOptions{Signal: "SIGKILL"})
	if errdefs.IsNotFound(err) || errdefs.IsConflict(err) {
		return nil
	}
	return err
}

// SetWriterLeaseCheck binds the executor's lease guard before execution starts.
func (r *Runtime) SetWriterLeaseCheck(check func(string) error) { r.writerLeaseCheck = check }
