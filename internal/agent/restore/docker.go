package restore

import (
	"context"
	"errors"
	"maps"
	"net/netip"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/containerd/errdefs"
	"github.com/google/uuid"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/network"
	"github.com/moby/moby/client"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

// pullImages accepts a cached image of the target architecture when the registry is unavailable.
func pullImages(ctx context.Context, api *client.Client, p *plan, architecture string) error {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Minute)
	defer cancel()
	done := make(map[string]bool)
	for _, image := range p.Snapshot.Images {
		if done[image.Reference] {
			continue
		}
		done[image.Reference] = true
		if !image.LocalOnly {
			response, err := api.ImagePull(ctx, image.Reference, client.ImagePullOptions{Platforms: []ocispec.Platform{{OS: "linux", Architecture: architecture}}})
			if err == nil {
				err = response.Wait(ctx)
			}
			if err == nil {
				continue
			}
		}
		inspected, err := api.ImageInspect(ctx, image.Reference)
		if err != nil || inspected.Architecture != architecture {
			return &failure{Code: "image_unavailable", Resource: image.Reference, Cause: err}
		}
	}
	return nil
}

// findConflicts returns the container IDs of the one existing app that a replace may remove.
// It refuses a name, a published port, or a network that another app holds.
func findConflicts(ctx context.Context, api *client.Client, p *plan, replace bool) ([]string, string, error) {
	list, err := api.ContainerList(ctx, client.ContainerListOptions{All: true})
	if err != nil {
		return nil, "", &failure{Code: "app_conflict", Cause: err}
	}
	names := make(map[string]bool)
	for _, service := range p.Definition.Services {
		names[service.ContainerName] = true
	}
	project := p.Definition.Name
	var replaced []container.Summary
	for _, c := range list.Items {
		if c.Labels["com.docker.compose.project"] == project || slices.ContainsFunc(c.Names, func(name string) bool { return names[strings.TrimPrefix(name, "/")] }) {
			replaced = append(replaced, c)
		}
	}
	var ids []string
	key := ""
	if len(replaced) > 0 {
		resource := strings.TrimPrefix(slices.Concat(replaced[0].Names, []string{replaced[0].ID})[0], "/")
		if !replace {
			return nil, "", &failure{Code: "app_conflict", Resource: resource}
		}
		switch {
		case !slices.ContainsFunc(replaced, func(c container.Summary) bool { return c.Labels["com.docker.compose.project"] != project }):
			key = "compose:" + project
		case len(replaced) == 1 && replaced[0].Labels["com.docker.compose.project"] == "":
			key = "container:" + replaced[0].ID
		default:
			return nil, "", &failure{Code: "app_conflict", Resource: resource}
		}
		for _, c := range replaced {
			ids = append(ids, c.ID)
		}
	}
	for _, c := range list.Items {
		if c.State != container.StateRunning || slices.Contains(ids, c.ID) {
			continue
		}
		for _, used := range c.Ports {
			if used.PublicPort == 0 {
				continue
			}
			for _, service := range p.Definition.Services {
				for _, port := range service.Ports {
					low, high, ok := portRange(port.Published)
					address, err := netip.ParseAddr(port.HostIP)
					if !ok || err != nil || port.Protocol != used.Type || used.PublicPort < low || used.PublicPort > high {
						continue
					}
					if address.IsUnspecified() || !used.IP.IsValid() || used.IP.IsUnspecified() || address == used.IP {
						return nil, "", &failure{Code: "port_conflict", Resource: used.Type + "/" + strconv.Itoa(int(used.PublicPort))}
					}
				}
			}
		}
	}
	for _, key := range slices.Sorted(maps.Keys(p.Definition.Networks)) {
		name := p.Definition.Networks[key].Name
		existing, err := api.NetworkInspect(ctx, name, client.NetworkInspectOptions{})
		if errdefs.IsNotFound(err) {
			continue
		}
		if err != nil || existing.Network.Driver != p.Networks[name].Driver {
			return nil, "", &failure{Code: "network_conflict", Resource: name, Cause: err}
		}
	}
	return ids, key, nil
}

// portRange reads a published port or range. An empty value asks Docker for a free port.
func portRange(published string) (uint16, uint16, bool) {
	first, last, isRange := strings.Cut(published, "-")
	low, err := strconv.ParseUint(first, 10, 16)
	if err != nil || low == 0 {
		return 0, 0, false
	}
	high := low
	if isRange {
		if high, err = strconv.ParseUint(last, 10, 16); err != nil || high < low {
			return 0, 0, false
		}
	}
	return uint16(low), uint16(high), true
}

// createNetworks creates each missing network with its saved driver and options.
// It keeps a saved subnet only when an attachment uses a static address on it.
func createNetworks(ctx context.Context, api *client.Client, p *plan, id uuid.UUID) error {
	static := make(map[string]bool)
	for _, service := range p.Definition.Services {
		for key, attachment := range service.Networks {
			if attachment.IPv4Address != "" || attachment.IPv6Address != "" {
				static[p.Definition.Networks[key].Name] = true
			}
		}
	}
	for _, key := range slices.Sorted(maps.Keys(p.Definition.Networks)) {
		name := p.Definition.Networks[key].Name
		if _, err := api.NetworkInspect(ctx, name, client.NetworkInspectOptions{}); !errdefs.IsNotFound(err) {
			if err != nil {
				return &failure{Code: "network_conflict", Resource: name, Cause: err}
			}
			continue
		}
		saved := p.Networks[name]
		labels := maps.Clone(saved.Labels)
		if labels == nil {
			labels = make(map[string]string)
		}
		maps.DeleteFunc(labels, func(key, _ string) bool { return strings.HasPrefix(key, "com.docker.compose.") })
		labels[operationLabel] = id.String()
		options := client.NetworkCreateOptions{Driver: saved.Driver, IPAM: &network.IPAM{Driver: saved.IPAM.Driver, Options: saved.IPAM.Options}, Internal: saved.Internal, Attachable: saved.Attachable, Options: saved.Options, Labels: labels}
		if saved.EnableIPv6 {
			options.EnableIPv6 = new(true)
			if !saved.EnableIPv4 {
				options.EnableIPv4 = new(false)
			}
		}
		if static[name] {
			options.IPAM.Config = saved.IPAM.Config
		}
		if _, err := api.NetworkCreate(ctx, name, options); err != nil {
			return &failure{Code: "network_conflict", Resource: name, Cause: err}
		}
	}
	return nil
}

// checkStarted refuses a restored app whose containers stop or restart soon after the start.
func checkStarted(ctx context.Context, api *client.Client, p *plan, id uuid.UUID) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(3 * time.Second):
	}
	for _, key := range slices.Sorted(maps.Keys(p.Definition.Services)) {
		name := p.Definition.Services[key].ContainerName
		response, err := api.ContainerInspect(ctx, name, client.ContainerInspectOptions{})
		if err != nil || response.Container.State == nil || response.Container.Config == nil || response.Container.Config.Labels[operationLabel] != id.String() {
			return &failure{Code: "compose_start_failed", Resource: name, Cause: err}
		}
		state := response.Container.State
		if !(state.Running && !state.Restarting) && !(state.Status == container.StateExited && state.ExitCode == 0) {
			return &failure{Code: "compose_start_failed", Resource: name}
		}
	}
	return nil
}

// removeLabelled removes the containers of one restore. With all, it also removes its volumes and networks.
func removeLabelled(ctx context.Context, api *client.Client, id uuid.UUID, all bool) error {
	filters := make(client.Filters)
	filters.Add("label", operationLabel+"="+id.String())
	containers, err := api.ContainerList(ctx, client.ContainerListOptions{All: true, Filters: filters})
	if err != nil {
		return err
	}
	var failures []error
	for _, c := range containers.Items {
		if _, err := api.ContainerRemove(ctx, c.ID, client.ContainerRemoveOptions{Force: true, RemoveVolumes: true}); err != nil && !errdefs.IsNotFound(err) {
			failures = append(failures, err)
		}
	}
	if !all || len(failures) > 0 {
		return errors.Join(failures...)
	}
	volumes, err := api.VolumeList(ctx, client.VolumeListOptions{Filters: filters})
	if err != nil {
		return err
	}
	for _, volume := range volumes.Items {
		if _, err := api.VolumeRemove(ctx, volume.Name, client.VolumeRemoveOptions{}); err != nil && !errdefs.IsNotFound(err) {
			failures = append(failures, err)
		}
	}
	networks, err := api.NetworkList(ctx, client.NetworkListOptions{Filters: filters})
	if err != nil {
		return errors.Join(append(failures, err)...)
	}
	for _, item := range networks.Items {
		if _, err := api.NetworkRemove(ctx, item.ID, client.NetworkRemoveOptions{}); err != nil && !errdefs.IsNotFound(err) {
			failures = append(failures, err)
		}
	}
	return errors.Join(failures...)
}

// recoverReplaced creates each removed container again from its saved settings and the same image.
// It starts the containers that ran before the replace.
func recoverReplaced(ctx context.Context, api *client.Client, replaced []container.InspectResponse) error {
	// Containers that join another container's namespaces need that container first.
	var ordered []container.InspectResponse
	for _, joined := range []bool{false, true} {
		for _, c := range replaced {
			modes := string(c.HostConfig.NetworkMode) + " " + string(c.HostConfig.IpcMode) + " " + string(c.HostConfig.PidMode)
			if strings.Contains(" "+modes, " container:") == joined {
				ordered = append(ordered, c)
			}
		}
	}
	current := make(map[string]string)
	var failures []error
	for _, old := range ordered {
		id := old.ID
		_, err := api.ContainerInspect(ctx, old.ID, client.ContainerInspectOptions{})
		if errdefs.IsNotFound(err) {
			config, host := *old.Config, *old.HostConfig
			config.Image = old.Image
			modes := []string{string(host.NetworkMode), string(host.IpcMode), string(host.PidMode)}
			for i, mode := range modes {
				if target, ok := strings.CutPrefix(mode, "container:"); ok && current[target] != "" {
					modes[i] = "container:" + current[target]
				}
			}
			host.NetworkMode, host.IpcMode, host.PidMode = container.NetworkMode(modes[0]), container.IpcMode(modes[1]), container.PidMode(modes[2])
			endpoints := make(map[string]*network.EndpointSettings)
			if old.NetworkSettings != nil && !host.NetworkMode.IsContainer() {
				for name, endpoint := range old.NetworkSettings.Networks {
					if endpoint != nil {
						endpoints[name] = &network.EndpointSettings{Aliases: endpoint.Aliases, IPAMConfig: endpoint.IPAMConfig, MacAddress: endpoint.MacAddress, DriverOpts: endpoint.DriverOpts, GwPriority: endpoint.GwPriority, Links: endpoint.Links}
					}
				}
			}
			created, createErr := api.ContainerCreate(ctx, client.ContainerCreateOptions{Name: strings.TrimPrefix(old.Name, "/"), Config: &config, HostConfig: &host, NetworkingConfig: &network.NetworkingConfig{EndpointsConfig: endpoints}})
			if createErr != nil {
				failures = append(failures, createErr)
				continue
			}
			id = created.ID
		} else if err != nil {
			failures = append(failures, err)
			continue
		}
		current[old.ID] = id
		if old.State != nil && old.State.Running {
			if _, err := api.ContainerStart(ctx, id, client.ContainerStartOptions{}); err != nil {
				failures = append(failures, err)
			}
		}
	}
	return errors.Join(failures...)
}
