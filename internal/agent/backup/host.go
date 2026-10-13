package backup

import (
	"context"
	"slices"

	"github.com/moby/moby/api/types/container"
	"github.com/sirrobot01/synod/internal/backup"
	"github.com/sirrobot01/synod/internal/runtime/host"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
)

func (w *Worker) captureHost(ctx context.Context, containers []container.InspectResponse, app *agentv1.DiscoveredApp) []backup.HostRequirements {
	var requirements []backup.HostRequirements
	checked, local := false, false
	for _, c := range containers {
		r := backup.HostRequirements{ContainerID: c.ID}
		var paths []string
		for _, d := range c.HostConfig.Devices {
			paths = append(paths, d.PathOnHost)
		}
		for _, m := range c.Mounts {
			if m.Type == "bind" && slices.ContainsFunc(app.Paths, func(p *agentv1.DiscoveredPath) bool {
				// A restore attaches Never and uncopied Remote mounts at their original host path.
				return p.ContainerId == c.ID && p.Target == m.Destination && p.Source == m.Source && (p.Status == "Never" || p.Status == "Remote")
			}) {
				paths = append(paths, m.Source)
			}
		}
		if !checked && (host.UsesHostNetwork(c, containers) || len(paths) > 0) {
			checked, local = true, host.CheckLocal(ctx, w.Docker) == nil
		}
		if host.UsesHostNetwork(c, containers) && local {
			if sockets, err := host.ContainerSockets(ctx, w.Docker, c); err == nil {
				r.PortsKnown = true
				for _, socket := range sockets {
					if !slices.Contains(r.Ports, socket.Port) {
						r.Ports = append(r.Ports, socket.Port)
					}
				}
			}
		}
		slices.Sort(paths)
		for _, path := range slices.Compact(paths) {
			p := backup.HostPath{Path: path}
			if local {
				p = host.Path(path)
			}
			r.Paths = append(r.Paths, p)
		}
		requirements = append(requirements, r)
	}
	return requirements
}
