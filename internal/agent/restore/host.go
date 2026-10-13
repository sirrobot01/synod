package restore

import (
	"context"
	"errors"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"net"
	"net/netip"
	"os"
	"slices"
	"strconv"

	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/client"
	"github.com/sirrobot01/synod/internal/backup"
	"github.com/sirrobot01/synod/internal/runtime/host"
	"golang.org/x/sys/unix"
)

func requiredPorts(p *plan) ([]backup.HostPort, error) {
	var ports []backup.HostPort
	for _, c := range p.Containers {
		service := p.Definition.Services[p.Services[c.ID]]
		if !host.UsesHostNetwork(c, p.Containers) {
			continue
		}
		index := slices.IndexFunc(p.Snapshot.Host, func(r backup.HostRequirements) bool { return r.ContainerID == c.ID })
		if index < 0 || !p.Snapshot.Host[index].PortsKnown {
			return nil, &failure{Code: "host_requirements_unverified", Resource: service.ContainerName + ": host ports"}
		}
		for _, port := range p.Snapshot.Host[index].Ports {
			if _, err := netip.ParseAddr(port.Address); err != nil || port.Port == 0 || (port.Protocol != "tcp" && port.Protocol != "udp") {
				return nil, &failure{Code: "snapshot_invalid"}
			}
			ports = append(ports, port)
		}
	}
	for _, service := range p.Definition.Services {
		for _, port := range service.Ports {
			if port.Published == "" {
				continue // Docker assigns a free port.
			}
			low, high, ok := portRange(port.Published)
			if !ok || (port.Protocol != "tcp" && port.Protocol != "udp") {
				return nil, &failure{Code: "host_requirements_unverified", Resource: service.ContainerName + ": published ports"}
			}
			address := port.HostIP
			if address == "" {
				address = "0.0.0.0"
			}
			if _, err := netip.ParseAddr(address); err != nil {
				return nil, &failure{Code: "snapshot_invalid"}
			}
			for n := uint32(low); n <= uint32(high); n++ {
				ports = append(ports, backup.HostPort{Address: address, Port: uint16(n), Protocol: port.Protocol})
			}
		}
	}
	return ports, nil
}

// checkHost runs before image loading, directory creation, or replacement.
func checkHost(ctx context.Context, api *client.Client, p *plan, replaced []string, ports []backup.HostPort) error {
	needsHost := len(ports) > 0
	for _, c := range p.Containers {
		s := p.Definition.Services[p.Services[c.ID]]
		needsHost = needsHost || host.UsesHostNetwork(c, p.Containers) || len(s.Devices) > 0
		for _, m := range s.Volumes {
			if m.Type == "bind" {
				// Copied mounts already point into the new restore directory.
				for _, original := range c.Mounts {
					needsHost = needsHost || (original.Destination == m.Target && original.Source == m.Source)
				}
			}
		}
	}
	var view *host.Local
	if needsHost {
		var err error
		if view, err = host.CheckLocal(ctx, api); err != nil {
			return &failure{Code: "host_requirements_unverified", Resource: "target Docker host", Cause: err}
		}
		defer view.Close()
	}
	for _, c := range p.Containers {
		s := p.Definition.Services[p.Services[c.ID]]
		var required backup.HostRequirements
		for _, r := range p.Snapshot.Host {
			if r.ContainerID == c.ID {
				required = r
			}
		}
		for _, device := range c.HostConfig.Devices {
			if err := checkHostPath(view, required, device.PathOnHost, true); err != nil {
				return err
			}
		}
		for _, m := range s.Volumes {
			for _, original := range c.Mounts {
				if original.Destination != m.Target {
					continue
				}
				if m.Type == "bind" && original.Source == m.Source {
					if err := checkHostPath(view, required, m.Source, false); err != nil {
						return err
					}
				}
				if m.Type == "volume" && p.Definition.Volumes[m.Source].Name == original.Name {
					v, err := api.VolumeInspect(ctx, original.Name, client.VolumeInspectOptions{})
					// A network share volume keeps its mount options. The target must mount the same kind of share.
					remote := slices.ContainsFunc(p.Snapshot.App.Definition.Paths, func(item *agentv1.DiscoveredPath) bool {
						return item.ContainerId == c.ID && item.Target == m.Target && item.Status == "Remote"
					})
					if err != nil || v.Volume.Driver != original.Driver || (len(v.Volume.Options) > 0 && (!remote || !host.Remote(v.Volume.Options["type"]))) {
						return &failure{Code: "mount_unavailable", Resource: original.Name, Cause: err}
					}
				}
			}
		}
	}
	if len(ports) == 0 {
		return nil
	}
	owned := make(map[uint64]bool)
	var replacing []container.InspectResponse
	for _, id := range replaced {
		c, err := api.ContainerInspect(ctx, id, client.ContainerInspectOptions{})
		if err != nil {
			return &failure{Code: "host_requirements_unverified", Resource: id, Cause: err}
		}
		replacing = append(replacing, c.Container)
	}
	for _, c := range replacing {
		if c.State == nil || !c.State.Running {
			continue
		}
		var sockets []host.Socket
		var err error
		if host.UsesHostNetwork(c, replacing) {
			sockets, err = host.ContainerSockets(ctx, api, c)
		} else {
			sockets, err = host.PublishedSockets(c)
		}
		if err != nil {
			return &failure{Code: "host_requirements_unverified", Resource: c.Name + ": host ports", Cause: err}
		}
		for _, socket := range sockets {
			owned[socket.Inode] = true
		}
	}
	used, err := host.Sockets()
	if err != nil {
		return &failure{Code: "host_requirements_unverified", Resource: "target host ports", Cause: err}
	}
	addresses, err := net.InterfaceAddrs()
	if err != nil {
		return &failure{Code: "host_requirements_unverified", Resource: "target host addresses", Cause: err}
	}
	for _, port := range ports {
		resource := port.Protocol + "/" + net.JoinHostPort(port.Address, strconv.Itoa(int(port.Port)))
		address, _ := netip.ParseAddr(port.Address)
		address = address.Unmap()
		if !address.IsUnspecified() {
			present := false
			for _, local := range addresses {
				prefix, err := netip.ParsePrefix(local.String())
				if err == nil && (prefix.Addr() == address || (address.IsLoopback() && prefix.Addr().IsLoopback() && prefix.Contains(address))) {
					present = true
				}
			}
			if !present || (address.Is6() && address.IsLinkLocalUnicast()) {
				return &failure{Code: "host_port_unavailable", Resource: resource}
			}
		}
		inUseByReplaced := false
		for _, socket := range used {
			if !host.Overlaps(port, socket.Port) {
				continue
			}
			if !owned[socket.Inode] {
				return &failure{Code: "port_conflict", Resource: resource}
			}
			inUseByReplaced = true
		}
		if inUseByReplaced {
			continue
		}
		// A bind also checks local addresses and sockets absent from the proc table.
		endpoint := net.JoinHostPort(port.Address, strconv.Itoa(int(port.Port)))
		var err error
		if port.Protocol == "tcp" {
			var listener net.Listener
			listener, err = new(net.ListenConfig).Listen(ctx, "tcp", endpoint)
			if err == nil {
				err = listener.Close()
			}
		} else {
			var socket net.PacketConn
			socket, err = new(net.ListenConfig).ListenPacket(ctx, "udp", endpoint)
			if err == nil {
				err = socket.Close()
			}
		}
		if err != nil {
			code := "host_port_unavailable"
			if errors.Is(err, unix.EADDRINUSE) {
				code = "port_conflict"
			}
			return &failure{Code: code, Resource: resource, Cause: err}
		}
	}
	return nil
}

func checkHostPath(view *host.Local, required backup.HostRequirements, name string, device bool) error {
	index := slices.IndexFunc(required.Paths, func(p backup.HostPath) bool { return p.Path == name })
	if index < 0 || !required.Paths[index].Known {
		return &failure{Code: "host_requirements_unverified", Resource: name}
	}
	want, got := required.Paths[index], view.Path(name)
	if !got.Known || want.Mode != got.Mode || want.Device != got.Device || want.UserID != got.UserID || want.GroupID != got.GroupID || (device && os.FileMode(got.Mode)&os.ModeDevice == 0) {
		code := "mount_unavailable"
		if device {
			code = "device_unavailable"
		}
		return &failure{Code: code, Resource: name}
	}
	return nil
}
