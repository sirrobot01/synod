package host

import (
	"context"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/client"
	"github.com/prometheus/procfs"
	"github.com/sirrobot01/synod/internal/backup"
)

// Sockets reads host TCP listeners and bound UDP sockets, including non-Docker processes.
func Sockets() ([]Socket, error) {
	fs, err := procfs.NewFS("/proc")
	if err != nil {
		return nil, err
	}
	var sockets []Socket
	for _, protocol := range []string{"tcp", "tcp6", "udp", "udp6"} {
		var table procfs.NetIPSocket
		switch protocol {
		case "tcp":
			var rows procfs.NetTCP
			rows, err = fs.NetTCP()
			table = procfs.NetIPSocket(rows)
		case "tcp6":
			var rows procfs.NetTCP
			rows, err = fs.NetTCP6()
			table = procfs.NetIPSocket(rows)
		case "udp":
			var rows procfs.NetUDP
			rows, err = fs.NetUDP()
			table = procfs.NetIPSocket(rows)
		case "udp6":
			var rows procfs.NetUDP
			rows, err = fs.NetUDP6()
			table = procfs.NetIPSocket(rows)
		}
		if os.IsNotExist(err) && strings.HasSuffix(protocol, "6") {
			continue // The host can disable IPv6.
		}
		if err != nil {
			return nil, err
		}
		for _, row := range table {
			if row.LocalPort == 0 || (strings.HasPrefix(protocol, "tcp") && row.St != 0x0a) {
				continue
			}
			sockets = append(sockets, Socket{Port: backup.HostPort{Address: row.LocalAddr.String(), Port: uint16(row.LocalPort), Protocol: strings.TrimSuffix(protocol, "6")}, Inode: row.Inode})
		}
	}
	return sockets, nil
}

// ContainerSockets returns only sockets held by this running host-network container.
// It refuses process churn and unreadable descriptors instead of reporting an empty port list.
func ContainerSockets(ctx context.Context, api *client.Client, c container.InspectResponse) ([]Socket, error) {
	if c.State == nil || !c.State.Running || c.State.Paused || c.State.Restarting || c.HostConfig == nil || (c.HostConfig.NetworkMode != "host" && !c.HostConfig.NetworkMode.IsContainer()) {
		return nil, ErrUnverified
	}
	fs, err := procfs.NewFS("/proc")
	if err != nil {
		return nil, err
	}
	var previous map[int]uint64
	var previousInodes map[uint64]bool
	var sockets []Socket
	for pass := range 2 {
		top, err := api.ContainerTop(ctx, c.ID, client.ContainerTopOptions{Arguments: []string{"-eo", "pid"}})
		if err != nil {
			return nil, err
		}
		column := slices.Index(top.Titles, "PID")
		if column < 0 || len(top.Processes) == 0 {
			return nil, ErrUnverified
		}
		processes := make(map[int]uint64)
		inodes := make(map[uint64]bool)
		for _, row := range top.Processes {
			if column >= len(row) {
				return nil, ErrUnverified
			}
			pid, err := strconv.Atoi(row[column])
			if err != nil || pid <= 0 {
				return nil, ErrUnverified
			}
			base := filepath.Join("/proc", strconv.Itoa(pid))
			group, err := os.ReadFile(filepath.Join(base, "cgroup"))
			if err != nil || !strings.Contains(string(group), c.ID) {
				return nil, ErrUnverified
			}
			ns, err := os.Readlink(filepath.Join(base, "ns/net"))
			self, selfErr := os.Readlink("/proc/self/ns/net")
			if err != nil || selfErr != nil || ns != self {
				return nil, ErrUnverified
			}
			proc, err := fs.Proc(pid)
			if err != nil {
				return nil, err
			}
			before, err := proc.Stat()
			if err != nil {
				return nil, err
			}
			fds, err := os.ReadDir(filepath.Join(base, "fd"))
			if err != nil {
				return nil, err
			}
			for _, fd := range fds {
				target, err := os.Readlink(filepath.Join(base, "fd", fd.Name()))
				if err != nil {
					return nil, err
				}
				if value, ok := strings.CutPrefix(target, "socket:["); ok {
					inode, err := strconv.ParseUint(strings.TrimSuffix(value, "]"), 10, 64)
					if err != nil {
						return nil, err
					}
					inodes[inode] = true
				}
			}
			after, err := proc.Stat()
			if err != nil || before.Starttime != after.Starttime {
				return nil, ErrUnverified
			}
			processes[pid] = before.Starttime
		}
		if _, ok := processes[c.State.Pid]; !ok {
			return nil, ErrUnverified
		}
		if pass == 0 {
			previous, previousInodes = processes, inodes
			sockets, err = Sockets()
			if err != nil {
				return nil, err
			}
		} else if !maps.Equal(previous, processes) || !maps.Equal(previousInodes, inodes) {
			return nil, ErrUnverified
		}
	}
	current, err := api.ContainerInspect(ctx, c.ID, client.ContainerInspectOptions{})
	if err != nil || current.Container.State == nil || !current.Container.State.Running || current.Container.State.Pid != c.State.Pid || current.Container.State.StartedAt != c.State.StartedAt {
		return nil, fmt.Errorf("container changed during host inspection: %w", ErrUnverified)
	}
	return slices.DeleteFunc(sockets, func(s Socket) bool { return !previousInodes[s.Inode] }), nil
}

// PublishedSockets attributes a Docker proxy only when its executable, daemon parent,
// target IP, target port, and published address match the inspected container.
func PublishedSockets(c container.InspectResponse) ([]Socket, error) {
	if c.NetworkSettings == nil || len(c.NetworkSettings.Ports) == 0 {
		return nil, nil
	}
	fs, err := procfs.NewFS("/proc")
	if err != nil {
		return nil, err
	}
	processes, err := fs.AllProcs()
	if err != nil {
		return nil, err
	}
	owned := make(map[uint64]bool)
	for _, proc := range processes {
		exe, err := proc.Executable()
		if err != nil || filepath.Base(exe) != "docker-proxy" {
			continue
		}
		before, err := proc.Stat()
		if err != nil {
			continue
		}
		parent, err := fs.Proc(before.PPID)
		if err != nil {
			continue
		}
		exe, err = parent.Executable()
		if err != nil || filepath.Base(exe) != "dockerd" {
			continue
		}
		args, err := proc.CmdLine()
		if err != nil {
			continue
		}
		flags := make(map[string]string)
		for i := 1; i+1 < len(args); i++ {
			if strings.HasPrefix(args[i], "-") {
				flags[args[i]] = args[i+1]
			}
		}
		matched := false
		for port, bindings := range c.NetworkSettings.Ports {
			if string(port.Proto()) != flags["-proto"] || strconv.Itoa(int(port.Num())) != flags["-container-port"] {
				continue
			}
			for _, binding := range bindings {
				if binding.HostIP.String() != flags["-host-ip"] || binding.HostPort != flags["-host-port"] {
					continue
				}
				for _, endpoint := range c.NetworkSettings.Networks {
					if endpoint != nil && (endpoint.IPAddress.String() == flags["-container-ip"] || endpoint.GlobalIPv6Address.String() == flags["-container-ip"]) {
						matched = true
					}
				}
			}
		}
		if !matched {
			continue
		}
		base := filepath.Join("/proc", strconv.Itoa(proc.PID), "fd")
		fds, err := os.ReadDir(base)
		if err != nil {
			return nil, err
		}
		for _, fd := range fds {
			target, err := os.Readlink(filepath.Join(base, fd.Name()))
			if err != nil {
				return nil, err
			}
			if value, ok := strings.CutPrefix(target, "socket:["); ok {
				inode, err := strconv.ParseUint(strings.TrimSuffix(value, "]"), 10, 64)
				if err != nil {
					return nil, err
				}
				owned[inode] = true
			}
		}
		after, err := proc.Stat()
		if err != nil || after.Starttime != before.Starttime {
			return nil, ErrUnverified
		}
	}
	sockets, err := Sockets()
	return slices.DeleteFunc(sockets, func(s Socket) bool { return !owned[s.Inode] }), err
}
