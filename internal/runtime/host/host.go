// Package host reads the Linux resources that Docker app recovery needs.
package host

import (
	"context"
	"errors"
	"net/netip"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"syscall"

	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/client"
	"github.com/sirrobot01/synod/internal/backup"
)

var ErrUnverified = errors.New("host requirements cannot be verified")

// Local is the agent's view of the Docker daemon's host. An agent that runs on the host
// reads host paths directly. An agent in a container, with pid: host and network: host,
// reads them through the daemon's root directory, so it sees what Docker sees.
type Local struct{ root *os.Root }

// CheckLocal refuses checks against a daemon outside the agent's Linux host. The reference
// is the dockerd process, because Docker resolves bind sources in its mount namespace.
// The agent must share its network namespace, so host ports are the daemon's ports.
func CheckLocal(ctx context.Context, api *client.Client) (*Local, error) {
	if runtime.GOOS != "linux" || !strings.HasPrefix(api.DaemonHost(), "unix://") {
		return nil, ErrUnverified
	}
	name, err := os.Hostname()
	if err != nil {
		return nil, err
	}
	info, err := api.Info(ctx, client.InfoOptions{})
	if err != nil {
		return nil, err
	}
	if info.Info.OSType != "linux" || info.Info.Name != name {
		return nil, ErrUnverified
	}
	daemon, err := daemonProcess()
	if err != nil {
		return nil, err
	}
	namespace := func(pid, kind string) string {
		link, _ := os.Readlink("/proc/" + pid + "/ns/" + kind)
		return link
	}
	if network := namespace("self", "net"); network == "" || network != namespace(daemon, "net") {
		return nil, ErrUnverified
	}
	if mounts := namespace("self", "mnt"); mounts != "" && mounts == namespace(daemon, "mnt") {
		return &Local{}, nil
	}
	root, err := os.OpenRoot("/proc/" + daemon + "/root")
	if err != nil {
		return nil, errors.Join(ErrUnverified, err)
	}
	return &Local{root: root}, nil
}

// daemonProcess finds the one dockerd process that the agent can see. An agent in a
// container sees it only with pid: host.
func daemonProcess() (string, error) {
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return "", errors.Join(ErrUnverified, err)
	}
	var found []string
	for _, entry := range entries {
		if _, err := strconv.Atoi(entry.Name()); err != nil {
			continue
		}
		if comm, err := os.ReadFile("/proc/" + entry.Name() + "/comm"); err == nil && strings.TrimSpace(string(comm)) == "dockerd" {
			found = append(found, entry.Name())
		}
	}
	if len(found) != 1 {
		return "", ErrUnverified
	}
	return found[0], nil
}

func (l *Local) Close() {
	if l != nil && l.root != nil {
		l.root.Close()
	}
}

// Path reads a host path as the Docker daemon sees it. A link inside the daemon's root
// directory must be relative, or the path is unknown.
func (l *Local) Path(name string) backup.HostPath {
	if l == nil || l.root == nil {
		return Path(name)
	}
	info, err := l.root.Stat(strings.TrimPrefix(filepath.Clean(name), "/"))
	return pathInfo(name, info, err)
}

// UsesHostNetwork follows namespace joins within the saved app.
func UsesHostNetwork(c container.InspectResponse, app []container.InspectResponse) bool {
	for range len(app) + 1 {
		if c.HostConfig == nil {
			return false
		}
		if c.HostConfig.NetworkMode == "host" {
			return true
		}
		id, ok := strings.CutPrefix(string(c.HostConfig.NetworkMode), "container:")
		if !ok {
			return false
		}
		i := slices.IndexFunc(app, func(other container.InspectResponse) bool { return other.ID == id })
		if i < 0 {
			return false
		}
		c = app[i]
	}
	return false
}

// Path reads a path in the agent's own mount namespace.
func Path(name string) backup.HostPath {
	info, err := os.Stat(name)
	return pathInfo(name, info, err)
}

func pathInfo(name string, info os.FileInfo, err error) backup.HostPath {
	p := backup.HostPath{Path: name}
	if err != nil {
		return p
	}
	p.Known, p.Mode = true, uint32(info.Mode())
	if stat, ok := info.Sys().(*syscall.Stat_t); ok {
		p.UserID, p.GroupID = stat.Uid, stat.Gid
		if info.Mode()&os.ModeDevice != 0 {
			p.Device = uint64(stat.Rdev)
		}
	}
	return p
}

// Overlaps includes IPv4-mapped addresses and dual-stack wildcard listeners.
func Overlaps(a, b backup.HostPort) bool {
	if a.Port != b.Port || a.Protocol != b.Protocol {
		return false
	}
	x, err := netip.ParseAddr(a.Address)
	if err != nil {
		return true
	}
	y, err := netip.ParseAddr(b.Address)
	if err != nil {
		return true
	}
	x, y = x.Unmap(), y.Unmap()
	if x == y || (x.IsUnspecified() && x.Is6()) || (y.IsUnspecified() && y.Is6()) {
		return true
	}
	return x.Is4() == y.Is4() && (x.IsUnspecified() || y.IsUnspecified())
}

type Socket struct {
	Port  backup.HostPort
	Inode uint64
}
