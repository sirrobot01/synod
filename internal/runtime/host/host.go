// Package host reads the Linux resources that Docker app recovery needs.
package host

import (
	"context"
	"errors"
	"net/netip"
	"os"
	"runtime"
	"slices"
	"strings"
	"syscall"

	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/client"
	"github.com/sirrobot01/synod/internal/backup"
)

var ErrUnverified = errors.New("host requirements cannot be verified")

// CheckLocal refuses checks against a daemon outside the agent's Linux host.
func CheckLocal(ctx context.Context, api *client.Client) error {
	if runtime.GOOS != "linux" || !strings.HasPrefix(api.DaemonHost(), "unix://") {
		return ErrUnverified
	}
	name, err := os.Hostname()
	if err != nil {
		return err
	}
	info, err := api.Info(ctx, client.InfoOptions{})
	if err != nil {
		return err
	}
	if info.Info.OSType != "linux" || info.Info.Name != name {
		return ErrUnverified
	}
	for _, namespace := range []string{"net", "mnt"} {
		self, err := os.Readlink("/proc/self/ns/" + namespace)
		init, initErr := os.Readlink("/proc/1/ns/" + namespace)
		if err != nil || initErr != nil || self != init {
			return ErrUnverified
		}
	}
	return nil
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

func Path(name string) backup.HostPath {
	p := backup.HostPath{Path: name}
	info, err := os.Stat(name)
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
