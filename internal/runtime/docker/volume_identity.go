package docker

import (
	"context"
	"errors"
	"path"
	"strings"

	"github.com/containerd/errdefs"
	"github.com/moby/moby/api/types/volume"
	"github.com/moby/moby/client"
	"github.com/sirrobot01/synod/internal/runtime"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
)

// volumeRoot is where the local driver keeps every volume it manages. A volume
// whose mountpoint sits outside it is not a volume this daemon created.
const volumeRoot = "/volumes/"

// verifyVolume proves that the native volume behind one command is the volume this
// installation declared, before anything reads or writes it.
//
// Specification 33 states that an empty mount path is not proof of a new database
// volume, and that a replacement directory must never be initialized when an expected
// mount is missing. A container runtime will happily create a named volume that is
// absent, and the engine would then initialize a fresh database over what an operator
// believes is their data. This refuses instead.
func (r *Runtime) verifyVolume(ctx context.Context, command *agentv1.Command, volumeID string) error {
	if err := r.inspectOwnVolume(ctx, command, volumeID); err != nil {
		if errors.Is(err, errVolumeAbsent) {
			// A missing volume is a known configuration problem, not an unknown
			// outcome. Reporting it as unknown would hold the mutation slot for an
			// operator to inspect something that is simply not there.
			return runtime.ErrConfigurationConflict
		}
		return err
	}
	return nil
}

// errVolumeAbsent reports a volume that is not on the host. The two callers want
// opposite things from it: an engine cannot start over storage that is not there,
// and a purge that finds nothing to remove is already finished.
var errVolumeAbsent = errors.New("volume absent")

// inspectOwnVolume checks volume ownership, driver, options, and mount path.
func (r *Runtime) inspectOwnVolume(ctx context.Context, command *agentv1.Command, volumeID string) error {
	item, err := r.api.VolumeInspect(ctx, "synod-volume-"+volumeID, client.VolumeInspectOptions{})
	if err != nil {
		if errdefs.IsNotFound(err) {
			return errVolumeAbsent
		}
		return &runtime.Error{Kind: runtime.ErrOutcomeUnknown, Cause: err}
	}
	if err := r.checkLabels(command, item.Volume.Labels, "volume", volumeID); err != nil {
		return err
	}
	return checkVolumePath(item.Volume)
}

// checkVolumePath refuses a volume that redirects storage somewhere else.
//
// The local driver with no options always keeps its data under the daemon's own
// volume root. A driver or an option that points elsewhere is how a volume named like
// one of ours ends up holding, or destroying, something else. Specification 33
// requires volume paths to stay inside approved roots and to reject traversal and
// symlink escapes, and this is where that is enforced: the path is never handled by
// Synod, so the check is that the daemon still owns it.
func checkVolumePath(item volume.Volume) error {
	if item.Driver != "local" || len(item.Options) != 0 {
		return runtime.ErrConfigurationConflict
	}
	mount := item.Mountpoint
	// A relative or unclean mountpoint names something other than it appears to.
	if mount == "" || !strings.HasPrefix(mount, "/") || path.Clean(mount) != mount {
		return runtime.ErrConfigurationConflict
	}
	// The path must sit under a volume root and end in the volume's own directory,
	// so a mountpoint cannot climb out of the root it claims to be in.
	index := strings.LastIndex(mount, volumeRoot)
	if index < 0 {
		return runtime.ErrConfigurationConflict
	}
	rest := mount[index+len(volumeRoot):]
	if rest == "" || strings.Contains(rest, "..") || !strings.HasPrefix(rest, item.Name+"/") {
		return runtime.ErrConfigurationConflict
	}
	return nil
}

// RemoveVolume deletes one volume this node owns, and nothing else.
//
// Specification 33 keeps purge separate from delete: a normal delete retains data, and
// only an explicit, validated purge removes it. This is that removal, and it is the
// most dangerous call in the driver, so every check happens immediately before it
// rather than at planning time.
//
// A timeout never widens what a second attempt touches. The only thing this can remove
// is the single volume whose identity it just proved, named from the resource
// identity; there is no path, no pattern, and no recursion, so a retry can only ever
// repeat the same narrow removal.
func (r *Runtime) RemoveVolume(ctx context.Context, command *agentv1.Command, volumeID string) error {
	// Identity is checked here, not by the caller. A check the caller could skip is
	// a check that will one day be skipped.
	if err := r.inspectOwnVolume(ctx, command, volumeID); err != nil {
		if errors.Is(err, errVolumeAbsent) {
			// Already gone. A repeated purge request is ordinary and must not fail.
			return nil
		}
		return err
	}
	name := "synod-volume-" + volumeID
	// Force is never set. A volume still in use by a container has not finished
	// being released, and tearing it out from under one is how a stop becomes a
	// corruption.
	if _, err := r.api.VolumeRemove(ctx, name, client.VolumeRemoveOptions{Force: false}); err != nil {
		if errdefs.IsNotFound(err) {
			// Removed between the check and the call. The end state is the one asked
			// for, so this is a success rather than a failed purge.
			return nil
		}
		if errdefs.IsConflict(err) {
			return runtime.ErrBusy
		}
		return &runtime.Error{Kind: runtime.ErrOutcomeUnknown, Cause: err}
	}
	return nil
}
