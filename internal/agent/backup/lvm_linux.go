package backup

import (
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/google/uuid"
)

const (
	lvmSnapshotTag       = "synod-backup"
	lvmSnapshotDirectory = "/run/synod-snapshots"
)

type logicalVolume struct {
	Path        string `json:"lv_path"`
	UUID        string `json:"lv_uuid"`
	Name        string `json:"lv_name"`
	Group       string `json:"vg_name"`
	Attributes  string `json:"lv_attr"`
	OriginUUID  string `json:"origin_uuid"`
	Pool        string `json:"pool_lv"`
	Tags        string `json:"lv_tags"`
	Size        string `json:"lv_size"`
	Free        string `json:"vg_free"`
	Extent      string `json:"vg_extent_size"`
	DataPercent string `json:"data_percent"`
}

func logicalVolumes(ctx context.Context) ([]logicalVolume, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	command := exec.CommandContext(ctx, "lvs", "--reportformat", "json", "--units", "b", "--nosuffix", "--options", "lv_path,lv_uuid,lv_name,vg_name,lv_attr,origin_uuid,pool_lv,lv_tags,lv_size,vg_free,vg_extent_size,data_percent")
	command.Env = append(os.Environ(), "LC_ALL=C")
	output, err := command.Output()
	if err != nil {
		return nil, fmt.Errorf("read LVM volumes: %w", err)
	}
	var report struct {
		Report []struct {
			Volumes []logicalVolume `json:"lv"`
		} `json:"report"`
	}
	if err := json.Unmarshal(output, &report); err != nil {
		return nil, fmt.Errorf("read LVM report: %w", err)
	}
	var result []logicalVolume
	for _, group := range report.Report {
		result = append(result, group.Volumes...)
	}
	return result, nil
}

// cowExtents limits snapshot storage and leaves free space for the source volume group.
// Use a thick snapshot even for a thin origin. A full thin pool can stop source writes.
func (v logicalVolume) cowExtents() (int64, bool) {
	if len(v.Attributes) != 10 || !strings.ContainsRune("-omrV", rune(v.Attributes[0])) || v.Attributes[4] != 'a' || v.Attributes[8] != '-' {
		return 0, false
	}
	size, sizeErr := strconv.ParseFloat(strings.TrimSpace(v.Size), 64)
	free, freeErr := strconv.ParseFloat(strings.TrimSpace(v.Free), 64)
	extent, extentErr := strconv.ParseFloat(strings.TrimSpace(v.Extent), 64)
	if sizeErr != nil || freeErr != nil || extentErr != nil || !(size > 0 && size < 1<<60 && free > 0 && free < 1<<60 && extent > 0 && extent <= 1<<30) {
		return 0, false
	}
	// Reserve up to 10% of the origin, from 64 MiB to 1 GiB, rounded to extents.
	count := int64(math.Ceil(max(64<<20, min(size/10, 1<<30)) / extent))
	if float64(count)*extent > free/2 {
		return 0, false
	}
	return count, true
}

func (v logicalVolume) ownedSnapshot() bool {
	if !strings.HasPrefix(v.Name, snapshotPrefix) || v.Path != filepath.Join("/dev", v.Group, v.Name) || v.UUID == "" || v.OriginUUID == "" || v.Pool != "" || len(v.Attributes) != 10 || v.Attributes[0] != 's' {
		return false
	}
	id, err := uuid.Parse(strings.TrimPrefix(v.Name, snapshotPrefix))
	if err != nil || v.Name != snapshotPrefix+id.String() {
		return false
	}
	return slices.Contains(strings.Split(v.Tags, ","), lvmSnapshotTag)
}

func (v logicalVolume) validSnapshot() bool {
	percent, err := strconv.ParseFloat(strings.TrimSpace(v.DataPercent), 64)
	return v.ownedSnapshot() && v.Attributes[4] == 'a' && v.Attributes[8] == '-' && err == nil && percent >= 0 && percent < 100
}

type lvmSnapshot struct {
	name, group, originUUID, uuid, directory string
}

func (s *fsSnapshots) lvmView(ctx context.Context, m mountEntry) (string, bool) {
	if view, done := s.views[m.point]; done {
		return view, true
	}
	if m.fstype != "ext4" && m.fstype != "xfs" {
		return "", false
	}
	volumes, err := logicalVolumes(ctx)
	if err != nil {
		return "", false
	}
	var mounted syscall.Stat_t
	if err := syscall.Stat(m.point, &mounted); err != nil {
		return "", false
	}
	for _, volume := range volumes {
		var device syscall.Stat_t
		if volume.Path == "" || syscall.Stat(volume.Path, &device) != nil || device.Mode&syscall.S_IFMT != syscall.S_IFBLK || uint64(device.Rdev) != uint64(mounted.Dev) {
			continue
		}
		for _, captured := range s.lvm {
			if captured.originUUID == volume.UUID {
				s.views[m.point] = captured.directory
				return captured.directory, true
			}
		}
		extents, ok := volume.cowExtents()
		if !ok {
			return "", false
		}
		if err := os.Mkdir(lvmSnapshotDirectory, 0700); err != nil && !errors.Is(err, os.ErrExist) {
			return "", false
		}
		info, err := os.Lstat(lvmSnapshotDirectory)
		if err != nil || !info.IsDir() || info.Mode().Perm() != 0700 || info.Sys().(*syscall.Stat_t).Uid != uint32(os.Geteuid()) {
			return "", false
		}
		name := snapshotPrefix + uuid.NewString()
		view := filepath.Join(lvmSnapshotDirectory, name)
		if err := os.Mkdir(view, 0700); err != nil {
			return "", false
		}
		snap := &lvmSnapshot{name: name, group: volume.Group, originUUID: volume.UUID, directory: view}
		// Register cleanup before lvcreate. A lost command response can leave an LV.
		s.removes = append(s.removes, func() error { return snap.remove(context.Background()) })
		ready := false
		defer func() {
			if !ready {
				// Close retries a failed cleanup and reports its error.
				snap.remove(context.Background())
			}
		}()
		if err := run(ctx, "lvcreate", "--snapshot", "--type", "snapshot", "--extents", strconv.FormatInt(extents, 10), "--name", name, "--addtag", lvmSnapshotTag, "--permission", "rw", "--monitor", "n", "--yes", volume.Path); err != nil {
			return "", false
		}
		created, err := snap.lookup(ctx)
		if err != nil || !created.validSnapshot() {
			return "", false
		}
		snap.uuid = created.UUID
		options := ""
		if m.fstype == "xfs" {
			options = "nouuid"
		}
		// Keep the LV writable for journal recovery. The mounted view is read-only.
		if err := syscall.Mount(created.Path, view, m.fstype, syscall.MS_RDONLY|syscall.MS_NOSUID|syscall.MS_NODEV|syscall.MS_NOEXEC, options); err != nil {
			return "", false
		}
		s.lvm = append(s.lvm, snap)
		s.views[m.point] = view
		ready = true
		return view, true
	}
	return "", false
}

func (s *lvmSnapshot) lookup(ctx context.Context) (logicalVolume, error) {
	volumes, err := logicalVolumes(ctx)
	if err != nil {
		return logicalVolume{}, err
	}
	for _, volume := range volumes {
		if volume.Name != s.name || volume.Group != s.group {
			continue
		}
		if !volume.ownedSnapshot() || volume.OriginUUID != s.originUUID || (s.uuid != "" && volume.UUID != s.uuid) {
			return logicalVolume{}, fmt.Errorf("LVM snapshot %s/%s changed identity; inspect it before retrying", s.group, s.name)
		}
		return volume, nil
	}
	return logicalVolume{}, os.ErrNotExist
}

// Check rejects overflow even when incremental upload reused every file.
func (s *fsSnapshots) Check(ctx context.Context) error {
	for _, snap := range s.lvm {
		volume, err := snap.lookup(ctx)
		if err != nil || !volume.validSnapshot() {
			return fmt.Errorf("LVM snapshot %s/%s is unavailable or full; free snapshot space and retry: %w", snap.group, snap.name, errors.Join(err, ErrSource))
		}
	}
	return nil
}

func (s *lvmSnapshot) remove(ctx context.Context) error {
	volume, err := s.lookup(ctx)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err == nil {
		all, err := mounts()
		if err != nil {
			return err
		}
		for _, m := range all {
			if m.point != s.directory {
				continue
			}
			// Never unmount a different device that took the staging path.
			live, liveErr := os.Stat(m.source)
			device, deviceErr := os.Stat(volume.Path)
			if liveErr != nil || deviceErr != nil || live.Sys().(*syscall.Stat_t).Rdev != device.Sys().(*syscall.Stat_t).Rdev {
				return fmt.Errorf("LVM snapshot mount %s changed device; inspect it before retrying", s.directory)
			}
			if err := syscall.Unmount(s.directory, 0); err != nil {
				return fmt.Errorf("unmount LVM snapshot %s: %w", s.directory, err)
			}
		}
		// A UUID selection also prevents removal of an LV replaced after lookup.
		if err := run(ctx, "lvremove", "--yes", "--select", "lv_uuid="+volume.UUID, volume.Path); err != nil {
			return err
		}
	}
	if err := os.Remove(s.directory); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

func removeStaleLVM(ctx context.Context) error {
	if _, err := exec.LookPath("lvs"); errors.Is(err, exec.ErrNotFound) {
		return nil
	}
	volumes, err := logicalVolumes(ctx)
	if err != nil {
		return err
	}
	var failures []error
	for _, volume := range volumes {
		if !volume.ownedSnapshot() {
			continue
		}
		snap := &lvmSnapshot{name: volume.Name, group: volume.Group, originUUID: volume.OriginUUID, uuid: volume.UUID, directory: filepath.Join(lvmSnapshotDirectory, volume.Name)}
		failures = append(failures, snap.remove(ctx))
	}
	return errors.Join(failures...)
}
