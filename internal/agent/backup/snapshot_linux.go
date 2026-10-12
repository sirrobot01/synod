package backup

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"time"
)

const (
	btrfsMagic = 0x9123683E
	zfsMagic   = 0x2FC12FC1
	// snapshotDirectory holds btrfs snapshots at the top of the mounted filesystem.
	snapshotDirectory = ".synod-snapshots"
	snapshotPrefix    = "synod-"
)

type mountEntry struct {
	root, point, fstype, source string
}

// mounts reads the mount table of this process.
func mounts() ([]mountEntry, error) {
	file, err := os.Open("/proc/self/mountinfo")
	if err != nil {
		return nil, err
	}
	defer file.Close()
	var result []mountEntry
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 64<<10), 1<<20)
	for scanner.Scan() {
		before, after, found := strings.Cut(scanner.Text(), " - ")
		fields, tail := strings.Fields(before), strings.Fields(after)
		if !found || len(fields) < 5 || len(tail) < 2 {
			continue
		}
		result = append(result, mountEntry{root: unescape(fields[3]), point: unescape(fields[4]), fstype: tail[0], source: unescape(tail[1])})
	}
	return result, scanner.Err()
}

// unescape decodes the octal escapes of mountinfo, such as \040 for a space.
func unescape(field string) string {
	var b strings.Builder
	for i := 0; i < len(field); i++ {
		if field[i] == '\\' && i+3 < len(field) {
			if v, err := strconv.ParseUint(field[i+1:i+4], 8, 8); err == nil {
				b.WriteByte(byte(v))
				i += 3
				continue
			}
		}
		b.WriteByte(field[i])
	}
	return b.String()
}

func within(path, parent string) bool {
	return path == parent || parent == "/" || strings.HasPrefix(path, parent+"/")
}

// containing returns the mount that holds path, and whether another mount lies below path.
func containing(all []mountEntry, path string) (mountEntry, bool, bool) {
	var best mountEntry
	found, nested := false, false
	for _, m := range all {
		if within(path, m.point) && (!found || len(m.point) >= len(best.point)) {
			best, found = m, true
		}
		if m.point != path && within(m.point, path) {
			nested = true
		}
	}
	return best, found, nested
}

// fsSnapshots holds the read-only snapshots of one backup. Each subvolume, dataset,
// or logical volume gets one snapshot. Separate filesystems have separate capture times.
type fsSnapshots struct {
	name   string
	mounts []mountEntry
	// views maps a snapshotted directory to the same directory in its snapshot.
	views   map[string]string
	removes []func() error
	lvm     []*lvmSnapshot
	// groups maps a snapshot base to a capture point that several bases share.
	groups map[string]string
}

// useSnapshots moves each supported tree root to a filesystem snapshot.
// A root stays live when its filesystem cannot give a complete snapshot of it:
// another filesystem or subvolume is mounted or nested below it, or the tools are absent.
func (t *Tree) useSnapshots(ctx context.Context, operation string) (*fsSnapshots, error) {
	all, err := mounts()
	if err != nil {
		return nil, err
	}
	s := &fsSnapshots{name: snapshotPrefix + operation, mounts: all, views: make(map[string]string), groups: make(map[string]string)}
	s.zfsTogether(ctx, t.roots)
	for _, root := range t.roots {
		directory := root.source
		if root.path != "." {
			directory = filepath.Dir(root.source)
		}
		view, ok := s.view(ctx, directory)
		if !ok {
			continue
		}
		opened, err := os.OpenRoot(view)
		if err != nil {
			continue
		}
		live, err := root.root.Lstat(root.path)
		seen, viewErr := opened.Lstat(root.path)
		if err != nil || viewErr != nil || live.Mode().Type() != seen.Mode().Type() {
			opened.Close()
			continue
		}
		root.root.Close()
		root.root, root.snapshot = opened, true
		for _, base := range s.views {
			if within(view, base) && len(base) > len(root.capturePoint) {
				root.capturePoint = base
			}
		}
		if group, ok := s.groups[root.capturePoint]; ok {
			root.capturePoint = group
		}
	}
	// A btrfs snapshot directory sits inside a mounted filesystem. Never copy it.
	for _, m := range all {
		if m.fstype == "btrfs" {
			t.excluded[filepath.Join(m.point, snapshotDirectory)] = "Synod snapshots are not app data."
		}
	}
	return s, nil
}

// zfsTogether snapshots the ZFS datasets of the roots with one command for each pool.
// ZFS takes the snapshots of one command at the same moment, so the roots share one
// capture point. If the command fails, view snapshots each dataset alone.
func (s *fsSnapshots) zfsTogether(ctx context.Context, roots []*sourceRoot) {
	pools := make(map[string][]mountEntry)
	for _, root := range roots {
		directory := root.source
		if root.path != "." {
			directory = filepath.Dir(root.source)
		}
		var stat syscall.Statfs_t
		if syscall.Statfs(directory, &stat) != nil || uint32(stat.Type) != zfsMagic {
			continue
		}
		m, ok, nested := containing(s.mounts, directory)
		if !ok || nested || m.root != "/" || m.fstype != "zfs" {
			continue
		}
		pool, _, _ := strings.Cut(m.source, "/")
		if !slices.ContainsFunc(pools[pool], func(known mountEntry) bool { return known.point == m.point }) {
			pools[pool] = append(pools[pool], m)
		}
	}
	for pool, datasets := range pools {
		if len(datasets) < 2 {
			continue
		}
		arguments := []string{"snapshot"}
		for _, m := range datasets {
			arguments = append(arguments, m.source+"@"+s.name)
		}
		if run(ctx, "zfs", arguments...) != nil {
			continue
		}
		for _, m := range datasets {
			base := filepath.Join(m.point, ".zfs", "snapshot", s.name)
			dataset := m.source + "@" + s.name
			s.removes = append(s.removes, func() error { return run(context.Background(), "zfs", "destroy", dataset) })
			s.views[m.point] = base
			s.groups[base] = "zfs:" + pool + "@" + s.name
		}
	}
}

// view returns the snapshot path of a live directory, and takes the snapshot when needed.
func (s *fsSnapshots) view(ctx context.Context, directory string) (string, bool) {
	var stat syscall.Statfs_t
	if syscall.Statfs(directory, &stat) != nil {
		return "", false
	}
	m, ok, nested := containing(s.mounts, directory)
	if !ok || nested || m.root != "/" && stat.Type != btrfsMagic {
		return "", false
	}
	switch uint32(stat.Type) {
	case zfsMagic:
		if m.fstype != "zfs" {
			return "", false
		}
		base := filepath.Join(m.point, ".zfs", "snapshot", s.name)
		if _, done := s.views[m.point]; !done {
			if run(ctx, "zfs", "snapshot", m.source+"@"+s.name) != nil {
				return "", false
			}
			dataset := m.source + "@" + s.name
			s.removes = append(s.removes, func() error { return run(context.Background(), "zfs", "destroy", dataset) })
			s.views[m.point] = base
		}
		relative, err := filepath.Rel(m.point, directory)
		if err != nil {
			return "", false
		}
		return filepath.Join(base, relative), true
	case btrfsMagic:
		top, ok := subvolume(directory, m.point)
		if !ok || hasNestedSubvolume(ctx, top, directory, m) {
			return "", false
		}
		base, done := s.views[top]
		if !done {
			parent := filepath.Join(m.point, snapshotDirectory)
			if err := os.Mkdir(parent, 0o700); err != nil && !errors.Is(err, os.ErrExist) {
				return "", false
			}
			base = filepath.Join(parent, fmt.Sprintf("%s-%d", s.name, len(s.views)))
			if run(ctx, "btrfs", "subvolume", "snapshot", "-r", top, base) != nil {
				return "", false
			}
			target := base
			s.removes = append(s.removes, func() error { return run(context.Background(), "btrfs", "subvolume", "delete", target) })
			s.views[top] = base
		}
		relative, err := filepath.Rel(top, directory)
		if err != nil {
			return "", false
		}
		return filepath.Join(base, relative), true
	}
	if base, ok := s.lvmView(ctx, m); ok {
		relative, err := filepath.Rel(m.point, directory)
		if err == nil {
			return filepath.Join(base, relative), true
		}
	}
	return "", false
}

// subvolume returns the root of the btrfs subvolume that holds directory. It does not
// leave the mount point, because a parent mount can be another view of the filesystem.
func subvolume(directory, point string) (string, bool) {
	var start syscall.Stat_t
	if syscall.Stat(directory, &start) != nil {
		return "", false
	}
	top := directory
	for top != point && top != "/" {
		var parent syscall.Stat_t
		if syscall.Stat(filepath.Dir(top), &parent) != nil || parent.Dev != start.Dev {
			break
		}
		top = filepath.Dir(top)
	}
	var root syscall.Stat_t
	if syscall.Stat(top, &root) != nil || root.Ino != 256 {
		return "", false
	}
	return top, true
}

// hasNestedSubvolume reports whether another subvolume lies below directory. A snapshot
// shows such a subvolume as an empty directory, so the copy would lose its files.
func hasNestedSubvolume(ctx context.Context, top, directory string, m mountEntry) bool {
	output, err := exec.CommandContext(ctx, "btrfs", "subvolume", "list", "-o", top).Output()
	if err != nil {
		return true
	}
	// Listed paths start at the top of the filesystem. The mount shows m.root at m.point.
	relative, err := filepath.Rel(m.point, directory)
	if err != nil {
		return true
	}
	inside := filepath.Join(strings.TrimPrefix(m.root, "/"), relative)
	for line := range strings.Lines(string(output)) {
		_, path, found := strings.Cut(strings.TrimSpace(line), " path ")
		if found && (inside == "." || within(path, inside)) {
			return true
		}
	}
	return false
}

func run(ctx context.Context, name string, arguments ...string) error {
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	if output, err := exec.CommandContext(ctx, name, arguments...).CombinedOutput(); err != nil {
		return fmt.Errorf("%s %s: %w: %s", name, strings.Join(arguments, " "), err, strings.TrimSpace(string(output)))
	}
	return nil
}

// Close removes the snapshots of this backup.
func (s *fsSnapshots) Close() error {
	var failures []error
	var pending []func() error
	for _, remove := range s.removes {
		if err := remove(); err != nil {
			failures = append(failures, err)
			pending = append(pending, remove)
		}
	}
	s.removes = pending
	return errors.Join(failures...)
}

// RemoveStaleSnapshots removes snapshots that an interrupted backup left. Call it before
// the agent starts backups, because it removes every Synod snapshot.
func RemoveStaleSnapshots(ctx context.Context) error {
	all, err := mounts()
	if err != nil {
		return err
	}
	var failures []error
	failures = append(failures, removeStaleLVM(ctx))
	if output, err := exec.CommandContext(ctx, "zfs", "list", "-H", "-t", "snapshot", "-o", "name").Output(); err == nil {
		for line := range strings.Lines(string(output)) {
			if name := strings.TrimSpace(line); strings.Contains(name, "@"+snapshotPrefix) {
				failures = append(failures, run(ctx, "zfs", "destroy", name))
			}
		}
	}
	for _, m := range all {
		if m.fstype != "btrfs" {
			continue
		}
		entries, err := os.ReadDir(filepath.Join(m.point, snapshotDirectory))
		if err != nil {
			continue
		}
		for _, entry := range entries {
			if strings.HasPrefix(entry.Name(), snapshotPrefix) {
				failures = append(failures, run(ctx, "btrfs", "subvolume", "delete", filepath.Join(m.point, snapshotDirectory, entry.Name())))
			}
		}
	}
	return errors.Join(failures...)
}
