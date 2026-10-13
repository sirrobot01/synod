package discovery

import (
	"context"
	"errors"
	"github.com/sirrobot01/synod/internal/runtime/host"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"syscall"
	"time"

	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
)

const largeBytes = 20 << 30

// Forbidden checks before a path is opened. Broad host roots are not app data.
var (
	// hostRoots are refused as whole paths. Paths below them can hold app data.
	hostRoots = []string{"/", "/var", "/home", "/Users", "/mnt", "/media", "/srv", "/opt", "/tmp", "/private"}
	// hostPrefixes are refused with every path below them.
	hostPrefixes = []string{"/etc", "/private/etc", "/proc", "/sys", "/dev", "/run", "/var/run", "/boot", "/usr", "/bin", "/sbin", "/lib", "/lib64", "/var/lib/docker", "/var/lib/containerd"}
)

func Forbidden(path string, volume bool) bool {
	path = filepath.Clean(path)
	if !filepath.IsAbs(path) || slices.Contains(hostRoots, path) {
		return true
	}
	for _, prefix := range hostPrefixes {
		if path == prefix || strings.HasPrefix(path, prefix+"/") {
			// Docker reports the exact local volume data directory.
			if parts := strings.Split(strings.TrimPrefix(path, "/var/lib/docker/volumes/"), "/"); volume && prefix == "/var/lib/docker" && strings.HasPrefix(path, prefix+"/volumes/") && len(parts) >= 2 && parts[0] != "" && parts[1] == "_data" {
				continue
			}
			return true
		}
	}
	return strings.HasSuffix(path, ".sock")
}

// ReachesForbidden reports whether a path below directory can be refused by Forbidden for
// a reason other than its .sock name. A walk checks each child only then.
func ReachesForbidden(directory string) bool {
	directory = filepath.Clean(directory)
	for _, path := range slices.Concat(hostRoots, hostPrefixes) {
		if directory == "/" || strings.HasPrefix(path, directory+"/") {
			return true
		}
	}
	return false
}

// measure reads metadata only. It never follows a link during a directory walk.
// table is the mount table. A path on a network share is Remote and is not walked.
func measure(ctx context.Context, path *agentv1.DiscoveredPath, table []host.Mount) {
	if path.Status == "Never" {
		return
	}
	volume := path.Kind == "volume"
	if Forbidden(path.Source, volume) || strings.HasSuffix(path.Target, ".sock") {
		path.Status, path.Reason = "Never", "Host system paths are never copied."
		return
	}
	for _, part := range strings.Split(strings.ToLower(path.Source+"/"+path.Target), "/") {
		if path.Status != "Dumped" && slices.Contains([]string{"media", "movies", "tv", "music", "downloads", "videos", "photos", "pictures", "upload", "uploads", "originals"}, part) {
			path.Status, path.Reason = "Large", "Media is skipped by default. Its size was not scanned."
			break
		}
	}
	resolved, err := filepath.EvalSymlinks(path.Source)
	if err != nil {
		if path.Status == "" {
			path.Status = "Included"
		}
		path.Reason += " Size is unknown. Check that the path exists and is readable on this device."
		return
	}
	if Forbidden(resolved, volume) {
		path.Status, path.Reason = "Never", "This path resolves to a host system path."
		return
	}
	if m, ok := host.Containing(table, resolved); ok && host.Remote(m.Type) && path.Status != "Dumped" {
		path.Status, path.Reason = "Remote", "This path is on a network share ("+m.Type+"). Network shares are skipped by default."
		return
	}
	if path.Status == "Large" {
		return
	}
	if path.Status == "" {
		path.Status, path.Reason = "Included", "App files and state."
	}
	info, err := os.Lstat(resolved)
	if err != nil {
		path.Reason = "Size is unknown. Check access to this path."
		return
	}
	if info.Mode().IsRegular() {
		path.SizeBytes, path.SizeKnown = info.Size(), true
	} else if info.IsDir() {
		if size, ok := sizes.get(resolved); ok {
			path.SizeBytes, path.SizeKnown = size.bytes, size.known
			if !size.known {
				path.Reason = "Size is incomplete. The scan reached a limit or could not read a path."
			}
		} else {
			if measureDirectory(ctx, path, resolved, info, volume) {
				sizes.put(resolved, measuredSize{bytes: path.SizeBytes, known: path.SizeKnown})
			}
		}
	} else {
		path.Status, path.Reason = "Never", "Only regular files and directories can be copied."
		return
	}
	if path.SizeBytes > largeBytes && path.Status != "Dumped" {
		path.Status, path.Reason = "Large", "This path exceeds 20 GiB and is skipped by default."
	}
}

// measureDirectory adds the sizes of the regular files below a directory. It reports
// whether the result can be kept: a directory that cannot be opened or that changes
// during the scan is measured again next time.
func measureDirectory(ctx context.Context, path *agentv1.DiscoveredPath, resolved string, info os.FileInfo, volume bool) bool {
	root, err := os.OpenRoot(resolved)
	if err != nil {
		path.Reason = "Size is unknown. Check access to this directory."
		return false
	}
	defer root.Close()
	opened, err := root.Stat(".")
	if err != nil || !os.SameFile(info, opened) {
		path.Reason = "Size is unknown. The directory changed during the scan."
		return false
	}
	scan, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	entries := 0
	err = fs.WalkDir(root.FS(), ".", func(name string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if err := scan.Err(); err != nil {
			return err
		}
		entries++
		if entries > 100000 || path.SizeBytes > largeBytes {
			return fs.SkipAll
		}
		// Another filesystem below the path is listed and measured as its own path.
		if entry.IsDir() && name != "." {
			child, err := entry.Info()
			if err != nil {
				return err
			}
			if child.Sys().(*syscall.Stat_t).Dev != info.Sys().(*syscall.Stat_t).Dev {
				return fs.SkipDir
			}
		}
		if name != "." && Forbidden(filepath.Join(resolved, name), volume) {
			return errors.New("directory contains a host system path")
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return nil
		}
		if entry.Type().IsRegular() {
			info, err := entry.Info()
			if err != nil {
				return err
			}
			path.SizeBytes += info.Size()
		}
		return nil
	})
	path.SizeKnown = err == nil && entries <= 100000 && path.SizeBytes <= largeBytes
	if !path.SizeKnown {
		path.Reason = "Size is incomplete. The scan reached a limit or could not read a path."
	}
	return true
}

// sizeLifetime is how long a measured directory size stays in use. The agent scans each
// minute and each backup scans again, so the cache prevents a full walk on every scan.
// A directory that grows past the Large limit is noticed within this time.
const sizeLifetime = 10 * time.Minute

type measuredSize struct {
	bytes    int64
	known    bool
	measured time.Time
}

type sizeCache struct {
	mu    sync.Mutex
	sizes map[string]measuredSize
}

var sizes = &sizeCache{sizes: make(map[string]measuredSize)}

func (c *sizeCache) get(path string) (measuredSize, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	size, ok := c.sizes[path]
	if ok && time.Since(size.measured) > sizeLifetime {
		delete(c.sizes, path)
		return measuredSize{}, false
	}
	return size, ok
}

func (c *sizeCache) put(path string, size measuredSize) {
	c.mu.Lock()
	defer c.mu.Unlock()
	size.measured = time.Now()
	c.sizes[path] = size
	if len(c.sizes) > 4096 {
		for key, old := range c.sizes {
			if time.Since(old.measured) > sizeLifetime {
				delete(c.sizes, key)
			}
		}
	}
}
