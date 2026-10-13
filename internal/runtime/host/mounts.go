package host

import (
	"bufio"
	"errors"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
)

// Mount is one entry of the mount table. Root is the path inside the filesystem that the
// mount shows at Point.
type Mount struct {
	Root, Point, Type, Source string
}

// remoteTypes read their data over the network. Local FUSE pools, such as mergerfs and
// the Unraid user share (shfs), are not in this list.
var remoteTypes = []string{"nfs", "nfs4", "cifs", "smb3", "smbfs", "9p", "ceph", "glusterfs", "fuse.glusterfs", "fuse.sshfs", "sshfs", "fuse.rclone", "rclone", "fuse.s3fs", "fuse.gcsfuse", "davfs", "fuse.davfs2"}

// Remote reports whether a filesystem type reads its data over the network.
func Remote(fstype string) bool { return slices.Contains(remoteTypes, fstype) }

// ReadMounts reads the mount table of this process. A system without /proc has no table.
func ReadMounts() ([]Mount, error) {
	file, err := os.Open("/proc/self/mountinfo")
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer file.Close()
	return ParseMounts(file)
}

// ParseMounts reads mountinfo lines.
func ParseMounts(input io.Reader) ([]Mount, error) {
	var result []Mount
	scanner := bufio.NewScanner(input)
	scanner.Buffer(make([]byte, 64<<10), 1<<20)
	for scanner.Scan() {
		before, after, found := strings.Cut(scanner.Text(), " - ")
		fields, tail := strings.Fields(before), strings.Fields(after)
		if !found || len(fields) < 5 || len(tail) < 2 {
			continue
		}
		result = append(result, Mount{Root: unescape(fields[3]), Point: unescape(fields[4]), Type: tail[0], Source: unescape(tail[1])})
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

// Containing returns the mount that holds path. A later mount at the same point hides
// an earlier one.
func Containing(mounts []Mount, path string) (Mount, bool) {
	path = filepath.Clean(path)
	var best Mount
	found := false
	for _, m := range mounts {
		if within(path, m.Point) && (!found || len(m.Point) >= len(best.Point)) {
			best, found = m, true
		}
	}
	return best, found
}

// Below returns the mount points strictly below path, each once, in order.
func Below(mounts []Mount, path string) []Mount {
	path = filepath.Clean(path)
	seen := make(map[string]int)
	var result []Mount
	for _, m := range mounts {
		if m.Point == path || !within(m.Point, path) {
			continue
		}
		if i, ok := seen[m.Point]; ok {
			result[i] = m // A later mount at the same point hides the earlier one.
			continue
		}
		seen[m.Point] = len(result)
		result = append(result, m)
	}
	slices.SortFunc(result, func(a, b Mount) int { return strings.Compare(a.Point, b.Point) })
	return result
}
