package repository

import (
	"context"
	"errors"
	"io"
	"maps"
	"path"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
	kfs "github.com/kopia/kopia/fs"
	"github.com/sirrobot01/synod/internal/backup"
)

// DatabasesPath is the virtual directory that holds the native database dumps of a snapshot.
const DatabasesPath = "@databases"

var ErrPathMissing = errors.New("path is not saved in the snapshot")

// Entry describes one saved path. Mode holds the permission bits only.
type Entry struct {
	Name    string    `json:"name"`
	Type    string    `json:"type"`
	Size    int64     `json:"size"`
	Mode    uint32    `json:"mode"`
	UserID  uint32    `json:"uid"`
	GroupID uint32    `json:"gid"`
	ModTime time.Time `json:"mod_time,omitzero"`
	Target  string    `json:"target,omitempty"`
}

// Locate maps an original absolute path, or a dump below DatabasesPath, to its saved archive path.
// It also returns the names that deeper saved paths add below it. A path that only leads to
// saved paths has no archive path.
func Locate(saved backup.Snapshot, name string) (string, []string, error) {
	if name == "" {
		name = "/"
	}
	if strings.ContainsAny(name, "\\\x00") {
		return "", nil, backup.ErrInvalid
	}
	if name == DatabasesPath || strings.HasPrefix(name, DatabasesPath+"/") {
		var names []string
		for _, dump := range saved.Dumps {
			file := dumpName(saved, dump)
			if name == DatabasesPath+"/"+file {
				return dump.Archive, nil, nil
			}
			names = append(names, file)
		}
		if name != DatabasesPath || len(names) == 0 {
			return "", nil, ErrPathMissing
		}
		return "", names, nil
	}
	if !path.IsAbs(name) || path.Clean(name) != name {
		return "", nil, backup.ErrInvalid
	}
	prefix := strings.TrimSuffix(name, "/") + "/"
	archive, longest := "", -1
	children := make(map[string]bool)
	for _, item := range saved.Paths {
		source := filepath.ToSlash(item.Source)
		switch {
		case source == name || strings.HasPrefix(name, source+"/"):
			if len(source) > longest {
				longest, archive = len(source), path.Join(item.Archive, strings.TrimPrefix(name, source))
			}
		case strings.HasPrefix(source, prefix):
			child, _, _ := strings.Cut(strings.TrimPrefix(source, prefix), "/")
			children[child] = true
		}
	}
	if name == "/" && len(saved.Dumps) > 0 {
		children[DatabasesPath] = true
	}
	if archive == "" && len(children) == 0 {
		return "", nil, ErrPathMissing
	}
	return archive, slices.Sorted(maps.Keys(children)), nil
}

// dumpName uses the container name, so a user can find the dump of a service.
func dumpName(saved backup.Snapshot, dump backup.DatabaseDump) string {
	name := dump.ContainerID[:min(12, len(dump.ContainerID))]
	if saved.App.Definition != nil {
		for _, c := range saved.App.Definition.Containers {
			if c.Id == dump.ContainerID && c.Name != "" {
				name = strings.TrimPrefix(c.Name, "/")
			}
		}
	}
	return name + "." + dump.Format
}

var errListLimit = errors.New("listing limit reached")

// List returns at most limit entries of one saved directory, sorted by name. It reports a truncated listing.
// For a saved file, it returns that file.
func (r *Repository) List(ctx context.Context, saved backup.Snapshot, name string, limit int) ([]Entry, bool, error) {
	if name == "" {
		name = "/"
	}
	if limit < 1 || limit > 10000 {
		return nil, false, backup.ErrInvalid
	}
	archive, children, err := Locate(saved, name)
	if err != nil {
		return nil, false, err
	}
	entries := make(map[string]Entry)
	truncated := false
	if archive != "" {
		entry, err := r.savedEntry(ctx, saved.ID, archive)
		if err != nil {
			return nil, false, err
		}
		defer entry.Close()
		directory, ok := entry.(kfs.Directory)
		if !ok {
			described, err := describe(ctx, entry)
			if err != nil {
				return nil, false, err
			}
			described.Name = path.Base(name)
			return []Entry{described}, false, nil
		}
		err = kfs.IterateEntries(ctx, directory, func(ctx context.Context, child kfs.Entry) error {
			if len(entries) == limit {
				truncated = true
				return errListLimit
			}
			defer child.Close()
			described, err := describe(ctx, child)
			entries[described.Name] = described
			return err
		})
		if err != nil && !errors.Is(err, errListLimit) {
			return nil, false, &Error{Action: "list saved directory", Cause: err}
		}
	}
	for _, child := range children {
		if _, ok := entries[child]; !ok {
			entries[child] = Entry{Name: child, Type: "directory"}
		}
		// A deeper saved path can be a file, such as a separately saved .env file.
		if nested, _, err := Locate(saved, path.Join(name, child)); err == nil && nested != "" {
			entry, err := r.savedEntry(ctx, saved.ID, nested)
			if err != nil {
				return nil, false, err
			}
			described, err := describe(ctx, entry)
			entry.Close()
			if err != nil {
				return nil, false, err
			}
			described.Name = child
			entries[child] = described
		}
	}
	result := make([]Entry, 0, len(entries))
	for _, key := range slices.Sorted(maps.Keys(entries)) {
		result = append(result, entries[key])
	}
	if len(result) > limit {
		result, truncated = result[:limit], true
	}
	return result, truncated, nil
}

func describe(ctx context.Context, entry kfs.Entry) (Entry, error) {
	owner := entry.Owner()
	result := Entry{Name: entry.Name(), Type: "file", Size: entry.Size(), Mode: uint32(entry.Mode().Perm()), UserID: owner.UserID, GroupID: owner.GroupID, ModTime: entry.ModTime().UTC()}
	switch item := entry.(type) {
	case kfs.Directory:
		result.Type, result.Size = "directory", 0
	case kfs.Symlink:
		target, err := item.Readlink(ctx)
		if err != nil {
			return result, err
		}
		result.Type, result.Target = "symlink", target
	case kfs.File:
	default:
		result.Type = "other"
	}
	return result, nil
}

// OpenFile streams one saved regular file. The caller closes the reader.
func (r *Repository) OpenFile(ctx context.Context, id uuid.UUID, archive string) (io.ReadCloser, int64, error) {
	entry, err := r.savedEntry(ctx, id, archive)
	if err != nil {
		return nil, 0, err
	}
	defer entry.Close()
	file, ok := entry.(kfs.File)
	if !ok {
		return nil, 0, backup.ErrInvalid
	}
	reader, err := file.Open(ctx)
	if err != nil {
		return nil, 0, &Error{Action: "open saved file", Cause: err}
	}
	return reader, entry.Size(), nil
}
