// Package backup runs app backups on their source device.
package backup

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"syscall"

	kfs "github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/fs/virtualfs"
	"github.com/sirrobot01/synod/internal/agent/discovery"
	"github.com/sirrobot01/synod/internal/backup"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
)

var ErrSource = errors.New("app source changed or is not readable; check the app paths and try again")
var ErrSourceLimit = errors.New("app source exceeds a scan limit; reduce its included paths")

type sourceRoot struct {
	root                  *os.Root
	path, source, archive string
	volume                bool
}

// Tree confines reads to the selected roots and applies exclusions to overlapping paths.
// Close it after both upload passes finish.
type Tree struct {
	live          bool
	sqliteMu      sync.Mutex
	sqliteErr     error
	sqlite        map[string]*os.File
	roots         []*sourceRoot
	Paths         []backup.Path
	excluded      map[string]string
	mu            sync.Mutex
	observed      map[string]string
	observedBytes int
}

func NewTree(ctx context.Context, app *agentv1.DiscoveredApp, excluded []backup.Exclusion) (*Tree, error) {
	tree := &Tree{excluded: make(map[string]string), observed: make(map[string]string)}
	ready := false
	defer func() {
		if !ready {
			tree.Close()
		}
	}()
	for _, item := range app.Paths {
		if item.Status != "Included" && filepath.IsAbs(item.Source) {
			excluded = append(excluded, backup.Exclusion{Source: filepath.Clean(item.Source), Reason: item.Reason})
		}
	}
	for _, item := range excluded {
		if !filepath.IsAbs(item.Source) {
			return nil, ErrSource
		}
		tree.excluded[filepath.Clean(item.Source)] = item.Reason
		if resolved, err := filepath.EvalSymlinks(item.Source); err == nil {
			tree.excluded[resolved] = item.Reason
		}
	}
	seen := make(map[string]string)
	for _, item := range app.Paths {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if item.Status != "Included" {
			continue
		}
		if archive, ok := seen[item.Source]; ok {
			tree.Paths = append(tree.Paths, backup.Path{Source: item.Source, Archive: archive, Kind: item.Kind})
			continue
		}
		if discovery.Forbidden(item.Source, item.Kind == "volume") {
			return nil, ErrSource
		}
		resolved, err := filepath.EvalSymlinks(item.Source)
		if errors.Is(err, os.ErrNotExist) && item.Kind == "env" && item.Source == filepath.Join(app.ProjectDirectory, ".env") {
			if err := tree.exclude(item.Source, "The optional .env file does not exist."); err != nil {
				return nil, err
			}
			continue
		}
		if err != nil || discovery.Forbidden(resolved, item.Kind == "volume") {
			return nil, ErrSource
		}
		if tree.skip(item.Source) || tree.skip(resolved) {
			continue
		}
		info, err := os.Lstat(resolved)
		if err != nil || (!info.IsDir() && !info.Mode().IsRegular()) {
			return nil, ErrSource
		}
		rootPath, relative := resolved, "."
		if !info.IsDir() {
			rootPath, relative = filepath.Dir(resolved), filepath.Base(resolved)
		}
		root, err := os.OpenRoot(rootPath)
		if err != nil {
			return nil, ErrSource
		}
		current, err := root.Lstat(relative)
		if err != nil || !os.SameFile(info, current) {
			root.Close()
			return nil, ErrSource
		}
		digest := sha256.Sum256([]byte(item.Source))
		archive := fmt.Sprintf("%x", digest[:16])
		tree.roots = append(tree.roots, &sourceRoot{root: root, path: relative, source: resolved, archive: archive, volume: item.Kind == "volume"})
		tree.Paths = append(tree.Paths, backup.Path{Source: item.Source, Archive: "paths/" + archive, Kind: item.Kind})
		seen[item.Source] = "paths/" + archive
	}
	ready = true
	return tree, nil
}

// Root refreshes source metadata for each pass. Definition files stay in memory.
func (t *Tree) Root(ctx context.Context, definitions map[string][]byte, dumps ...kfs.Entry) (kfs.Directory, error) {
	var entries []kfs.Entry
	for _, root := range t.roots {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if root.path != "." {
			sidecar, err := t.sqliteSidecar(root.root, root.path)
			if err != nil {
				return nil, err
			}
			if sidecar {
				if err := t.exclude(root.source, "SQLite sidecars are excluded from live copies."); err != nil {
					return nil, err
				}
				t.Paths = slices.DeleteFunc(t.Paths, func(path backup.Path) bool { return path.Archive == "paths/"+root.archive })
				continue
			}
		}
		info, err := root.root.Lstat(root.path)
		if err != nil || info.Mode()&os.ModeSymlink != 0 {
			return nil, ErrSource
		}
		entries = append(entries, newSourceEntry(t, root, root.path, root.source, root.archive, info))
	}
	var files []kfs.Entry
	for name, data := range definitions {
		if name == "" || name == "." || name == ".." || strings.ContainsAny(name, "/\\\x00") {
			return nil, ErrSource
		}
		files = append(files, &definitionFile{virtualfs.StreamingFileFromReader(name, io.NopCloser(bytes.NewReader(data)))})
	}
	kfs.Sort(files)
	return virtualfs.NewStaticDirectory("app", []kfs.Entry{virtualfs.NewStaticDirectory("paths", entries), virtualfs.NewStaticDirectory("definition", files), virtualfs.NewStaticDirectory("databases", dumps)}), nil
}

type definitionFile struct{ kfs.StreamingFile }

func (*definitionFile) Mode() os.FileMode { return 0600 }

func (t *Tree) Close() {
	t.sqliteMu.Lock()
	defer t.sqliteMu.Unlock()
	for _, file := range t.sqlite {
		file.Close()
	}
	for _, root := range t.roots {
		root.root.Close()
	}
}

func (t *Tree) Exclusions() []backup.Exclusion {
	t.mu.Lock()
	defer t.mu.Unlock()
	result := make([]backup.Exclusion, 0, len(t.excluded)+len(t.observed))
	for path, reason := range t.excluded {
		result = append(result, backup.Exclusion{Source: path, Reason: reason})
	}
	for path, reason := range t.observed {
		if _, ok := t.excluded[path]; !ok {
			result = append(result, backup.Exclusion{Source: path, Reason: reason})
		}
	}
	slices.SortFunc(result, func(a, b backup.Exclusion) int { return strings.Compare(a.Source, b.Source) })
	return result
}

func (t *Tree) skip(path string) bool {
	for excluded := range t.excluded {
		if path == excluded || strings.HasPrefix(path, excluded+string(filepath.Separator)) {
			return true
		}
	}
	return false
}

func (t *Tree) exclude(path, reason string) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if old, ok := t.observed[path]; ok {
		t.observedBytes -= len(path) + len(old) + 80
	}
	if len(t.observed) >= 4096 || t.observedBytes+len(path)+len(reason)+80 > 512<<10 {
		return ErrSourceLimit
	}
	t.observedBytes += len(path) + len(reason) + 80
	t.observed[path] = reason
	return nil
}

type sourceEntry struct {
	os.FileInfo
	tree                 *Tree
	source               *sourceRoot
	path, absolute, name string
}

func (e sourceEntry) Name() string { return e.name }
func (e sourceEntry) Owner() kfs.OwnerInfo {
	s := e.Sys().(*syscall.Stat_t)
	return kfs.OwnerInfo{UserID: s.Uid, GroupID: s.Gid}
}
func (e sourceEntry) Device() kfs.DeviceInfo {
	s := e.Sys().(*syscall.Stat_t)
	return kfs.DeviceInfo{Dev: uint64(s.Dev), Rdev: uint64(s.Rdev)}
}
func (e sourceEntry) LocalFilesystemPath() string { return "" }
func (e sourceEntry) Close()                      {}

func newSourceEntry(tree *Tree, root *sourceRoot, path, absolute, name string, info os.FileInfo) kfs.Entry {
	entry := sourceEntry{FileInfo: info, tree: tree, source: root, path: path, absolute: absolute, name: name}
	switch {
	case info.IsDir():
		return &sourceDirectory{entry}
	case info.Mode().IsRegular():
		if tree.live {
			return &liveFile{entry}
		}
		return &sourceFile{entry}
	default:
		return &sourceLink{entry}
	}
}

// openDirectory pins each parent before it opens the next. It does not traverse links.
func openDirectory(root *os.Root, path string) (*os.Root, error) {
	current, err := root.OpenRoot(".")
	if err != nil {
		return nil, err
	}
	if path == "." {
		return current, nil
	}
	for part := range strings.SplitSeq(path, string(filepath.Separator)) {
		if part == "" || part == "." || part == ".." {
			current.Close()
			return nil, ErrSource
		}
		info, err := current.Lstat(part)
		if err != nil || !info.IsDir() {
			current.Close()
			return nil, ErrSource
		}
		next, err := current.OpenRoot(part)
		current.Close()
		if err != nil {
			return nil, err
		}
		opened, err := next.Stat(".")
		if err != nil || !os.SameFile(info, opened) {
			next.Close()
			return nil, ErrSource
		}
		current = next
	}
	return current, nil
}

type sourceDirectory struct{ sourceEntry }

func (*sourceDirectory) SupportsMultipleIterations() bool { return true }
func (d *sourceDirectory) Child(ctx context.Context, name string) (kfs.Entry, error) {
	return kfs.IterateEntriesAndFindChild(ctx, d, name)
}
func (d *sourceDirectory) Iterate(ctx context.Context) (kfs.DirectoryIterator, error) {
	root, err := openDirectory(d.source.root, d.path)
	if err != nil {
		return nil, err
	}
	defer root.Close()
	file, err := root.Open(".")
	if err != nil {
		return nil, err
	}
	defer file.Close()
	var entries []kfs.Entry
	count := 0
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		batch, err := file.ReadDir(256)
		if err != nil && !errors.Is(err, io.EOF) {
			return nil, err
		}
		if len(batch) == 0 {
			break
		}
		count += len(batch)
		if count > 100000 {
			return nil, ErrSourceLimit
		}
		for _, child := range batch {
			absolute := filepath.Join(d.absolute, child.Name())
			if d.tree.skip(absolute) {
				continue
			}
			reason := ""
			sidecar, err := d.tree.sqliteSidecar(root, child.Name())
			if err != nil {
				return nil, err
			}
			if sidecar {
				reason = "SQLite sidecars are excluded from live copies."
			}
			if discovery.Forbidden(absolute, d.source.volume) {
				reason = "Host system paths are never copied."
			}
			if slices.Contains([]string{"media", "movies", "tv", "music", "downloads", "videos", "photos", "pictures", "upload", "uploads", "originals"}, strings.ToLower(child.Name())) {
				reason = "Media is skipped by default."
			}
			info, err := root.Lstat(child.Name())
			if err != nil {
				return nil, err
			}
			if !info.Mode().IsRegular() && !info.IsDir() && info.Mode()&os.ModeSymlink == 0 {
				reason = "This path is not a regular file, directory, or link."
			}
			if info.Mode().IsRegular() && info.Size() > 20<<30 {
				reason = "This file exceeds 20 GiB and is skipped by default."
			}
			if reason != "" {
				if err := d.tree.exclude(absolute, reason); err != nil {
					return nil, err
				}
				continue
			}
			entries = append(entries, newSourceEntry(d.tree, d.source, filepath.Join(d.path, child.Name()), absolute, child.Name(), info))
		}
	}
	kfs.Sort(entries)
	return kfs.StaticIterator(entries, nil), nil
}

type sourceFile struct{ sourceEntry }

func (f *sourceFile) Open(ctx context.Context) (kfs.Reader, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	parent, err := openDirectory(f.source.root, filepath.Dir(f.path))
	if err != nil {
		return nil, err
	}
	defer parent.Close()
	file, err := parent.OpenFile(filepath.Base(f.path), os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if err != nil {
		return nil, err
	}
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || !os.SameFile(info, f.FileInfo) {
		file.Close()
		return nil, ErrSource
	}
	return &sourceReader{File: file, entry: f.sourceEntry}, nil
}

type sourceReader struct {
	*os.File
	entry sourceEntry
}

func (r *sourceReader) Entry() (kfs.Entry, error) {
	info, err := r.Stat()
	if err != nil {
		return nil, err
	}
	entry := r.entry
	entry.FileInfo = info
	return &sourceFile{entry}, nil
}

type sourceLink struct{ sourceEntry }

func (l *sourceLink) Readlink(ctx context.Context) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	parent, err := openDirectory(l.source.root, filepath.Dir(l.path))
	if err != nil {
		return "", err
	}
	defer parent.Close()
	return parent.Readlink(filepath.Base(l.path))
}
func (*sourceLink) Resolve(context.Context) (kfs.Entry, error) { return nil, ErrSource }
