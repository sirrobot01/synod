// Package backup runs app backups on their source device.
package backup

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"syscall"
	"time"

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
	// snapshot is set when root reads a filesystem snapshot. Its files do not change during the copy.
	snapshot bool
}

// nestedPath is an included path that its parent's walk copies. Each pass checks that it still exists.
type nestedPath struct {
	parent   *sourceRoot
	relative string
	mode     os.FileMode
}

var mediaNames = []string{"media", "movies", "tv", "music", "downloads", "videos", "photos", "pictures", "upload", "uploads", "originals"}

const largeFileBytes = 20 << 30

// Tree confines reads to the selected roots and applies exclusions to overlapping paths.
// Close it after both upload passes finish.
type Tree struct {
	live          bool
	sqliteMu      sync.Mutex
	sqliteErr     error
	sqlite        map[string]*os.File
	roots         []*sourceRoot
	nested        []nestedPath
	Paths         []backup.Path
	excluded      map[string]string
	included      []string
	mu            sync.Mutex
	observed      map[string]string
	observedBytes int
	// changing holds files that changed while a live copy read them.
	changing map[string]struct{}
}

// NewTree selects the app's Included paths. include adds Large paths and paths that the walk
// skips by default, such as media folders and files over 20 GiB. Never paths stay excluded.
func NewTree(ctx context.Context, app *agentv1.DiscoveredApp, excluded []backup.Exclusion, include []string) (*Tree, error) {
	if backup.ValidateIncluded(include) != nil {
		return nil, ErrSource
	}
	tree := &Tree{excluded: make(map[string]string), observed: make(map[string]string), changing: make(map[string]struct{})}
	for _, p := range include {
		tree.included = append(tree.included, p)
		if resolved, err := filepath.EvalSymlinks(p); err == nil && resolved != p {
			tree.included = append(tree.included, resolved)
		}
	}
	selected := func(item *agentv1.DiscoveredPath) bool {
		return item.Status == "Included" || (item.Status == "Large" && slices.Contains(include, filepath.Clean(item.Source)))
	}
	ready := false
	defer func() {
		if !ready {
			tree.Close()
		}
	}()
	for _, item := range app.Paths {
		if !selected(item) && filepath.IsAbs(item.Source) {
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
		if !selected(item) {
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
		if err != nil {
			return nil, fmt.Errorf("%w: %s: %w", ErrSource, item.Source, err)
		}
		if discovery.Forbidden(resolved, item.Kind == "volume") {
			return nil, fmt.Errorf("%w: %s is a forbidden path", ErrSource, resolved)
		}
		if tree.skip(item.Source) || tree.skip(resolved) {
			continue
		}
		info, err := os.Lstat(resolved)
		if err != nil {
			return nil, fmt.Errorf("%w: %s: %w", ErrSource, resolved, err)
		}
		if !info.IsDir() && !info.Mode().IsRegular() {
			return nil, fmt.Errorf("%w: %s is not a directory or a regular file", ErrSource, resolved)
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
	tree.nest()
	ready = true
	return tree, nil
}

// nest copies an included path once when it lies inside another included directory.
// A Compose project often mounts ./data, so the project walk already reads that data.
// The path keeps its own mapping into the parent's archive. Restore and file browsing
// resolve such mappings. A path stays separate when the parent walk would skip a part of it.
func (t *Tree) nest() {
	for i := 0; i < len(t.roots); i++ {
		child := t.roots[i]
		for _, parent := range t.roots {
			if parent == child || parent.path != "." {
				continue
			}
			relative, err := filepath.Rel(parent.source, child.source)
			if err != nil || relative == "." || strings.HasPrefix(relative, "..") || child.volume != parent.volume {
				continue
			}
			walked := true
			current := parent.source
			for _, part := range strings.Split(relative, string(filepath.Separator)) {
				current = filepath.Join(current, part)
				name := strings.ToLower(part)
				if t.skip(current) || discovery.Forbidden(current, parent.volume) || (slices.Contains(mediaNames, name) && !t.chosen(current)) || strings.HasSuffix(name, "-wal") || strings.HasSuffix(name, "-shm") || strings.HasSuffix(name, "-journal") {
					walked = false
					break
				}
			}
			info, err := child.root.Lstat(child.path)
			if err != nil || (info.Mode().IsRegular() && info.Size() > largeFileBytes && !t.chosen(child.source)) {
				walked = false
			}
			if !walked {
				continue
			}
			t.nested = append(t.nested, nestedPath{parent: parent, relative: relative, mode: info.Mode().Type()})
			archive := "paths/" + parent.archive + "/" + filepath.ToSlash(relative)
			for j := range t.Paths {
				if t.Paths[j].Archive == "paths/"+child.archive {
					t.Paths[j].Archive = archive
				}
			}
			child.root.Close()
			t.roots = slices.Delete(t.roots, i, i+1)
			i--
			break
		}
	}
}

// Root refreshes source metadata for each pass. Definition files stay in memory.
func (t *Tree) Root(ctx context.Context, definitions map[string][]byte, dumps ...kfs.Entry) (kfs.Directory, error) {
	var entries []kfs.Entry
	for _, root := range t.roots {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if root.path != "." {
			sidecar, err := t.sqliteSidecar(root, root.root, root.path)
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
		var wal os.FileInfo
		if t.live && !root.snapshot && info.Mode().IsRegular() {
			if wal, err = root.root.Lstat(root.path + "-wal"); errors.Is(err, os.ErrNotExist) {
				wal = nil
			} else if err != nil {
				return nil, ErrSource
			}
		}
		entries = append(entries, newSourceEntry(t, root, root.path, root.source, root.archive, info, wal))
	}
	// A missing path would be absent from its parent's walk and from the snapshot.
	for _, nested := range t.nested {
		info, err := nested.parent.root.Lstat(nested.relative)
		if err != nil || info.Mode().Type() != nested.mode {
			return nil, ErrSource
		}
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

// chosen reports whether the user included path or one of its parents.
func (t *Tree) chosen(path string) bool {
	for _, included := range t.included {
		if path == included || strings.HasPrefix(path, included+string(filepath.Separator)) {
			return true
		}
	}
	return false
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

// newSourceEntry takes the -wal sidecar of a live SQLite file, or nil.
func newSourceEntry(tree *Tree, root *sourceRoot, path, absolute, name string, info, wal os.FileInfo) kfs.Entry {
	entry := sourceEntry{FileInfo: info, tree: tree, source: root, path: path, absolute: absolute, name: name}
	switch {
	case info.IsDir():
		return &sourceDirectory{entry}
	case info.Mode().IsRegular():
		if tree.live && !root.snapshot {
			// A WAL commit changes only the -wal file. Its time makes Kopia read the database again.
			if wal != nil && wal.ModTime().After(info.ModTime()) {
				return &liveFile{entry, wal.ModTime()}
			}
			return &liveFile{entry, info.ModTime()}
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
	var children []os.DirEntry
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
		children = append(children, batch...)
		if len(children) > 100000 {
			return nil, ErrSourceLimit
		}
	}
	names := make(map[string]bool, len(children))
	for _, child := range children {
		names[child.Name()] = true
	}
	var entries []kfs.Entry
	for _, child := range children {
		absolute := filepath.Join(d.absolute, child.Name())
		if d.tree.skip(absolute) {
			continue
		}
		reason := ""
		sidecar, err := d.tree.sqliteSidecar(d.source, root, child.Name())
		if err != nil {
			return nil, err
		}
		if sidecar {
			reason = "SQLite sidecars are excluded from live copies."
		}
		if discovery.Forbidden(absolute, d.source.volume) {
			reason = "Host system paths are never copied."
		}
		if slices.Contains(mediaNames, strings.ToLower(child.Name())) && !d.tree.chosen(absolute) {
			reason = "Media is skipped by default."
		}
		info, err := root.Lstat(child.Name())
		if err != nil {
			return nil, err
		}
		if !info.Mode().IsRegular() && !info.IsDir() && info.Mode()&os.ModeSymlink == 0 {
			reason = "This path is not a regular file, directory, or link."
		}
		if info.Mode().IsRegular() && info.Size() > largeFileBytes && !d.tree.chosen(absolute) {
			reason = "This file exceeds 20 GiB and is skipped by default."
		}
		if reason != "" {
			if err := d.tree.exclude(absolute, reason); err != nil {
				return nil, err
			}
			continue
		}
		var wal os.FileInfo
		if d.tree.live && !d.source.snapshot && info.Mode().IsRegular() && names[child.Name()+"-wal"] {
			if wal, err = root.Lstat(child.Name() + "-wal"); err != nil {
				return nil, err
			}
		}
		entries = append(entries, newSourceEntry(d.tree, d.source, filepath.Join(d.path, child.Name()), absolute, child.Name(), info, wal))
	}
	kfs.Sort(entries)
	return kfs.StaticIterator(entries, nil), nil
}

// Changing returns up to maxChanging files that changed while a live copy read them.
func (t *Tree) Changing() []string {
	t.mu.Lock()
	defer t.mu.Unlock()
	paths := slices.Sorted(maps.Keys(t.changing))
	return paths[:min(len(paths), maxChanging)]
}

func (t *Tree) noteChanging(path string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.changing[path] = struct{}{}
}

const (
	// maxChanging bounds the changed-file list in the snapshot metadata.
	maxChanging = 256
	// stableReadLimit bounds the memory of one checked read. Up to 16 uploads run at once.
	stableReadLimit = 8 << 20
	// stableReadAttempts is how many times a live copy reads a small file that keeps changing.
	stableReadAttempts = 3
)

// afterLiveRead runs between a checked read and its second status check. Tests change files there.
var afterLiveRead = func(string) {}

// unchanged reports whether a file kept its size and modification time.
func unchanged(before, after os.FileInfo) bool {
	return before.Size() == after.Size() && before.ModTime().Equal(after.ModTime())
}

// stable makes a live read consistent. A small file is read whole, and read again if it
// changed during the read. A large file streams and is checked when the read ends.
// A file that still changes is saved as read and reported by Changing.
func (t *Tree) stable(r *sourceReader) (kfs.Reader, error) {
	for attempt := 1; ; attempt++ {
		before, err := r.Stat()
		if err != nil {
			r.Close()
			return nil, err
		}
		if before.Size() > stableReadLimit {
			if _, err := r.Seek(0, io.SeekStart); err != nil {
				r.Close()
				return nil, err
			}
			return &watchedReader{sourceReader: r, opened: before}, nil
		}
		data, err := io.ReadAll(io.NewSectionReader(r, 0, stableReadLimit+1))
		if err != nil {
			r.Close()
			return nil, err
		}
		afterLiveRead(r.entry.absolute)
		after, err := r.Stat()
		if err != nil {
			r.Close()
			return nil, err
		}
		same := int64(len(data)) == after.Size() && unchanged(before, after)
		if !same && attempt < stableReadAttempts {
			time.Sleep(20 * time.Millisecond)
			continue
		}
		if !same && len(data) > stableReadLimit {
			// The file grew past the memory bound. Stream it from the start.
			if _, err := r.Seek(0, io.SeekStart); err != nil {
				r.Close()
				return nil, err
			}
			t.noteChanging(r.entry.absolute)
			return &watchedReader{sourceReader: r, opened: after}, nil
		}
		r.Close()
		if !same {
			t.noteChanging(r.entry.absolute)
		}
		entry := r.entry
		entry.FileInfo = after
		return &bufferReader{Reader: bytes.NewReader(data), entry: &sourceFile{entry}}, nil
	}
}

// watchedReader streams a large live file and checks it for changes at the end.
type watchedReader struct {
	*sourceReader
	opened os.FileInfo
}

func (r *watchedReader) Read(p []byte) (int, error) {
	n, err := r.sourceReader.Read(p)
	if errors.Is(err, io.EOF) {
		if now, statErr := r.Stat(); statErr != nil || !unchanged(r.opened, now) {
			r.entry.tree.noteChanging(r.entry.absolute)
		}
	}
	return n, err
}

// bufferReader serves a small file that was read whole.
type bufferReader struct {
	*bytes.Reader
	entry kfs.Entry
}

func (*bufferReader) Close() error                { return nil }
func (r *bufferReader) Entry() (kfs.Entry, error) { return r.entry, nil }

type sourceFile struct{ sourceEntry }

func (f *sourceFile) Open(ctx context.Context) (kfs.Reader, error) {
	reader, err := f.open(ctx)
	if err != nil {
		return nil, err
	}
	if f.tree.live && !f.source.snapshot {
		return f.tree.stable(reader)
	}
	return reader, nil
}

func (f *sourceFile) open(ctx context.Context) (*sourceReader, error) {
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
