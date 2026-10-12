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
	"sync/atomic"
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
	// snapshot is set when root reads a filesystem snapshot. Its files do not change during the copy.
	snapshot bool
	// capturePoint identifies the filesystem snapshot, including shared mount aliases.
	capturePoint string
	// database holds the data directory of a detected database. The copy keeps it only
	// when it reads the same filesystem snapshot as every other root.
	database bool
	item     backup.Path
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
	images        []kfs.Entry
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
	// changing holds files that changed during every live copy pass.
	changing map[string]struct{}
	// retry holds files that changed during the current live copy pass.
	retry map[string]struct{}
	// databaseMissing is set when a database data directory could not be opened.
	databaseMissing bool
	// databaseFiles is set when the copy keeps the database data directories.
	databaseFiles bool
	// skippedData is set when a walk skipped media or a file over 20 GiB.
	skippedData atomic.Bool
}

// NewTree selects the app's Included paths. include adds Large paths and paths that the walk
// skips by default, such as media folders and files over 20 GiB. Never paths stay excluded.
func NewTree(ctx context.Context, app *agentv1.DiscoveredApp, excluded []backup.Exclusion, include []string) (*Tree, error) {
	if backup.ValidateIncluded(include) != nil {
		return nil, ErrSource
	}
	tree := &Tree{excluded: make(map[string]string), observed: make(map[string]string), changing: make(map[string]struct{}), retry: make(map[string]struct{})}
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
		database := item.Status == "Dumped"
		if database {
			tree.openDatabase(item)
			continue
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
		if tree.skip("", item.Source) || tree.skip("", resolved) {
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

// openDatabase adds a candidate root for a database data directory. The native export
// stays the database copy when the directory is not available.
func (t *Tree) openDatabase(item *agentv1.DiscoveredPath) {
	for _, root := range t.roots {
		if root.database && root.item.Source == item.Source {
			return
		}
	}
	resolved, err := filepath.EvalSymlinks(item.Source)
	if err != nil || discovery.Forbidden(item.Source, item.Kind == "volume") || discovery.Forbidden(resolved, item.Kind == "volume") || t.skip(item.Source, item.Source) || t.skip(resolved, resolved) {
		t.databaseMissing = true
		return
	}
	info, err := os.Lstat(resolved)
	if err != nil || !info.IsDir() {
		t.databaseMissing = true
		return
	}
	root, err := os.OpenRoot(resolved)
	if err != nil {
		t.databaseMissing = true
		return
	}
	if current, err := root.Lstat("."); err != nil || !os.SameFile(info, current) {
		root.Close()
		t.databaseMissing = true
		return
	}
	digest := sha256.Sum256([]byte(item.Source))
	archive := fmt.Sprintf("%x", digest[:16])
	t.roots = append(t.roots, &sourceRoot{root: root, path: ".", source: resolved, archive: archive, volume: item.Kind == "volume", database: true, item: backup.Path{Source: item.Source, Archive: "paths/" + archive, Kind: item.Kind}})
}

// keepDatabaseFiles keeps the database roots only when every root reads one filesystem
// snapshot. The database files and the app files then hold the state of one moment, as
// after a power loss, and the database recovers from it as from a crash. Otherwise it
// closes the database roots, and the native exports stay the only database copies.
func (t *Tree) keepDatabaseFiles() bool {
	coherent := !t.databaseMissing && slices.ContainsFunc(t.roots, func(root *sourceRoot) bool { return root.database })
	for _, root := range t.roots {
		if !root.snapshot || root.capturePoint == "" || root.capturePoint != t.roots[0].capturePoint {
			coherent = false
		}
	}
	t.roots = slices.DeleteFunc(t.roots, func(root *sourceRoot) bool {
		if !root.database {
			return false
		}
		if coherent {
			// The directory is now app data. A parent walk can copy it, as for any nested path.
			delete(t.excluded, root.item.Source)
			delete(t.excluded, root.source)
			t.Paths = append(t.Paths, root.item)
			root.database = false
			return false
		}
		root.root.Close()
		return true
	})
	if coherent {
		t.nest()
	}
	t.databaseFiles = coherent
	return coherent
}

// nest copies an included path once when it lies inside another included directory.
// A Compose project often mounts ./data, so the project walk already reads that data.
// The path keeps its own mapping into the parent's archive. Restore and file browsing
// resolve such mappings. A path stays separate when the parent walk would skip a part of it.
func (t *Tree) nest() {
	for i := 0; i < len(t.roots); i++ {
		child := t.roots[i]
		for _, parent := range t.roots {
			if parent == child || parent.path != "." || parent.database || child.database {
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
				if t.skip("", current) || discovery.Forbidden(current, parent.volume) || (slices.Contains(mediaNames, name) && !t.chosen(current)) || strings.HasSuffix(name, "-wal") || strings.HasSuffix(name, "-shm") || strings.HasSuffix(name, "-journal") {
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
		// A database root is copied only after keepDatabaseFiles accepts it.
		if root.database {
			continue
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
	return virtualfs.NewStaticDirectory("app", []kfs.Entry{virtualfs.NewStaticDirectory("paths", entries), virtualfs.NewStaticDirectory("definition", files), virtualfs.NewStaticDirectory("databases", dumps), virtualfs.NewStaticDirectory("images", t.images)}), nil
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

// skip reports whether path is excluded. An exclusion of root or of a parent of root does
// not apply below root. A database root is excluded from the other walks, not from its own.
func (t *Tree) skip(root, path string) bool {
	for excluded := range t.excluded {
		if root != "" && (root == excluded || strings.HasPrefix(root, excluded+string(filepath.Separator))) {
			continue
		}
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
				return &liveFile{sourceEntry: entry, modified: wal.ModTime()}
			}
			return &liveFile{sourceEntry: entry, modified: info.ModTime()}
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
	// Most app directories cannot hold a host system path, so they skip the check per child.
	hostBelow := discovery.ReachesForbidden(d.absolute)
	names := make(map[string]bool, len(children))
	for _, child := range children {
		names[child.Name()] = true
	}
	var entries []kfs.Entry
	for _, child := range children {
		absolute := filepath.Join(d.absolute, child.Name())
		if d.tree.skip(d.source.source, absolute) {
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
		if strings.HasSuffix(child.Name(), ".sock") || (hostBelow && discovery.Forbidden(absolute, d.source.volume)) {
			reason = "Host system paths are never copied."
		}
		if slices.Contains(mediaNames, strings.ToLower(child.Name())) && !d.tree.chosen(absolute) {
			reason = "Media is skipped by default."
			d.tree.skippedData.Store(true)
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
			d.tree.skippedData.Store(true)
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

// Changing returns up to maxChanging files that changed during every live copy pass.
func (t *Tree) Changing() []string {
	t.mu.Lock()
	defer t.mu.Unlock()
	paths := slices.Sorted(maps.Keys(t.changing))
	return paths[:min(len(paths), maxChanging)]
}

// maxChanging bounds the changed-file list in the snapshot metadata.
const maxChanging = 256

type sourceFile struct{ sourceEntry }

func (f *sourceFile) Open(ctx context.Context) (kfs.Reader, error) {
	return f.open(ctx)
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
