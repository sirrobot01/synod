package replica

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"syscall"
	"time"
)

// clockSlack covers the coarse kernel clock that stamps modification times.
const clockSlack = 100 * time.Millisecond

// Snapshotter copies the live volumes of an active app into an agent-owned
// snapshot directory. Keep one Snapshotter per app so that it can skip
// unchanged files and rehash only changed blocks.
type Snapshotter struct {
	Live     string
	Snapshot string
	Freezer  Freezer

	copied map[string]copyState
	// dirty holds the blocks written since the last manifest, per path.
	dirty map[string]map[int]bool
	last  map[string]File
}

type copyState struct {
	size     int64
	mtime    time.Time
	copiedAt time.Time
}

// Take copies the live volumes twice while the app runs, then freezes the app,
// copies what changed, and thaws it. The result is the state that a power cut
// would leave. Take returns ErrFreezeTimeout when the app did not freeze in
// time; the app runs again and no manifest is produced.
func (s *Snapshotter) Take(ctx context.Context, epoch, sequence uint64) (*Manifest, error) {
	if s.copied == nil {
		s.copied, s.dirty, s.last = map[string]copyState{}, map[string]map[int]bool{}, map[string]File{}
	}
	live, err := os.OpenRoot(s.Live)
	if err != nil {
		return nil, err
	}
	defer live.Close()
	snapshot, err := os.OpenRoot(s.Snapshot)
	if err != nil {
		return nil, err
	}
	defer snapshot.Close()
	// A second pass picks up a large write, such as a checkpoint, that ran during the first.
	for range 2 {
		if _, err := s.sync(live, snapshot, false); err != nil {
			return nil, err
		}
	}
	if err := s.Freezer.Freeze(ctx); err != nil {
		return nil, err
	}
	frozenAt := time.Now()
	skipped, syncErr := s.sync(live, snapshot, true)
	frozen := time.Since(frozenAt)
	if err := s.Freezer.Thaw(ctx); err != nil {
		return nil, errors.Join(syncErr, err)
	}
	if syncErr != nil {
		return nil, syncErr
	}
	m := &Manifest{Epoch: epoch, Sequence: sequence, CreatedAt: frozenAt.UTC(), Frozen: frozen, Skipped: skipped}
	if err := s.describe(snapshot, m); err != nil {
		return nil, err
	}
	return m, nil
}

// TakeStopped copies all committed files after the caller proves that the app stopped.
func (s *Snapshotter) TakeStopped(ctx context.Context, epoch, sequence uint64) (*Manifest, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.copied, s.dirty, s.last = map[string]copyState{}, map[string]map[int]bool{}, map[string]File{}
	live, err := os.OpenRoot(s.Live)
	if err != nil {
		return nil, err
	}
	defer live.Close()
	snapshot, err := os.OpenRoot(s.Snapshot)
	if err != nil {
		return nil, err
	}
	defer snapshot.Close()
	skipped, err := s.sync(live, snapshot, true)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m := &Manifest{Epoch: epoch, Sequence: sequence, CreatedAt: time.Now().UTC(), Skipped: skipped}
	if err := s.describe(snapshot, m); err != nil {
		return nil, err
	}
	return m, nil
}

// sync makes snapshot match live. While the app runs, entries can vanish
// during the walk; those are skipped. The frozen pass must be exact.
func (s *Snapshotter) sync(live, snapshot *os.Root, exact bool) ([]string, error) {
	present := map[string]bool{".": true}
	var skipped []string
	err := fs.WalkDir(live.FS(), ".", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			if !exact && errors.Is(err, fs.ErrNotExist) {
				return nil
			}
			return err
		}
		if path == "." {
			return nil
		}
		info, err := live.Lstat(path)
		if err != nil {
			if !exact && errors.Is(err, fs.ErrNotExist) {
				return nil
			}
			return err
		}
		switch {
		case info.IsDir():
			err = s.syncDir(snapshot, path, info)
		case info.Mode()&fs.ModeSymlink != 0:
			err = s.syncSymlink(live, snapshot, path, info)
		case info.Mode().IsRegular():
			err = s.syncFile(live, snapshot, path, info)
		default:
			skipped = append(skipped, path)
			return nil
		}
		if !exact && errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("replica: copy %s: %w", path, err)
		}
		present[path] = true
		return nil
	})
	if err != nil {
		return nil, err
	}
	// A stale file in the copy can change its meaning, such as a hot SQLite journal.
	var stale []string
	err = fs.WalkDir(snapshot.FS(), ".", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if present[path] {
			return nil
		}
		stale = append(stale, path)
		if d.IsDir() {
			return fs.SkipDir
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	for _, path := range stale {
		if err := snapshot.RemoveAll(path); err != nil {
			return nil, err
		}
		for p := range s.copied {
			if p == path || isUnder(p, path) {
				delete(s.copied, p)
				delete(s.dirty, p)
			}
		}
	}
	return skipped, nil
}

func (s *Snapshotter) syncDir(snapshot *os.Root, path string, info fs.FileInfo) error {
	if existing, err := snapshot.Lstat(path); err == nil && !existing.IsDir() {
		if err := snapshot.RemoveAll(path); err != nil {
			return err
		}
	}
	if err := snapshot.Mkdir(path, 0o700); err != nil && !errors.Is(err, fs.ErrExist) {
		return err
	}
	return setMetadata(snapshot, path, info)
}

func (s *Snapshotter) syncSymlink(live, snapshot *os.Root, path string, info fs.FileInfo) error {
	target, err := live.Readlink(path)
	if err != nil {
		return err
	}
	if existing, err := snapshot.Readlink(path); err == nil && existing == target {
		return setOwner(snapshot, path, info)
	}
	if err := snapshot.RemoveAll(path); err != nil {
		return err
	}
	if err := snapshot.Symlink(target, path); err != nil {
		return err
	}
	return setOwner(snapshot, path, info)
}

func (s *Snapshotter) syncFile(live, snapshot *os.Root, path string, info fs.FileInfo) error {
	last, ok := s.copied[path]
	if ok && last.size == info.Size() && last.mtime.Equal(info.ModTime()) && info.ModTime().Before(last.copiedAt.Add(-clockSlack)) {
		return nil
	}
	if existing, err := snapshot.Lstat(path); err == nil && !existing.Mode().IsRegular() {
		if err := snapshot.RemoveAll(path); err != nil {
			return err
		}
	}
	copiedAt := time.Now()
	in, err := live.Open(path)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := snapshot.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return err
	}
	defer out.Close()
	dirty := s.dirty[path]
	if dirty == nil {
		dirty = map[int]bool{}
		s.dirty[path] = dirty
	}
	a, b := make([]byte, BlockSize), make([]byte, BlockSize)
	var size int64
	for i := 0; ; i++ {
		n, readErr := io.ReadFull(in, a)
		if n > 0 {
			m, _ := out.ReadAt(b[:n], size)
			if m != n || !bytes.Equal(a[:n], b[:n]) {
				if _, err := out.WriteAt(a[:n], size); err != nil {
					return err
				}
				dirty[i] = true
			}
			size += int64(n)
		}
		if errors.Is(readErr, io.EOF) || errors.Is(readErr, io.ErrUnexpectedEOF) {
			break
		}
		if readErr != nil {
			return readErr
		}
	}
	if err := out.Truncate(size); err != nil {
		return err
	}
	if err := out.Close(); err != nil {
		return err
	}
	if err := setMetadata(snapshot, path, info); err != nil {
		return err
	}
	s.copied[path] = copyState{size: info.Size(), mtime: info.ModTime(), copiedAt: copiedAt}
	return nil
}

// describe builds the manifest from the snapshot. It reuses the previous hash
// of every block that no pass has written since then.
func (s *Snapshotter) describe(snapshot *os.Root, m *Manifest) error {
	next := map[string]File{}
	err := fs.WalkDir(snapshot.FS(), ".", func(path string, d fs.DirEntry, err error) error {
		if err != nil || path == "." {
			return err
		}
		info, err := snapshot.Lstat(path)
		if err != nil {
			return err
		}
		f := File{Path: path, Mode: info.Mode() & (fs.ModePerm | fs.ModeSetuid | fs.ModeSetgid | fs.ModeSticky), ModTime: info.ModTime().UTC()}
		if st, ok := info.Sys().(*syscall.Stat_t); ok {
			f.UID, f.GID = int(st.Uid), int(st.Gid)
		}
		switch {
		case info.IsDir():
			f.Type = TypeDir
		case info.Mode()&fs.ModeSymlink != 0:
			f.Type = TypeSymlink
			if f.Link, err = snapshot.Readlink(path); err != nil {
				return err
			}
		default:
			f.Type, f.Size = TypeFile, info.Size()
			if f.Blocks, err = s.hash(snapshot, path, f.Size); err != nil {
				return err
			}
		}
		m.Files = append(m.Files, f)
		next[path] = f
		return nil
	})
	if err != nil {
		return err
	}
	s.last, s.dirty = next, map[string]map[int]bool{}
	return nil
}

func (s *Snapshotter) hash(snapshot *os.Root, path string, size int64) ([]Hash, error) {
	count := int((size + BlockSize - 1) / BlockSize)
	previous, dirty := s.last[path], s.dirty[path]
	blocks := make([]Hash, count)
	var file *os.File
	defer func() {
		if file != nil {
			file.Close()
		}
	}()
	buf := make([]byte, BlockSize)
	for i := range blocks {
		// The last block changes when the size changes, even without a write.
		reusable := previous.Type == TypeFile && i < len(previous.Blocks) && !dirty[i] &&
			(previous.Size == size || i < min(len(previous.Blocks), count)-1)
		if reusable {
			blocks[i] = previous.Blocks[i]
			continue
		}
		if file == nil {
			var err error
			if file, err = snapshot.Open(path); err != nil {
				return nil, err
			}
		}
		n := blockLen(size, i)
		if _, err := file.ReadAt(buf[:n], int64(i)*BlockSize); err != nil && !errors.Is(err, io.EOF) {
			return nil, err
		}
		blocks[i] = sha256.Sum256(buf[:n])
	}
	return blocks, nil
}

// setMetadata copies mode, owner, and modification time. It does not follow symlinks.
func setMetadata(root *os.Root, path string, info fs.FileInfo) error {
	if err := setOwner(root, path, info); err != nil {
		return err
	}
	if err := root.Chmod(path, info.Mode()&(fs.ModePerm|fs.ModeSetuid|fs.ModeSetgid|fs.ModeSticky)); err != nil {
		return err
	}
	return root.Chtimes(path, info.ModTime(), info.ModTime())
}

// setOwner changes the owner only when it differs, so an unprivileged test
// agent can copy its own files.
func setOwner(root *os.Root, path string, info fs.FileInfo) error {
	want, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return nil
	}
	current, err := root.Lstat(path)
	if err != nil {
		return err
	}
	if have, ok := current.Sys().(*syscall.Stat_t); ok && have.Uid == want.Uid && have.Gid == want.Gid {
		return nil
	}
	return root.Lchown(path, int(want.Uid), int(want.Gid))
}

func isUnder(path, dir string) bool {
	return len(path) > len(dir) && path[len(dir)] == '/' && path[:len(dir)] == dir
}
