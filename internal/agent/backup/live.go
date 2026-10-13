package backup

import (
	"context"
	"io"
	"maps"
	"os"
	"slices"
	"sync"
	"time"

	kfs "github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/snapshot"
	"github.com/sirrobot01/synod/internal/backup"
	"github.com/sirrobot01/synod/internal/repository"
)

// liveCopyPasses bounds the copy passes of a live backup. A file that still changes
// during the last pass is saved as read and reported by Changing.
const liveCopyPasses = 3

// afterLiveRead runs when a live reader closes, before its status check. Tests change files there.
var afterLiveRead = func(string) {}

// liveFile reads a file in place while the app runs. Each reader compares the file's size
// and time at close with the values at open. A change marks the file, and copyLive reads it
// again in another pass. Nothing is staged, so a copy costs no extra disk or memory.
// An SQLite file uses its WAL time so that a new commit prevents incremental reuse,
// and its content comes from the SQLite backup API.
type liveFile struct {
	sourceEntry
	modified time.Time
	mu       sync.Mutex
	checked  bool
	sqlite   *fileCopy
}

type fileCopy struct {
	io.ReaderAt
	size int64
}

func (f *liveFile) ModTime() time.Time { return f.modified }

func (f *liveFile) Open(ctx context.Context) (kfs.Reader, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f.mu.Lock()
	if !f.checked {
		copied, err := f.capture(ctx)
		if err != nil {
			f.mu.Unlock()
			return nil, err
		}
		f.sqlite, f.checked = copied, true
	}
	copied := f.sqlite
	f.mu.Unlock()
	if copied != nil {
		return &copyReader{io.NewSectionReader(copied, 0, copied.size), f}, nil
	}
	source, err := (&sourceFile{f.sourceEntry}).open(ctx)
	if err != nil {
		return nil, err
	}
	opened, err := source.Stat()
	if err != nil {
		source.Close()
		return nil, err
	}
	return &liveReader{sourceReader: source, opened: opened}, nil
}

// liveReader reads the source file directly and checks it for changes when it closes.
type liveReader struct {
	*sourceReader
	opened os.FileInfo
	closed bool
}

// Entry reports the file as it was at open, so a later pass sees a change as new metadata.
func (r *liveReader) Entry() (kfs.Entry, error) {
	entry := r.entry
	entry.FileInfo = r.opened
	return &sourceFile{entry}, nil
}

func (r *liveReader) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true
	afterLiveRead(r.entry.absolute)
	current, err := r.File.Stat()
	if err != nil || current.Size() != r.opened.Size() || !current.ModTime().Equal(r.opened.ModTime()) {
		r.entry.tree.noteRetry(r.entry.absolute)
	}
	return r.File.Close()
}

type copyReader struct {
	*io.SectionReader
	entry *liveFile
}

func (*copyReader) Close() error                { return nil }
func (r *copyReader) Entry() (kfs.Entry, error) { return r.entry, nil }

func (t *Tree) noteRetry(path string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.retry[path] = struct{}{}
}

// takeRetries returns the files that changed during the last pass and clears the list.
func (t *Tree) takeRetries() []string {
	t.mu.Lock()
	defer t.mu.Unlock()
	paths := slices.Collect(maps.Keys(t.retry))
	clear(t.retry)
	return paths
}

func (t *Tree) noteChanging(paths []string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, path := range paths {
		t.changing[path] = struct{}{}
	}
}

// copyLive copies a live tree in up to liveCopyPasses passes. A later pass reads only the
// files that changed during the pass before it. The pass without changes, or the last
// pass, publishes the snapshot that finish describes in its own write session, and applies
// retention there, so the repository writes its index once. Files that changed during every
// pass are saved as read and reported by Changing.
func (t *Tree) copyLive(ctx context.Context, r *repository.Repository, root func() (kfs.Directory, error), source snapshot.SourceInfo, previous *snapshot.Manifest, retention *backup.Retention, finish func(*snapshot.Manifest) (backup.Snapshot, error)) (repository.Upload, error) {
	for pass := 1; ; pass++ {
		directory, err := root()
		if err != nil {
			return repository.Upload{}, err
		}
		result, err := r.UploadSnapshot(ctx, directory, source, previous, retention, func(prepared *snapshot.Manifest) (backup.Snapshot, bool, error) {
			changed := t.takeRetries()
			if len(changed) > 0 && pass < liveCopyPasses {
				return backup.Snapshot{}, false, nil
			}
			t.noteChanging(changed)
			metadata, err := finish(prepared)
			return metadata, err == nil, err
		})
		if err != nil || result.Snapshot != nil {
			return result, err
		}
		previous = result.Files
	}
}
