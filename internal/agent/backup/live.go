package backup

import (
	"context"
	"io"
	"os"
	"sync"
	"time"

	kfs "github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/snapshot"
	"github.com/sirrobot01/synod/internal/repository"
)

// liveCopyPasses bounds the copy passes of a live backup. A file that still changes
// during the last pass is saved as read and reported by Changing.
const liveCopyPasses = 3

// afterLiveRead runs when a live reader closes, before its status check. Tests change files there.
var afterLiveRead = func(string) {}

// liveFile reads a file in place while the app runs. Each reader compares the file's size
// and time at close with the values at open. A change marks the file, and recopy reads it
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
func (t *Tree) takeRetries() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	n := len(t.retry)
	clear(t.retry)
	return n
}

// recopy copies the files that changed during a live pass again, up to liveCopyPasses passes
// in total. Unchanged files keep their entries from the previous pass and are not read again.
// Files that still change in the last pass are saved as read and reported by Changing.
func (t *Tree) recopy(ctx context.Context, r *repository.Repository, root func() (kfs.Directory, error), source snapshot.SourceInfo, prepared *snapshot.Manifest) (*snapshot.Manifest, error) {
	for pass := 2; pass <= liveCopyPasses; pass++ {
		if t.takeRetries() == 0 {
			return prepared, nil
		}
		directory, err := root()
		if err != nil {
			return nil, err
		}
		if prepared, err = r.UploadFiles(ctx, directory, source, false, prepared); err != nil {
			return nil, err
		}
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	for path := range t.retry {
		t.changing[path] = struct{}{}
	}
	clear(t.retry)
	return prepared, nil
}
