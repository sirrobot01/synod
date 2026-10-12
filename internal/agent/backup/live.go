package backup

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"sync"
	"time"

	kfs "github.com/kopia/kopia/fs"
)

const (
	stableReadLimit    = 8 << 20
	stableReadAttempts = 3
)

// afterLiveRead runs before the second status check. Tests change files there.
var afterLiveRead = func(string) {}

// liveFile shares one captured version across all of Kopia's parallel file parts.
// An SQLite file uses its WAL time so that a new commit prevents incremental reuse.
type liveFile struct {
	sourceEntry
	modified time.Time
	mu       sync.Mutex
	copy     *fileCopy
	readers  int
}

type fileCopy struct {
	io.ReaderAt
	size    int64
	staging *os.File
}

func (f *liveFile) ModTime() time.Time { return f.modified }

func (f *liveFile) Open(ctx context.Context) (kfs.Reader, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.copy == nil {
		copied, err := f.capture(ctx)
		if err != nil {
			return nil, err
		}
		f.copy = copied
	}
	f.readers++
	return &copyReader{SectionReader: io.NewSectionReader(f.copy, 0, f.copy.size), entry: f}, nil
}

// Close releases staging after all parts finish. Tree.Close also cleans up on failure.
func (f *liveFile) Close() {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.copy != nil && f.copy.staging != nil {
		f.tree.mu.Lock()
		defer f.tree.mu.Unlock()
		f.copy.staging.Close()
		delete(f.tree.staged, f.copy.staging)
	}
}

type copyReader struct {
	*io.SectionReader
	entry  *liveFile
	closed bool
}

// Close frees an in-memory copy after its last reader. Kopia reads a small file once,
// so the copies of a large directory do not stay in memory until the walk ends.
// A staged copy stays until the entry closes.
func (r *copyReader) Close() error {
	f := r.entry
	f.mu.Lock()
	defer f.mu.Unlock()
	if r.closed {
		return nil
	}
	r.closed = true
	f.readers--
	if f.readers == 0 && f.copy != nil && f.copy.staging == nil {
		f.copy = nil
	}
	return nil
}

func (r *copyReader) Entry() (kfs.Entry, error) { return r.entry, nil }

// stable retries changed files before upload. Large files use private disk staging.
// A file that changes on every attempt is saved as read and reported by Changing.
// This check does not establish consistency between files in an app.
func (t *Tree) stable(ctx context.Context, r *sourceReader) (*fileCopy, error) {
	defer r.Close()
	var staging *os.File
	defer func() {
		if staging != nil {
			staging.Close()
		}
	}()
	for attempt := 1; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		before, err := r.Stat()
		if err != nil {
			return nil, err
		}
		// Bound each attempt to the starting size even when writers keep appending.
		input := io.NewSectionReader(r, 0, before.Size())
		copied := &fileCopy{}
		if before.Size() <= stableReadLimit {
			data, err := io.ReadAll(input)
			if err != nil {
				return nil, err
			}
			copied.ReaderAt, copied.size = bytes.NewReader(data), int64(len(data))
		} else {
			if staging == nil {
				staging, err = os.CreateTemp("", "synod-live-")
				if err != nil {
					return nil, err
				}
				// No plaintext copy remains after a crash.
				if err := os.Remove(staging.Name()); err != nil {
					return nil, err
				}
			} else {
				if err := staging.Truncate(0); err != nil {
					return nil, err
				}
				if _, err := staging.Seek(0, io.SeekStart); err != nil {
					return nil, err
				}
			}
			for copied.size < before.Size() {
				if err := ctx.Err(); err != nil {
					return nil, err
				}
				n, err := io.CopyN(staging, input, min(1<<20, before.Size()-copied.size))
				copied.size += n
				if errors.Is(err, io.EOF) {
					break // A shrink is a change. Retry it below.
				}
				if err != nil {
					return nil, err
				}
			}
			copied.ReaderAt, copied.staging = staging, staging
		}
		afterLiveRead(r.entry.absolute)
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		after, err := r.Stat()
		if err != nil {
			return nil, err
		}
		same := copied.size == after.Size() && before.Size() == after.Size() && before.ModTime().Equal(after.ModTime())
		if !same && attempt < stableReadAttempts {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(20 * time.Millisecond):
			}
			continue
		}
		if !same {
			t.noteChanging(r.entry.absolute)
		}
		if copied.staging != nil {
			t.mu.Lock()
			if t.staged == nil {
				t.staged = make(map[*os.File]struct{})
			}
			t.staged[staging] = struct{}{}
			t.mu.Unlock()
			staging = nil // The entry and tree now own it.
		}
		return copied, nil
	}
}
