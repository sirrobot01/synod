package backup

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"

	kfs "github.com/kopia/kopia/fs"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
)

func TestLiveBackupRestoresOneLargeFileVersion(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "changing")
	// More than 128 MiB makes Kopia open parallel parts of the same entry.
	const size = 129 << 20
	write := func(version byte) {
		t.Helper()
		f, err := os.Create(path)
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		chunk := bytes.Repeat([]byte{version}, 1<<20)
		for range size / len(chunk) {
			if _, err := f.Write(chunk); err != nil {
				t.Fatal(err)
			}
		}
	}
	write(1)
	var attempts atomic.Int32
	afterLiveRead = func(string) { write(byte(attempts.Add(1) + 1)) }
	t.Cleanup(func() { afterLiveRead = func(string) {} })
	f := newIncrementalFixture(t, directory, true)
	restored := f.backup("large-live-copy")
	if got := attempts.Load(); got != stableReadAttempts {
		t.Fatalf("got %d captures, want %d shared across all parts", got, stableReadAttempts)
	}
	file, err := os.Open(filepath.Join(restored, "changing"))
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	actual, expected := sha256.New(), sha256.New()
	count, err := io.Copy(actual, file)
	if err != nil || count != size {
		t.Fatalf("restored %d bytes, want %d: %v", count, size, err)
	}
	chunk := bytes.Repeat([]byte{stableReadAttempts}, 1<<20)
	for range size / len(chunk) {
		expected.Write(chunk)
	}
	if !bytes.Equal(actual.Sum(nil), expected.Sum(nil)) {
		t.Fatal("restored file mixes versions or misses data")
	}
}

func TestLiveFileReleasesStaging(t *testing.T) {
	for _, stop := range []string{"entry", "tree", "cancel", "staging-error"} {
		t.Run(stop, func(t *testing.T) {
			directory := t.TempDir()
			path := filepath.Join(directory, "large")
			if err := os.WriteFile(path, bytes.Repeat([]byte("L"), stableReadLimit+1), 0600); err != nil {
				t.Fatal(err)
			}
			tree, err := NewTree(t.Context(), &agentv1.DiscoveredApp{Paths: []*agentv1.DiscoveredPath{{Source: path, Kind: "bind", Status: "Included"}}}, nil, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer tree.Close()
			tree.live = true
			root, err := tree.Root(t.Context(), nil)
			if err != nil {
				t.Fatal(err)
			}
			paths, err := root.Child(t.Context(), "paths")
			if err != nil {
				t.Fatal(err)
			}
			entries, err := kfs.GetAllEntries(t.Context(), paths.(kfs.Directory))
			if err != nil || len(entries) != 1 {
				t.Fatal("missing source entry", err)
			}
			file := entries[0].(*liveFile)
			defer file.Close()
			staging := t.TempDir()
			t.Setenv("TMPDIR", staging)
			if stop == "staging-error" {
				t.Setenv("TMPDIR", filepath.Join(staging, "missing"))
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if stop == "cancel" {
				afterLiveRead = func(string) { cancel() }
				t.Cleanup(func() { afterLiveRead = func(string) {} })
			}
			reader, err := file.Open(ctx)
			if stop == "cancel" || stop == "staging-error" {
				if reader != nil || err == nil || (stop == "cancel" && !errors.Is(err, context.Canceled)) {
					t.Fatalf("capture did not fail: reader %v, error %v", reader, err)
				}
				if len(tree.staged) != 0 {
					t.Fatal("failed capture kept staging open")
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				if len(tree.staged) != 1 {
					t.Fatal("large file was not staged")
				}
				// Separate readers have independent cursors and the same captured bytes.
				var readers sync.WaitGroup
				for range 3 {
					readers.Go(func() {
						r, err := file.Open(ctx)
						if err != nil {
							t.Error(err)
							return
						}
						defer r.Close()
						if n, err := io.Copy(io.Discard, r); err != nil || n != stableReadLimit+1 {
							t.Errorf("read %d bytes: %v", n, err)
						}
					})
				}
				readers.Wait()
				reader.Close()
				if stop == "entry" {
					file.Close()
					file.Close()
				} else {
					tree.Close()
				}
				if _, err := file.copy.staging.Stat(); !errors.Is(err, os.ErrClosed) || len(tree.staged) != 0 {
					t.Fatalf("staging remains open: %v", err)
				}
			}
			left, err := os.ReadDir(staging)
			if err != nil || len(left) != 0 {
				t.Fatalf("plaintext remains in staging: %v, %v", left, err)
			}
		})
	}
}
