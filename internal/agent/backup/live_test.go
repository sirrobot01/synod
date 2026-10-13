package backup

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"github.com/sirrobot01/synod/internal/repository"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	kfs "github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/snapshot"
	"github.com/sirrobot01/synod/internal/backup"
)

// liveCopy copies directory with every live pass and restores the published snapshot.
func liveCopy(t *testing.T, directory string) (string, *Tree) {
	t.Helper()
	f := newIncrementalFixture(t, directory, true)
	tree, err := NewTree(t.Context(), f.app, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(tree.Close)
	tree.live = true
	source := snapshot.SourceInfo{Host: "test", UserName: "test", Path: f.app.Key}
	root := func() (kfs.Directory, error) { return tree.Root(t.Context(), nil) }
	metadata := f.metadata
	metadata.ID, metadata.StartedAt = uuid.New(), time.Now().UTC()
	upload, err := tree.copyLive(t.Context(), f.r, root, source, nil, nil, func(*snapshot.Manifest) (backup.Snapshot, error) {
		metadata.CompletedAt, metadata.Paths = time.Now().UTC(), tree.Paths
		return metadata, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if listed, err := f.r.Snapshot(t.Context(), upload.Snapshot.ID); err != nil || listed.ID != metadata.ID {
		t.Fatal("the copy session did not publish the snapshot", err)
	}
	target := filepath.Join(t.TempDir(), "restore")
	if err := f.r.RestoreFiles(t.Context(), metadata.ID, target); err != nil {
		t.Fatal(err)
	}
	return filepath.Join(target, tree.Paths[0].Archive), tree
}

func TestLiveCopyReadsChangedFilesAgainInLaterPasses(t *testing.T) {
	directory := t.TempDir()
	write := func(name, data string) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(directory, name), []byte(data), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	large := strings.Repeat("L", 9<<20)
	write("steady", "steady-v1")
	write("settles", "settles-v1")
	write("busy", "busy-v1")
	write("grows", "small")
	write("shrinks", large)
	var mu sync.Mutex
	reads := make(map[string]int)
	afterLiveRead = func(path string) {
		mu.Lock()
		defer mu.Unlock()
		name := filepath.Base(path)
		reads[name]++
		switch {
		case name == "settles" && reads[name] == 1:
			write(name, "settles-v2-longer")
		case name == "busy":
			write(name, fmt.Sprintf("busy-v%d-%s", reads[name]+1, strings.Repeat("x", reads[name])))
		case name == "grows" && reads[name] == 1:
			write(name, large)
		case name == "shrinks" && reads[name] == 1:
			write(name, "small")
		}
	}
	t.Cleanup(func() { afterLiveRead = func(string) {} })
	restored, tree := liveCopy(t, directory)
	// The last pass reads busy-v3, then the app writes busy-v4.
	for name, want := range map[string]string{"steady": "steady-v1", "settles": "settles-v2-longer", "busy": "busy-v3-xx", "grows": large, "shrinks": "small"} {
		data, err := os.ReadFile(filepath.Join(restored, name))
		if err != nil || string(data) != want {
			t.Fatalf("%s: restored %d bytes, want %d: %v", name, len(data), len(want), err)
		}
	}
	for name, want := range map[string]int{"steady": 1, "settles": 2, "busy": liveCopyPasses, "grows": 2, "shrinks": 2} {
		if reads[name] != want {
			t.Fatalf("%s: read %d times, want %d", name, reads[name], want)
		}
	}
	resolved, err := filepath.EvalSymlinks(directory)
	if err != nil {
		t.Fatal(err)
	}
	if got := tree.Changing(); !slices.Equal(got, []string{filepath.Join(resolved, "busy")}) {
		t.Fatalf("Changing() = %v", got)
	}
}

func TestLiveCopyRestoresOneVersionOfALargeFileReadInParts(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "changing")
	// More than 128 MiB makes Kopia read parts of the same file in parallel.
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
	var once sync.Once
	// The app rewrites the file while the first pass reads its parts.
	afterLiveRead = func(string) { once.Do(func() { write(2) }) }
	t.Cleanup(func() { afterLiveRead = func(string) {} })
	restored, tree := liveCopy(t, directory)
	file, err := os.Open(filepath.Join(restored, "changing"))
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	actual, expected := sha256.New(), sha256.New()
	if count, err := io.Copy(actual, file); err != nil || count != size {
		t.Fatalf("restored %d bytes, want %d: %v", count, size, err)
	}
	chunk := bytes.Repeat([]byte{2}, 1<<20)
	for range size / len(chunk) {
		expected.Write(chunk)
	}
	if !bytes.Equal(actual.Sum(nil), expected.Sum(nil)) || len(tree.Changing()) != 0 {
		t.Fatalf("restored file mixes versions, or the settled file was reported: %v", tree.Changing())
	}
}

func TestCopySessionAppliesRetention(t *testing.T) {
	directory := t.TempDir()
	if err := os.WriteFile(filepath.Join(directory, "state"), []byte("v1"), 0o600); err != nil {
		t.Fatal(err)
	}
	f := newIncrementalFixture(t, directory, true)
	source := snapshot.SourceInfo{Host: "test", UserName: "test", Path: f.app.Key}
	var ids []uuid.UUID
	for range 2 {
		tree, err := NewTree(t.Context(), f.app, nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		tree.live = true
		metadata := f.metadata
		metadata.ID, metadata.StartedAt = uuid.New(), time.Now().UTC()
		upload, err := tree.copyLive(t.Context(), f.r, func() (kfs.Directory, error) { return tree.Root(t.Context(), nil) }, source, nil, &backup.Retention{}, func(*snapshot.Manifest) (backup.Snapshot, error) {
			metadata.CompletedAt, metadata.Paths = time.Now().UTC(), tree.Paths
			return metadata, nil
		})
		tree.Close()
		if err != nil || upload.RetentionErr != nil {
			t.Fatal(err, upload.RetentionErr)
		}
		ids = append(ids, upload.Snapshot.ID)
		time.Sleep(10 * time.Millisecond)
	}
	// Zero counts keep only the newest snapshot. The second session expired the first.
	if _, err := f.r.Snapshot(t.Context(), ids[0]); !errors.Is(err, repository.ErrSnapshotMissing) {
		t.Fatalf("the older snapshot was kept: %v", err)
	}
	if _, err := f.r.Snapshot(t.Context(), ids[1]); err != nil {
		t.Fatalf("the new snapshot was removed: %v", err)
	}
}
