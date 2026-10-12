package backup

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	kfs "github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/snapshot"
)

// copyWithPrevious takes one snapshot that may reuse directories of previous.
func copyWithPrevious(t *testing.T, f *incrementalFixture, previous *snapshot.Manifest) (*snapshot.Manifest, string) {
	t.Helper()
	tree, err := NewTree(t.Context(), f.app, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tree.Close()
	if previous != nil {
		stored, err := f.r.PreparedEntry(t.Context(), previous, ".")
		if err != nil {
			t.Fatal(err)
		}
		tree.previous = stored.(kfs.Directory)
	}
	root, err := tree.Root(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := f.r.UploadFiles(t.Context(), root, snapshot.SourceInfo{Host: "test", UserName: "test", Path: f.app.Key}, false, previous)
	if err != nil {
		t.Fatal(err)
	}
	metadata := f.metadata
	metadata.ID, metadata.StartedAt, metadata.CompletedAt, metadata.Paths = uuid.New(), time.Now().UTC(), time.Now().UTC(), tree.Paths
	if _, err := f.r.PublishSnapshot(t.Context(), prepared, metadata); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(t.TempDir(), "restore")
	if err := f.r.RestoreFiles(t.Context(), metadata.ID, target); err != nil {
		t.Fatal(err)
	}
	return prepared, filepath.Join(target, tree.Paths[0].Archive)
}

func TestUnchangedDirectoriesReuseThePreviousSnapshot(t *testing.T) {
	source := t.TempDir()
	write := func(name, data string) {
		t.Helper()
		if err := os.MkdirAll(filepath.Dir(filepath.Join(source, name)), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(source, name), []byte(data), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	write("top", "top-v1")
	write("deep/nested/state", "state-v1")
	f := newIncrementalFixture(t, source, false)
	first, _ := copyWithPrevious(t, f, nil)

	second, restored := copyWithPrevious(t, f, first)
	// Kopia visits no file of a reused directory.
	if second.Stats.CachedFiles+second.Stats.NonCachedFiles != 0 {
		t.Fatalf("an unchanged tree was walked again: %+v", second.Stats)
	}
	if data, err := os.ReadFile(filepath.Join(restored, "deep", "nested", "state")); err != nil || string(data) != "state-v1" {
		t.Fatalf("reused tree restored %q, %v", data, err)
	}

	for _, change := range []struct {
		name  string
		apply func()
		check string
		want  string
	}{
		{"deep file", func() { time.Sleep(10 * time.Millisecond); write("deep/nested/state", "state-v2") }, "deep/nested/state", "state-v2"},
		{"mode", func() { os.Chmod(filepath.Join(source, "top"), 0o640) }, "top", "top-v1"},
		{"new file", func() { write("deep/added", "added") }, "deep/added", "added"},
		{"link", func() { os.Symlink("top", filepath.Join(source, "link")) }, "top", "top-v1"},
	} {
		previous := second
		change.apply()
		var restored string
		second, restored = copyWithPrevious(t, f, previous)
		if second.Stats.CachedFiles+second.Stats.NonCachedFiles == 0 {
			t.Fatalf("%s: a changed tree reused the previous snapshot", change.name)
		}
		if data, err := os.ReadFile(filepath.Join(restored, change.check)); err != nil || string(data) != change.want {
			t.Fatalf("%s: restored %q, %v", change.name, data, err)
		}
	}
}
