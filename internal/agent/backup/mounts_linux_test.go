package backup

import (
	"os"
	"path/filepath"
	"slices"
	"syscall"
	"testing"

	"github.com/kopia/kopia/snapshot"
	"github.com/sirrobot01/synod/internal/backup"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
)

func TestWalkStopsAtANestedMount(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("mounting a filesystem needs root")
	}
	source := t.TempDir()
	nested := filepath.Join(source, "share")
	if err := os.Mkdir(nested, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := syscall.Mount("tmpfs", nested, "tmpfs", 0, "size=1m"); err != nil {
		t.Skip("tmpfs mount unavailable:", err)
	}
	t.Cleanup(func() { syscall.Unmount(nested, 0) })
	for name, directory := range map[string]string{"state": source, "secret": nested} {
		if err := os.WriteFile(filepath.Join(directory, name), []byte(name), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	f := newIncrementalFixture(t, source, true)
	tree, err := NewTree(t.Context(), f.app, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tree.Close()
	root, err := tree.Root(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.r.UploadFiles(t.Context(), root, snapshot.SourceInfo{Host: "test", UserName: "test", Path: f.app.Key}, false); err != nil {
		t.Fatal(err)
	}
	if !slices.ContainsFunc(tree.Exclusions(), func(e backup.Exclusion) bool { return e.Source == nested }) {
		t.Fatalf("the walk entered the nested mount: %+v", tree.Exclusions())
	}
	// Selected as its own path, the nested mount is copied as its own root.
	f.app.Paths = append(f.app.Paths, &agentv1.DiscoveredPath{Source: nested, Kind: "mount", Status: "Included"})
	separate, err := NewTree(t.Context(), f.app, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer separate.Close()
	if len(separate.roots) != 2 || len(separate.nested) != 0 {
		t.Fatalf("a root on another filesystem was folded into its parent: %d roots, %d nested", len(separate.roots), len(separate.nested))
	}
}
