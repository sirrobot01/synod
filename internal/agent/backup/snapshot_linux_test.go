package backup

import (
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	kfs "github.com/kopia/kopia/fs"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
)

// A live copy on btrfs reads one moment from a snapshot, keeps a root with a nested
// subvolume live, and removes its snapshots. Set SYNOD_TEST_BTRFS to an empty directory
// at the top of a btrfs mount. The test needs root and btrfs-progs.
func TestLiveCopyReadsBtrfsSnapshot(t *testing.T) {
	top := os.Getenv("SYNOD_TEST_BTRFS")
	if top == "" {
		t.Skip("set SYNOD_TEST_BTRFS to the top directory of a btrfs mount")
	}
	btrfs := func(arguments ...string) {
		t.Helper()
		if output, err := exec.Command("btrfs", arguments...).CombinedOutput(); err != nil {
			t.Fatalf("btrfs %v: %v: %s", arguments, err, output)
		}
	}
	volume := filepath.Join(top, "app")
	btrfs("subvolume", "create", volume)
	t.Cleanup(func() { exec.Command("btrfs", "subvolume", "delete", volume).Run() })
	if err := os.MkdirAll(filepath.Join(volume, "data"), 0o700); err != nil {
		t.Fatal(err)
	}
	state := filepath.Join(volume, "data", "state")
	if err := os.WriteFile(state, []byte("at-snapshot"), 0o600); err != nil {
		t.Fatal(err)
	}
	nested := filepath.Join(top, "nested")
	btrfs("subvolume", "create", nested)
	btrfs("subvolume", "create", filepath.Join(nested, "inner"))
	t.Cleanup(func() {
		exec.Command("btrfs", "subvolume", "delete", filepath.Join(nested, "inner")).Run()
		exec.Command("btrfs", "subvolume", "delete", nested).Run()
	})

	app := &agentv1.DiscoveredApp{ProjectDirectory: volume, Paths: []*agentv1.DiscoveredPath{
		{Source: filepath.Join(volume, "data"), Kind: "bind", Status: "Included"},
		{Source: nested, Kind: "bind", Status: "Included"},
	}}
	tree, err := NewTree(t.Context(), app, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	tree.live = true
	snapshots, err := tree.useSnapshots(t.Context(), "test")
	if err != nil {
		t.Fatal(err)
	}
	for _, root := range tree.roots {
		if want := root.source != nested; root.snapshot != want {
			t.Fatalf("%s: snapshot = %t, want %t", root.source, root.snapshot, want)
		}
	}
	// A write after the snapshot is not in the copy.
	if err := os.WriteFile(state, []byte("after-snapshot"), 0o600); err != nil {
		t.Fatal(err)
	}
	root, err := tree.Root(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	paths, err := root.Child(t.Context(), "paths")
	if err != nil {
		t.Fatal(err)
	}
	entries, err := kfs.GetAllEntries(t.Context(), paths.(kfs.Directory))
	if err != nil {
		t.Fatal(err)
	}
	var data []byte
	for _, entry := range entries {
		if file, err := entry.(kfs.Directory).Child(t.Context(), "state"); err == nil {
			reader, err := file.(kfs.File).Open(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			data, err = io.ReadAll(reader)
			reader.Close()
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	if string(data) != "at-snapshot" {
		t.Fatalf("copy read %q, want the snapshot content", data)
	}
	if len(tree.Changing()) != 0 {
		t.Fatal("a snapshot copy reported changing files")
	}
	tree.Close()
	if err := snapshots.Close(); err != nil {
		t.Fatal(err)
	}
	left, err := os.ReadDir(filepath.Join(top, snapshotDirectory))
	if err != nil || len(left) != 0 {
		t.Fatalf("snapshots remain: %v %v", left, err)
	}

	// An interrupted backup leaves a snapshot. The agent removes it at start.
	btrfs("subvolume", "snapshot", "-r", volume, filepath.Join(top, snapshotDirectory, snapshotPrefix+"interrupted"))
	if err := RemoveStaleSnapshots(t.Context()); err != nil {
		t.Fatal(err)
	}
	if left, _ := os.ReadDir(filepath.Join(top, snapshotDirectory)); len(left) != 0 {
		t.Fatalf("stale snapshots remain: %v", left)
	}
}
