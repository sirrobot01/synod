package backup

import (
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/kopia/kopia/snapshot"
	"github.com/sirrobot01/synod/internal/backup"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
)

// databaseApp has a project directory, a separate database directory, and a database
// directory inside the project, as a Compose ./data/db bind mount.
func databaseApp(t *testing.T) (*agentv1.DiscoveredApp, string, string) {
	project, separate := t.TempDir(), t.TempDir()
	nested := filepath.Join(project, "data", "db")
	for _, directory := range []string{nested, separate} {
		if err := os.MkdirAll(directory, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(directory, "PG_VERSION"), []byte(directory), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(project, "compose.yaml"), []byte("services: {}"), 0o600); err != nil {
		t.Fatal(err)
	}
	app := &agentv1.DiscoveredApp{Key: "compose:db", Name: "db", Kind: "compose", ProjectDirectory: project, Paths: []*agentv1.DiscoveredPath{
		{Source: project, Kind: "project", Status: "Included"},
		{Source: nested, Kind: "project", Status: "Dumped", ContainerId: "nested"},
		{Source: separate, Kind: "bind", Status: "Dumped", ContainerId: "separate"},
	}}
	return app, nested, separate
}

func snapshotAll(tree *Tree, points ...string) {
	for i, root := range tree.roots {
		root.snapshot, root.capturePoint = true, points[min(i, len(points)-1)]
	}
}

func TestDatabaseFilesNeedOneSnapshotOfTheWholeApp(t *testing.T) {
	dumps := []backup.DatabaseDump{{ContainerID: "nested", Database: "postgres"}, {ContainerID: "separate", Database: "postgres"}}
	for _, test := range []struct {
		name     string
		points   []string
		coherent bool
	}{
		{name: "live copy", coherent: false},
		{name: "one snapshot", points: []string{"pool"}, coherent: true},
		{name: "two snapshots", points: []string{"first", "second"}, coherent: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			app, nested, separate := databaseApp(t)
			tree, err := NewTree(t.Context(), app, nil, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer tree.Close()
			tree.live = true
			if len(test.points) > 0 {
				snapshotAll(tree, test.points...)
			}
			if got := tree.keepDatabaseFiles(); got != test.coherent {
				t.Fatalf("keepDatabaseFiles = %t", got)
			}
			kept := slices.ContainsFunc(tree.Paths, func(path backup.Path) bool { return path.Source == separate }) && slices.ContainsFunc(tree.Paths, func(path backup.Path) bool { return path.Source == nested })
			excluded := slices.ContainsFunc(tree.Exclusions(), func(e backup.Exclusion) bool { return e.Source == nested })
			if kept != test.coherent || excluded == test.coherent {
				t.Fatalf("database paths kept %t, excluded %t: %+v", kept, excluded, tree.Paths)
			}
			if slices.ContainsFunc(tree.roots, func(root *sourceRoot) bool { return root.database }) {
				t.Fatal("a candidate database root survived the decision")
			}
			consistency, _ := tree.captureEvidence(backup.Dump, nil, dumps)
			if (consistency == backup.ConsistencyFilesystem) != test.coherent {
				t.Fatalf("consistency %q", consistency)
			}
		})
	}
}

func TestMissingDatabaseDirectoryKeepsTheExport(t *testing.T) {
	app, _, separate := databaseApp(t)
	if err := os.RemoveAll(separate); err != nil {
		t.Fatal(err)
	}
	tree, err := NewTree(t.Context(), app, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tree.Close()
	snapshotAll(tree, "pool")
	if tree.keepDatabaseFiles() {
		t.Fatal("database files were kept without every data directory")
	}
	if consistency, _ := tree.captureEvidence(backup.Dump, nil, []backup.DatabaseDump{{ContainerID: "separate"}}); consistency != backup.ConsistencyUnverified {
		t.Fatal("an export claimed agreement with the files")
	}
}

func TestUnkeptDatabaseRootsAreNotCopied(t *testing.T) {
	app, nested, separate := databaseApp(t)
	tree, err := NewTree(t.Context(), app, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tree.Close()
	root, err := tree.Root(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	f := newIncrementalFixture(t, separate, true)
	prepared, err := f.r.UploadFiles(t.Context(), root, snapshot.SourceInfo{Host: "test", UserName: "test", Path: app.Key}, false)
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
	entries, err := os.ReadDir(filepath.Join(target, "paths"))
	if err != nil || len(entries) != 1 {
		t.Fatalf("copied roots: %v, %v", entries, err)
	}
	if _, err := os.Stat(filepath.Join(target, "paths", entries[0].Name(), "data", "db")); !os.IsNotExist(err) {
		t.Fatalf("the project walk copied the database directory %s: %v", nested, err)
	}
}

func TestKeptNestedDatabaseDirectoryRestoresWithItsProject(t *testing.T) {
	app, nested, separate := databaseApp(t)
	tree, err := NewTree(t.Context(), app, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tree.Close()
	snapshotAll(tree, "pool")
	if !tree.keepDatabaseFiles() {
		t.Fatal("database files were not kept")
	}
	root, err := tree.Root(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	f := newIncrementalFixture(t, separate, true)
	prepared, err := f.r.UploadFiles(t.Context(), root, snapshot.SourceInfo{Host: "test", UserName: "test", Path: app.Key}, false)
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
	for _, source := range []string{nested, separate} {
		index := slices.IndexFunc(tree.Paths, func(path backup.Path) bool { return path.Source == source })
		if index < 0 {
			t.Fatalf("%s has no archive path", source)
		}
		data, err := os.ReadFile(filepath.Join(target, tree.Paths[index].Archive, "PG_VERSION"))
		if err != nil || string(data) != source {
			t.Fatalf("restored %s: %q, %v", source, data, err)
		}
	}
	project := slices.IndexFunc(tree.Paths, func(path backup.Path) bool { return path.Source == app.ProjectDirectory })
	if nestedIndex := slices.IndexFunc(tree.Paths, func(path backup.Path) bool { return path.Source == nested }); tree.Paths[nestedIndex].Archive != tree.Paths[project].Archive+"/data/db" {
		t.Fatalf("the nested database directory is not part of its project walk: %+v", tree.Paths)
	}
}
