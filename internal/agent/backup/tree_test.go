package backup

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/google/uuid"
	kfs "github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/snapshot"
	"github.com/sirrobot01/synod/internal/backup"
	"github.com/sirrobot01/synod/internal/inventory"
	"github.com/sirrobot01/synod/internal/repository"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
)

func TestTreeRestoresIncludedPathsAndExcludesOverlaps(t *testing.T) {
	project := t.TempDir()
	for _, dir := range []string{"data", "data/db", "cache", "media", "repository"} {
		if err := os.MkdirAll(filepath.Join(project, dir), 0700); err != nil {
			t.Fatal(err)
		}
	}
	for _, file := range []string{"compose.yaml", ".env", "data/state", "data/db/secret", "cache/secret", "media/secret", "repository/secret"} {
		if err := os.WriteFile(filepath.Join(project, file), []byte(file), 0600); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Symlink("/etc", filepath.Join(project, "host-link")); err != nil {
		t.Fatal(err)
	}
	app := &agentv1.DiscoveredApp{Key: "compose:test", Name: "test", Kind: "compose", ProjectDirectory: project, Paths: []*agentv1.DiscoveredPath{
		{Source: project, Kind: "project", Status: "Included"},
		{Source: filepath.Join(project, "compose.yaml"), Kind: "compose", Status: "Included"},
		{Source: filepath.Join(project, ".env"), Kind: "env", Status: "Included"},
		{Source: filepath.Join(project, "data"), Kind: "bind", Status: "Included"},
		{Source: filepath.Join(project, "data", "db"), Kind: "bind", Status: "Dumped", Reason: "Database data needs a dump."},
		{Source: filepath.Join(project, "cache"), Kind: "bind", Status: "Large", Reason: "This path exceeds 20 GiB."},
	}}
	tree, err := NewTree(t.Context(), app, []backup.Exclusion{{Source: filepath.Join(project, "repository"), Reason: "Do not copy the destination into itself."}})
	if err != nil {
		t.Fatal(err)
	}
	defer tree.Close()
	root, err := tree.Root(t.Context(), map[string][]byte{"inspect.json": []byte("private-container-definition")})
	if err != nil {
		t.Fatal(err)
	}
	r, err := repository.Create(t.Context(), repository.Location{Kind: "local", Path: t.TempDir()}, repository.Credentials{Password: "test-password"})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close(context.Background())
	prepared, err := r.UploadFiles(t.Context(), root, snapshot.SourceInfo{Host: "test", UserName: "test", Path: app.Key}, false)
	if err != nil {
		t.Fatal(err)
	}
	metadata := backup.Snapshot{ID: uuid.New(), InstallationID: uuid.New(), DestinationID: uuid.New(), Method: backup.Live, Architecture: "arm64", StartedAt: time.Now().UTC(), CompletedAt: time.Now().UTC(), Paths: tree.Paths, Exclusions: tree.Exclusions(), App: inventory.App{ID: uuid.New(), NodeID: uuid.New(), Definition: app}}
	if _, err := r.PublishSnapshot(t.Context(), prepared, metadata); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(t.TempDir(), "restore")
	if err := r.RestoreFiles(t.Context(), metadata.ID, target); err != nil {
		t.Fatal(err)
	}
	for _, mapping := range tree.Paths {
		restored := filepath.Join(target, mapping.Archive)
		switch mapping.Source {
		case project:
			for _, name := range []string{"compose.yaml", ".env", "data/state"} {
				data, err := os.ReadFile(filepath.Join(restored, name))
				if err != nil || string(data) != name {
					t.Fatalf("included %s: %v", name, err)
				}
			}
			for _, name := range []string{"data/db", "cache", "media", "repository"} {
				if _, err := os.Lstat(filepath.Join(restored, name)); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("excluded %s was copied: %v", name, err)
				}
			}
			if link, err := os.Readlink(filepath.Join(restored, "host-link")); err != nil || link != "/etc" {
				t.Fatalf("symlink changed or followed: %v", err)
			}
		case filepath.Join(project, "data"):
			if _, err := os.Stat(filepath.Join(restored, "db")); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("overlap copied database: %v", err)
			}
		}
	}
	media, err := filepath.EvalSymlinks(filepath.Join(project, "media"))
	if err != nil {
		t.Fatal(err)
	}
	if !slices.ContainsFunc(metadata.Exclusions, func(item backup.Exclusion) bool { return item.Source == media }) {
		t.Fatal("dynamic media exclusion is missing")
	}
	info, err := os.Stat(filepath.Join(target, "definition", "inspect.json"))
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatalf("definition is not private: %v", err)
	}
}

func TestTreeRefusesPathEscapeAndChangedParents(t *testing.T) {
	for _, source := range []string{"/etc", "relative"} {
		if tree, err := NewTree(t.Context(), &agentv1.DiscoveredApp{Paths: []*agentv1.DiscoveredPath{{Source: source, Kind: "bind", Status: "Included"}}}, nil); err == nil {
			tree.Close()
			t.Fatalf("unsafe root accepted: %s", source)
		}
	}
	project := t.TempDir()
	if err := os.Mkdir(filepath.Join(project, "state"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(project, "state", "file"), []byte("safe"), 0600); err != nil {
		t.Fatal(err)
	}
	tree, err := NewTree(t.Context(), &agentv1.DiscoveredApp{ProjectDirectory: project, Paths: []*agentv1.DiscoveredPath{{Source: project, Kind: "project", Status: "Included"}, {Source: filepath.Join(project, ".env"), Kind: "env", Status: "Included"}}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tree.Close()
	if len(tree.Paths) != 1 {
		t.Fatal("missing optional env was included")
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
	state, err := entries[0].(kfs.Directory).Child(t.Context(), "state")
	if err != nil {
		t.Fatal(err)
	}
	file, err := state.(kfs.Directory).Child(t.Context(), "file")
	if err != nil {
		t.Fatal(err)
	}
	outside := t.TempDir()
	if err := os.WriteFile(filepath.Join(outside, "file"), []byte("secret"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(filepath.Join(project, "state")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(project, "state")); err != nil {
		t.Fatal(err)
	}
	if input, err := file.(kfs.File).Open(t.Context()); err == nil {
		input.Close()
		t.Fatal("changed parent symlink was followed")
	}
	if _, err := state.(kfs.Directory).Iterate(t.Context()); err == nil {
		t.Fatal("changed directory symlink was followed")
	}
	if err := os.Remove(filepath.Join(project, "state")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(".", filepath.Join(project, "state")); err != nil {
		t.Fatal(err)
	}
	if input, err := file.(kfs.File).Open(t.Context()); err == nil {
		input.Close()
		t.Fatal("internal parent symlink was followed")
	}
}
