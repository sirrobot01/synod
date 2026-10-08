package backup

import (
	"database/sql"
	"maps"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"testing"
	"time"

	"github.com/google/uuid"
	kfs "github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/snapshot"
	"github.com/sirrobot01/synod/internal/backup"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
)

func TestLinkedDatabaseAndFileCopiesDoNotClaimAppConsistency(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("Linux SQLite source paths")
	}
	directory := t.TempDir()
	database, file := filepath.Join(directory, "app.sqlite"), filepath.Join(directory, "state")
	db, err := sql.Open("sqlite", (&url.URL{Scheme: "file", Path: database}).String())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.ExecContext(t.Context(), "PRAGMA journal_mode=WAL; CREATE TABLE state(version INTEGER); INSERT INTO state VALUES(1)"); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(file, []byte("version-1"), 0600); err != nil {
		t.Fatal(err)
	}
	f := newIncrementalFixture(t, directory, true)
	f.app.Paths = []*agentv1.DiscoveredPath{{Source: database, Kind: "bind", Status: "Included"}, {Source: file, Kind: "bind", Status: "Included"}}
	tree, err := NewTree(t.Context(), f.app, nil, nil)
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
	for _, source := range tree.roots {
		if source.source != database {
			continue
		}
		entry, err := paths.(kfs.Directory).Child(t.Context(), source.archive)
		if err != nil {
			t.Fatal(err)
		}
		reader, err := entry.(kfs.File).Open(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		reader.Close()
	}
	// The app commits a linked update between the two captures. Neither source
	// changes during its own read, so per-file retries cannot detect the mismatch.
	if _, err := db.ExecContext(t.Context(), "UPDATE state SET version=2"); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(file, []byte("version-2"), 0600); err != nil {
		t.Fatal(err)
	}
	prepared, err := f.r.UploadFiles(t.Context(), root, snapshot.SourceInfo{Host: "test", UserName: "test", Path: f.app.Key}, false)
	if err != nil {
		t.Fatal(err)
	}
	metadata := f.metadata
	metadata.ID, metadata.StartedAt, metadata.CompletedAt, metadata.Paths = uuid.New(), time.Now().UTC(), time.Now().UTC(), tree.Paths
	metadata.Consistency, metadata.Captures = tree.captureEvidence(backup.Live, slices.Sorted(maps.Keys(tree.sqlite)), nil)
	if metadata.Consistency != backup.ConsistencyUnverified || len(tree.Changing()) != 0 {
		t.Fatal("per-file success was mistaken for app consistency")
	}
	if _, err := f.r.PublishSnapshot(t.Context(), prepared, metadata); err != nil {
		t.Fatal(err)
	}
	saved, err := f.r.Snapshot(t.Context(), metadata.ID)
	if err != nil || saved.Consistency != backup.ConsistencyUnverified || len(saved.Captures) != 3 {
		t.Fatalf("repository lost the capture limits: %+v, %v", saved.Captures, err)
	}
	target := filepath.Join(t.TempDir(), "restored")
	if err := f.r.RestoreFiles(t.Context(), metadata.ID, target); err != nil {
		t.Fatal(err)
	}
	for _, path := range saved.Paths {
		restored := filepath.Join(target, path.Archive)
		if path.Source == file {
			if data, err := os.ReadFile(restored); err != nil || string(data) != "version-2" {
				t.Fatalf("restored file: %q, %v", data, err)
			}
		} else {
			copied, err := sql.Open("sqlite", (&url.URL{Scheme: "file", Path: restored}).String())
			if err != nil {
				t.Fatal(err)
			}
			defer copied.Close()
			var version int
			if err := copied.QueryRowContext(t.Context(), "SELECT version FROM state").Scan(&version); err != nil || version != 1 {
				t.Fatalf("restored database: version %d, %v", version, err)
			}
		}
	}
}
