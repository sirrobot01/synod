package backup

import (
	"context"
	"database/sql"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/kopia/kopia/snapshot"
	"github.com/sirrobot01/synod/internal/backup"
	"github.com/sirrobot01/synod/internal/inventory"
	"github.com/sirrobot01/synod/internal/repository"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
)

// incrementalFixture backs up one directory twice and restores the second snapshot.
type incrementalFixture struct {
	t        *testing.T
	r        *repository.Repository
	app      *agentv1.DiscoveredApp
	metadata backup.Snapshot
	live     bool
}

func newIncrementalFixture(t *testing.T, directory string, live bool) *incrementalFixture {
	r, err := repository.Create(t.Context(), repository.Location{Kind: "local", Path: t.TempDir()}, repository.Credentials{Password: "incremental-test-password"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { r.Close(context.Background()) })
	app := &agentv1.DiscoveredApp{Key: "compose:incremental", Name: "incremental", Kind: "compose", Paths: []*agentv1.DiscoveredPath{{Source: directory, Kind: "bind", Status: "Included"}}}
	method := backup.Stop
	if live {
		method = backup.Live
	}
	metadata := backup.Snapshot{InstallationID: uuid.New(), DestinationID: uuid.New(), App: inventory.App{ID: uuid.New(), NodeID: uuid.New(), Definition: app}, Method: method, Architecture: "arm64"}
	return &incrementalFixture{t: t, r: r, app: app, metadata: metadata, live: live}
}

// backup takes one snapshot with the newest earlier snapshot as the base, and restores it.
func (f *incrementalFixture) backup(definition string) string {
	t := f.t
	tree, err := NewTree(t.Context(), f.app, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tree.Close()
	tree.live = f.live
	root, err := tree.Root(t.Context(), map[string][]byte{"inspect.json": []byte(definition)})
	if err != nil {
		t.Fatal(err)
	}
	previous, err := f.r.LatestFiles(t.Context(), f.metadata.InstallationID, f.metadata.App.ID, f.metadata.DestinationID)
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
	target := filepath.Join(t.TempDir(), "restored")
	if err := f.r.RestoreFiles(t.Context(), metadata.ID, target); err != nil {
		t.Fatal(err)
	}
	if data, err := os.ReadFile(filepath.Join(target, "definition", "inspect.json")); err != nil || string(data) != definition {
		t.Fatalf("restored definition %q differs from %q: %v", data, definition, err)
	}
	return filepath.Join(target, tree.Paths[0].Archive)
}

func TestIncrementalBackupReadsOnlyChangedFiles(t *testing.T) {
	directory := t.TempDir()
	kept, changed := filepath.Join(directory, "kept"), filepath.Join(directory, "changed")
	for path, data := range map[string]string{kept: "kept-original", changed: "changed-before"} {
		if err := os.WriteFile(path, []byte(data), 0600); err != nil {
			t.Fatal(err)
		}
	}
	f := newIncrementalFixture(t, directory, false)
	f.backup("definition-one")

	// The same size and a later time mark a changed file.
	if err := os.WriteFile(changed, []byte("changed-after!"), 0600); err != nil {
		t.Fatal(err)
	}
	later := time.Now().Add(time.Hour)
	if err := os.Chtimes(changed, later, later); err != nil {
		t.Fatal(err)
	}
	// Like rsync, the backup trusts an unchanged size and time and does not read the file.
	// New bytes behind the old time prove that the backup reused the earlier copy.
	info, err := os.Stat(kept)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(kept, []byte("kept-REWRITES"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(kept, info.ModTime(), info.ModTime()); err != nil {
		t.Fatal(err)
	}
	restored := f.backup("definition-two")
	for name, want := range map[string]string{"kept": "kept-original", "changed": "changed-after!"} {
		if data, err := os.ReadFile(filepath.Join(restored, name)); err != nil || string(data) != want {
			t.Fatalf("restored %s is %q, want %q: %v", name, data, want, err)
		}
	}
}

func TestIncrementalLiveBackupCopiesCommitsInTheWAL(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("Linux agent source paths")
	}
	directory := t.TempDir()
	path := filepath.Join(directory, "app.sqlite")
	uri := url.URL{Scheme: "file", Path: path, RawQuery: "_pragma=journal_mode(WAL)&_pragma=wal_autocheckpoint(0)"}
	db, err := sql.Open("sqlite", uri.String())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	if _, err := db.ExecContext(t.Context(), "CREATE TABLE state(id INTEGER PRIMARY KEY); INSERT INTO state VALUES (1); PRAGMA wal_checkpoint(TRUNCATE);"); err != nil {
		t.Fatal(err)
	}
	f := newIncrementalFixture(t, directory, true)
	f.backup("definition-one")
	main, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(10 * time.Millisecond)
	// Without a checkpoint, this commit changes only the -wal file.
	if _, err := db.ExecContext(t.Context(), "INSERT INTO state VALUES (2)"); err != nil {
		t.Fatal(err)
	}
	if after, err := os.Stat(path); err != nil || !after.ModTime().Equal(main.ModTime()) || after.Size() != main.Size() {
		t.Fatalf("the commit changed the main database file, so this test proves nothing: %v", err)
	}
	restored := f.backup("definition-two")
	copied, err := sql.Open("sqlite", (&url.URL{Scheme: "file", Path: filepath.Join(restored, "app.sqlite")}).String())
	if err != nil {
		t.Fatal(err)
	}
	defer copied.Close()
	var count int
	if err := copied.QueryRowContext(t.Context(), "SELECT count(*) FROM state").Scan(&count); err != nil || count != 2 {
		t.Fatalf("restored %d rows, want the commit from the WAL: %v", count, err)
	}
}
