package backup

import (
	"bytes"
	"context"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	kfs "github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/fs/virtualfs"
	"github.com/kopia/kopia/snapshot"
	"github.com/sirrobot01/synod/internal/backup"
	"github.com/sirrobot01/synod/internal/inventory"
	"github.com/sirrobot01/synod/internal/repository"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
)

func TestStagedExportsRemainPrivateAndRestoreWithLaterFiles(t *testing.T) {
	storage, cache, source := t.TempDir(), t.TempDir(), t.TempDir()
	r, err := repository.Create(t.Context(), repository.Location{Kind: "local", Path: storage}, repository.Credentials{Password: "export-test-password", CacheDirectory: cache})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close(context.Background())
	app := &agentv1.DiscoveredApp{Key: "compose:exports", Paths: []*agentv1.DiscoveredPath{{Source: source, Kind: "bind", Status: "Included"}}}
	metadata := backup.Snapshot{ID: uuid.New(), InstallationID: uuid.New(), DestinationID: uuid.New(), App: inventory.App{ID: uuid.New(), NodeID: uuid.New(), Definition: app}, Method: backup.Dump, Architecture: "arm64", StartedAt: time.Now().UTC()}
	info := snapshot.SourceInfo{Host: "source", UserName: "synod", Path: app.Key}
	const secret = "private-native-export-value-which-must-not-appear-in-staging-files"
	payload := []byte("CREATE TABLE proof(value TEXT); INSERT INTO proof VALUES ('" + secret + "');\n")
	export, cancel := context.WithCancel(t.Context())
	dumps, err := stageDumps(export, r, []kfs.Entry{virtualfs.StreamingFileFromReader("app.sql", io.NopCloser(bytes.NewReader(payload)))}, info)
	cancel()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		for _, dump := range dumps {
			dump.Close()
		}
	}()
	clear(payload) // The native export has ended. The final upload must not read it again.
	if snapshots, err := r.Snapshots(t.Context(), metadata.InstallationID, metadata.App.ID); err != nil || len(snapshots) != 0 {
		t.Fatal("export staging published an app snapshot", err)
	}
	if err := os.WriteFile(filepath.Join(source, "state"), []byte("file-created-during-export"), 0600); err != nil {
		t.Fatal(err)
	}
	tree, err := NewTree(t.Context(), app, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tree.Close()
	tree.live = true
	root, err := tree.Root(t.Context(), nil, dumps...)
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := r.UploadFiles(t.Context(), root, info, false)
	if err != nil {
		t.Fatal(err)
	}
	metadata.Dumps = []backup.DatabaseDump{{ContainerID: "app", Database: "postgres", Archive: "databases/app.sql", Format: "sql"}}
	metadata.Paths, metadata.CompletedAt = tree.Paths, time.Now().UTC()
	metadata.Consistency, metadata.Captures = tree.captureEvidence(backup.Dump, nil, metadata.Dumps)
	if metadata.Consistency != backup.ConsistencyUnverified {
		t.Fatal("ordered exports claimed app consistency")
	}
	if _, err := r.PublishSnapshot(t.Context(), prepared, metadata); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(t.TempDir(), "restore")
	if err := r.RestoreFiles(t.Context(), metadata.ID, target); err != nil {
		t.Fatal(err)
	}
	if data, err := os.ReadFile(filepath.Join(target, "databases", "app.sql")); err != nil || !bytes.Contains(data, []byte(secret)) {
		t.Fatal("restored export changed", err)
	}
	if data, err := os.ReadFile(filepath.Join(target, tree.Paths[0].Archive, "state")); err != nil || string(data) != "file-created-during-export" {
		t.Fatal("later file capture is missing", err)
	}
	for _, directory := range []string{storage, cache} {
		if err := filepath.WalkDir(directory, func(path string, entry fs.DirEntry, err error) error {
			if err != nil || entry.IsDir() {
				return err
			}
			data, err := os.ReadFile(path)
			if err == nil && bytes.Contains(data, []byte(secret)) {
				t.Error("native export reached plaintext storage")
			}
			return err
		}); err != nil {
			t.Fatal(err)
		}
	}
}

func TestFailedExportStagingPublishesNothing(t *testing.T) {
	f := newIncrementalFixture(t, t.TempDir(), true)
	reader, writer := io.Pipe()
	defer reader.Close()
	done := make(chan struct{})
	go func() {
		defer close(done)
		writer.Write([]byte("unfinished native export"))
		writer.CloseWithError(errors.New("injected export failure"))
	}()
	entries, err := stageDumps(t.Context(), f.r, []kfs.Entry{virtualfs.StreamingFileFromReader("app.sql", reader)}, snapshot.SourceInfo{Host: "source", UserName: "synod", Path: "failed"})
	<-done
	if err == nil || len(entries) != 0 {
		t.Fatal("an incomplete export was accepted")
	}
	if snapshots, err := f.r.Snapshots(t.Context(), f.metadata.InstallationID, f.metadata.App.ID); err != nil || len(snapshots) != 0 {
		t.Fatal("failed export staging published a snapshot", err)
	}
}
