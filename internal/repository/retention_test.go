package repository

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/kopia/kopia/fs/localfs"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/snapshot"
	"github.com/sirrobot01/synod/internal/backup"
)

func TestRetentionKeepsCalendarHistoryPinsAndOtherApps(t *testing.T) {
	location := Location{Kind: "local", Path: t.TempDir()}
	credentials := Credentials{Password: "retention-test-password"}
	r, err := Create(t.Context(), location, credentials)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { r.Close(context.Background()) }()
	source := t.TempDir()
	if err := os.WriteFile(filepath.Join(source, "state"), []byte("retained-content"), 0600); err != nil {
		t.Fatal(err)
	}
	root, err := localfs.NewEntry(source)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	catalog := testCatalog()
	app := catalog.Apps[0]
	destination := uuid.New()
	prepared, err := r.UploadFiles(t.Context(), root, snapshot.SourceInfo{Host: app.NodeID.String(), UserName: "synod", Path: app.ID.String()}, false)
	if err != nil {
		t.Fatal(err)
	}
	kept, expired := make(map[uuid.UUID]bool), make(map[uuid.UUID]bool)
	var newest, current uuid.UUID
	for _, test := range []struct {
		at        string
		keep, pin bool
	}{
		{"2026-10-06T03:00:00Z", true, false},
		{"2026-10-05T03:00:00Z", true, false},
		{"2026-10-04T03:00:00Z", true, false},
		{"2026-10-03T03:00:00Z", true, false},
		{"2026-10-02T03:00:00Z", true, false},
		{"2026-10-01T03:00:00Z", true, false},
		{"2026-09-30T03:00:00Z", true, false},
		{"2026-09-27T03:00:00Z", true, false},
		{"2026-09-20T03:00:00Z", true, false},
		{"2026-08-31T03:00:00Z", true, false},
		{"2026-07-31T03:00:00Z", true, false},
		{"2026-06-30T03:00:00Z", true, false},
		{"2026-05-31T03:00:00Z", true, false},
		{"2026-04-30T03:00:00Z", true, true},
		{"2026-10-06T02:00:00Z", false, false},
		{"2026-09-29T03:00:00Z", false, false},
		{"2026-09-13T03:00:00Z", false, false},
		{"2026-08-15T03:00:00Z", false, false},
		{"2026-03-31T03:00:00Z", false, false},
	} {
		at, _ := time.Parse(time.RFC3339, test.at)
		metadata := backup.Snapshot{ID: uuid.New(), InstallationID: catalog.InstallationID, DestinationID: destination, App: app, Method: backup.Live, Architecture: "arm64", StartedAt: at.Add(-time.Minute), CompletedAt: at}
		files := prepared.Clone()
		if test.pin {
			files.Pins = []string{"keep-for-investigation"}
		}
		if _, err := r.PublishSnapshot(t.Context(), files, metadata); err != nil {
			t.Fatal(err)
		}
		if test.at == "2026-05-31T03:00:00Z" {
			current = metadata.ID
		}
		if newest == uuid.Nil {
			newest = metadata.ID
		}
		if test.keep {
			kept[metadata.ID] = true
		} else {
			expired[metadata.ID] = true
		}
	}
	var unrelated []uuid.UUID
	for i := range 2 {
		metadata := backup.Snapshot{ID: uuid.New(), InstallationID: catalog.InstallationID, DestinationID: destination, App: app, Method: backup.Live, Architecture: "arm64", StartedAt: time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC), CompletedAt: time.Date(2020, 1, 1, 0, 1, 0, 0, time.UTC)}
		if i == 0 {
			metadata.App.ID = uuid.New()
		} else {
			metadata.DestinationID = uuid.New()
		}
		if _, err := r.PublishSnapshot(t.Context(), prepared, metadata); err != nil {
			t.Fatal(err)
		}
		unrelated = append(unrelated, metadata.ID)
	}
	removed, err := r.ApplyRetention(t.Context(), catalog.InstallationID, app.ID, destination, backup.Retention{Daily: 7, Weekly: 4, Monthly: 6}, uuid.Nil)
	if err != nil || len(removed) != len(expired) {
		t.Fatalf("retention removed %d snapshots, want %d: %v", len(removed), len(expired), err)
	}
	for _, id := range removed {
		if !expired[id] {
			t.Fatalf("retention removed protected snapshot %s", id)
		}
	}
	if err := r.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	r, err = Open(t.Context(), location, credentials)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(source); err != nil {
		t.Fatal(err)
	}
	for id := range kept {
		target := filepath.Join(t.TempDir(), "restored")
		if err := r.RestoreFiles(t.Context(), id, target); err != nil {
			t.Fatalf("restore retained snapshot %s: %v", id, err)
		}
		data, err := os.ReadFile(filepath.Join(target, "state"))
		if err != nil || string(data) != "retained-content" {
			t.Fatalf("retained content differs: %v", err)
		}
	}
	for id := range expired {
		if _, err := r.Snapshot(t.Context(), id); !errors.Is(err, ErrSnapshotMissing) {
			t.Fatalf("expired snapshot remains: %v", err)
		}
	}
	for _, id := range unrelated {
		if _, err := r.Snapshot(t.Context(), id); err != nil {
			t.Fatalf("unrelated snapshot lost: %v", err)
		}
	}
	if ids, err := r.ApplyRetention(t.Context(), catalog.InstallationID, app.ID, destination, backup.Retention{Daily: 7, Weekly: 4, Monthly: 6}, uuid.Nil); err != nil || len(ids) != 0 {
		t.Fatalf("retention retry: %v", err)
	}
	cancelled, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := r.ApplyRetention(cancelled, catalog.InstallationID, app.ID, destination, backup.Retention{}, uuid.Nil); err == nil {
		t.Fatal("cancelled retention succeeded")
	}
	if _, err := r.ApplyRetention(t.Context(), catalog.InstallationID, app.ID, destination, backup.Retention{}, current); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Snapshot(t.Context(), current); err != nil {
		t.Fatalf("retention removed the current copy after a clock change: %v", err)
	}
	if _, err := r.Snapshot(t.Context(), newest); err != nil {
		t.Fatalf("zero counts removed newest snapshot: %v", err)
	}
}

func TestRetentionReadsSnapshotsWithoutLabels(t *testing.T) {
	r, err := Create(t.Context(), Location{Kind: "local", Path: t.TempDir()}, Credentials{Password: "retention-label-password"})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close(context.Background())
	source := t.TempDir()
	if err := os.WriteFile(filepath.Join(source, "state"), []byte("label-test"), 0o600); err != nil {
		t.Fatal(err)
	}
	root, err := localfs.NewEntry(source)
	if err != nil {
		t.Fatal(err)
	}
	catalog := testCatalog()
	app, destination := catalog.Apps[0], uuid.New()
	prepared, err := r.UploadFiles(t.Context(), root, snapshot.SourceInfo{Host: app.NodeID.String(), UserName: "synod", Path: app.ID.String()}, false)
	if err != nil {
		t.Fatal(err)
	}
	var older, newer uuid.UUID
	for i, at := range []time.Time{time.Date(2026, 1, 1, 3, 0, 0, 0, time.UTC), time.Date(2026, 1, 2, 3, 0, 0, 0, time.UTC)} {
		metadata := backup.Snapshot{ID: uuid.New(), InstallationID: catalog.InstallationID, DestinationID: destination, App: app, Method: backup.Live, Architecture: "arm64", StartedAt: at.Add(-time.Minute), CompletedAt: at}
		saved, err := r.PublishSnapshot(t.Context(), prepared.Clone(), metadata)
		if err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			older = saved.ID
			// Rewrite this snapshot's metadata as a build without the retention labels saved it.
			err = repo.WriteSession(t.Context(), r.repo, repo.WriteSessionOptions{Purpose: "old labels"}, func(ctx context.Context, w repo.RepositoryWriter) error {
				entries, err := w.FindManifests(ctx, map[string]string{"type": "synod.backup", "snapshot": saved.ID.String()})
				if err != nil || len(entries) != 1 {
					return errors.Join(err, errors.New("snapshot manifest not found"))
				}
				if err := w.DeleteManifest(ctx, entries[0].ID); err != nil {
					return err
				}
				_, err = w.PutManifest(ctx, map[string]string{"type": "synod.backup", "snapshot": saved.ID.String(), "installation": saved.InstallationID.String(), "app": saved.App.ID.String(), "destination": saved.DestinationID.String()}, saved)
				return err
			})
			if err != nil {
				t.Fatal(err)
			}
		} else {
			newer = saved.ID
		}
	}
	removed, err := r.ApplyRetention(t.Context(), catalog.InstallationID, app.ID, destination, backup.Retention{}, newer)
	if err != nil || len(removed) != 1 || removed[0] != older {
		t.Fatalf("retention with an unlabelled snapshot removed %v: %v", removed, err)
	}
	if _, err := r.Snapshot(t.Context(), newer); err != nil {
		t.Fatalf("the current snapshot was removed: %v", err)
	}
}
