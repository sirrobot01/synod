package repository

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/kopia/kopia/fs/localfs"
	"github.com/kopia/kopia/snapshot"
	"github.com/sirrobot01/synod/internal/backup"
)

func TestSnapshotRestoresFinalContentWithoutControllerState(t *testing.T) {
	source := t.TempDir()
	if err := os.Mkdir(filepath.Join(source, "nested"), 0750); err != nil {
		t.Fatal(err)
	}
	original := []byte("private-backup-original-value")
	final := []byte("private-backup-modified-value")
	if len(original) != len(final) {
		t.Fatal("fixture needs an equal length update")
	}
	file := filepath.Join(source, "nested", "state")
	if err := os.WriteFile(file, original, 0640); err != nil {
		t.Fatal(err)
	}
	fixed := time.Now().Add(-time.Hour).Truncate(time.Second)
	if err := os.Chtimes(file, fixed, fixed); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("nested/state", filepath.Join(source, "link")); err != nil {
		t.Fatal(err)
	}
	location := Location{Kind: "local", Path: filepath.Join(t.TempDir(), "repository")}
	credentials := Credentials{Password: "private-snapshot-password"}
	r, err := Create(t.Context(), location, credentials)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if r != nil {
			r.Close(context.Background())
		}
	}()
	entry, err := localfs.NewEntry(source)
	if err != nil {
		t.Fatal(err)
	}
	defer entry.Close()
	src := snapshot.SourceInfo{Host: "test-device", UserName: "synod", Path: "test-app"}
	if _, err := r.UploadFiles(t.Context(), entry, src, true); err != nil {
		t.Fatal(err)
	}
	metadata := backup.Snapshot{ID: uuid.New(), InstallationID: uuid.New(), DestinationID: uuid.New(), App: testCatalog().Apps[0], Method: backup.Stop, Architecture: "arm64", StartedAt: time.Now().UTC(), DowntimeMillis: 12}
	if _, err := r.Snapshot(t.Context(), metadata.ID); !errors.Is(err, ErrSnapshotMissing) {
		t.Fatalf("first pass published snapshot: %v", err)
	}
	if err := os.WriteFile(file, final, 0640); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(file, fixed, fixed); err != nil {
		t.Fatal(err)
	}
	prepared, err := r.UploadFiles(t.Context(), entry, src, false)
	if err != nil {
		t.Fatal(err)
	}
	metadata.CompletedAt = time.Now().UTC()
	saved, err := r.PublishSnapshot(t.Context(), prepared, metadata)
	if err != nil {
		t.Fatal(err)
	}
	retry, err := r.PublishSnapshot(t.Context(), prepared, metadata)
	if err != nil || retry.FilesID != saved.FilesID {
		t.Fatalf("publish retry: %v", err)
	}
	metadata.DowntimeMillis++
	if _, err := r.PublishSnapshot(t.Context(), prepared, metadata); !errors.Is(err, ErrCatalogConflict) {
		t.Fatalf("conflicting retry: %v", err)
	}
	if err := r.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	r = nil
	if err := os.RemoveAll(source); err != nil {
		t.Fatal(err)
	}
	r, err = Open(t.Context(), location, credentials)
	if err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(t.TempDir(), "restored")
	if err := r.RestoreFiles(t.Context(), metadata.ID, target); err != nil {
		t.Fatal(err)
	}
	restored, err := os.ReadFile(filepath.Join(target, "nested", "state"))
	if err != nil || !bytes.Equal(restored, final) {
		t.Fatalf("restored final content differs: %v", err)
	}
	info, err := os.Stat(filepath.Join(target, "nested", "state"))
	if err != nil || info.Mode().Perm() != 0640 || !info.ModTime().Equal(fixed) {
		t.Fatalf("restored file metadata differs: %v %v", info, err)
	}
	link, err := os.Readlink(filepath.Join(target, "link"))
	if err != nil || link != "nested/state" {
		t.Fatalf("restored symlink differs: %v", err)
	}
	if err := r.RestoreFiles(t.Context(), metadata.ID, target); !errors.Is(err, os.ErrExist) {
		t.Fatalf("existing restore target accepted: %v", err)
	}
	if err := filepath.WalkDir(location.Path, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		content, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if bytes.Contains(content, final) || bytes.Contains(content, []byte(credentials.Password)) {
			t.Error("repository contains plaintext")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestFailedUploadCannotPublishSnapshot(t *testing.T) {
	r, err := Create(t.Context(), Location{Kind: "local", Path: t.TempDir()}, Credentials{Password: "password"})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close(context.Background())
	path := filepath.Join(t.TempDir(), "lost")
	if err := os.Mkdir(path, 0700); err != nil {
		t.Fatal(err)
	}
	entry, err := localfs.NewEntry(path)
	if err != nil {
		t.Fatal(err)
	}
	defer entry.Close()
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if prepared, err := r.UploadFiles(t.Context(), entry, snapshot.SourceInfo{Host: "test", UserName: "test", Path: "test"}, false); err == nil || prepared != nil {
		t.Fatal("missing source accepted")
	}
	cancelled, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := r.UploadFiles(cancelled, entry, snapshot.SourceInfo{}, false); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled upload: %v", err)
	}
	if _, err := r.PublishSnapshot(t.Context(), &snapshot.Manifest{}, backup.Snapshot{}); !errors.Is(err, backup.ErrInvalid) {
		t.Fatalf("empty snapshot accepted: %v", err)
	}
}

func TestSnapshotRefusesMissingPreparedObjects(t *testing.T) {
	source := t.TempDir()
	if err := os.WriteFile(filepath.Join(source, "file"), []byte("snapshot from another repository"), 0600); err != nil {
		t.Fatal(err)
	}
	entry, err := localfs.NewEntry(source)
	if err != nil {
		t.Fatal(err)
	}
	defer entry.Close()
	first, err := Create(t.Context(), Location{Kind: "local", Path: t.TempDir()}, Credentials{Password: "test-password"})
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close(context.Background())
	prepared, err := first.UploadFiles(t.Context(), entry, snapshot.SourceInfo{Host: "test", UserName: "test", Path: "test"}, false)
	if err != nil {
		t.Fatal(err)
	}
	second, err := Create(t.Context(), Location{Kind: "local", Path: t.TempDir()}, Credentials{Password: "test-password"})
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close(context.Background())
	metadata := backup.Snapshot{ID: uuid.New(), InstallationID: uuid.New(), DestinationID: uuid.New(), App: testCatalog().Apps[0], Method: backup.Live, Architecture: "arm64", StartedAt: time.Now().UTC(), CompletedAt: time.Now().UTC()}
	if _, err := second.PublishSnapshot(t.Context(), prepared, metadata); !errors.Is(err, ErrSnapshotIncomplete) {
		t.Fatalf("missing content was published: %v", err)
	}
	if _, err := second.Snapshot(t.Context(), metadata.ID); !errors.Is(err, ErrSnapshotMissing) {
		t.Fatalf("failed publication left a snapshot: %v", err)
	}
}
