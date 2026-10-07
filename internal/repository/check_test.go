package repository

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/kopia/kopia/fs/localfs"
	"github.com/kopia/kopia/snapshot"
	"github.com/sirrobot01/synod/internal/backup"
)

func TestCheckReadsRealContentAndDetectsStorageDamage(t *testing.T) {
	location := Location{Kind: "local", Path: t.TempDir()}
	credentials := Credentials{Password: "private-check-password"}
	r, err := Create(t.Context(), location, credentials)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if r != nil {
			r.Close(context.Background())
		}
	}()
	source := t.TempDir()
	if err := os.WriteFile(filepath.Join(source, "state"), []byte("private-verified-state"), 0600); err != nil {
		t.Fatal(err)
	}
	root, err := localfs.NewEntry(source)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	files, err := r.UploadFiles(t.Context(), root, snapshot.SourceInfo{Host: "check", UserName: "synod", Path: source}, false)
	if err != nil {
		t.Fatal(err)
	}
	saved := backup.Snapshot{ID: uuid.New(), InstallationID: uuid.New(), DestinationID: uuid.New(), App: testCatalog().Apps[0], Method: backup.Live, Architecture: "arm64", StartedAt: time.Now().UTC(), CompletedAt: time.Now().UTC()}
	if _, err := r.PublishSnapshot(t.Context(), files, saved); err != nil {
		t.Fatal(err)
	}
	if err := r.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	r, err = Open(t.Context(), location, credentials)
	if err != nil {
		t.Fatal(err)
	}
	result, err := r.Check(t.Context(), saved.InstallationID, 1)
	if err != nil || result.Contents == 0 || result.Read == 0 || result.BytesRead == 0 {
		t.Fatalf("sample did not read repository content: %+v %v", result, err)
	}
	result, err = r.Check(t.Context(), saved.InstallationID, 100)
	if err != nil || result.Read != result.Contents {
		t.Fatalf("full check: %+v %v", result, err)
	}
	if _, err := r.Check(t.Context(), uuid.New(), 100); !errors.Is(err, ErrCatalogConflict) {
		t.Fatalf("checked another installation: %v", err)
	}
	cancelled, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := r.Check(cancelled, saved.InstallationID, 100); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled check: %v", err)
	}
	if err := r.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	var damaged bool
	if err := filepath.WalkDir(location.Path, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		relative, err := filepath.Rel(location.Path, path)
		if err != nil || damaged || entry.IsDir() || !strings.HasPrefix(relative, "p") {
			return err
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		for i := range data {
			data[i] ^= 0xff
		}
		damaged = true
		return os.WriteFile(path, data, 0600)
	}); err != nil || !damaged {
		t.Fatalf("fixture did not damage a content pack: %v", err)
	}
	r, err = Open(t.Context(), location, credentials)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.Check(t.Context(), saved.InstallationID, 100); err == nil {
		t.Fatal("storage damage passed verification")
	}
}
