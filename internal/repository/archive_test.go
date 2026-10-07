package repository

import (
	"archive/tar"
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	kfs "github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/fs/localfs"
	"github.com/kopia/kopia/fs/virtualfs"
	"github.com/kopia/kopia/snapshot"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/client"
	"github.com/sirrobot01/synod/internal/backup"
)

type archiveOwnedFile struct{ kfs.File }

func (archiveOwnedFile) Owner() kfs.OwnerInfo { return kfs.OwnerInfo{UserID: 12345, GroupID: 23456} }

func TestArchivePreservesContentOwnersAndLinks(t *testing.T) {
	source := t.TempDir()
	file := filepath.Join(source, "state")
	if err := os.WriteFile(file, []byte("private-restored-data"), 0640); err != nil {
		t.Fatal(err)
	}
	fixed := time.Now().Add(-time.Hour).Truncate(time.Second)
	if err := os.Chtimes(file, fixed, fixed); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("state", filepath.Join(source, "link")); err != nil {
		t.Fatal(err)
	}
	entry, err := localfs.NewEntry(file)
	if err != nil {
		t.Fatal(err)
	}
	defer entry.Close()
	link, err := localfs.NewEntry(filepath.Join(source, "link"))
	if err != nil {
		t.Fatal(err)
	}
	defer link.Close()
	root := virtualfs.NewStaticDirectory("root", []kfs.Entry{virtualfs.NewStaticDirectory("data", []kfs.Entry{archiveOwnedFile{entry.(kfs.File)}, link})})
	r, err := Create(t.Context(), Location{Kind: "local", Path: filepath.Join(t.TempDir(), "repository")}, Credentials{Password: "archive-password"})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close(context.Background())
	prepared, err := r.UploadFiles(t.Context(), root, snapshot.SourceInfo{Host: "fixture", UserName: "synod", Path: "archive"}, false)
	if err != nil {
		t.Fatal(err)
	}
	saved, err := r.PublishSnapshot(t.Context(), prepared, backup.Snapshot{ID: uuid.New(), InstallationID: uuid.New(), DestinationID: uuid.New(), App: testCatalog().Apps[0], Method: backup.Stop, Architecture: "arm64", StartedAt: fixed, CompletedAt: time.Now().UTC()})
	if err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	if err := r.Archive(t.Context(), saved.ID, "data", "", &output); err != nil {
		t.Fatal(err)
	}
	if host := os.Getenv("SYNOD_TEST_DOCKER_HOST"); host != "" {
		api, err := client.New(client.WithHost(host), client.WithAPIVersionNegotiation())
		if err != nil {
			t.Fatal(err)
		}
		defer api.Close()
		created, err := api.ContainerCreate(t.Context(), client.ContainerCreateOptions{Config: &container.Config{Image: "alpine@sha256:5291449c3df73caf6ed85e649dec1b9e818b39a5d8c871e97afc13e9cd5e8fa8", Cmd: []string{"true"}, Volumes: map[string]struct{}{"/restore": {}}}, HostConfig: &container.HostConfig{NetworkMode: "none"}})
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if _, err := api.ContainerRemove(context.Background(), created.ID, client.ContainerRemoveOptions{Force: true, RemoveVolumes: true}); err != nil {
				t.Error(err)
			}
		}()
		if _, err := api.CopyToContainer(t.Context(), created.ID, client.CopyToContainerOptions{DestinationPath: "/restore", Content: bytes.NewReader(output.Bytes()), CopyUIDGID: true}); err != nil {
			t.Fatal(err)
		}
		copied, err := api.CopyFromContainer(t.Context(), created.ID, client.CopyFromContainerOptions{SourcePath: "/restore/state"})
		if err != nil {
			t.Fatal(err)
		}
		actual := tar.NewReader(copied.Content)
		header, err := actual.Next()
		if err != nil || header.Uid != 12345 || header.Gid != 23456 || header.Mode != 0640 {
			copied.Content.Close()
			t.Fatalf("Docker restore changed owners or mode: %+v %v", header, err)
		}
		content, err := io.ReadAll(actual)
		closeErr := copied.Content.Close()
		if err != nil || closeErr != nil || string(content) != "private-restored-data" {
			t.Fatal("Docker restore changed content")
		}
	}
	reader := tar.NewReader(&output)
	seen := make(map[string]bool)
	for {
		header, err := reader.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		seen[header.Name] = true
		switch header.Name {
		case ".":
			if header.Typeflag != tar.TypeDir {
				t.Fatal("missing directory root")
			}
		case "link":
			if header.Typeflag != tar.TypeSymlink || header.Linkname != "state" {
				t.Fatal("changed symlink")
			}
		case "state":
			content, err := io.ReadAll(reader)
			if err != nil || string(content) != "private-restored-data" || header.Uid != 12345 || header.Gid != 23456 || header.Mode != 0640 || !header.ModTime.Equal(fixed) {
				t.Fatalf("changed file metadata or contents: %+v %v", header, err)
			}
		default:
			t.Fatalf("unexpected entry %q", header.Name)
		}
	}
	if len(seen) != 3 {
		t.Fatal("missing restored entries")
	}
	for _, path := range []string{"../data", "/data", "data/../state", "data\\state", "data/link/child"} {
		if err := r.Archive(t.Context(), saved.ID, path, "", io.Discard); !errors.Is(err, backup.ErrInvalid) {
			t.Fatalf("accepted unsafe source %q: %v", path, err)
		}
	}
	cancelled, cancel := context.WithCancel(t.Context())
	cancel()
	if err := r.Archive(cancelled, saved.ID, "data", "", io.Discard); err == nil {
		t.Fatal("ignored cancellation")
	}
	output.Reset()
	if err := r.Archive(t.Context(), saved.ID, "data/state", "", &output); err != nil {
		t.Fatal(err)
	}
	header, err := tar.NewReader(&output).Next()
	if err != nil || header.Name != "state" || header.Uid != 12345 {
		t.Fatal("single file archive lost its name or owner")
	}
}
