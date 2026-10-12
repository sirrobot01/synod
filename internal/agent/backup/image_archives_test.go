package backup

import (
	"bytes"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/registry"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/empty"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/google/go-containerregistry/pkg/v1/random"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/google/go-containerregistry/pkg/v1/tarball"
	"github.com/google/uuid"
	kfs "github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/fs/virtualfs"
	"github.com/kopia/kopia/snapshot"
	"github.com/moby/moby/client"
	"github.com/sirrobot01/synod/internal/backup"
	imagecheck "github.com/sirrobot01/synod/internal/runtime/image"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
)

func TestImageArchivesSurviveRegistryLossAndRetention(t *testing.T) {
	registry := httptest.NewServer(registry.New(registry.Logger(log.New(io.Discard, "", 0))))
	defer registry.Close()
	ref, _ := name.NewTag(strings.TrimPrefix(registry.URL, "http://") + "/test:current")
	var index v1.ImageIndex = empty.Index
	ids := make(map[string]string)
	var local bytes.Buffer
	for _, arch := range []string{"amd64", "arm64"} {
		img, err := random.Image(4096, 2)
		if err != nil {
			t.Fatal(err)
		}
		config, _ := img.ConfigFile()
		config.OS, config.Architecture = "linux", arch
		img, err = mutate.ConfigFile(img, config)
		if err != nil {
			t.Fatal(err)
		}
		id, _ := img.ConfigName()
		ids[arch] = id.String()
		index = mutate.AppendManifests(index, mutate.IndexAddendum{Add: img, Descriptor: v1.Descriptor{Platform: &v1.Platform{OS: "linux", Architecture: arch}}})
		if arch == "arm64" {
			digest, _ := name.NewDigest("synod.invalid/image@" + id.String())
			if err := tarball.Write(digest, img, &local); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := remote.WriteIndex(ref, mutate.AppendManifests(empty.Index, mutate.IndexAddendum{Add: index}), remote.WithContext(t.Context())); err != nil {
		t.Fatal(err)
	}
	digest, _ := index.Digest()
	pin := ref.Context().Digest(digest.String()).Name()
	docker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" || !strings.HasSuffix(r.URL.Path, "/images/get") || r.URL.Query().Get("names") != ids["arm64"] {
			t.Errorf("unexpected Docker read: %s %s", r.Method, r.URL)
			http.Error(w, "unexpected", 500)
			return
		}
		w.Write(local.Bytes())
	}))
	defer docker.Close()
	api, err := client.New(client.WithHost(docker.URL), client.WithAPIVersion("1.56"), client.WithHTTPClient(docker.Client()))
	if err != nil {
		t.Fatal(err)
	}
	defer api.Close()
	f := newIncrementalFixture(t, t.TempDir(), true)
	f.app.Containers = []*agentv1.DiscoveredContainer{{Id: "one", ImageId: ids["arm64"], Platform: "linux/arm64/v8", RepoDigests: []string{pin}}, {Id: "two", ImageId: ids["arm64"], Platform: "linux/arm64/v8", RepoDigests: []string{pin}}}
	worker := &Worker{Docker: api}
	var prior *backup.Snapshot
	var priorFiles *snapshot.Manifest
	var first uuid.UUID
	for pass := range 2 {
		archives, err := worker.captureImages(t.Context(), f.r, f.app, prior, priorFiles)
		if err != nil {
			t.Fatal(err)
		}
		if len(archives.images) != 2 || len(archives.entries) != 2 {
			t.Fatalf("image deduplication: %+v", archives.images)
		}
		if pass == 1 && len(archives.files) != 0 {
			t.Fatal("unchanged images were read again")
		}
		root := virtualfs.NewStaticDirectory("app", []kfs.Entry{virtualfs.NewStaticDirectory("images", archives.entries)})
		prepared, err := f.r.UploadFiles(t.Context(), root, snapshot.SourceInfo{Host: "fixture", UserName: "test", Path: f.app.Key}, false)
		if err != nil {
			t.Fatal(err)
		}
		metadata := f.metadata
		metadata.ID, metadata.StartedAt, metadata.CompletedAt, metadata.Images = uuid.New(), time.Now().UTC(), time.Now().UTC(), archives.images
		saved, err := f.r.PublishSnapshot(t.Context(), prepared, metadata)
		archives.Close()
		if err != nil {
			t.Fatal(err)
		}
		prior = &saved
		if priorFiles, err = f.r.Files(t.Context(), saved); err != nil {
			t.Fatal(err)
		}
		if pass == 0 {
			first = saved.ID
			registry.Close()
			docker.Close()
			worker = &Worker{Docker: api}
		}
	}
	expired, err := f.r.ApplyRetention(t.Context(), prior.InstallationID, prior.App.ID, prior.DestinationID, backup.Retention{Daily: 1, Weekly: 1, Monthly: 1}, prior.ID)
	if err != nil || !slices.Contains(expired, first) {
		t.Fatalf("old snapshot was not expired: %v %v", expired, err)
	}
	for _, variant := range prior.Images[0].Variants {
		input, size, err := f.r.OpenFile(t.Context(), prior.ID, variant.Archive)
		if err != nil {
			t.Fatal(err)
		}
		file, digest, copied, err := imagecheck.StageArchive(t.Context(), func(w io.Writer) error { _, err := io.Copy(w, input); return err })
		input.Close()
		if err != nil {
			t.Fatal(err)
		}
		if size != copied || size != variant.Size || digest != variant.Digest || variant.ID != ids[variant.Architecture] {
			t.Fatal("saved image changed")
		}
		if _, err := imagecheck.CheckArchive(t.Context(), file, variant.ID, variant.Architecture); err != nil {
			t.Fatal(err)
		}
		file.Close()
	}
	target := filepath.Join(t.TempDir(), "restored")
	if err := f.r.RestoreFiles(t.Context(), prior.ID, target); err != nil {
		t.Fatal(err)
	}
	if entries, err := os.ReadDir(filepath.Join(target, "images")); err != nil || len(entries) != 2 {
		t.Fatal("retention lost saved image files", err)
	}
}
