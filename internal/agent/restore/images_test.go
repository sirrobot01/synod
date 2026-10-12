package restore

import (
	"bytes"
	"context"
	"encoding/json/v2"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/containerd/errdefs"
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
	"github.com/sirrobot01/synod/internal/inventory"
	"github.com/sirrobot01/synod/internal/repository"
	"github.com/sirrobot01/synod/internal/runtime/compose"
	imagecheck "github.com/sirrobot01/synod/internal/runtime/image"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
)

func TestImageRestoreChecksAllContentBeforeLoading(t *testing.T) {
	for _, test := range []string{"good", "checksum", "wrong platform", "missing", "docker error", "changed layers"} {
		t.Run(test, func(t *testing.T) {
			r, err := repository.Create(t.Context(), repository.Location{Kind: "local", Path: t.TempDir()}, repository.Credentials{Password: "image-test-password"})
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close(context.Background())
			metadata := backup.Snapshot{ID: uuid.New(), InstallationID: uuid.New(), DestinationID: uuid.New(), App: inventory.App{ID: uuid.New(), NodeID: uuid.New(), Definition: &agentv1.DiscoveredApp{}}, Method: backup.Live, Architecture: "arm64", StartedAt: time.Now().UTC(), CompletedAt: time.Now().UTC()}
			var entries []kfs.Entry
			for index := range 2 {
				img, err := random.Image(4096, 1)
				if err != nil {
					t.Fatal(err)
				}
				config, _ := img.ConfigFile()
				config.OS, config.Architecture = "linux", "arm64"
				if test == "wrong platform" && index == 1 {
					config.Architecture = "amd64"
				}
				img, err = mutate.ConfigFile(img, config)
				if err != nil {
					t.Fatal(err)
				}
				id, _ := img.ConfigName()
				// A saved source tag must never overwrite a target tag.
				ref, _ := name.NewTag("synod.invalid/existing:latest")
				file, digest, size, err := imagecheck.StageArchive(t.Context(), func(w io.Writer) error { return tarball.Write(ref, img, w) })
				if err != nil {
					t.Fatal(err)
				}
				defer file.Close()
				variant := backup.ImageVariant{Architecture: "arm64", ID: id.String(), Archive: "images/" + id.Hex + ".tar", Digest: digest, Size: size}
				metadata.Images = append(metadata.Images, backup.Image{ContainerID: uuid.NewString(), SourceID: id.String(), Reference: "unreachable.invalid/image@" + id.String(), Architectures: []string{"arm64"}, SourceArchitecture: "arm64", Variants: []backup.ImageVariant{variant}})
				if test == "checksum" && index == 1 {
					file.WriteAt([]byte("broken"), 0)
				}
				if test == "missing" && index == 1 {
					continue
				}
				entries = append(entries, virtualfs.StreamingFileFromReader(path.Base(variant.Archive), io.NopCloser(io.NewSectionReader(file, 0, size))))
			}
			prepared, err := r.UploadFiles(t.Context(), virtualfs.NewStaticDirectory("app", []kfs.Entry{virtualfs.NewStaticDirectory("images", entries)}), snapshot.SourceInfo{Host: "test", UserName: "test", Path: "images"}, false)
			if err != nil {
				t.Fatal(err)
			}
			saved, err := r.PublishSnapshot(t.Context(), prepared, metadata)
			if err != nil {
				t.Fatal(err)
			}
			requests, loaded := 0, 0
			// Like the containerd image store, the fake names a loaded image by its manifest digest.
			layers := make(map[string][]string)
			docker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				requests++
				if req.Method == "POST" && strings.HasSuffix(req.URL.Path, "/images/load") {
					loaded++
					file, _, size, err := imagecheck.StageArchive(t.Context(), func(w io.Writer) error { _, err := io.Copy(w, req.Body); return err })
					if err != nil {
						t.Error(err)
						http.Error(w, "read", 500)
						return
					}
					defer file.Close()
					manifests, err := tarball.LoadManifest(func() (io.ReadCloser, error) { return io.NopCloser(io.NewSectionReader(file, 0, size)), nil })
					if err != nil || len(manifests) != 1 || len(manifests[0].RepoTags) != 0 {
						t.Error("load retained source tags or extra images", err)
					}
					if test == "docker error" {
						io.WriteString(w, `{"error":"private daemon error"}`)
						return
					}
					img, err := tarball.Image(func() (io.ReadCloser, error) { return io.NopCloser(io.NewSectionReader(file, 0, size)), nil }, nil)
					if err != nil {
						t.Error(err)
						return
					}
					digest, _ := img.Digest()
					config, _ := img.ConfigFile()
					layers[digest.String()] = layerDigests(config.RootFS.DiffIDs)
					if test == "changed layers" {
						layers[digest.String()] = []string{"sha256:" + strings.Repeat("0", 64)}
					}
					io.WriteString(w, `{"stream":"Loaded image ID: `+digest.String()+`\n"}`)
					return
				}
				if req.Method == "GET" && strings.HasSuffix(req.URL.Path, "/json") {
					id := strings.TrimSuffix(strings.Split(req.URL.Path, "/images/")[1], "/json")
					json.MarshalWrite(w, struct {
						ID           string `json:"Id"`
						OS           string `json:"Os"`
						Architecture string
						RootFS       struct {
							Type   string
							Layers []string
						}
					}{id, "linux", "arm64", struct {
						Type   string
						Layers []string
					}{"layers", layers[id]}})
					return
				}
				t.Errorf("unexpected Docker action %s %s", req.Method, req.URL.Path)
				http.Error(w, "unexpected", 500)
			}))
			defer docker.Close()
			api, err := client.New(client.WithHost(docker.URL), client.WithAPIVersion("1.56"), client.WithHTTPClient(docker.Client()))
			if err != nil {
				t.Fatal(err)
			}
			defer api.Close()
			p := &plan{Snapshot: saved, Definition: &compose.Definition{Services: make(map[string]compose.Service)}}
			for i, saved := range saved.Images {
				p.Definition.Services[strconv.Itoa(i)] = compose.Service{Image: saved.Variants[0].ID}
			}
			err = loadImages(t.Context(), api, r, p, "arm64")
			if test == "good" {
				if err != nil || loaded != 2 {
					t.Fatalf("offline image load: %d %v", loaded, err)
				}
				// Services start the image that Docker named on load.
				for _, service := range p.Definition.Services {
					if _, ok := layers[service.Image]; !ok {
						t.Fatalf("service image %s is not a loaded image", service.Image)
					}
				}
			} else if test == "docker error" || test == "changed layers" {
				if err == nil || strings.Contains(err.Error(), "private") {
					t.Fatal("Docker load error was lost or exposed", err)
				}
			} else if err == nil || requests != 0 {
				t.Fatalf("invalid archive reached Docker: requests=%d err=%v", requests, err)
			}
		})
	}
}

func TestRestoreUsesSavedImagesWithEmptyCache(t *testing.T) {
	host := os.Getenv("SYNOD_TEST_DOCKER_HOST")
	if host == "" || runtime.GOOS != "linux" {
		t.Skip("set SYNOD_TEST_DOCKER_HOST on Linux for source-loss recovery")
	}
	api, err := client.New(client.WithHost(host), client.WithAPIVersionNegotiation())
	if err != nil {
		t.Fatal(err)
	}
	defer api.Close()
	inspected, err := api.ImageInspect(t.Context(), helperImage)
	if err != nil {
		t.Fatal(err)
	}
	input, err := api.ImageSave(t.Context(), []string{inspected.ID})
	if err != nil {
		t.Fatal(err)
	}
	base, _, _, err := imagecheck.StageArchive(t.Context(), func(w io.Writer) error { _, err := io.Copy(w, input); return err })
	input.Close()
	if err != nil {
		t.Fatal(err)
	}
	defer base.Close()
	baseImage, err := imagecheck.CheckArchive(t.Context(), base, inspected.ID, inspected.Architecture)
	if err != nil {
		t.Fatal(err)
	}
	for _, source := range []string{"local", "registry"} {
		t.Run(source, func(t *testing.T) {
			config, err := baseImage.ConfigFile()
			if err != nil {
				t.Fatal(err)
			}
			config.Config.Labels = map[string]string{"synod.test": uuid.NewString()}
			img, err := mutate.ConfigFile(baseImage, config)
			if err != nil {
				t.Fatal(err)
			}
			id, err := img.ConfigName()
			if err != nil {
				t.Fatal(err)
			}
			var archive bytes.Buffer
			ref, _ := name.NewDigest("synod.invalid/image@" + id.String())
			if err := tarball.Write(ref, img, &archive); err != nil {
				t.Fatal(err)
			}
			loaded, err := api.ImageLoad(t.Context(), bytes.NewReader(archive.Bytes()))
			if err != nil {
				t.Fatal(err)
			}
			_, err = io.Copy(io.Discard, loaded)
			loaded.Close()
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { api.ImageRemove(context.Background(), id.String(), client.ImageRemoveOptions{Force: true}) })
			sourceImage := id.String()
			var registryServer *httptest.Server
			if source == "registry" {
				registryServer = httptest.NewServer(registry.New(registry.Logger(log.New(io.Discard, "", 0))))
				defer registryServer.Close()
				tag, _ := name.NewTag(strings.TrimPrefix(registryServer.URL, "http://") + "/test:current")
				otherArchitecture := "amd64"
				if inspected.Architecture == "amd64" {
					otherArchitecture = "arm64"
				}
				baseRef, _ := name.NewDigest(helperImage)
				other, err := remote.Image(baseRef, remote.WithContext(t.Context()), remote.WithPlatform(v1.Platform{OS: "linux", Architecture: otherArchitecture}))
				if err != nil {
					t.Fatal(err)
				}
				otherConfig, err := other.ConfigFile()
				if err != nil {
					t.Fatal(err)
				}
				otherConfig.Config.Labels = config.Config.Labels
				other, err = mutate.ConfigFile(other, otherConfig)
				if err != nil {
					t.Fatal(err)
				}
				index := mutate.AppendManifests(empty.Index, mutate.IndexAddendum{Add: img, Descriptor: v1.Descriptor{Platform: &v1.Platform{OS: "linux", Architecture: inspected.Architecture}}}, mutate.IndexAddendum{Add: other, Descriptor: v1.Descriptor{Platform: &v1.Platform{OS: "linux", Architecture: otherArchitecture}}})
				if err := remote.WriteIndex(tag, index, remote.WithContext(t.Context())); err != nil {
					t.Fatal(err)
				}
				digest, _ := index.Digest()
				sourceImage = tag.Context().Digest(digest.String()).Name()
				pulled, err := api.ImagePull(t.Context(), sourceImage, client.ImagePullOptions{})
				if err != nil {
					t.Fatal(err)
				}
				if err := pulled.Wait(t.Context()); err != nil {
					t.Fatal(err)
				}
			}
			f := buildFixture(t, false, false, sourceImage)
			expectedVariants := 1
			if source == "registry" {
				expectedVariants = 2
			}
			if len(f.saved.Images) != 1 || len(f.saved.Images[0].Variants) != expectedVariants {
				t.Fatal("source image platforms were not saved")
			}
			for _, variant := range f.saved.Images[0].Variants {
				if variant.Architecture == inspected.Architecture && variant.ID != id.String() {
					t.Fatal("source image identity changed")
				}
			}
			if (source == "local") != f.saved.Images[0].LocalOnly {
				t.Fatal("source coverage is wrong")
			}
			removeFixture(api, f.name)
			if registryServer != nil {
				registryServer.Close()
			}
			if _, err := api.ImageRemove(t.Context(), id.String(), client.ImageRemoveOptions{Force: true}); err != nil {
				t.Fatal(err)
			}
			if _, err := api.ImageInspect(t.Context(), id.String()); !errdefs.IsNotFound(err) {
				t.Fatal("fixture image cache is not empty", err)
			}
			if err := os.RemoveAll(f.project); err != nil {
				t.Fatal(err)
			}
			w := &Worker{Backup: f.worker, Directory: t.TempDir()}
			request := newRequest(f, false)
			t.Cleanup(func() { removeLabelled(context.Background(), api, request.ID, true) })
			result, err := w.Execute(t.Context(), request)
			if err != nil || result.FailureCode != "" {
				t.Fatalf("recover after source and cache loss: %+v %v", result, err)
			}
			actual, err := api.ContainerInspect(t.Context(), f.name, client.ContainerInspectOptions{})
			if err != nil || !actual.Container.State.Running || actual.Container.Image != id.String() {
				t.Fatal("restored image is not running", err)
			}
			var output bytes.Buffer
			if err := execCommand(t.Context(), api, f.name, []string{"cat", "/config/state"}, nil, &output); err != nil || output.String() != "private-restored-value" {
				t.Fatal("restored app data changed", err)
			}
			if _, err := os.Stat(filepath.Join(result.ProjectDirectory, "compose.json")); err != nil {
				t.Fatal(err)
			}
		})
	}
}
