package image

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/registry"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/empty"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/google/go-containerregistry/pkg/v1/tarball"
	"github.com/google/go-containerregistry/pkg/v1/types"
	"github.com/google/uuid"
	"github.com/moby/moby/client"
)

func TestResolveRegistryPlatforms(t *testing.T) {
	server := httptest.NewServer(registry.New(registry.Logger(log.New(io.Discard, "", 0))))
	defer server.Close()
	repository := strings.TrimPrefix(server.URL, "http://") + "/apps/web"
	amd64, err := mutate.ConfigFile(empty.Image, &v1.ConfigFile{OS: "linux", Architecture: "amd64"})
	if err != nil {
		t.Fatal(err)
	}
	arm64, err := mutate.ConfigFile(empty.Image, &v1.ConfigFile{OS: "linux", Architecture: "arm64", Variant: "v8"})
	if err != nil {
		t.Fatal(err)
	}
	windows, err := mutate.ConfigFile(empty.Image, &v1.ConfigFile{OS: "windows", Architecture: "amd64"})
	if err != nil {
		t.Fatal(err)
	}
	for _, media := range []types.MediaType{types.OCIImageIndex, types.DockerManifestList} {
		t.Run(string(media), func(t *testing.T) {
			index := mutate.IndexMediaType(mutate.AppendManifests(empty.Index,
				mutate.IndexAddendum{Add: amd64, Descriptor: v1.Descriptor{Platform: &v1.Platform{OS: "linux", Architecture: "amd64"}}},
				mutate.IndexAddendum{Add: arm64, Descriptor: v1.Descriptor{Platform: &v1.Platform{OS: "linux", Architecture: "arm64", Variant: "v8"}}},
				mutate.IndexAddendum{Add: windows, Descriptor: v1.Descriptor{Platform: &v1.Platform{OS: "windows", Architecture: "amd64"}}},
			), media)
			ref, err := name.NewTag(repository + ":multi")
			if err != nil {
				t.Fatal(err)
			}
			if err := remote.WriteIndex(ref, index, remote.WithContext(t.Context())); err != nil {
				t.Fatal(err)
			}
			digest, err := index.Digest()
			if err != nil {
				t.Fatal(err)
			}
			input := ref.Name()
			result, err := (Inspector{}).Resolve(t.Context(), input)
			if err != nil {
				t.Fatal(err)
			}
			if result.LocalOnly || result.Image != repository+"@"+digest.String() || !reflect.DeepEqual(result.Architectures, []string{"amd64", "arm64"}) {
				t.Fatalf("resolution: %+v", result)
			}
			// Reusing the pin must preserve its architectures after the tag moves.
			if err := remote.Write(ref, amd64, remote.WithContext(t.Context())); err != nil {
				t.Fatal(err)
			}
			pinned, err := (Inspector{}).Resolve(t.Context(), result.Image)
			if err != nil || !reflect.DeepEqual(pinned.Architectures, []string{"amd64", "arm64"}) {
				t.Fatalf("pin changed: %+v %v", pinned, err)
			}
			moved, err := (Inspector{}).Resolve(t.Context(), input)
			if err != nil || !reflect.DeepEqual(moved.Architectures, []string{"amd64"}) {
				t.Fatalf("single image: %+v %v", moved, err)
			}
		})
	}
	// Nested indexes may omit platform descriptors. Inspect the leaf configs.
	nested := mutate.AppendManifests(empty.Index, mutate.IndexAddendum{Add: mutate.AppendManifests(empty.Index, mutate.IndexAddendum{Add: arm64})})
	ref, err := name.NewTag(repository + ":nested")
	if err != nil {
		t.Fatal(err)
	}
	if err := remote.WriteIndex(ref, nested, remote.WithContext(t.Context())); err != nil {
		t.Fatal(err)
	}
	result, err := (Inspector{}).Resolve(t.Context(), ref.Name())
	if err != nil || !reflect.DeepEqual(result.Architectures, []string{"arm64"}) {
		t.Fatalf("nested: %+v %v", result, err)
	}
}

func TestResolveRefusesUnusableManifests(t *testing.T) {
	server := httptest.NewServer(registry.New(registry.Logger(log.New(io.Discard, "", 0))))
	defer server.Close()
	ref, err := name.NewTag(strings.TrimPrefix(server.URL, "http://") + "/test:latest")
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name     string
		config   v1.ConfigFile
		platform *v1.Platform
		code     ErrorCode
	}{
		{"windows", v1.ConfigFile{OS: "windows", Architecture: "amd64"}, nil, UnsupportedPlatform},
		{"missing architecture", v1.ConfigFile{OS: "linux"}, nil, UnsupportedPlatform},
		{"unsupported ISA", v1.ConfigFile{OS: "linux", Architecture: "amd64", Variant: "v3"}, nil, UnsupportedPlatform},
		{"platform mismatch", v1.ConfigFile{OS: "linux", Architecture: "arm64"}, &v1.Platform{OS: "linux", Architecture: "amd64"}, InvalidManifest},
	} {
		t.Run(test.name, func(t *testing.T) {
			image, err := mutate.ConfigFile(empty.Image, &test.config)
			if err != nil {
				t.Fatal(err)
			}
			if test.platform != nil {
				err = remote.WriteIndex(ref, mutate.AppendManifests(empty.Index, mutate.IndexAddendum{Add: image, Descriptor: v1.Descriptor{Platform: test.platform}}), remote.WithContext(t.Context()))
			} else {
				err = remote.Write(ref, image, remote.WithContext(t.Context()))
			}
			if err != nil {
				t.Fatal(err)
			}
			result, err := (Inspector{}).Resolve(t.Context(), ref.Name())
			failure, ok := errors.AsType[*Error](err)
			if !ok || failure.Code != test.code || result.Image != "" {
				t.Fatalf("result: %+v %v", result, err)
			}
		})
	}
	_, err = (Inspector{}).Resolve(t.Context(), "https://bad/image")
	if failure, ok := errors.AsType[*Error](err); !ok || failure.Code != InvalidReference {
		t.Fatalf("invalid reference: %v", err)
	}
}

func TestResolveLocalDockerFallback(t *testing.T) {
	unavailable := httptest.NewServer(http.NotFoundHandler())
	defer unavailable.Close()
	var calls atomic.Int32
	imageID := "sha256:" + strings.Repeat("a", 64)
	docker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.Method != "GET" || !strings.Contains(r.URL.Path, "/images/") || !strings.HasSuffix(r.URL.Path, "/json") {
			t.Errorf("unexpected Docker action: %s %s", r.Method, r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(map[string]any{"Id": imageID, "Architecture": "arm64", "Os": "linux", "Variant": "v8"}); err != nil {
			t.Error(err)
		}
	}))
	defer docker.Close()
	daemon, err := client.New(client.WithHost(docker.URL), client.WithAPIVersion("1.56"), client.WithHTTPClient(docker.Client()))
	if err != nil {
		t.Fatal(err)
	}
	defer daemon.Close()
	spec := strings.TrimPrefix(unavailable.URL, "http://") + "/local:latest"
	result, err := (Inspector{Docker: daemon}).Resolve(t.Context(), spec)
	if err != nil || !result.LocalOnly || result.Image != imageID || !reflect.DeepEqual(result.Architectures, []string{"arm64"}) || calls.Load() != 1 {
		t.Fatalf("fallback: %+v %v, calls %d", result, err, calls.Load())
	}
	_, err = (Inspector{}).Resolve(t.Context(), spec)
	if failure, ok := errors.AsType[*Error](err); !ok || failure.Code != Unavailable {
		t.Fatalf("registry failure: %v", err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = (Inspector{Docker: daemon}).Resolve(ctx, spec)
	if !errors.Is(err, context.Canceled) || calls.Load() != 1 {
		t.Fatalf("cancel: %v; local calls %d", err, calls.Load())
	}
}

type testKeychain struct{}

func (testKeychain) Resolve(authn.Resource) (authn.Authenticator, error) {
	return authn.FromConfig(authn.AuthConfig{Username: "test", Password: "test-secret"}), nil
}

func TestResolveAuthenticatedRegistry(t *testing.T) {
	registryHandler := registry.New(registry.Logger(log.New(io.Discard, "", 0)))
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, password, ok := r.BasicAuth()
		if !ok || user != "test" || password != "test-secret" {
			w.Header().Set("WWW-Authenticate", `Basic realm="registry"`)
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		registryHandler.ServeHTTP(w, r)
	}))
	defer server.Close()
	ref, err := name.NewTag(strings.TrimPrefix(server.URL, "http://") + "/private:latest")
	if err != nil {
		t.Fatal(err)
	}
	image, err := mutate.ConfigFile(empty.Image, &v1.ConfigFile{OS: "linux", Architecture: "amd64"})
	if err != nil {
		t.Fatal(err)
	}
	if err := remote.Write(ref, image, remote.WithContext(t.Context()), remote.WithAuthFromKeychain(testKeychain{})); err != nil {
		t.Fatal(err)
	}
	result, err := (Inspector{Keychain: testKeychain{}}).Resolve(t.Context(), ref.Name())
	if err != nil || !reflect.DeepEqual(result.Architectures, []string{"amd64"}) {
		t.Fatalf("authenticated read: %+v %v", result, err)
	}
	_, err = (Inspector{}).Resolve(t.Context(), ref.Name())
	if failure, ok := errors.AsType[*Error](err); !ok || failure.Code != Unavailable {
		t.Fatalf("anonymous read: %v", err)
	}
}

func TestResolveRealDockerCache(t *testing.T) {
	host := os.Getenv("SYNOD_TEST_DOCKER_HOST")
	if host == "" {
		t.Skip("set SYNOD_TEST_DOCKER_HOST for the Docker cache proof")
	}
	daemon, err := client.New(client.WithHost(host))
	if err != nil {
		t.Fatal(err)
	}
	defer daemon.Close()
	registry := httptest.NewServer(http.NotFoundHandler())
	defer registry.Close()
	ref, err := name.NewTag(strings.TrimPrefix(registry.URL, "http://") + "/synod-inspect:" + uuid.NewString())
	if err != nil {
		t.Fatal(err)
	}
	image, err := mutate.ConfigFile(empty.Image, &v1.ConfigFile{OS: "linux", Architecture: "amd64", Config: v1.Config{Env: []string{"SYNOD_TEST=" + uuid.NewString()}}})
	if err != nil {
		t.Fatal(err)
	}
	var archive bytes.Buffer
	if err := tarball.Write(ref, image, &archive); err != nil {
		t.Fatal(err)
	}
	loaded, err := daemon.ImageLoad(t.Context(), &archive)
	if err != nil {
		t.Fatal(err)
	}
	_, readErr := io.Copy(io.Discard, loaded)
	if err := errors.Join(readErr, loaded.Close()); err != nil {
		t.Fatal(err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if _, err := daemon.ImageRemove(ctx, ref.Name(), client.ImageRemoveOptions{}); err != nil {
			t.Error(err)
		}
	}()
	result, err := (Inspector{Docker: daemon}).Resolve(t.Context(), ref.Name())
	if err != nil || !result.LocalOnly || !strings.HasPrefix(result.Image, "sha256:") || !reflect.DeepEqual(result.Architectures, []string{"amd64"}) {
		t.Fatalf("real cache resolution: %+v %v", result, err)
	}
	// Docker can change the imported config. The returned ID must still find the same cached image.
	pinned, err := daemon.ImageInspect(t.Context(), result.Image)
	if err != nil || pinned.ID != result.Image || pinned.Architecture != "amd64" {
		t.Fatalf("cached pin: %+v %v", pinned, err)
	}
}

func TestResolveMalformedRegistryManifest(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v2/" {
			w.WriteHeader(200)
			return
		}
		w.Header().Set("Content-Type", string(types.OCIImageIndex))
		_, _ = io.WriteString(w, `{"schemaVersion":2,"manifests":"invalid"}`)
	}))
	defer server.Close()
	result, err := (Inspector{}).Resolve(t.Context(), strings.TrimPrefix(server.URL, "http://")+"/invalid:latest")
	if failure, ok := errors.AsType[*Error](err); !ok || failure.Code != InvalidManifest || result.Image != "" {
		t.Fatalf("malformed manifest: %+v %v", result, err)
	}
}

func TestResolveRejectsInvalidDockerEvidence(t *testing.T) {
	registry := httptest.NewServer(http.NotFoundHandler())
	defer registry.Close()
	for _, test := range []struct {
		name, os, arch, variant, id string
		code                        ErrorCode
	}{
		{"windows", "windows", "amd64", "", "sha256:" + strings.Repeat("a", 64), UnsupportedPlatform},
		{"unknown architecture", "linux", "", "", "sha256:" + strings.Repeat("a", 64), UnsupportedPlatform},
		{"unsupported ISA", "linux", "arm64", "v9", "sha256:" + strings.Repeat("a", 64), UnsupportedPlatform},
		{"invalid ID", "linux", "arm64", "", "invalid", InvalidManifest},
	} {
		t.Run(test.name, func(t *testing.T) {
			docker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				if err := json.NewEncoder(w).Encode(map[string]string{"Id": test.id, "Architecture": test.arch, "Os": test.os, "Variant": test.variant}); err != nil {
					t.Error(err)
				}
			}))
			defer docker.Close()
			daemon, err := client.New(client.WithHost(docker.URL), client.WithAPIVersion("1.56"), client.WithHTTPClient(docker.Client()))
			if err != nil {
				t.Fatal(err)
			}
			defer daemon.Close()
			result, err := (Inspector{Docker: daemon}).Resolve(t.Context(), strings.TrimPrefix(registry.URL, "http://")+"/local:latest")
			failure, ok := errors.AsType[*Error](err)
			if !ok || failure.Code != test.code || result.Image != "" {
				t.Fatalf("unsafe local evidence: %+v %v", result, err)
			}
		})
	}
}
