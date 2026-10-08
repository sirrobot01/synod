package backup

import (
	"context"
	"github.com/sirrobot01/synod/internal/runtime/image"
	"io"
	"log"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/registry"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/empty"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
)

func TestImageEvidenceUsesRunningDigestAfterTagMoves(t *testing.T) {
	server := httptest.NewServer(registry.New(registry.Logger(log.New(io.Discard, "", 0))))
	defer server.Close()
	amd64, err := mutate.ConfigFile(empty.Image, &v1.ConfigFile{OS: "linux", Architecture: "amd64"})
	if err != nil {
		t.Fatal(err)
	}
	arm64, err := mutate.ConfigFile(empty.Image, &v1.ConfigFile{OS: "linux", Architecture: "arm64"})
	if err != nil {
		t.Fatal(err)
	}
	index := mutate.AppendManifests(empty.Index, mutate.IndexAddendum{Add: amd64}, mutate.IndexAddendum{Add: arm64})
	ref, err := name.NewTag(strings.TrimPrefix(server.URL, "http://") + "/fixture:current")
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
	pin := ref.Context().Digest(digest.String()).Name()
	if err := remote.Write(ref, amd64, remote.WithContext(t.Context())); err != nil {
		t.Fatal(err)
	}
	app := &agentv1.DiscoveredApp{Containers: []*agentv1.DiscoveredContainer{{Id: "first", Image: ref.Name(), ImageId: "sha256:" + strings.Repeat("a", 64), Platform: "linux/arm64/v8", RepoDigests: []string{pin}}, {Id: "second", Image: ref.Name(), ImageId: "sha256:" + strings.Repeat("a", 64), Platform: "linux/arm64/v8", RepoDigests: []string{pin}}}}
	evidence, err := (&Worker{}).imageEvidence(t.Context(), app)
	if err != nil || len(evidence) != 2 {
		t.Fatalf("resolve running images: %v", err)
	}
	for i, image := range evidence {
		if image.ContainerID != app.Containers[i].Id || image.Reference != pin || image.LocalOnly || !slices.Equal(image.Architectures, []string{"amd64", "arm64"}) {
			t.Fatalf("tag changed restore evidence: %+v", image)
		}
	}
	app.Containers = []*agentv1.DiscoveredContainer{{Id: "local", ImageId: "sha256:" + strings.Repeat("a", 64), Platform: "linux/arm64/v8"}}
	evidence, err = (&Worker{}).imageEvidence(t.Context(), app)
	if err != nil || len(evidence) != 1 || !evidence[0].LocalOnly || !slices.Equal(evidence[0].Architectures, []string{"arm64"}) || evidence[0].Reference != "sha256:"+strings.Repeat("a", 64) {
		t.Fatalf("local image evidence: %+v %v", evidence, err)
	}
	app.Containers[0].RepoDigests = []string{strings.TrimPrefix(server.URL, "http://") + "/missing@sha256:" + strings.Repeat("b", 64)}
	evidence, err = (&Worker{}).imageEvidence(t.Context(), app)
	if err != nil || !evidence[0].LocalOnly || !slices.Equal(evidence[0].Architectures, []string{"arm64"}) {
		t.Fatalf("offline registry prevented backup: %+v %v", evidence, err)
	}
	for _, platform := range []string{"windows/amd64", "linux/arm64/v9"} {
		app.Containers[0].Platform = platform
		if _, err := (&Worker{}).imageEvidence(t.Context(), app); err == nil {
			t.Fatal("accepted unsupported local image")
		}
	}
}

func TestImageEvidenceReusesCanonicalRegistryNames(t *testing.T) {
	worker := &Worker{}
	digest := "sha256:" + strings.Repeat("b", 64)
	canonical := "index.docker.io/library/alpine@" + digest
	worker.images.Store(canonical, image.Resolution{Image: canonical, Architectures: []string{"amd64", "arm64"}})
	// Docker reports shorthand names. A registry outage must not discard saved coverage.
	for _, reference := range []string{"alpine@" + digest, "docker.io/library/alpine@" + digest, canonical} {
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		evidence, err := worker.imageEvidence(ctx, &agentv1.DiscoveredApp{Containers: []*agentv1.DiscoveredContainer{{Id: "local", ImageId: "sha256:" + strings.Repeat("a", 64), Platform: "linux/arm64", RepoDigests: []string{"unavailable.invalid/alias@" + digest, reference}}}})
		cancel()
		if err != nil || len(evidence) != 1 || evidence[0].LocalOnly || evidence[0].Reference != canonical || len(evidence[0].Architectures) != 2 {
			t.Fatalf("lost saved coverage for %s: %+v %v", reference, evidence, err)
		}
	}
}
