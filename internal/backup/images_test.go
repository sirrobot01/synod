package backup

import (
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"strings"
	"testing"
)

func TestImageNamesItsRunningPlatformNotItsStoreID(t *testing.T) {
	variant := func(architecture, hex string) ImageVariant {
		id := "sha256:" + strings.Repeat(hex, 64)
		return ImageVariant{Architecture: architecture, ID: id, Archive: "images/" + strings.Repeat(hex, 64) + ".tar", Digest: id, Size: 1}
	}
	// The containerd image store gives the running image an index digest that no variant has.
	image := Image{ContainerID: "web", SourceID: "sha256:" + strings.Repeat("c", 64), Reference: "nginx@sha256:" + strings.Repeat("c", 64), Architectures: []string{"amd64", "arm64"}, SourceArchitecture: "arm64", Variants: []ImageVariant{variant("amd64", "a"), variant("arm64", "b")}}
	if err := image.Validate(); err != nil {
		t.Fatalf("a containerd-store image was refused: %v", err)
	}
	missing := image
	missing.Variants = missing.Variants[:1]
	missing.Architectures = []string{"amd64"}
	if missing.Validate() == nil {
		t.Fatal("an image without its running platform was accepted")
	}
	unnamed := image
	unnamed.SourceArchitecture = ""
	if unnamed.Validate() == nil {
		t.Fatal("an image without its running platform name was accepted")
	}
}

func TestNetworkSharesCanBeIncludedLikeLargePaths(t *testing.T) {
	app := &agentv1.DiscoveredApp{Paths: []*agentv1.DiscoveredPath{
		{Source: "/srv/app", Status: "Included"},
		{Source: "/mnt/nas/photos", Status: "Remote"},
		{Source: "/srv/app/media", Kind: "mount", Status: "Remote"},
	}}
	if err := CheckIncluded(app, []string{"/mnt/nas/photos", "/srv/app/media"}); err != nil {
		t.Fatalf("whole network shares were refused: %v", err)
	}
	if CheckIncluded(app, []string{"/mnt/nas/photos/2024"}) == nil {
		t.Fatal("a folder inside a network share was accepted")
	}
}
