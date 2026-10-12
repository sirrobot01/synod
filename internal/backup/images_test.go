package backup

import (
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
