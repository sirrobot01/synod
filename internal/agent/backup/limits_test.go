package backup

import (
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/kopia/kopia/snapshot"
	"github.com/moby/moby/api/types/container"
	"github.com/sirrobot01/synod/internal/backup"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
)

func TestSnapshotLimitsNameEachGapInCoverage(t *testing.T) {
	source := t.TempDir()
	app := &agentv1.DiscoveredApp{Key: "compose:limits", Containers: []*agentv1.DiscoveredContainer{{Id: "web", RepoDigests: []string{"nginx@sha256:1"}}, {Id: "local"}}, Paths: []*agentv1.DiscoveredPath{{Source: source, Kind: "bind", Status: "Included"}}}
	tree, err := NewTree(t.Context(), app, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tree.Close()
	small, large := int64(4<<20), int64(containerLayerLimit+1)
	full := backup.Snapshot{Images: []backup.Image{{ContainerID: "web", Architectures: []string{"amd64", "arm64"}}, {ContainerID: "local", LocalOnly: true, Architectures: []string{"arm64"}}}, Host: []backup.HostRequirements{{ContainerID: "web", PortsKnown: true, Paths: []backup.HostPath{{Path: "/dev/null", Known: true}}}}}
	inspected := []container.InspectResponse{{SizeRw: &small}, {}}
	if limits := snapshotLimits(app, nil, tree, inspected, full); len(limits) != 0 {
		t.Fatalf("a complete copy has limits: %v", limits)
	}
	withLarge := &agentv1.DiscoveredApp{Key: app.Key, Containers: app.Containers, Paths: []*agentv1.DiscoveredPath{app.Paths[0], {Source: "/srv/media", Status: "Large"}}}
	if limits := snapshotLimits(withLarge, nil, tree, inspected, full); !slices.Equal(limits, []string{backup.DataExcluded}) {
		t.Fatalf("skipped Large path: %v", limits)
	}
	if limits := snapshotLimits(withLarge, []string{"/srv/media"}, tree, inspected, full); len(limits) != 0 {
		t.Fatalf("an included Large path is still a limit: %v", limits)
	}
	withShare := &agentv1.DiscoveredApp{Key: app.Key, Containers: app.Containers, Paths: []*agentv1.DiscoveredPath{app.Paths[0], {Source: "/mnt/nas/photos", Status: "Remote"}}}
	if limits := snapshotLimits(withShare, nil, tree, inspected, full); !slices.Equal(limits, []string{backup.DataExcluded}) {
		t.Fatalf("skipped network share: %v", limits)
	}
	if limits := snapshotLimits(withShare, []string{"/mnt/nas/photos"}, tree, inspected, full); len(limits) != 0 {
		t.Fatalf("an included network share is still a limit: %v", limits)
	}
	offline := full
	offline.Images = []backup.Image{{ContainerID: "web", LocalOnly: true, Architectures: []string{"arm64"}}, full.Images[1]}
	if limits := snapshotLimits(app, nil, tree, inspected, offline); !slices.Equal(limits, []string{backup.ImagePlatformsLimited}) {
		t.Fatalf("registry image saved without the registry: %v", limits)
	}
	// A service without host networking has no recorded ports, and restore needs none.
	bridged := full
	bridged.Host = []backup.HostRequirements{{ContainerID: "web"}}
	webInspected := []container.InspectResponse{{ID: "web", SizeRw: &small, HostConfig: &container.HostConfig{NetworkMode: "bridge"}}}
	if limits := snapshotLimits(app, nil, tree, webInspected, bridged); len(limits) != 0 {
		t.Fatalf("a bridge service reported host requirements: %v", limits)
	}
	hostInspected := []container.InspectResponse{{ID: "web", SizeRw: &small, HostConfig: &container.HostConfig{NetworkMode: "host"}}}
	if limits := snapshotLimits(app, nil, tree, hostInspected, bridged); !slices.Equal(limits, []string{backup.HostRequirementsUnverified}) {
		t.Fatalf("a host-network service without recorded ports: %v", limits)
	}
	unknown := full
	unknown.Host = []backup.HostRequirements{{ContainerID: "web", PortsKnown: true, Paths: []backup.HostPath{{Path: "/dev/null"}}}}
	if limits := snapshotLimits(app, nil, tree, inspected, unknown); !slices.Equal(limits, []string{backup.HostRequirementsUnverified}) {
		t.Fatalf("unknown host path: %v", limits)
	}
	if limits := snapshotLimits(app, nil, tree, []container.InspectResponse{{SizeRw: &large}}, full); !slices.Equal(limits, []string{backup.ContainerLayerData}) {
		t.Fatalf("large writable layer: %v", limits)
	}
}

func TestWalkMarksSkippedMediaAsExcludedData(t *testing.T) {
	source := t.TempDir()
	if err := os.MkdirAll(filepath.Join(source, "media"), 0o700); err != nil {
		t.Fatal(err)
	}
	f := newIncrementalFixture(t, source, true)
	tree, err := NewTree(t.Context(), f.app, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tree.Close()
	root, err := tree.Root(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.r.UploadFiles(t.Context(), root, snapshot.SourceInfo{Host: "test", UserName: "test", Path: f.app.Key}, false); err != nil {
		t.Fatal(err)
	}
	if !tree.skippedData.Load() {
		t.Fatal("a skipped media folder was not reported")
	}
}
