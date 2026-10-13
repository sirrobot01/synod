package discovery

import (
	"os"
	"path/filepath"
	"testing"

	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
)

func TestPathSafetyAndSizes(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "state"), []byte("state bytes"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("/etc", filepath.Join(dir, "system-link")); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		source, target, status string
		known                  bool
		size                   int64
	}{
		{dir, "/config", "Included", true, 11},
		{filepath.Join(dir, "state"), "/config", "Included", true, 11},
		{dir, "/movies", "Large", false, 0},
		{filepath.Join(dir, "system-link"), "/config", "Never", false, 0},
		{"/", "/host", "Never", false, 0},
		{"/etc/synod-does-not-exist", "/config", "Never", false, 0},
		{"/proc", "/proc", "Never", false, 0},
		{dir, "/var/run/docker.sock", "Never", false, 0},
		{filepath.Join(dir, "missing"), "/config", "Included", false, 0},
	} {
		path := &agentv1.DiscoveredPath{Source: test.source, Target: test.target, Kind: "bind"}
		measure(t.Context(), path, nil)
		if path.Status != test.status || path.SizeKnown != test.known || path.SizeBytes != test.size || path.Reason == "" {
			t.Fatalf("%s -> %s: %+v", test.source, test.target, path)
		}
	}
	large, err := os.Create(filepath.Join(t.TempDir(), "sparse"))
	if err != nil {
		t.Fatal(err)
	}
	defer large.Close()
	if err := large.Truncate(largeBytes + 1); err != nil {
		t.Fatal(err)
	}
	path := &agentv1.DiscoveredPath{Source: large.Name(), Kind: "bind"}
	measure(t.Context(), path, nil)
	if path.Status != "Large" || path.SizeBytes != largeBytes+1 || !path.SizeKnown {
		t.Fatalf("large file: %+v", path)
	}
	path = &agentv1.DiscoveredPath{Source: filepath.Dir(large.Name()), Kind: "bind"}
	measure(t.Context(), path, nil)
	if path.Status != "Large" || path.SizeBytes <= largeBytes || path.SizeKnown {
		t.Fatalf("bounded size walk: %+v", path)
	}
}

func TestDockerVolumeBoundary(t *testing.T) {
	for _, test := range []struct {
		path              string
		volume, forbidden bool
	}{
		{"/var/lib/docker", true, true},
		{"/var/lib/docker/volumes", true, true},
		{"/var/lib/docker/volumes/test/_data", true, false},
		{"/var/lib/docker/volumes/test/_data/state", true, false},
		{"/var/lib/docker/volumes/test/_data", false, true},
		{"/var/lib/docker/volumes/test/_data/../../", true, true},
	} {
		if got := Forbidden(test.path, test.volume); got != test.forbidden {
			t.Fatalf("%+v: %t", test, got)
		}
	}
}
