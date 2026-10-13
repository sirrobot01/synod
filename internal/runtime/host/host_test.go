package host

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/moby/moby/api/types/container"
	"github.com/sirrobot01/synod/internal/backup"
)

func TestPortOverlap(t *testing.T) {
	for _, test := range []struct {
		a, b string
		want bool
	}{
		{"127.0.0.1", "127.0.0.1", true}, {"127.0.0.1", "127.0.0.2", false},
		{"0.0.0.0", "127.0.0.1", true}, {"::", "127.0.0.1", true},
		{"::ffff:127.0.0.1", "127.0.0.1", true}, {"::1", "127.0.0.1", false},
		{"::1", "0.0.0.0", false}, {"::1", "::", true},
	} {
		a := backup.HostPort{Address: test.a, Port: 1234, Protocol: "tcp"}
		b := backup.HostPort{Address: test.b, Port: 1234, Protocol: "tcp"}
		if Overlaps(a, b) != test.want || Overlaps(b, a) != test.want {
			t.Errorf("overlap %s and %s: want %t", test.a, test.b, test.want)
		}
		b.Protocol = "udp"
		if Overlaps(a, b) {
			t.Fatal("TCP and UDP conflict")
		}
		b.Protocol, b.Port = "tcp", 1235
		if Overlaps(a, b) {
			t.Fatal("different ports conflict")
		}
	}
}

func TestHostNetworkIncludesJoinedServices(t *testing.T) {
	app := []container.InspectResponse{
		{ID: "main", HostConfig: &container.HostConfig{NetworkMode: "host"}},
		{ID: "joined", HostConfig: &container.HostConfig{NetworkMode: "container:main"}},
		{ID: "bridge", HostConfig: &container.HostConfig{NetworkMode: "bridge"}},
		{ID: "cycle", HostConfig: &container.HostConfig{NetworkMode: "container:cycle"}},
	}
	for i, c := range app {
		if UsesHostNetwork(c, app) != (i < 2) {
			t.Fatal("wrong host network scope", c.ID)
		}
	}
}

func TestLocalPathReadsTheDaemonRoot(t *testing.T) {
	directory := t.TempDir()
	if err := os.MkdirAll(filepath.Join(directory, "srv", "app"), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("app", filepath.Join(directory, "srv", "relative")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("/etc", filepath.Join(directory, "srv", "absolute")); err != nil {
		t.Fatal(err)
	}
	root, err := os.OpenRoot(directory)
	if err != nil {
		t.Fatal(err)
	}
	view := &Local{root: root}
	defer view.Close()
	if p := view.Path("/srv/app"); !p.Known || os.FileMode(p.Mode).Perm() != 0o750 || !os.FileMode(p.Mode).IsDir() {
		t.Fatalf("daemon path: %+v", p)
	}
	if p := view.Path("/srv/relative"); !p.Known {
		t.Fatal("a relative link inside the daemon root was not followed")
	}
	if p := view.Path("/srv/absolute"); p.Known {
		t.Fatal("an absolute link left the daemon root")
	}
	if p := view.Path("/missing"); p.Known {
		t.Fatal("a missing path is known")
	}
	var native *Local
	if p := native.Path(directory); !p.Known {
		t.Fatal("the agent's own view cannot read its paths")
	}
}
