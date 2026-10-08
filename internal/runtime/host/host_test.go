package host

import (
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
