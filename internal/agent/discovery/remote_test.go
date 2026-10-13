package discovery

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/sirrobot01/synod/internal/runtime/host"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
)

func TestNetworkSharesAreRemoteAndNestedMountsGetTheirOwnPath(t *testing.T) {
	app, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	for _, directory := range []string{"media", "pool", "cache"} {
		if err := os.MkdirAll(filepath.Join(app, directory), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	share, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	table := []host.Mount{
		{Root: "/", Point: "/", Type: "ext4"},
		{Root: "/", Point: share, Type: "nfs4"},
		{Root: "/", Point: filepath.Join(app, "media"), Type: "cifs"},
		{Root: "/", Point: filepath.Join(app, "pool"), Type: "fuse.mergerfs"},
	}
	remote := &agentv1.DiscoveredPath{Source: share, Kind: "bind", Target: "/photos"}
	measure(t.Context(), remote, table)
	if remote.Status != "Remote" || remote.SizeKnown {
		t.Fatalf("a network share was not Remote or was walked: %+v", remote)
	}
	parent := &agentv1.DiscoveredPath{Source: app, Kind: "bind", Target: "/data", ContainerId: "c1"}
	measure(t.Context(), parent, table)
	nested := nestedMounts(t.Context(), []*agentv1.DiscoveredPath{parent, remote}, table)
	if len(nested) != 2 {
		t.Fatalf("nested mounts: %+v", nested)
	}
	byTarget := map[string]*agentv1.DiscoveredPath{}
	for _, path := range nested {
		byTarget[path.Target] = path
		if path.Kind != "mount" || path.ContainerId != "c1" {
			t.Fatalf("nested mount: %+v", path)
		}
	}
	if byTarget["/data/media"].Status != "Remote" || byTarget["/data/pool"].Status != "Included" {
		t.Fatalf("nested statuses: media %s, pool %s", byTarget["/data/media"].Status, byTarget["/data/pool"].Status)
	}
}
