package restore

import (
	"os"
	"testing"

	"github.com/sirrobot01/synod/internal/backup"
	"github.com/sirrobot01/synod/internal/inventory"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
)

func TestRestoreAttachesUncopiedSharesAndDrillsDoNot(t *testing.T) {
	share := &agentv1.DiscoveredPath{Source: "/mnt/nas/photos", Status: "Remote"}
	never := &agentv1.DiscoveredPath{Source: "/var/run/docker.sock", Status: "Never"}
	large := &agentv1.DiscoveredPath{Source: "/srv/media", Status: "Large"}
	saved := backup.Snapshot{}
	for _, test := range []struct {
		item          *agentv1.DiscoveredPath
		copied, drill bool
		want          bool
	}{
		{item: share, want: true},
		{item: share, drill: true, want: false},
		{item: share, copied: true, want: false},
		{item: never, want: true},
		{item: never, drill: true, want: true},
		{item: large, want: false},
	} {
		saved.Paths = nil
		if test.copied {
			saved.Paths = []backup.Path{{Source: test.item.Source}}
		}
		if got := attached(saved, test.item, test.drill); got != test.want {
			t.Errorf("%s copied=%t drill=%t: attached = %t", test.item.Source, test.copied, test.drill, got)
		}
	}
}

func TestNestedMountsGoInsideTheirDeepestParent(t *testing.T) {
	saved := backup.Snapshot{App: inventory.App{Definition: &agentv1.DiscoveredApp{Paths: []*agentv1.DiscoveredPath{
		{Source: "/srv/app/data/media", Kind: "mount", Status: "Remote"},
		{Source: "/srv/other", Kind: "bind", Status: "Included"},
	}}}}
	p := &plan{Data: []dataRoot{
		{Source: "/srv/app", Mode: os.ModeDir},
		{Source: "/srv/app/data", Mode: os.ModeDir},
		{Source: "/srv/app/data/file", Mode: 0o600},
	}}
	parent, subpath, ok := mountParent(p, saved, "/srv/app/data/media")
	if !ok || parent != 1 || subpath != "media" {
		t.Fatalf("parent %d, subpath %q, ok %t", parent, subpath, ok)
	}
	if _, _, ok := mountParent(p, saved, "/srv/other"); ok {
		t.Fatal("a path that is not a nested mount was grafted")
	}
}
