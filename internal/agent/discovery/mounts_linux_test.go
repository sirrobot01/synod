package discovery

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/sirrobot01/synod/internal/runtime/host"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
)

func TestSizeScanStopsAtANestedMount(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("mounting a filesystem needs root")
	}
	source, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	nested := filepath.Join(source, "share")
	if err := os.Mkdir(nested, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := syscall.Mount("tmpfs", nested, "tmpfs", 0, "size=1m"); err != nil {
		t.Skip("tmpfs mount unavailable:", err)
	}
	t.Cleanup(func() { syscall.Unmount(nested, 0) })
	os.WriteFile(filepath.Join(source, "a"), make([]byte, 10), 0o600)
	os.WriteFile(filepath.Join(nested, "b"), make([]byte, 100), 0o600)
	table, err := host.ReadMounts()
	if err != nil {
		t.Fatal(err)
	}
	parent := &agentv1.DiscoveredPath{Source: source, Kind: "bind", Target: "/data"}
	measure(t.Context(), parent, table)
	if !parent.SizeKnown || parent.SizeBytes != 10 {
		t.Fatalf("the size scan crossed into the nested mount: %d bytes", parent.SizeBytes)
	}
	found := nestedMounts(t.Context(), []*agentv1.DiscoveredPath{parent}, table)
	if len(found) != 1 || found[0].Source != nested || found[0].Target != "/data/share" || found[0].SizeBytes != 100 {
		t.Fatalf("nested mount: %+v", found)
	}
}
