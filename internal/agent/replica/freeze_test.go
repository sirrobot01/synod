package replica

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// A directory of plain files stands in for a cgroup that never reports frozen.
func TestCgroupFreezeTimeoutThaws(t *testing.T) {
	dir := t.TempDir()
	for name, content := range map[string]string{"cgroup.freeze": "0", "cgroup.events": "populated 1\nfrozen 0\n"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	freezer := CgroupFreezer{Dir: dir, Limit: 20 * time.Millisecond}
	start := time.Now()
	if err := freezer.Freeze(t.Context()); !errors.Is(err, ErrFreezeTimeout) {
		t.Fatalf("freeze: %v", err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("freeze waited %s", elapsed)
	}
	if state, err := os.ReadFile(filepath.Join(dir, "cgroup.freeze")); err != nil || string(state) != "0" {
		t.Fatalf("the cgroup was left frozen: %q %v", state, err)
	}
}
