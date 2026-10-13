package discovery

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
)

func TestDirectorySizesAreReusedUntilTheyExpire(t *testing.T) {
	directory, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(directory, "a"), make([]byte, 100), 0o600); err != nil {
		t.Fatal(err)
	}
	size := func() int64 {
		t.Helper()
		path := &agentv1.DiscoveredPath{Source: directory, Kind: "bind"}
		measure(t.Context(), path, nil)
		if !path.SizeKnown {
			t.Fatalf("size unknown: %s", path.Reason)
		}
		return path.SizeBytes
	}
	if got := size(); got != 100 {
		t.Fatalf("first size %d", got)
	}
	if err := os.WriteFile(filepath.Join(directory, "b"), make([]byte, 50), 0o600); err != nil {
		t.Fatal(err)
	}
	if got := size(); got != 100 {
		t.Fatalf("a fresh size was measured again: %d", got)
	}
	sizes.mu.Lock()
	old := sizes.sizes[directory]
	old.measured = time.Now().Add(-sizeLifetime - time.Second)
	sizes.sizes[directory] = old
	sizes.mu.Unlock()
	if got := size(); got != 150 {
		t.Fatalf("an expired size was reused: %d", got)
	}
}
