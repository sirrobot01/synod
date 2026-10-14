package backup

import (
	"bytes"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/agent/journal"
	"github.com/sirrobot01/synod/internal/backup"
	"github.com/sirrobot01/synod/internal/logging"
	"github.com/sirrobot01/synod/internal/secret"
)

// A backup that fails before it copies anything still logs its start, code, and cause.
func TestBackupLogsItsFailureCause(t *testing.T) {
	installation, node := uuid.New(), uuid.New()
	cache, err := journal.Open(t.Context(), filepath.Join(t.TempDir(), "agent.db"), installation, node)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cache.Close() })
	keyID := uuid.New()
	ring, err := secret.NewKeyring(keyID, map[uuid.UUID][]byte{keyID: bytes.Repeat([]byte{9}, 32)})
	if err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	logger, err := logging.New(&out, "", "")
	if err != nil {
		t.Fatal(err)
	}
	w := &Worker{Journal: cache, Keyring: ring, InstallationID: installation, NodeID: node, Logger: logger}
	request := backup.Request{ID: uuid.New(), InstallationID: installation, AppID: uuid.NewSHA1(node, []byte("compose:notes")), NodeID: node, SourceKey: "compose:notes", DestinationID: uuid.New(), Method: backup.Auto, RequestedAt: time.Now().UTC()}
	result, err := w.Execute(t.Context(), request)
	if err != nil || result.FailureCode != "destination_unavailable" {
		t.Fatalf("backup without destination settings: %+v %v", result, err)
	}
	log := out.String()
	for _, want := range []string{`msg="backup started" app=notes`, `msg="backup failed" app=notes`, "code=destination_unavailable", "error="} {
		if !strings.Contains(log, want) {
			t.Errorf("log lacks %s:\n%s", want, log)
		}
	}
}
