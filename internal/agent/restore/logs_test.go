package restore

import (
	"bytes"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	agentbackup "github.com/sirrobot01/synod/internal/agent/backup"
	"github.com/sirrobot01/synod/internal/agent/journal"
	"github.com/sirrobot01/synod/internal/destination"
	"github.com/sirrobot01/synod/internal/logging"
	"github.com/sirrobot01/synod/internal/proof"
	"github.com/sirrobot01/synod/internal/repository"
	"github.com/sirrobot01/synod/internal/secret"
)

// A failed check names the destination, the code, and the cause, and never the password.
func TestRepositoryCheckLogsItsResultAndCause(t *testing.T) {
	installation, node, destID := uuid.New(), uuid.New(), uuid.New()
	credentials := repository.Credentials{Password: "private-log-test-password"}
	cache, err := journal.Open(t.Context(), filepath.Join(t.TempDir(), "agent.db"), installation, node)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cache.Close() })
	keyID := uuid.New()
	ring, err := secret.NewKeyring(keyID, map[uuid.UUID][]byte{keyID: bytes.Repeat([]byte{7}, 32)})
	if err != nil {
		t.Fatal(err)
	}
	entry := destination.Entry{Destination: destination.Destination{ID: destID, Name: "backup disk", NodeID: node, Location: repository.Location{Kind: "local", Path: t.TempDir()}, CreatedAt: time.Now().UTC()}, Credentials: credentials}
	if err := cache.SaveDestinationPlan(t.Context(), ring, destination.Plan{InstallationID: installation, Revision: 1, Destinations: []destination.Entry{entry}}); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	logger, err := logging.New(&out, "", "")
	if err != nil {
		t.Fatal(err)
	}
	w := &Worker{Backup: &agentbackup.Worker{Journal: cache, Keyring: ring, InstallationID: installation, NodeID: node, Logger: logger}, Directory: t.TempDir(), Apps: t.TempDir()}
	request := proof.Request{Submission: proof.Submission{ID: uuid.New(), Kind: proof.Integrity, NodeID: node, DestinationID: destID}, InstallationID: installation, RequestedAt: time.Now().UTC()}
	if result, err := w.ExecuteProof(t.Context(), request); err != nil || result.FailureCode != "repository_uninitialized" {
		t.Fatalf("check: %+v %v", result, err)
	}
	log := out.String()
	for _, want := range []string{`msg="repository check started" destination="backup disk"`, `msg="repository check failed" destination="backup disk"`, "code=repository_uninitialized", `error="repository is not initialized"`} {
		if !strings.Contains(log, want) {
			t.Errorf("log lacks %s:\n%s", want, log)
		}
	}
	if strings.Contains(log, credentials.Password) {
		t.Fatal("the log holds the repository password")
	}
}
