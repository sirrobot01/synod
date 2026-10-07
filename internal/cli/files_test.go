package cli_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/cli"
	"github.com/sirrobot01/synod/internal/inventory"
	"github.com/sirrobot01/synod/internal/restore"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
)

func TestFileCLIListsAndPublishesOnlyCompleteDownloads(t *testing.T) {
	appID, snapshot, destination := uuid.New(), uuid.New(), uuid.New()
	var incomplete atomic.Bool
	var downloads atomic.Int32
	flags := fixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/apps":
			json.NewEncoder(w).Encode(inventory.Catalog{Apps: []inventory.App{{ID: appID, Definition: &agentv1.DiscoveredApp{Name: "notes"}}}})
		case "/v1/apps/" + appID.String() + "/snapshots":
			json.NewEncoder(w).Encode(restore.FileHeader{Snapshots: []restore.SnapshotSummary{{ID: snapshot}}})
		case "/v1/apps/" + appID.String() + "/files/content":
			downloads.Add(1)
			if r.URL.Query().Get("path") != "/srv/notes/file ?#" || r.URL.Query().Get("snapshot") != snapshot.String() {
				t.Error("file selection changed")
			}
			w.Header().Set("X-Synod-Snapshot-ID", snapshot.String())
			w.Header().Set("X-Synod-Destination-ID", destination.String())
			w.Header().Set("X-Synod-Format", "file")
			w.Header().Set("Trailer", "X-Synod-Transfer")
			w.Write([]byte("private-saved-content"))
			if !incomplete.Load() {
				w.Header().Set("X-Synod-Transfer", "complete")
			}
		default:
			t.Errorf("unexpected request: %s", r.URL.Path)
			http.NotFound(w, r)
		}
	}))
	var stdout, stderr bytes.Buffer
	if code := cli.Run(t.Context(), append(append([]string{}, flags...), "snapshot", "list", "notes"), &stdout, &stderr); code != 0 || !bytes.Contains(stdout.Bytes(), []byte(snapshot.String())) {
		t.Fatal("snapshot list failed", stderr.String())
	}
	directory := t.TempDir()
	output := filepath.Join(directory, "saved")
	args := append(append([]string{}, flags...), "file", "get", "notes", "/srv/notes/file ?#", "--snapshot", snapshot.String(), "--output", output)
	for _, bad := range []bool{true, false} {
		incomplete.Store(bad)
		stdout.Reset()
		stderr.Reset()
		code := cli.Run(t.Context(), args, &stdout, &stderr)
		if (code != 0) != bad {
			t.Fatal("wrong download result", code, stderr.String())
		}
		entries, err := os.ReadDir(directory)
		if err != nil {
			t.Fatal(err)
		}
		if bad {
			if len(entries) != 0 {
				t.Fatal("partial download was retained")
			}
			continue
		}
		data, err := os.ReadFile(output)
		if err != nil || string(data) != "private-saved-content" || len(entries) != 1 {
			t.Fatal("saved content changed", err)
		}
		info, err := os.Stat(output)
		if err != nil || info.Mode().Perm() != 0600 {
			t.Fatal("download is not private", err)
		}
	}
	if code := cli.Run(t.Context(), args, &stdout, &stderr); code == 0 || downloads.Load() != 2 {
		t.Fatal("existing output was replaced")
	}
	if bytes.Contains(stdout.Bytes(), []byte("private-saved-content")) {
		t.Fatal("content reached command output")
	}
}
