package cli_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"sync/atomic"
	"testing"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/backup"
	"github.com/sirrobot01/synod/internal/cli"
	"github.com/sirrobot01/synod/internal/destination"
	"github.com/sirrobot01/synod/internal/inventory"
	"github.com/sirrobot01/synod/internal/operation"
	"github.com/sirrobot01/synod/internal/repository"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
)

func TestBackupCLISelectsAppAndReportsSavedResult(t *testing.T) {
	node, appID, destID := uuid.New(), uuid.New(), uuid.New()
	var submitted atomic.Int32
	var recorded backup.Submission
	flags := fixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/apps":
			json.NewEncoder(w).Encode(inventory.Catalog{Apps: []inventory.App{{ID: appID, NodeID: node, Present: true, Definition: &agentv1.DiscoveredApp{Name: "notes"}}}})
		case "/v1/destinations":
			json.NewEncoder(w).Encode(destination.Listing{Destinations: []destination.Destination{{ID: destID, Name: "local", NodeID: node, Location: repository.Location{Kind: "local", Path: "/srv/backup"}}}})
		case "/v1/backups":
			if r.Method != "POST" || json.NewDecoder(r.Body).Decode(&recorded) != nil {
				t.Error("invalid backup request")
			}
			submitted.Add(1)
			if recorded.AppID != appID || recorded.DestinationID != destID || recorded.Method != backup.Stop || recorded.ID == uuid.Nil || r.Header.Get("Idempotency-Key") != recorded.ID.String() {
				t.Error("incorrect backup choice or retry key")
			}
			result, _ := json.Marshal(backup.Result{SnapshotID: recorded.ID, DestinationID: destID, FileBytes: 42, DowntimeMillis: 7, RepositoryOpened: true})
			json.NewEncoder(w).Encode(operation.Operation{ID: recorded.ID, AppID: appID, NodeID: node, Kind: "backup", Status: operation.Succeeded, Result: result})
		default:
			t.Errorf("unexpected request %s", r.URL.Path)
			http.NotFound(w, r)
		}
	}))
	var output, diagnostics bytes.Buffer
	args := append(append([]string{}, flags...), "backup", "notes")
	if code := cli.Run(t.Context(), args, &output, &diagnostics); code != 0 {
		t.Fatalf("backup command: %d %s", code, diagnostics.String())
	}
	var result cli.Result
	if json.Unmarshal(output.Bytes(), &result) != nil || result.Operation == nil || result.Operation.ID != recorded.ID || len(result.Warnings) != 1 || submitted.Load() != 1 {
		t.Fatal("CLI lost backup identity or source-device warning")
	}
}

func TestBackupCLIRefusesAmbiguousAppsAndDestinations(t *testing.T) {
	for _, ambiguousApp := range []bool{true, false} {
		t.Run(map[bool]string{true: "app", false: "destination"}[ambiguousApp], func(t *testing.T) {
			apps := []inventory.App{{ID: uuid.New(), NodeID: uuid.New(), Present: true, Definition: &agentv1.DiscoveredApp{Name: "notes"}}}
			if ambiguousApp {
				apps = append(apps, inventory.App{ID: uuid.New(), NodeID: uuid.New(), Present: true, Definition: &agentv1.DiscoveredApp{Name: "notes"}})
			}
			flags := fixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/v1/apps":
					json.NewEncoder(w).Encode(inventory.Catalog{Apps: apps})
				case "/v1/destinations":
					json.NewEncoder(w).Encode(destination.Listing{Destinations: []destination.Destination{{ID: uuid.New(), Name: "one", Location: repository.Location{Kind: "s3"}}, {ID: uuid.New(), Name: "two", Location: repository.Location{Kind: "s3"}}}})
				default:
					t.Errorf("ambiguous choice submitted work: %s", r.URL.Path)
				}
			}))
			var output, diagnostics bytes.Buffer
			if code := cli.Run(t.Context(), append(append([]string{}, flags...), "backup", "notes", "--wait=false"), &output, &diagnostics); code != 1 {
				t.Fatalf("ambiguous command succeeded: %d", code)
			}
		})
	}
}
