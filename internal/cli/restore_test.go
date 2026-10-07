package cli_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"sync/atomic"
	"testing"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/cli"
	"github.com/sirrobot01/synod/internal/client"
	"github.com/sirrobot01/synod/internal/destination"
	"github.com/sirrobot01/synod/internal/inventory"
	"github.com/sirrobot01/synod/internal/operation"
	"github.com/sirrobot01/synod/internal/repository"
	"github.com/sirrobot01/synod/internal/restore"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
)

func TestRestoreCLISelectsHistoricalAppAndReportsSavedResult(t *testing.T) {
	node, appID, destID := uuid.New(), uuid.New(), uuid.New()
	var submitted atomic.Int32
	var recorded restore.Submission
	flags := fixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/nodes/by-name/target":
			json.NewEncoder(w).Encode(client.Node{ID: node, Name: "target"})
		case "/v1/apps":
			json.NewEncoder(w).Encode(inventory.Catalog{Apps: []inventory.App{{ID: appID, NodeID: node, Present: false, Definition: &agentv1.DiscoveredApp{Name: "notes"}}}})
		case "/v1/destinations":
			json.NewEncoder(w).Encode(destination.Listing{Destinations: []destination.Destination{{ID: destID, Name: "local", NodeID: node, Location: repository.Location{Kind: "local", Path: "/srv/restore"}}}})
		case "/v1/restores":
			if r.Method != "POST" || json.NewDecoder(r.Body).Decode(&recorded) != nil {
				t.Error("invalid restore request")
			}
			submitted.Add(1)
			if recorded.AppID != appID || recorded.DestinationID != destID || !recorded.Replace || recorded.NodeID != node || recorded.ID == uuid.Nil || r.Header.Get("Idempotency-Key") != recorded.ID.String() {
				t.Error("incorrect restore choice or retry key")
			}
			result, _ := json.Marshal(restore.Result{SnapshotID: uuid.New(), DestinationID: destID, TargetAppID: uuid.New()})
			json.NewEncoder(w).Encode(operation.Operation{ID: recorded.ID, AppID: appID, NodeID: node, Kind: "restore", Status: operation.Succeeded, Result: result})
		default:
			t.Errorf("unexpected request %s", r.URL.Path)
			http.NotFound(w, r)
		}
	}))
	var output, diagnostics bytes.Buffer
	args := append(append([]string{}, flags...), "restore", "notes", "--to", "target", "--replace", "--destination", "local")
	if code := cli.Run(t.Context(), args, &output, &diagnostics); code != 0 {
		t.Fatalf("restore command: %d %s", code, diagnostics.String())
	}
	var result cli.Result
	if json.Unmarshal(output.Bytes(), &result) != nil || result.Operation == nil || result.Operation.ID != recorded.ID || len(result.Warnings) != 0 || submitted.Load() != 1 {
		t.Fatal("CLI lost restore identity or source-device warning")
	}
}
