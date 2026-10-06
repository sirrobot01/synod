package cli_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/cli"
	"github.com/sirrobot01/synod/internal/inventory"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
)

func TestAppListShowsPathsAndScanFailure(t *testing.T) {
	catalog := inventory.Catalog{Apps: []inventory.App{{ID: uuid.New(), NodeID: uuid.New(), NodeName: "odin", ObservedAt: time.Now(), Present: true, Definition: &agentv1.DiscoveredApp{Name: "notes", Kind: "compose", Paths: []*agentv1.DiscoveredPath{{Source: "/srv/notes", Status: "Included", Reason: "App state.", SizeKnown: true, SizeBytes: 42}, {Source: "/mnt/media", Status: "Large", Reason: "Media is skipped."}}}}}, Devices: []inventory.Device{{NodeName: "odin", ReceivedAt: new(time.Now()), FailureCode: "docker_scan_failed"}}}
	flags := fixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" || r.URL.Path != "/v1/apps" {
			t.Errorf("unexpected app request %s %s", r.Method, r.URL.Path)
		}
		json.NewEncoder(w).Encode(catalog)
	}))
	for _, jsonOutput := range []bool{false, true} {
		args := append([]string{}, flags...)
		// The fixture enables JSON. Set its value explicitly for the text case.
		if !jsonOutput {
			args = append(args, "--json=false")
		}
		args = append(args, "app", "list")
		var out, diagnostics bytes.Buffer
		if code := cli.Run(t.Context(), args, &out, &diagnostics); code != 0 {
			t.Fatalf("app list: %d %s", code, diagnostics.String())
		}
		for _, value := range []string{"notes", "Large", "/mnt/media", "docker_scan_failed"} {
			if !strings.Contains(out.String(), value) {
				t.Fatalf("missing %q: %s", value, out.String())
			}
		}
		if jsonOutput {
			var result cli.Result
			if json.Unmarshal(out.Bytes(), &result) != nil || len(result.Apps) != 1 || result.Apps[0].ID != catalog.Apps[0].ID {
				t.Fatalf("JSON inventory: %s", out.String())
			}
		}
	}
}
