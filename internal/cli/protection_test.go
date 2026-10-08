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
	"github.com/sirrobot01/synod/internal/repository"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
)

func TestProtectCLIReviewsBeforeSavingAndRefusesStaleConfirmation(t *testing.T) {
	node, appID, target := uuid.New(), uuid.New(), uuid.New()
	var saves atomic.Int32
	settings := backup.DefaultSettings()
	plan := backup.ProtectionPlan{Revision: 7, Defaults: settings, Apps: []backup.AppPlan{{App: inventory.App{ID: appID, NodeID: node, Present: true, Definition: &agentv1.DiscoveredApp{Name: "notes", Paths: []*agentv1.DiscoveredPath{{Source: "/srv/notes", Status: "Included"}}}}, Settings: settings}}}
	flags := fixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/v1/protection" && r.Method == "GET":
			json.NewEncoder(w).Encode(plan)
		case r.URL.Path == "/v1/destinations":
			json.NewEncoder(w).Encode(destination.Listing{Destinations: []destination.Destination{{ID: target, Name: "home", NodeID: node, Location: repository.Location{Kind: "local", Path: "/backup"}}}})
		case r.URL.Path == "/v1/protection" && r.Method == "PUT":
			var input backup.ProtectionUpdate
			if json.NewDecoder(r.Body).Decode(&input) != nil || input.ExpectedRevision != 7 || !input.All || !input.Enabled || len(input.AppIDs) != 0 || len(input.Settings.Destinations) != 1 || input.Settings.Destinations[0] != target || input.Settings.Method != backup.Live || input.Settings.At != "04:30" || input.Settings.TimeZone != "Africa/Lagos" || input.Settings.Retention != (backup.Retention{Daily: 2, Weekly: 4, Monthly: 6}) || r.Header.Get("Idempotency-Key") != input.ID.String() {
				t.Error("confirmation changed the selected plan")
			}
			saves.Add(1)
			json.NewEncoder(w).Encode(backup.ProtectionConfirmation{ID: input.ID, Revision: 8})
		default:
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
		}
	}))
	args := append(append([]string{}, flags...), "protect", "--all", "--destination", "home", "--method", "live", "--at", "04:30", "--time-zone", "Africa/Lagos", "--keep-daily", "2")
	var output, diagnostics bytes.Buffer
	if code := cli.Run(t.Context(), args, &output, &diagnostics); code != 0 {
		t.Fatalf("preview: %d %s", code, diagnostics.String())
	}
	var result cli.Result
	if json.Unmarshal(output.Bytes(), &result) != nil || result.ProtectionUpdate == nil || result.ProtectionUpdate.ExpectedRevision != 7 || result.Protection == nil || !result.Protection.Apps[0].OnlyOnSource || saves.Load() != 0 || !bytes.Contains(output.Bytes(), []byte("/srv/notes")) {
		t.Fatal("preview wrote settings or omitted paths and the source-device warning")
	}
	output.Reset()
	if code := cli.Run(t.Context(), append(append([]string{}, args...), "--confirm", "6"), &output, &diagnostics); code != 1 || saves.Load() != 0 {
		t.Fatal("stale confirmation changed the plan")
	}
	output.Reset()
	if code := cli.Run(t.Context(), append(append([]string{}, args...), "--confirm", "7"), &output, &diagnostics); code != 0 || saves.Load() != 1 {
		t.Fatalf("confirmation: %d %s", code, diagnostics.String())
	}
}

func TestProtectCLIPreservesSavedChoicesAndSupportsOptOut(t *testing.T) {
	node, appID, target := uuid.New(), uuid.New(), uuid.New()
	settings := backup.DefaultSettings()
	settings.Destinations, settings.Method, settings.At = []uuid.UUID{target}, backup.Live, "22:15"
	plan := backup.ProtectionPlan{Revision: 2, Defaults: backup.DefaultSettings(), Apps: []backup.AppPlan{{App: inventory.App{ID: appID, NodeID: node, Present: true, Definition: &agentv1.DiscoveredApp{Name: "notes"}}, Enabled: true, Settings: settings}}}
	flags := fixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/protection":
			json.NewEncoder(w).Encode(plan)
		case "/v1/destinations":
			json.NewEncoder(w).Encode(destination.Listing{Destinations: []destination.Destination{{ID: target, Name: "home", NodeID: node, Location: repository.Location{Kind: "local", Path: "/backup"}}}})
		default:
			t.Errorf("unexpected request: %s", r.URL.Path)
		}
	}))
	for _, off := range []bool{false, true} {
		args := append(append([]string{}, flags...), "protect", "notes", "--keep-monthly", "2")
		if off {
			args = append(args, "--off")
		}
		var output, diagnostics bytes.Buffer
		if code := cli.Run(t.Context(), args, &output, &diagnostics); code != 0 {
			t.Fatalf("preview: %d %s", code, diagnostics.String())
		}
		var result cli.Result
		if json.Unmarshal(output.Bytes(), &result) != nil || result.ProtectionUpdate == nil {
			t.Fatal("missing preview")
		}
		update := result.ProtectionUpdate
		if update.All || len(update.AppIDs) != 1 || update.AppIDs[0] != appID || update.Enabled == off || update.Settings.Method != backup.Live || update.Settings.At != "22:15" || update.Settings.Retention.Monthly != 2 {
			t.Fatal("an omitted flag replaced a saved choice")
		}
	}
}

func TestAppStatusExplainsTheConsistencyLimit(t *testing.T) {
	plan := backup.ProtectionPlan{Revision: 1, Apps: []backup.AppPlan{{
		App:     inventory.App{ID: uuid.New(), NodeID: uuid.New(), Definition: &agentv1.DiscoveredApp{Name: "notes"}},
		Enabled: true, State: backup.Partial, LimitCodes: []string{backup.AppConsistencyUnverified},
		Targets: []backup.TargetStatus{{DestinationID: uuid.New(), State: backup.Partial, LastGood: &backup.Result{SnapshotID: uuid.New(), Consistency: backup.ConsistencyFilesystem}}},
	}}}
	flags := fixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/protection" || r.Method != "GET" {
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
		}
		json.NewEncoder(w).Encode(plan)
	}))
	var output, diagnostics bytes.Buffer
	if code := cli.Run(t.Context(), append(flags, "--json=false", "app", "status"), &output, &diagnostics); code != 0 {
		t.Fatalf("status: %d %s", code, diagnostics.String())
	}
	for _, message := range []string{"Current status: partial", "agreement between app files and databases is not verified", "will not pause writes", "Capture scope: filesystem_snapshot"} {
		if !bytes.Contains(output.Bytes(), []byte(message)) {
			t.Fatalf("status omitted %q: %s", message, output.String())
		}
	}
}
