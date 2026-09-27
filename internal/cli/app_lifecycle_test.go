package cli_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/app"
	"github.com/sirrobot01/synod/internal/cli"
	"github.com/sirrobot01/synod/internal/operation"
)

func TestAppLifecycleCLI(t *testing.T) {
	for _, action := range []string{"stop", "start", "delete", "move"} {
		t.Run(action, func(t *testing.T) {
			id, op, target := uuid.New(), uuid.New(), uuid.New()
			calls := 0
			flags := fixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == "GET" {
					if r.URL.Path != "/v1/operations/"+op.String() {
						t.Errorf("retry read current app: %s", r.URL.Path)
					}
					json.NewEncoder(w).Encode(operation.Operation{ID: op, Status: operation.Succeeded})
					return
				}
				calls++
				path, method := "/v1/apps/"+id.String()+"/actions/"+action, "POST"
				if action == "delete" {
					path, method = "/v1/apps/"+id.String(), "DELETE"
				}
				if action == "move" {
					path = "/v1/apps/" + id.String() + "/move"
					var input struct {
						TargetNodeID uuid.UUID `json:"target_node_id"`
					}
					if err := json.NewDecoder(r.Body).Decode(&input); err != nil || input.TargetNodeID != target {
						t.Errorf("move target: %v %v", input, err)
					}
				}
				if r.URL.Path != path || r.Method != method || r.Header.Get("If-Match") != `"7"` || r.Header.Get("Idempotency-Key") != "same-key" {
					t.Errorf("request %s %s %+v", r.Method, r.URL.Path, r.Header)
				}
				if calls == 1 {
					w.WriteHeader(503)
					return
				}
				w.WriteHeader(202)
				json.NewEncoder(w).Encode(app.Admission{AppID: id, OperationID: op, DesiredRevision: 8, AcceptedAt: time.Now(), Replayed: true})
			}))
			args := append(flags, "app", action, id.String(), "--expected-revision", "7", "--idempotency-key", "same-key", "--wait")
			if action == "move" {
				args = append(args, "--to", target.String())
			}
			if action == "delete" {
				args = append(args, "--yes")
			}
			var stdout, stderr bytes.Buffer
			code := cli.Run(t.Context(), args, &stdout, &stderr)
			var result cli.Result
			if err := json.Unmarshal(stdout.Bytes(), &result); err != nil || code != 0 || calls != 2 || result.AppAdmission == nil || result.Operation == nil || result.Operation.Status != operation.Succeeded {
				t.Fatalf("result %d: %s %s %v", code, &stdout, &stderr, err)
			}
		})
	}
}

func TestAppDeletePreviewAndInspect(t *testing.T) {
	id := uuid.New()
	flags := fixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" || r.URL.Path != "/v1/apps/"+id.String() {
			t.Errorf("unexpected mutation: %s %s", r.Method, r.URL.Path)
		}
		json.NewEncoder(w).Encode(app.App{ID: id, Name: "web", DesiredRevision: 4, Status: &app.Status{State: "running", OperationID: uuid.New(), OperationState: "succeeded"}})
	}))
	for _, action := range []string{"inspect", "delete"} {
		var stdout, stderr bytes.Buffer
		code := cli.Run(t.Context(), append(flags, "app", action, id.String()), &stdout, &stderr)
		var result cli.Result
		if err := json.Unmarshal(stdout.Bytes(), &result); err != nil || result.App == nil {
			t.Fatalf("inspection: %s %v", &stdout, err)
		}
		if action == "inspect" && code != 0 {
			t.Fatalf("inspect: %s %s", &stdout, &stderr)
		}
		if action == "delete" && (code != 1 || result.Error == nil || result.Error.Code != "confirmation_required" || !strings.Contains(stderr.String(), "External bind paths")) {
			t.Fatalf("preview: %s %s", &stdout, &stderr)
		}
	}
}

func TestTier2InspectShowsReplicationEvidence(t *testing.T) {
	id, active, standby := uuid.New(), uuid.New(), uuid.New()
	status := &app.Status{State: "running", OperationID: uuid.New(), OperationState: "succeeded", ActiveNodeID: active, WriterEpoch: 2, Placement: "fallback", LossWindowSeconds: new(75.0), LastPauseSeconds: 0.1, MaxPauseSeconds: 0.2, SkippedSnapshots: 3,
		Standbys: []app.StandbyStatus{{NodeID: standby, Epoch: 2, Sequence: 7, CompletedAt: new(time.Date(2026, 9, 27, 1, 2, 3, 0, time.UTC)), LagSeconds: new(15.0), Usable: true}}}
	flags := fixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" || r.URL.Path != "/v1/apps/"+id.String() {
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
		}
		json.NewEncoder(w).Encode(app.App{ID: id, Name: "writer", Tier: app.TierSingleWriter, Status: status})
	}))
	args := []string{}
	for _, flag := range flags {
		if flag != "--json" {
			args = append(args, flag)
		}
	}
	args = append(args, "app", "inspect", id.String())
	var stdout, stderr bytes.Buffer
	if code := cli.Run(t.Context(), args, &stdout, &stderr); code != 0 {
		t.Fatalf("inspect: %d %s", code, &stderr)
	}
	for _, expected := range []string{"Active device: " + active.String(), "Writer epoch: 2", "Placement: fallback", "Loss window: 75.0 seconds", "Snapshot pause: 0.100 seconds (maximum 0.200)", "Skipped snapshots: 3", "Standby: " + standby.String(), "sequence 7, usable: true", "Completed: 2026-09-27T01:02:03Z", "Lag: 15.0 seconds"} {
		if !strings.Contains(stdout.String(), expected) {
			t.Fatalf("missing %q in %s", expected, &stdout)
		}
	}
	status.State, status.Placement, status.FailureCode = "down", "down", "no_replica"
	status.LossWindowSeconds = nil
	status.Standbys = nil
	stdout.Reset()
	if code := cli.Run(t.Context(), args, &stdout, &stderr); code != 0 || !strings.Contains(stdout.String(), "Loss window: no usable replica") || !strings.Contains(stdout.String(), "Failure: no_replica") {
		t.Fatalf("missing replica status: %d %s %s", code, &stdout, &stderr)
	}
}
