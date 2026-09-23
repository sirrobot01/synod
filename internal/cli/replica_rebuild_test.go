package cli_test

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/cli"
	"github.com/sirrobot01/synod/internal/client"
	"github.com/sirrobot01/synod/internal/cluster"
	"github.com/sirrobot01/synod/internal/operation"
)

func TestReplicaRebuildPreviewRequiresConfirmation(t *testing.T) {
	plan := cluster.ActionPlan{PlanToken: uuid.New(), ClusterID: uuid.New(), ClusterName: "photos", Action: "rebuild_replica", MemberID: uuid.New(), ResourceID: uuid.New(), ExpectedRevision: 7, DataDisposition: "retain", RetainedVolumes: []uuid.UUID{uuid.New()}, Downtime: "Rebuild the failed replica and retain its old volume.", ExpiresAt: time.Now().Add(5 * time.Minute)}
	var previews, mutations atomic.Int32
	flags := fixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/clusters/"+plan.ClusterID.String()+"/action-plans" {
			mutations.Add(1)
			http.Error(w, "unexpected mutation", http.StatusBadRequest)
			return
		}
		previews.Add(1)
		body, _ := io.ReadAll(r.Body)
		if string(body) != `{"action":"rebuild_replica"}` {
			t.Errorf("preview body=%s", body)
		}
		json.NewEncoder(w).Encode(plan)
	}))
	var stdout, stderr bytes.Buffer
	code := cli.Run(t.Context(), append(flags, "db", "rebuild-replica", plan.ClusterID.String(), "--idempotency-key", "preview-key"), &stdout, &stderr)
	var result cli.Result
	if err := json.Unmarshal(stdout.Bytes(), &result); err != nil || code != 1 || result.Error == nil || result.Error.Code != "confirmation_required" || result.ActionPlan == nil || result.ActionPlan.PlanToken != plan.PlanToken || result.ExpectedRevision != 7 || result.IdempotencyKey != "preview-key" {
		t.Fatalf("preview result=%s code=%d", stdout.String(), code)
	}
	if previews.Load() != 1 || mutations.Load() != 0 {
		t.Fatalf("preview changed the cluster: previews=%d mutations=%d", previews.Load(), mutations.Load())
	}
	for _, want := range []string{plan.ClusterName, plan.ClusterID.String(), plan.MemberID.String(), plan.ResourceID.String(), "retain", plan.RetainedVolumes[0].String(), plan.Downtime, plan.PlanToken.String(), "--yes", "--expected-revision 7", "--idempotency-key preview-key"} {
		if !strings.Contains(stderr.String(), want) {
			t.Errorf("preview omitted %q", want)
		}
	}
}

func TestReplicaRebuildLostReplyRetainsReviewedPlanAndWaits(t *testing.T) {
	plan := cluster.ActionPlan{PlanToken: uuid.New(), ClusterID: uuid.New(), ClusterName: "photos", Action: "rebuild_replica", MemberID: uuid.New(), ResourceID: uuid.New(), ExpectedRevision: 7, DataDisposition: "retain", RetainedVolumes: []uuid.UUID{uuid.New()}, Downtime: "Replace the failed replica and retain its old volume.", ExpiresAt: time.Now().Add(5 * time.Minute)}
	admission := client.Admission{ClusterID: plan.ClusterID, OperationID: uuid.New(), DesiredRevision: 8}
	var diagnostics actionDiagnostics
	var mu sync.Mutex
	var bodies, keys, revisions []string
	var previews atomic.Int32
	flags := fixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/clusters/"+plan.ClusterID.String()+"/action-plans":
			previews.Add(1)
			json.NewEncoder(w).Encode(plan)
		case r.Method == http.MethodPost && r.URL.Path == "/v1/clusters/"+plan.ClusterID.String()+"/actions/rebuild-replica":
			diagnostics.Lock()
			text := diagnostics.buffer.String()
			diagnostics.Unlock()
			for _, want := range []string{plan.ClusterName, plan.ClusterID.String(), plan.Downtime, plan.RetainedVolumes[0].String(), "Expected revision: 7", "Idempotency key: rebuild-key"} {
				if !strings.Contains(text, want) {
					t.Errorf("mutation preceded disclosure of %q", want)
				}
			}
			body, _ := io.ReadAll(r.Body)
			var confirmation cluster.ActionConfirmation
			if err := json.Unmarshal(body, &confirmation); err != nil || confirmation.PlanToken != plan.PlanToken || !confirmation.Confirm || confirmation.DataDisposition != "retain" {
				t.Errorf("rebuild changed the plan: %s", body)
			}
			mu.Lock()
			defer mu.Unlock()
			bodies = append(bodies, string(body))
			keys = append(keys, r.Header.Get("Idempotency-Key"))
			revisions = append(revisions, r.Header.Get("If-Match"))
			if len(bodies) == 1 {
				conn, _, err := w.(http.Hijacker).Hijack()
				if err != nil {
					t.Error(err)
					return
				}
				conn.Close()
				return
			}
			admission.Replayed = true
			w.WriteHeader(http.StatusAccepted)
			json.NewEncoder(w).Encode(admission)
		case r.Method == http.MethodGet && r.URL.Path == "/v1/operations/"+admission.OperationID.String():
			json.NewEncoder(w).Encode(operation.Operation{ID: admission.OperationID, Status: operation.Succeeded})
		default:
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
			http.Error(w, "unexpected route", http.StatusNotFound)
		}
	}))
	var stdout bytes.Buffer
	code := cli.Run(t.Context(), append(flags, "db", "rebuild-replica", plan.ClusterID.String(), "--yes", "--wait", "--idempotency-key", "rebuild-key", "--attempts", "2"), &stdout, &diagnostics)
	var result cli.Result
	if err := json.Unmarshal(stdout.Bytes(), &result); err != nil || code != 0 || result.ActionPlan == nil || result.ExpectedRevision != 7 || result.Admission == nil || !result.Admission.Replayed || result.Operation == nil || result.Operation.Status != operation.Succeeded {
		t.Fatalf("delete result=%s code=%d", stdout.String(), code)
	}
	mu.Lock()
	defer mu.Unlock()
	if previews.Load() != 1 || len(bodies) != 2 || bodies[0] != bodies[1] || keys[0] != "rebuild-key" || keys[1] != keys[0] || revisions[0] != `"7"` || revisions[1] != revisions[0] {
		t.Fatalf("retry changed the reviewed request: previews=%d attempts=%d", previews.Load(), len(bodies))
	}
}
