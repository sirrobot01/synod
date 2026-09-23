package cli_test

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/cli"
	"github.com/sirrobot01/synod/internal/client"
	"github.com/sirrobot01/synod/internal/cluster"
)

func TestFailedReplicaRemovalRequiresReviewedConfirmation(t *testing.T) {
	plan := cluster.ActionPlan{PlanToken: uuid.New(), ClusterID: uuid.New(), ClusterName: "db", MemberID: uuid.New(), ResourceID: uuid.New(), Action: "scale_down_failed", ExpectedRevision: 7, DataDisposition: "retain", RetainedVolumes: []uuid.UUID{uuid.New()}, Downtime: "Remove only after native failure is proved. Keep the data volume.", ExpiresAt: time.Now().Add(5 * time.Minute)}
	var previews, mutations atomic.Int32
	flags := fixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		switch r.URL.Path {
		case "/v1/clusters/" + plan.ClusterID.String() + "/action-plans":
			previews.Add(1)
			if string(body) != `{"action":"scale_down_failed"}` {
				t.Errorf("wrong preview: %s", body)
			}
			json.NewEncoder(w).Encode(plan)
		case "/v1/clusters/" + plan.ClusterID.String() + "/actions/scale":
			mutations.Add(1)
			var input struct {
				Replicas int `json:"replicas"`
				cluster.ActionConfirmation
			}
			if json.Unmarshal(body, &input) != nil || input.Replicas != 0 || input.PlanToken != plan.PlanToken || !input.Confirm || input.DataDisposition != "retain" || r.Header.Get("If-Match") != `"7"` || r.Header.Get("Idempotency-Key") != "remove-key" {
				t.Errorf("changed confirmation: %s", body)
			}
			w.WriteHeader(http.StatusAccepted)
			json.NewEncoder(w).Encode(client.Admission{ClusterID: plan.ClusterID, OperationID: uuid.New(), DesiredRevision: 8})
		default:
			t.Errorf("unexpected request: %s", r.URL.Path)
			http.Error(w, "unexpected", http.StatusBadRequest)
		}
	}))
	args := append(flags, "db", "scale", plan.ClusterID.String(), "--replicas", "0", "--failed-replica", "--expected-revision", "7", "--idempotency-key", "remove-key")
	var stdout, stderr bytes.Buffer
	code := cli.Run(t.Context(), args, &stdout, &stderr)
	var result cli.Result
	if json.Unmarshal(stdout.Bytes(), &result) != nil || code != 1 || result.Error == nil || result.Error.Code != "confirmation_required" || result.ActionPlan == nil || result.ActionPlan.Action != "scale_down_failed" || previews.Load() != 1 || mutations.Load() != 0 {
		t.Fatalf("preview: %s %s", stdout.String(), stderr.String())
	}
	for _, want := range []string{plan.MemberID.String(), plan.RetainedVolumes[0].String(), plan.Downtime, plan.PlanToken.String()} {
		if !strings.Contains(stderr.String(), want) {
			t.Errorf("omitted %q", want)
		}
	}
	stdout.Reset()
	stderr.Reset()
	// The stored token selects the mode on a later invocation without the mode flag.
	args = append(flags, "db", "scale", plan.ClusterID.String(), "--replicas", "0", "--expected-revision", "7", "--idempotency-key", "remove-key", "--yes", "--plan-token", plan.PlanToken.String())
	if code := cli.Run(t.Context(), args, &stdout, &stderr); code != 0 || previews.Load() != 1 || mutations.Load() != 1 {
		t.Fatalf("confirmation: %s %s", stdout.String(), stderr.String())
	}
	stdout.Reset()
	stderr.Reset()
	args = append(flags, "db", "scale", plan.ClusterID.String(), "--replicas", "1", "--failed-replica")
	if code := cli.Run(t.Context(), args, &stdout, &stderr); code != 1 || previews.Load() != 1 || mutations.Load() != 1 {
		t.Fatalf("invalid mode reached API: %s", stdout.String())
	}
	stdout.Reset()
	stderr.Reset()
	args = append(flags, "db", "scale", plan.ClusterID.String(), "--replicas", "0", "--failed-replica", "--plan-token", plan.PlanToken.String(), "--yes")
	if code := cli.Run(t.Context(), args, &stdout, &stderr); code != 1 || previews.Load() != 1 || mutations.Load() != 1 {
		t.Fatalf("mode flag silently ignored a supplied token: %s", stdout.String())
	}

}
