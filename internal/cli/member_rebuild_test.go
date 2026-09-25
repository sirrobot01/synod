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

func TestMemberRebuildNeedsMemberAndConfirmation(t *testing.T) {
	plan := cluster.ActionPlan{PlanToken: uuid.New(), ClusterID: uuid.New(), ClusterName: "ha", Action: "rebuild_member", MemberID: uuid.New(), ResourceID: uuid.New(), ExpectedRevision: 5, DataDisposition: "retain", RetainedVolumes: []uuid.UUID{uuid.New()}, Downtime: "Rebuild the failed member and retain its old volume.", ExpiresAt: time.Now().Add(5 * time.Minute)}
	admission := client.Admission{ClusterID: plan.ClusterID, OperationID: uuid.New(), DesiredRevision: 6}
	var previews, mutations atomic.Int32
	flags := fixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		switch r.URL.Path {
		case "/v1/clusters/" + plan.ClusterID.String() + "/action-plans":
			previews.Add(1)
			if string(body) != `{"action":"rebuild_member","member_id":"`+plan.MemberID.String()+`"}` {
				t.Errorf("preview body=%s", body)
			}
			json.NewEncoder(w).Encode(plan)
		case "/v1/clusters/" + plan.ClusterID.String() + "/actions/rebuild-member":
			mutations.Add(1)
			var confirmation cluster.ActionConfirmation
			if json.Unmarshal(body, &confirmation) != nil || confirmation.PlanToken != plan.PlanToken || r.Header.Get("If-Match") != `"5"` {
				t.Errorf("confirmation body=%s match=%s", body, r.Header.Get("If-Match"))
			}
			w.WriteHeader(http.StatusAccepted)
			json.NewEncoder(w).Encode(admission)
		default:
			http.Error(w, "unexpected request", http.StatusBadRequest)
		}
	}))
	var stdout, stderr bytes.Buffer
	if code := cli.Run(t.Context(), append(flags, "db", "rebuild-member", plan.ClusterID.String()), &stdout, &stderr); code == 0 || previews.Load() != 0 {
		t.Fatalf("rebuild without --member: code=%d previews=%d", code, previews.Load())
	}
	stdout.Reset()
	stderr.Reset()
	args := append(flags, "db", "rebuild-member", plan.ClusterID.String(), "--member", plan.MemberID.String(), "--idempotency-key", "member-key")
	code := cli.Run(t.Context(), args, &stdout, &stderr)
	var result cli.Result
	if err := json.Unmarshal(stdout.Bytes(), &result); err != nil || code != 1 || result.Error == nil || result.Error.Code != "confirmation_required" || mutations.Load() != 0 {
		t.Fatalf("preview result=%s code=%d mutations=%d", stdout.String(), code, mutations.Load())
	}
	for _, want := range []string{plan.MemberID.String(), plan.ResourceID.String(), plan.RetainedVolumes[0].String(), plan.Downtime} {
		if !strings.Contains(stderr.String(), want) {
			t.Errorf("preview omitted %q", want)
		}
	}
	stdout.Reset()
	if code := cli.Run(t.Context(), append(args, "--yes"), &stdout, &stderr); code != 0 || mutations.Load() != 1 {
		t.Fatalf("confirmed rebuild: code=%d mutations=%d out=%s", code, mutations.Load(), stdout.String())
	}
}
