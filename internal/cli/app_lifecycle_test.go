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
	for _, action := range []string{"stop", "start", "delete"} {
		t.Run(action, func(t *testing.T) {
			id, op := uuid.New(), uuid.New()
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
