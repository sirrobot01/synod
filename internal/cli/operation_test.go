package cli_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/cli"
	"github.com/sirrobot01/synod/internal/operation"
)

func TestOperationCommandsKeepFailedResults(t *testing.T) {
	op := operation.Operation{ID: uuid.New(), Kind: "backup", Status: operation.Failed, FailureCode: "destination_unavailable"}
	flags := fixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" || r.URL.Path != "/v1/operations/"+op.ID.String() {
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
		}
		json.NewEncoder(w).Encode(op)
	}))
	for command, want := range map[string]int{"get": 0, "wait": 1} {
		var out, diagnostics bytes.Buffer
		args := append(append([]string{}, flags...), "operation", command, op.ID.String())
		if code := cli.Run(t.Context(), args, &out, &diagnostics); code != want || !strings.Contains(out.String(), "destination_unavailable") {
			t.Fatalf("%s: %d %s %s", command, code, out.String(), diagnostics.String())
		}
	}
}

func TestOperationWaitInterruptionDoesNotCancelWork(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	id := uuid.New()
	flags := fixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			t.Errorf("wait changed server state: %s", r.Method)
		}
		json.NewEncoder(w).Encode(operation.Operation{ID: id, Status: operation.Running})
		cancel()
	}))
	var out, diagnostics bytes.Buffer
	if code := cli.Run(ctx, append(flags, "operation", "wait", id.String()), &out, &diagnostics); code != 130 {
		t.Fatalf("interruption: %d %s", code, diagnostics.String())
	}
}
