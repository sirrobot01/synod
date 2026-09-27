package client

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/app"
)

func TestGetAppChecksStatusIdentity(t *testing.T) {
	id := uuid.New()
	for name, value := range map[string]app.App{
		"complete":          {ID: id, Status: &app.Status{OperationID: uuid.New(), State: "pending", FailureCode: "image_inspection_unavailable"}},
		"other app":         {ID: uuid.New(), Status: &app.Status{OperationID: uuid.New()}},
		"missing status":    {ID: id},
		"missing operation": {ID: id, Status: &app.Status{State: "running"}},
	} {
		t.Run(name, func(t *testing.T) {
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != "GET" || r.URL.Path != "/v1/apps/"+id.String() {
					t.Errorf("request %s %s", r.Method, r.URL.Path)
				}
				json.NewEncoder(w).Encode(value)
			}))
			defer server.Close()
			api := &Client{endpoint: server.URL, token: "fixture", http: server.Client(), attempts: 1}
			got, err := api.GetApp(t.Context(), id)
			if name == "complete" {
				if err != nil || got.Status.FailureCode != value.Status.FailureCode {
					t.Fatalf("status: %+v %v", got, err)
				}
				return
			}
			if failure, ok := errors.AsType[*Error](err); !ok || failure.Code != "invalid_response" {
				t.Fatalf("invalid status accepted: %v", err)
			}
		})
	}
}
