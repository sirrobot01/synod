package client

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"
)

func TestGetInstallationChecksIdentities(t *testing.T) {
	for name, value := range map[string]Installation{
		"complete":         {InstallationID: uuid.New()},
		"missing identity": {},
	} {
		t.Run(name, func(t *testing.T) {
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodGet || r.URL.Path != "/v1/installation" {
					http.Error(w, "unexpected request", http.StatusBadRequest)
					return
				}
				json.NewEncoder(w).Encode(value)
			}))
			defer server.Close()
			api := &Client{endpoint: server.URL, token: "fixture", http: server.Client(), attempts: 1}
			got, err := api.GetInstallation(t.Context())
			if name == "complete" {
				if err != nil || got != value {
					t.Fatalf("installation: %+v %v", got, err)
				}
				return
			}
			if failure, ok := errors.AsType[*Error](err); !ok || failure.Code != "invalid_response" {
				t.Fatalf("incomplete installation accepted: %v", err)
			}
		})
	}
}
