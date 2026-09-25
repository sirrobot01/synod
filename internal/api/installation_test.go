package api

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/auth"
)

// An administrator reads the installation ID and recovery generation. Before a runner
// records a generation, the read says why no agent can be configured yet.
func TestInstallationRead(t *testing.T) {
	store, _, _ := apiTestStore(t)
	token := strings.Repeat("secret-", 8)
	verifier, err := auth.NewAdminToken(token, "test-admin")
	if err != nil {
		t.Fatal(err)
	}
	installation := uuid.New()
	server, err := NewServer(store, verifier, installation, testCatalog(t), slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	https := httptest.NewTLSServer(server)
	defer https.Close()
	read := func(bearer string) (int, []byte) {
		t.Helper()
		request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, https.URL+"/v1/installation", nil)
		if err != nil {
			t.Fatal(err)
		}
		if bearer != "" {
			request.Header.Set("Authorization", "Bearer "+bearer)
		}
		response, err := https.Client().Do(request)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		data, err := io.ReadAll(response.Body)
		if err != nil {
			t.Fatal(err)
		}
		return response.StatusCode, data
	}
	if status, data := read(""); status != http.StatusUnauthorized {
		t.Fatalf("anonymous read: %d %s", status, data)
	}
	if status, data := read(token); status != http.StatusServiceUnavailable || !strings.Contains(string(data), `"recovery_generation_unavailable"`) {
		t.Fatalf("read before a runner: %d %s", status, data)
	}
	generation := uuid.New()
	if err := store.InitializeGeneration(t.Context(), installation, generation); err != nil {
		t.Fatal(err)
	}
	status, data := read(token)
	var value struct {
		InstallationID     uuid.UUID `json:"installation_id"`
		RecoveryGeneration uuid.UUID `json:"recovery_generation"`
	}
	if status != http.StatusOK || json.Unmarshal(data, &value) != nil || value.InstallationID != installation || value.RecoveryGeneration != generation {
		t.Fatalf("installation read: %d %s", status, data)
	}
}
