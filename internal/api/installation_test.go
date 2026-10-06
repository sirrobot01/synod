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

// An administrator reads the installation ID.
func TestInstallationRead(t *testing.T) {
	metadata := apiTestStore(t)
	token := strings.Repeat("secret-", 8)
	verifier, err := auth.NewAdminToken(token, "test-admin")
	if err != nil {
		t.Fatal(err)
	}
	installation := uuid.New()
	server, err := NewServer(metadata, verifier, installation, slog.New(slog.NewTextHandler(io.Discard, nil)))
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
	status, data := read(token)
	var value struct {
		InstallationID uuid.UUID `json:"installation_id"`
	}
	if status != http.StatusOK || json.Unmarshal(data, &value) != nil || value.InstallationID != installation {
		t.Fatalf("installation read: %d %s", status, data)
	}
}
