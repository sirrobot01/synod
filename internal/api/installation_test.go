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
	"github.com/sirrobot01/synod/internal/buildinfo"
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
		Version        string    `json:"version"`
		Revision       string    `json:"revision"`
	}
	if status != http.StatusOK || json.Unmarshal(data, &value) != nil || value.InstallationID != installation {
		t.Fatalf("installation read: %d %s", status, data)
	}
	if value.Version != buildinfo.Version || value.Revision != buildinfo.Revision {
		t.Fatalf("controller build: %s", data)
	}
}

// Join tokens name the configured address, or the host that reached the controller.
func TestInstallationNamesTheJoinAddress(t *testing.T) {
	server, _, _ := identityServer(t)
	server.APICAFingerprint = "sha256:" + strings.Repeat("ab", 32)
	read := func() map[string]string {
		rec := call(t, server, "GET", "/v1/installation", "", withBootstrap, func(r *http.Request) { r.Host = "192.0.2.20:8443" })
		var body map[string]string
		if rec.Code != 200 || json.Unmarshal(rec.Body.Bytes(), &body) != nil {
			t.Fatalf("installation: %d %s", rec.Code, rec.Body)
		}
		return body
	}
	if body := read(); body["join_address"] != "192.0.2.20:8443" || body["api_ca_fingerprint"] != server.APICAFingerprint {
		t.Fatalf("join settings from the request: %v", body)
	}
	server.JoinAddress = "synod.lan:8443"
	if body := read(); body["join_address"] != "synod.lan:8443" {
		t.Fatalf("configured join address: %v", body)
	}
}
