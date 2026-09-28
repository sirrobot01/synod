package api

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sirrobot01/synod/internal/app"
	"github.com/sirrobot01/synod/internal/auth"
	"github.com/sirrobot01/synod/internal/engine"
	postgresengine "github.com/sirrobot01/synod/internal/engine/postgres"
	redisengine "github.com/sirrobot01/synod/internal/engine/redis"
	"github.com/sirrobot01/synod/internal/store/postgres"
)

// The fixture uses a separate database so API writes cannot affect other suites.
// testCatalog serves what this installation advertises.
func testCatalog(t *testing.T) *engine.Catalog {
	t.Helper()
	catalog, err := engine.NewCatalog(postgresengine.New(), redisengine.New())
	if err != nil {
		t.Fatal(err)
	}
	return catalog
}

func TestHTTPSAdmission(t *testing.T) {
	store, pool, dsn := apiTestStore(t)
	token := strings.Repeat("secret-", 8)
	verifier, err := auth.NewAdminToken(token, "test-admin")
	if err != nil {
		t.Fatal(err)
	}
	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, nil))
	installation := uuid.New()
	server, err := NewServer(store, verifier, installation, testCatalog(t), logger)
	if err != nil {
		t.Fatal(err)
	}
	https := httptest.NewTLSServer(server)
	defer https.Close()
	client := https.Client()
	// This helper keeps the wire checks identical for every success and failure.
	send := func(method, path, body, key, credential string) (int, http.Header, []byte) {
		t.Helper()
		request, err := http.NewRequestWithContext(t.Context(), method, https.URL+path, strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		request.Header.Set("Content-Type", "application/json")
		if key != "" {
			request.Header.Set("Idempotency-Key", key)
		}
		if credential != "" {
			request.Header.Set("Authorization", "Bearer "+credential)
		}
		response, err := client.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		data, err := io.ReadAll(response.Body)
		if err != nil {
			t.Fatal(err)
		}
		if response.Header.Get("X-Request-ID") == "" || response.Header.Get("Cache-Control") != "no-store" {
			t.Fatal("missing safe response headers")
		}
		if response.Header.Get("Content-Type") != "application/json" || !json.Valid(data) {
			t.Fatalf("invalid JSON response: %s", data)
		}
		if bytes.Contains(data, []byte(token)) {
			t.Fatal("response exposed credential")
		}
		return response.StatusCode, response.Header, data
	}
	body := `{"schema_version":1,"name":"example","tier":3,"copies":1,"service":{"image":"nginx","ports":[{"container_port":80,"protocol":"tcp"}]},"router":{"protocol":"tcp","port":8080}}`
	t.Run("authenticate before decode", func(t *testing.T) {
		status, _, _ := send("POST", "/v1/apps", "invalid", "", "")
		if status != 401 {
			t.Fatalf("status=%d", status)
		}
		var count int
		if err := pool.QueryRow(t.Context(), "SELECT count(*) FROM apps").Scan(&count); err != nil || count != 0 {
			t.Fatalf("unauthorized request wrote data: %d %v", count, err)
		}
	})
	status, _, data := send("POST", "/v1/apps", body, "stable-key", token)
	if status != 202 {
		t.Fatalf("create: %d %s", status, data)
	}
	var first app.Admission
	if err := json.Unmarshal(data, &first); err != nil {
		t.Fatal(err)
	}
	if first.Replayed || first.AppID == uuid.Nil || first.OperationID == uuid.Nil {
		t.Fatalf("bad admission: %s", data)
	}
	status, _, data = send("POST", "/v1/apps", body, "stable-key", token)
	var replay app.Admission
	if err := json.Unmarshal(data, &replay); err != nil {
		t.Fatal(err)
	}
	if status != 202 || !replay.Replayed || replay.AppID != first.AppID || replay.OperationID != first.OperationID || !replay.AcceptedAt.Equal(first.AcceptedAt) {
		t.Fatalf("bad replay: %s", data)
	}
	status, _, data = send("GET", "/v1/operations/"+first.OperationID.String(), "", "", token)
	var operation map[string]any
	if err := json.Unmarshal(data, &operation); err != nil {
		t.Fatal(err)
	}
	if status != 200 || operation["plan_version"] != float64(0) || operation["status"] != "pending" {
		t.Fatalf("get operation: %d %s", status, data)
	}
	for _, test := range []struct {
		name, method, path, body, key string
		status                        int
	}{
		{"large first object", "POST", "/v1/apps", `{"name":"` + strings.Repeat("x", 65536) + `"}`, "large", 413},
		{"large trailing space", "POST", "/v1/apps", body + strings.Repeat(" ", 65536), "trailing", 413},
		{"unknown field", "POST", "/v1/apps", strings.Replace(body, `"name":`, `"password":"private-input","name":`, 1), "unknown", 400},
		{"invalid ID", "GET", "/v1/apps/not-an-id", "", "", 400},
		{"missing app", "GET", "/v1/apps/" + uuid.NewString(), "", "", 404},
		{"missing endpoint", "GET", "/v1/unknown", "", "", 404},
		{"unsupported method", "PATCH", "/v1/apps/" + first.AppID.String(), "", "", 405},
		{"removed cluster route", "GET", "/v1/clusters", "", "", 404},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, _, data := send(test.method, test.path, test.body, test.key, token)
			if got != test.status {
				t.Fatalf("status=%d want=%d: %s", got, test.status, data)
			}
			if bytes.Contains(data, []byte("private-input")) {
				t.Fatal("input leaked")
			}
		})
	}
	other, err := NewServer(store, verifier, uuid.New(), nil, logger)
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"/v1/apps/" + first.AppID.String(), "/v1/operations/" + first.OperationID.String()} {
		request := httptest.NewRequest("GET", path, nil)
		request.Header.Set("Authorization", "Bearer "+token)
		recorder := httptest.NewRecorder()
		other.ServeHTTP(recorder, request)
		if recorder.Code != 404 {
			t.Fatalf("installation scope: %d %s", recorder.Code, recorder.Body)
		}
	}
	if strings.Contains(logs.String(), token) || strings.Contains(logs.String(), "private-input") || strings.Contains(logs.String(), dsn) {
		t.Fatal("logs exposed credentials or rejected body")
	}
}

func apiTestStore(t *testing.T) (*postgres.Store, *pgxpool.Pool, string) {
	t.Helper()
	dsn := os.Getenv("SYNOD_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("run scripts/test-postgres.sh")
	}
	adminPool, err := pgxpool.New(t.Context(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(adminPool.Close)
	name := "api_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if _, err := adminPool.Exec(t.Context(), "CREATE DATABASE "+pgx.Identifier{name}.Sanitize()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if _, err := adminPool.Exec(ctx, "DROP DATABASE "+pgx.Identifier{name}.Sanitize()+" WITH (FORCE)"); err != nil {
			t.Error(err)
		}
	})
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	config.ConnConfig.Database = name
	pool, err := pgxpool.NewWithConfig(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	store := postgres.New(pool)
	if err := store.Migrate(t.Context()); err != nil {
		t.Fatal(err)
	}

	return store, pool, dsn
}
