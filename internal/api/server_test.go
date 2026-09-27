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
	body := `{"schema_version":1,"name":"example","engine":"postgres","postgresql":{"version":"17.11","topology":"standalone"}}`
	t.Run("authenticate before decode", func(t *testing.T) {
		status, _, _ := send("POST", "/v1/clusters", "invalid", "", "")
		if status != 401 {
			t.Fatalf("status=%d", status)
		}
		var count int
		if err := pool.QueryRow(t.Context(), "SELECT count(*) FROM clusters").Scan(&count); err != nil || count != 0 {
			t.Fatalf("unauthorized request wrote data: %d %v", count, err)
		}
	})
	status, headers, data := send("POST", "/v1/clusters", body, "stable-key", token)
	if status != 202 {
		t.Fatalf("create: %d %s", status, data)
	}
	var first postgres.Admission
	if err := json.Unmarshal(data, &first); err != nil {
		t.Fatal(err)
	}
	if first.Replayed || first.ClusterID == uuid.Nil || first.OperationID == uuid.Nil || headers.Get("Location") != "/v1/operations/"+first.OperationID.String() {
		t.Fatalf("bad admission: %s", data)
	}
	status, _, data = send("POST", "/v1/clusters", body, "stable-key", token)
	var replay postgres.Admission
	if err := json.Unmarshal(data, &replay); err != nil {
		t.Fatal(err)
	}
	if status != 202 || !replay.Replayed || replay.ClusterID != first.ClusterID || replay.OperationID != first.OperationID || !replay.AcceptedAt.Equal(first.AcceptedAt) {
		t.Fatalf("bad replay: %s", data)
	}
	status, headers, data = send("GET", "/v1/clusters/"+first.ClusterID.String(), "", "", token)
	if status != 200 || headers.Get("ETag") != `"1"` {
		t.Fatalf("get cluster: %d %s", status, data)
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
		{"changed key input", "POST", "/v1/clusters", strings.Replace(body, "example", "changed", 1), "stable-key", 409},
		{"name conflict", "POST", "/v1/clusters", body, "new-key", 409},
		{"unsupported version", "POST", "/v1/clusters", strings.Replace(body, "17.11", "99.0", 1), "unsupported", 422},
		// Patroni was removed. Its topology is well formed but unsupported.
		{"patroni topology", "POST", "/v1/clusters", strings.Replace(body, `"topology":"standalone"`, `"topology":"patroni"`, 1), "patroni", 422},
		{"missing key", "POST", "/v1/clusters", body, "", 400},
		{"unknown field", "POST", "/v1/clusters", strings.Replace(body, `"name":`, `"password":"private-input","name":`, 1), "unknown", 400},
		{"second object", "POST", "/v1/clusters", body + `{}`, "extra", 400},
		{"null", "POST", "/v1/clusters", `null`, "null", 400},
		{"large first object", "POST", "/v1/clusters", `{"name":"` + strings.Repeat("x", 65536) + `"}`, "large", 413},
		{"large trailing space", "POST", "/v1/clusters", body + strings.Repeat(" ", 65536), "trailing", 413},
		{"invalid ID", "GET", "/v1/clusters/not-an-id", "", "", 400},
		{"missing cluster", "GET", "/v1/clusters/" + uuid.NewString(), "", "", 404},
		{"missing endpoint", "GET", "/v1/unknown", "", "", 404},
		{"unsupported method", "PATCH", "/v1/clusters/" + first.ClusterID.String(), "", "", 405},
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
	// A new catalog must preserve already accepted work.
	changed, err := NewServer(store, verifier, installation, nil, logger)
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest("POST", "/v1/clusters", strings.NewReader(body))
	request.Header.Set("Authorization", "Bearer "+token)
	request.Header.Set("Idempotency-Key", "stable-key")
	request.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()
	changed.ServeHTTP(recorder, request)
	if recorder.Code != 202 {
		t.Fatalf("catalog replay: %d %s", recorder.Code, recorder.Body)
	}
	other, err := NewServer(store, verifier, uuid.New(), nil, logger)
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"/v1/clusters/" + first.ClusterID.String(), "/v1/operations/" + first.OperationID.String()} {
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
