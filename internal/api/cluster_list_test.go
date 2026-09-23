package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/auth"
	"github.com/sirrobot01/synod/internal/cluster"
	"github.com/sirrobot01/synod/internal/store/postgres"
)

func TestClusterListHTTP(t *testing.T) {
	store, pool, _ := apiTestStore(t)
	installation := uuid.New()
	admin, err := auth.NewAdminToken(bootstrapToken, "test-admin")
	if err != nil {
		t.Fatal(err)
	}
	server, err := NewServer(store, admin, installation, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	https := httptest.NewTLSServer(server)
	defer https.Close()
	send := func(method, path string, authorized bool) (int, http.Header, []byte) {
		t.Helper()
		request, err := http.NewRequestWithContext(t.Context(), method, https.URL+path, nil)
		if err != nil {
			t.Fatal(err)
		}
		if authorized {
			request.Header.Set("Authorization", "Bearer "+bootstrapToken)
		}
		response, err := https.Client().Do(request)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		body, err := io.ReadAll(response.Body)
		if err != nil {
			t.Fatal(err)
		}
		if response.Header.Get("Cache-Control") != "no-store" || response.Header.Get("Content-Type") != "application/json" {
			t.Fatalf("unsafe headers: %v", response.Header)
		}
		if id, err := uuid.Parse(response.Header.Get("X-Request-ID")); err != nil || id == uuid.Nil {
			t.Fatal("missing request identity")
		}
		if method == http.MethodHead && len(body) != 0 {
			t.Fatalf("HEAD returned a body: %s", body)
		}
		if bytes.Contains(body, []byte(bootstrapToken)) || bytes.Contains(body, []byte("password")) || bytes.Contains(body, []byte("ciphertext")) {
			t.Fatal("list exposed credential material")
		}
		return response.StatusCode, response.Header, body
	}
	status, _, body := send(http.MethodGet, "/v1/clusters", true)
	if status != http.StatusOK || strings.TrimSpace(string(body)) != `{"items":[],"next_cursor":""}` {
		t.Fatalf("empty collection: %d %s", status, body)
	}
	var firstID uuid.UUID
	for i := range 52 {
		owner := installation
		if i == 51 {
			owner = uuid.New()
		}
		admission, err := store.CreateCluster(t.Context(), postgres.CreateClusterRequest{
			Identity:  postgres.RequestIdentity{InstallationID: owner, ActorID: "test-admin", APIOperation: postgres.CreateClusterAPIOperation, IdempotencyKey: fmt.Sprintf("list-%d", i)},
			RequestID: "list-fixture",
			Spec:      cluster.CreateSpec{SchemaVersion: 1, Name: fmt.Sprintf("db-%d", i), Engine: cluster.PostgreSQL, PostgreSQL: &cluster.PostgreSQLSpec{Version: "17.11", Topology: "standalone"}},
		})
		if err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			firstID = admission.ClusterID
		}
	}
	status, _, body = send(http.MethodGet, "/v1/clusters", true)
	var first cluster.Page
	if status != http.StatusOK || json.Unmarshal(body, &first) != nil || len(first.Items) != 50 || first.NextCursor == "" {
		t.Fatalf("default page: %d %s", status, body)
	}
	seen := make(map[uuid.UUID]bool)
	for _, item := range first.Items {
		seen[item.ID] = true
		if item.InstallationID != installation || item.Spec.PostgreSQL == nil || item.DesiredRevision != 1 || item.ObservedLifecycle != "provisioning" {
			t.Fatalf("invalid cluster shape: %#v", item)
		}
	}
	status, _, body = send(http.MethodGet, "/v1/clusters?limit=1&cursor="+url.QueryEscape(first.NextCursor), true)
	var last cluster.Page
	if status != http.StatusOK || json.Unmarshal(body, &last) != nil || len(last.Items) != 1 || last.NextCursor != "" || seen[last.Items[0].ID] {
		t.Fatalf("continuation: %d %s", status, body)
	}
	if _, err := pool.Exec(t.Context(), `UPDATE clusters SET desired_lifecycle='deleted',observed_lifecycle='deleted',deleted_at=clock_timestamp() WHERE id=$1`, firstID); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		query string
		want  int
	}{{"?limit=200", 50}, {"?limit=200&include_deleted=false", 50}, {"?limit=200&include_deleted=true", 51}, {"?limit=001", 1}} {
		status, _, body := send(http.MethodGet, "/v1/clusters"+test.query, true)
		var page cluster.Page
		if status != http.StatusOK || json.Unmarshal(body, &page) != nil || len(page.Items) != test.want {
			t.Fatalf("%s: %d %s", test.query, status, body)
		}
	}
	for _, query := range []string{
		"?&", "?limit=2&", "?limit=2&&include_deleted=true", "?limit=", "?limit", "?limit=0", "?limit=201", "?limit=-1", "?limit=%2B1", "?limit=+1", "?limit=1.0", "?limit=1e2", "?limit=9999999999999999999999999999999999999999",
		"?limit=1&limit=2", "?cursor=", "?cursor=invalid", "?cursor=one&cursor=two", "?cursor=" + strings.Repeat("x", 1025),
		"?include_deleted=", "?include_deleted=TRUE", "?include_deleted=1", "?include_deleted=true&include_deleted=true", "?unknown=value", "?=value", "?limit=2;include_deleted=true", "?limit=%zz",
		"?cursor=" + url.QueryEscape(first.NextCursor) + "&include_deleted=true",
	} {
		t.Run(query[:min(len(query), 80)], func(t *testing.T) {
			status, _, body := send(http.MethodGet, "/v1/clusters"+query, true)
			if status != http.StatusBadRequest || !bytes.Contains(body, []byte(`"code":"invalid_request"`)) {
				t.Fatalf("invalid query: %d %s", status, body)
			}
		})
	}
	for _, test := range []struct {
		method, path string
		authorized   bool
		want         int
	}{
		{http.MethodGet, "/v1/clusters?limit=invalid", false, http.StatusUnauthorized},
		{http.MethodHead, "/v1/clusters", false, http.StatusUnauthorized},
		{http.MethodHead, "/v1/clusters", true, http.StatusOK},
		{http.MethodHead, "/v1/clusters?limit=0", true, http.StatusBadRequest},
	} {
		if status, _, body := send(test.method, test.path, test.authorized); status != test.want {
			t.Fatalf("%s %s: %d %s", test.method, test.path, status, body)
		}
	}
	other, err := NewServer(store, admin, uuid.New(), nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	if response := call(t, other, http.MethodGet, "/v1/clusters?cursor="+url.QueryEscape(first.NextCursor), "", withBootstrap); response.Code != http.StatusBadRequest {
		t.Fatalf("foreign installation cursor: %d", response.Code)
	}
}
