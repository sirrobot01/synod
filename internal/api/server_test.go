package api

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"database/sql"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/auth"
	"github.com/sirrobot01/synod/internal/pki"
	"github.com/sirrobot01/synod/internal/store"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
)

// apiTestStore opens a private SQLite metadata database for one test.
func apiTestStore(t *testing.T) *store.Store {
	metadata, _ := apiTestDatabase(t)
	return metadata
}

// apiTestDatabase also returns a raw connection to the same file, so a test can age rows.
func apiTestDatabase(t *testing.T) (*store.Store, *sql.DB) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "synod.db")
	metadata, err := store.Open(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { metadata.Close() })
	raw, err := sql.Open("sqlite", "file:"+path+"?_pragma=busy_timeout(10000)")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { raw.Close() })
	return metadata, raw
}

// enrollTestNode enrolls one node directly through the store and returns its ID.
func enrollTestNode(t *testing.T, metadata *store.Store) uuid.UUID {
	t.Helper()
	installation := uuid.New()
	certificate, key, err := pki.CreateCA(installation, time.Now(), 2*store.AgentCertificateLifetime)
	if err != nil {
		t.Fatal(err)
	}
	ca, err := pki.LoadCA(installation, certificate, key)
	if err != nil {
		t.Fatal(err)
	}
	verifier := sha256.Sum256([]byte(uuid.NewString()))
	if _, err := metadata.RegisterEnrollmentToken(t.Context(), store.TokenRegistration{ActorID: "admin", RequestID: "r1", IdempotencyKey: uuid.NewString(), TokenVerifier: verifier, TTLSeconds: 3600}); err != nil {
		t.Fatal(err)
	}
	_, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	request, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{}, private)
	if err != nil {
		t.Fatal(err)
	}
	node := uuid.New()
	if _, err := metadata.EnrollAgent(t.Context(), store.EnrollmentRequest{NodeID: node, TokenVerifier: verifier, RequestID: uuid.New(), CSRDER: request}, ca); err != nil {
		t.Fatal(err)
	}
	return node
}

func TestHTTPSNodes(t *testing.T) {
	metadata := apiTestStore(t)
	token := strings.Repeat("secret-", 8)
	verifier, err := auth.NewAdminToken(token, "test-admin")
	if err != nil {
		t.Fatal(err)
	}
	var logs bytes.Buffer
	server, err := NewServer(metadata, verifier, uuid.New(), slog.New(slog.NewTextHandler(&logs, nil)))
	if err != nil {
		t.Fatal(err)
	}
	https := httptest.NewTLSServer(server)
	defer https.Close()
	send := func(method, path, credential string) (int, []byte) {
		t.Helper()
		request, err := http.NewRequestWithContext(t.Context(), method, https.URL+path, strings.NewReader("{}"))
		if err != nil {
			t.Fatal(err)
		}
		request.Header.Set("Content-Type", "application/json")
		if credential != "" {
			request.Header.Set("Authorization", "Bearer "+credential)
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
		if response.Header.Get("X-Request-ID") == "" || response.Header.Get("Cache-Control") != "no-store" || !json.Valid(data) {
			t.Fatalf("unsafe or invalid response: %s", data)
		}
		return response.StatusCode, data
	}
	if status, _ := send("GET", "/v1/nodes", ""); status != 401 {
		t.Fatalf("unauthenticated list: %d", status)
	}
	node := enrollTestNode(t, metadata)
	if status, data := send("POST", "/v1/nodes/"+node.String()+"/actions/approve", token); status != 409 || !strings.Contains(string(data), "node_facts_unavailable") {
		t.Fatalf("unseen node approved: %d %s", status, data)
	}
	if err := metadata.RecordAgentSession(t.Context(), node, uuid.New(), &agentv1.Hello{NodeId: node.String(), Architecture: "amd64", Hostname: "server"}); err != nil {
		t.Fatal(err)
	}
	status, data := send("POST", "/v1/nodes/"+node.String()+"/actions/approve", token)
	var approved store.Node
	if status != 200 || json.Unmarshal(data, &approved) != nil || approved.ApprovedAt == nil {
		t.Fatalf("approve: %d %s", status, data)
	}
	status, data = send("GET", "/v1/nodes/by-name/server", token)
	if status != 200 || !strings.Contains(string(data), node.String()) {
		t.Fatalf("by name: %d %s", status, data)
	}
	var nodes []store.Node
	if status, data := send("GET", "/v1/nodes", token); status != 200 || json.Unmarshal(data, &nodes) != nil || len(nodes) != 1 {
		t.Fatalf("list: %d %s", status, data)
	}
	for _, test := range []struct {
		method, path string
		status       int
	}{
		{"GET", "/v1/nodes/not-an-id", 400},
		{"GET", "/v1/nodes/" + uuid.NewString(), 404},
		{"GET", "/v1/unknown", 404},
		{"PUT", "/v1/nodes/" + node.String(), 405},
		{"GET", "/v1/apps", 200},
	} {
		if got, data := send(test.method, test.path, token); got != test.status {
			t.Fatalf("%s %s: %d %s", test.method, test.path, got, data)
		}
	}
	if strings.Contains(logs.String(), token) {
		t.Fatal("logs exposed the credential")
	}
}
