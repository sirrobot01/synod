package api

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/auth"
	"github.com/sirrobot01/synod/internal/placement"
)

// approved sends one policy for a node and returns the response.
func approved(t *testing.T, server *Server, token string, nodeID uuid.UUID, ifMatch, body string) (int, http.Header, []byte) {
	t.Helper()
	request := httptest.NewRequest("PUT", "/v1/nodes/"+nodeID.String()+"/placement-policy", strings.NewReader(body))
	request.Header.Set("Authorization", "Bearer "+token)
	request.Header.Set("Content-Type", "application/json")
	if ifMatch != "" {
		request.Header.Set("If-Match", ifMatch)
	}
	recorder := httptest.NewRecorder()
	server.ServeHTTP(recorder, request)
	response := recorder.Result()
	defer response.Body.Close()
	data, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	return response.StatusCode, response.Header, data
}

const soundPolicy = `{"architecture":"amd64","runtime_version":"27.1.1","storage_id":"pool-a",
 "engines":[{"engine":"postgres","version":"17.11"}],
 "headroom":{"cpu_millis":500,"memory_bytes":268435456,"disk_bytes":1073741824}}`

// An enrolled node with a policy becomes a candidate for work. Without one the planner
// refuses it, however much capacity it reports.
func TestNodePolicyMakesANodePlaceable(t *testing.T) {
	store, pool, _ := apiTestStore(t)
	token := strings.Repeat("secret-", 8)
	verifier, err := auth.NewAdminToken(token, "test-admin")
	if err != nil {
		t.Fatal(err)
	}
	installation, node := uuid.New(), uuid.New()
	server, err := NewServer(store, verifier, installation, testCatalog(t), slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(t.Context(), `INSERT INTO nodes (id,installation_id,public_key_hash,enrolled_at)
 VALUES ($1,$2,$3,clock_timestamp())`, node, installation, make([]byte, 32)); err != nil {
		t.Fatal(err)
	}
	// Before any policy the snapshot carries the node with none, which is what the
	// planner refuses on.
	before, err := store.ReadPlacementSnapshot(t.Context(), installation)
	if err != nil {
		t.Fatal(err)
	}
	if len(before.Nodes) != 1 || before.Nodes[0].Policy != nil {
		t.Fatalf("snapshot=%+v, want an enrolled node with no policy", before.Nodes)
	}
	body := strings.Replace(soundPolicy, `"architecture":`, `"client_address":"192.168.10.25","architecture":`, 1)
	status, headers, data := approved(t, server, token, node, "0", body)
	if status != 200 {
		t.Fatalf("status=%d body=%s", status, data)
	}
	if headers.Get("ETag") != `"1"` {
		t.Fatalf("etag=%q, want the first revision", headers.Get("ETag"))
	}
	after, err := store.ReadPlacementSnapshot(t.Context(), installation)
	if err != nil {
		t.Fatal(err)
	}
	if len(after.Nodes) != 1 || after.Nodes[0].Policy == nil {
		t.Fatal("the node has no policy after one was approved")
	}
	stored := after.Nodes[0].Policy
	if stored.Architecture != "amd64" || stored.StorageID != "pool-a" || stored.Revision != 1 || stored.ClientAddress != "192.168.10.25" {
		t.Fatalf("policy=%+v, want what was approved", stored)
	}
	if len(stored.Engines) != 1 || stored.Engines[0] != (placement.EngineVersion{Engine: "postgres", Version: "17.11"}) {
		t.Fatalf("engines=%+v, want the approved engine", stored.Engines)
	}
	for _, address := range []string{"127.0.0.1", "0.0.0.0", "::1", "fe80::1", "example.com", "::ffff:192.168.10.25"} {
		invalid := strings.Replace(body, "192.168.10.25", address, 1)
		if status, _, _ := approved(t, server, token, node, "1", invalid); status != http.StatusBadRequest {
			t.Fatalf("invalid client address %q returned %d", address, status)
		}
	}
}

// Two operators editing one node must not silently overwrite each other.
func TestNodePolicyCarriesItsExpectedRevision(t *testing.T) {
	store, pool, _ := apiTestStore(t)
	token := strings.Repeat("secret-", 8)
	verifier, err := auth.NewAdminToken(token, "test-admin")
	if err != nil {
		t.Fatal(err)
	}
	installation, node := uuid.New(), uuid.New()
	server, err := NewServer(store, verifier, installation, testCatalog(t), slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(t.Context(), `INSERT INTO nodes (id,installation_id,public_key_hash,enrolled_at)
 VALUES ($1,$2,$3,clock_timestamp())`, node, installation, make([]byte, 32)); err != nil {
		t.Fatal(err)
	}
	if status, _, data := approved(t, server, token, node, "0", soundPolicy); status != 200 {
		t.Fatalf("status=%d body=%s", status, data)
	}
	// A second writer still holding revision 0 is refused.
	changed := strings.Replace(soundPolicy, `"architecture":`, `"client_address":"192.168.10.50","cordoned":true,"architecture":`, 1)
	if status, _, data := approved(t, server, token, node, "0", changed); status != http.StatusPreconditionFailed || !bytes.Contains(data, []byte(`"code":"revision_conflict"`)) {
		t.Fatalf("stale policy revision: status=%d body=%s", status, data)
	}
	snapshot, err := store.ReadPlacementSnapshot(t.Context(), installation)
	if err != nil || len(snapshot.Nodes) != 1 || snapshot.Nodes[0].Policy == nil {
		t.Fatalf("read policy after refused update: %v", err)
	}
	if policy := snapshot.Nodes[0].Policy; policy.Revision != 1 || policy.ClientAddress != "" || policy.Cordoned {
		t.Fatalf("stale expected revision changed the policy: %+v", policy)
	}
	// The writer that read revision 1 succeeds.
	if status, headers, data := approved(t, server, token, node, `"1"`, soundPolicy); status != 200 || headers.Get("ETag") != `"2"` {
		t.Fatalf("status=%d etag=%q body=%s", status, headers.Get("ETag"), data)
	}
	// A missing header is refused rather than treated as zero. "I did not look" and
	// "I looked and there was nothing" are different claims.
	if status, _, _ := approved(t, server, token, node, "", soundPolicy); status != 400 {
		t.Fatal("a policy without an expected revision was accepted")
	}
}

// A node may only be approved for what this installation serves, and only if this
// installation enrolled it.
func TestNodePolicyRefusesWhatItCannotServe(t *testing.T) {
	store, pool, _ := apiTestStore(t)
	token := strings.Repeat("secret-", 8)
	verifier, err := auth.NewAdminToken(token, "test-admin")
	if err != nil {
		t.Fatal(err)
	}
	installation, node := uuid.New(), uuid.New()
	server, err := NewServer(store, verifier, installation, testCatalog(t), slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(t.Context(), `INSERT INTO nodes (id,installation_id,public_key_hash,enrolled_at)
 VALUES ($1,$2,$3,clock_timestamp())`, node, installation, make([]byte, 32)); err != nil {
		t.Fatal(err)
	}
	unserved := strings.Replace(soundPolicy, `"17.11"`, `"9.6"`, 1)
	if status, _, _ := approved(t, server, token, node, "0", unserved); status != 422 {
		t.Fatal("a node was approved for an engine version nothing serves")
	}
	// A node this installation never enrolled cannot be approved.
	if status, _, _ := approved(t, server, token, uuid.New(), "0", soundPolicy); status == 200 {
		t.Fatal("a node this installation has not enrolled was approved")
	}
	// Headroom is operator intent and the store requires it.
	noHeadroom := `{"architecture":"amd64","runtime_version":"27.1.1","storage_id":"pool-a",
 "engines":[{"engine":"postgres","version":"17.11"}]}`
	if status, _, _ := approved(t, server, token, node, "0", noHeadroom); status == 200 {
		t.Fatal("a policy with no declared headroom was accepted")
	}
}

// The route is an administrator action.
func TestNodePolicyNeedsTheAdministratorRole(t *testing.T) {
	store, _, _ := apiTestStore(t)
	token := strings.Repeat("secret-", 8)
	verifier, err := auth.NewAdminToken(token, "test-admin")
	if err != nil {
		t.Fatal(err)
	}
	server, err := NewServer(store, verifier, uuid.New(), testCatalog(t), slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest("PUT", "/v1/nodes/"+uuid.NewString()+"/placement-policy", bytes.NewReader([]byte(soundPolicy)))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("If-Match", "0")
	recorder := httptest.NewRecorder()
	server.ServeHTTP(recorder, request)
	if recorder.Code != 401 {
		t.Fatalf("status=%d, want an unauthenticated refusal", recorder.Code)
	}
	var body map[string]any
	if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
}
