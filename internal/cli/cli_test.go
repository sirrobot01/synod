package cli_test

import (
	"bytes"
	"context"
	"encoding/json"
	"encoding/pem"
	"io"
	"log"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sirrobot01/synod/internal/api"
	"github.com/sirrobot01/synod/internal/auth"
	"github.com/sirrobot01/synod/internal/cli"
	"github.com/sirrobot01/synod/internal/client"
	"github.com/sirrobot01/synod/internal/cluster"
	"github.com/sirrobot01/synod/internal/engine"
	postgresengine "github.com/sirrobot01/synod/internal/engine/postgres"
	redisengine "github.com/sirrobot01/synod/internal/engine/redis"
	"github.com/sirrobot01/synod/internal/operation"
	"github.com/sirrobot01/synod/internal/store/postgres"
)

const token = "cli-fixture-private-token-never-print-this-value"

// Each client test needs a real TLS listener and private token file.
// cliCatalog serves what this installation advertises.
func cliCatalog(t *testing.T) *engine.Catalog {
	t.Helper()
	catalog, err := engine.NewCatalog(postgresengine.New(), redisengine.New())
	if err != nil {
		t.Fatal(err)
	}
	return catalog
}

func fixture(t *testing.T, handler http.Handler) []string {
	t.Helper()
	server := httptest.NewUnstartedServer(handler)
	server.Config.ErrorLog = log.New(io.Discard, "", 0)
	server.StartTLS()
	t.Cleanup(server.Close)
	directory := t.TempDir()
	tokenFile, caFile := filepath.Join(directory, "token"), filepath.Join(directory, "ca.pem")
	if err := os.WriteFile(tokenFile, []byte(token+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(caFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw}), 0o644); err != nil {
		t.Fatal(err)
	}
	return []string{"--json", "--api-url", server.URL, "--token-file", tokenFile, "--ca-file", caFile, "--request-timeout", "1s", "--poll-interval", "5ms"}
}

func TestLostReplyRetainsKeyAndBody(t *testing.T) {
	admission := client.Admission{ClusterID: uuid.New(), OperationID: uuid.New(), DesiredRevision: 1, AcceptedAt: time.Now().UTC()}
	var mutex sync.Mutex
	var bodies, keys []string
	flags := fixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.TLS == nil || r.Header.Get("Authorization") != "Bearer "+token || r.Method != "POST" || r.URL.Path != "/v1/clusters" {
			t.Error("wrong authenticated TLS request")
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
		}
		mutex.Lock()
		defer mutex.Unlock()
		bodies = append(bodies, string(body))
		keys = append(keys, r.Header.Get("Idempotency-Key"))
		if len(bodies) == 1 {
			// The server accepted the effect but the client loses its reply.
			connection, _, err := w.(http.Hijacker).Hijack()
			if err != nil {
				t.Error(err)
				return
			}
			connection.Close()
			return
		}
		admission.Replayed = true
		w.WriteHeader(202)
		json.NewEncoder(w).Encode(admission)
	}))
	var stdout, stderr bytes.Buffer
	code := cli.Run(t.Context(), append(flags, "cluster", "create", "postgres", "--name", "photos", "--version", "17.11"), &stdout, &stderr)
	var result cli.Result
	if err := json.Unmarshal(stdout.Bytes(), &result); err != nil {
		t.Fatalf("invalid JSON: %v: %s", err, stdout.String())
	}
	mutex.Lock()
	defer mutex.Unlock()
	if code != 0 || len(keys) != 2 || keys[0] == "" || keys[0] != keys[1] || bodies[0] != bodies[1] {
		t.Fatalf("retry failure: code=%d requests=%d diagnostics=%s", code, len(keys), stderr.String())
	}
	if result.Admission == nil || result.Admission.OperationID != admission.OperationID || !result.Admission.Replayed || result.IdempotencyKey != keys[0] {
		t.Fatalf("unstable admission: %s", stdout.String())
	}
	if !strings.Contains(stderr.String(), keys[0]) || strings.Contains(stdout.String()+stderr.String(), token) {
		t.Fatal("key disclosure or token redaction failed")
	}
}

func TestRetryLimitsManualKeyAndRedaction(t *testing.T) {
	for _, status := range []int{503, 409} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			var requests atomic.Int32
			flags := fixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests.Add(1)
				if r.Header.Get("Idempotency-Key") != "manual-recovery-key" {
					t.Error("manual key changed")
				}
				w.WriteHeader(status)
				json.NewEncoder(w).Encode(map[string]string{"code": "outcome_unknown", "message": token, "request_id": token})
			}))
			var stdout, stderr bytes.Buffer
			code := cli.Run(t.Context(), append(flags, "db", "create", "redis", "--name", "cache", "--version", "8.2.7", "--persistence", "aof", "--idempotency-key", "manual-recovery-key", "--attempts", "2"), &stdout, &stderr)
			var result cli.Result
			if err := json.Unmarshal(stdout.Bytes(), &result); err != nil || code != 1 || result.Error == nil || result.IdempotencyKey != "manual-recovery-key" {
				t.Fatalf("wrong failure result: code=%d output=%s", code, stdout.String())
			}
			want := int32(1)
			if status == 503 {
				want = 2
			}
			if requests.Load() != want || strings.Contains(stdout.String()+stderr.String(), token) {
				t.Fatal("retry bound or error redaction failed")
			}
		})
	}
}

func TestTLSRedirectAndProtectedCredentials(t *testing.T) {
	var forwarded atomic.Int32
	target := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { forwarded.Add(1) }))
	defer target.Close()
	flags := fixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, target.URL, 307) }))
	for _, test := range []struct {
		name  string
		extra []string
	}{
		{"redirect rejected", nil},
		{"untrusted certificate", []string{"--ca-file", ""}},
		{"HTTP rejected", []string{"--api-url", "http://localhost:8443"}},
		{"URL credentials rejected", []string{"--api-url", "https://user:" + token + "@localhost:8443"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			args := append(append([]string{}, flags...), "operation", "get", uuid.NewString(), "--attempts", "1")
			code := cli.Run(t.Context(), append(args, test.extra...), &stdout, &stderr)
			if code != 1 || !json.Valid(stdout.Bytes()) || strings.Contains(stdout.String()+stderr.String(), token) {
				t.Fatalf("unsafe error: code=%d stdout=%s stderr=%s", code, stdout.String(), stderr.String())
			}
		})
	}
	if forwarded.Load() != 0 {
		t.Fatal("redirect forwarded a credential-bearing request")
	}
	for _, mode := range []os.FileMode{0o644, 0o640} {
		path := filepath.Join(t.TempDir(), "public-token")
		if err := os.WriteFile(path, []byte(token), mode); err != nil {
			t.Fatal(err)
		}
		var stdout, stderr bytes.Buffer
		args := append(append([]string{}, flags...), "operation", "get", uuid.NewString(), "--token-file", path)
		if code := cli.Run(t.Context(), args, &stdout, &stderr); code != 1 || strings.Contains(stdout.String()+stderr.String(), token) {
			t.Fatal("accepted an exposed token file")
		}
	}
	fifo := filepath.Join(t.TempDir(), "token-fifo")
	if err := syscall.Mkfifo(fifo, 0o600); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	if code := cli.Run(t.Context(), append(flags, "operation", "get", uuid.NewString(), "--token-file", fifo), &stdout, &stderr); code != 1 {
		t.Fatal("accepted a FIFO token")
	}
}

func TestWaitOutcomesNeverCancel(t *testing.T) {
	id := uuid.New()
	for _, test := range []struct {
		name      string
		status    operation.Status
		code      int
		errorCode string
	}{
		{"success", operation.Succeeded, 0, ""},
		{"timeout", operation.Pending, 2, "wait_timeout"},
		{"failed", operation.Failed, 3, "operation_failed"},
		{"cancelled", operation.Cancelled, 4, "operation_cancelled"},
	} {
		t.Run(test.name, func(t *testing.T) {
			var reads atomic.Int32
			flags := fixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != "GET" || r.URL.Path != "/v1/operations/"+id.String() {
					t.Error("wait sent a mutation")
				}
				reads.Add(1)
				json.NewEncoder(w).Encode(operation.Operation{ID: id, Status: test.status})
			}))
			var stdout, stderr bytes.Buffer
			code := cli.Run(t.Context(), append(flags, "operation", "wait", id.String(), "--timeout", "70ms"), &stdout, &stderr)
			var result cli.Result
			if err := json.Unmarshal(stdout.Bytes(), &result); err != nil || code != test.code || result.Operation == nil || result.Operation.ID != id {
				t.Fatalf("wait result: code=%d stdout=%s stderr=%s", code, stdout.String(), stderr.String())
			}
			if test.errorCode != "" && (result.Error == nil || result.Error.Code != test.errorCode) {
				t.Fatal("wait outcome was not distinct")
			}
			if test.code == 2 && reads.Load() < 2 {
				t.Fatal("wait did not poll")
			}
		})
	}
}

func TestHelpAndInvalidArguments(t *testing.T) {
	t.Setenv("SYNOD_API_URL", "https://user:"+token+"@localhost:8443")
	for _, command := range [][]string{{"--help"}, {"db", "create", "postgres", "--help"}, {"operation", "wait", "--help"}} {
		var stdout, stderr bytes.Buffer
		if code := cli.Run(t.Context(), command, &stdout, &stderr); code != 0 || stdout.Len() == 0 || stderr.Len() != 0 || strings.Contains(stdout.String()+stderr.String(), token) {
			t.Fatalf("help failed: %d %s", code, stderr.String())
		}
	}
	var stdout, stderr bytes.Buffer
	code := cli.Run(t.Context(), []string{"--json", "--request-timeout", token}, &stdout, &stderr)
	if code != 1 || !json.Valid(stdout.Bytes()) || strings.Contains(stdout.String()+stderr.String(), token) {
		t.Fatal("invalid arguments leaked input or broke JSON")
	}
}

func TestPostgresAPIFlow(t *testing.T) {
	dsn := os.Getenv("SYNOD_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("run scripts/test-postgres.sh")
	}
	admin, err := pgxpool.New(t.Context(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	name := "cli_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if _, err := admin.Exec(t.Context(), "CREATE DATABASE "+pgx.Identifier{name}.Sanitize()); err != nil {
		t.Fatal(err)
	}
	defer func() {
		// The bound guards against a hang, and is not an assertion about speed:
		// on a loaded host many suites drop their databases at once, and a teardown
		// that is merely slow must not fail a test that passed.
		ctx, cancel := context.WithTimeout(context.Background(), dropTimeout)
		defer cancel()
		if _, err := admin.Exec(ctx, "DROP DATABASE "+pgx.Identifier{name}.Sanitize()+" WITH (FORCE)"); err != nil {
			t.Error(err)
		}
	}()
	configuration, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	configuration.ConnConfig.Database = name
	pool, err := pgxpool.NewWithConfig(t.Context(), configuration)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	store := postgres.New(pool)
	if err := store.Migrate(t.Context()); err != nil {
		t.Fatal(err)
	}
	verifier, err := auth.NewAdminToken(token, "cli-test-admin")
	if err != nil {
		t.Fatal(err)
	}
	installation := uuid.New()
	handler, err := api.NewServer(store, verifier, installation, cliCatalog(t), slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	flags := fixture(t, handler)
	var first client.Admission
	for _, engine := range []string{"postgres", "redis"} {
		version := "17.11"
		if engine == "redis" {
			version = "8.2.7"
		}
		for attempt := range 2 {
			var stdout, stderr bytes.Buffer
			args := append(append([]string{}, flags...), "db", "create", engine, "--name", engine+"-cli", "--version", version, "--idempotency-key", engine+"-stable-key")
			code := cli.Run(t.Context(), args, &stdout, &stderr)
			var result cli.Result
			if err := json.Unmarshal(stdout.Bytes(), &result); err != nil || code != 0 || result.Admission == nil || result.Admission.Replayed != (attempt == 1) {
				t.Fatalf("admission: %d %s %s", code, stdout.String(), stderr.String())
			}
			if attempt == 0 {
				first = *result.Admission
			} else if result.Admission.ClusterID != first.ClusterID || result.Admission.OperationID != first.OperationID {
				t.Fatal("replay changed resource IDs")
			}
		}
		for _, action := range [][]string{{"cluster", "inspect", first.ClusterID.String()}, {"operation", "get", first.OperationID.String()}, {"operation", "wait", first.OperationID.String(), "--timeout", "70ms"}} {
			var stdout, stderr bytes.Buffer
			code := cli.Run(t.Context(), append(append([]string{}, flags...), action...), &stdout, &stderr)
			want := 0
			if action[1] == "wait" {
				want = 2
			}
			if code != want || !json.Valid(stdout.Bytes()) {
				t.Fatalf("inspection: %d %s %s", code, stdout.String(), stderr.String())
			}
		}
	}
	var clusters, operations, cancelled int
	var cursor string
	var listed []uuid.UUID
	for pageIndex := range 2 {
		var stdout, stderr bytes.Buffer
		args := append(append([]string{}, flags...), "db", "list", "--limit", "1")
		if cursor != "" {
			args = append(args, "--cursor", cursor)
		}
		code := cli.Run(t.Context(), args, &stdout, &stderr)
		var result cli.Result
		if err := json.Unmarshal(stdout.Bytes(), &result); err != nil || code != 0 || result.ClusterPage == nil || len(result.ClusterPage.Items) != 1 {
			t.Fatalf("list page %d: code=%d output=%s error=%s", pageIndex, code, stdout.String(), stderr.String())
		}
		item := result.ClusterPage.Items[0]
		wantEngine := cluster.Redis
		if pageIndex == 1 {
			wantEngine = cluster.PostgreSQL
		}
		if item.InstallationID != installation || item.Spec.Engine != wantEngine || item.DesiredRevision != 1 || item.ObservedRevision != 0 || item.ObservedLifecycle != cluster.ObservedProvisioning {
			t.Fatalf("unexpected stored cluster: %+v", item)
		}
		listed = append(listed, item.ID)
		cursor = result.ClusterPage.NextCursor
		if (cursor != "") != (pageIndex == 0) {
			t.Fatalf("page %d continuation=%q", pageIndex, cursor)
		}
	}
	if listed[0] == listed[1] {
		t.Fatal("continuation repeated a cluster")
	}
	if err := pool.QueryRow(t.Context(), "SELECT (SELECT count(*) FROM clusters), (SELECT count(*) FROM operations), (SELECT count(*) FROM operations WHERE cancellation_requested_at IS NOT NULL)").Scan(&clusters, &operations, &cancelled); err != nil || clusters != 2 || operations != 2 || cancelled != 0 {
		t.Fatalf("resource or cancellation counts: %d %d %d %v", clusters, operations, cancelled, err)
	}
}

func TestCreateWaitRetainsAdmission(t *testing.T) {
	for _, finish := range []bool{true, false} {
		t.Run(map[bool]string{true: "succeeded", false: "timeout"}[finish], func(t *testing.T) {
			admission := client.Admission{ClusterID: uuid.New(), OperationID: uuid.New(), DesiredRevision: 1, AcceptedAt: time.Now().UTC()}
			var writes, reads atomic.Int32
			flags := fixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch {
				case r.Method == "POST" && r.URL.Path == "/v1/clusters":
					writes.Add(1)
					w.WriteHeader(202)
					json.NewEncoder(w).Encode(admission)
				case r.Method == "GET" && r.URL.Path == "/v1/operations/"+admission.OperationID.String():
					status := operation.Pending
					if reads.Add(1) > 1 && finish {
						status = operation.Succeeded
					}
					json.NewEncoder(w).Encode(operation.Operation{ID: admission.OperationID, Status: status})
				default:
					t.Error("unexpected mutation while waiting")
					w.WriteHeader(400)
				}
			}))
			var stdout, stderr bytes.Buffer
			code := cli.Run(t.Context(), append(flags, "db", "create", "postgres", "--name", "photos", "--version", "17.11", "--wait", "--timeout", "100ms"), &stdout, &stderr)
			want := 2
			if finish {
				want = 0
			}
			var result cli.Result
			if err := json.Unmarshal(stdout.Bytes(), &result); err != nil || code != want || result.Admission == nil || result.Admission.OperationID != admission.OperationID || result.Operation == nil || result.IdempotencyKey == "" || writes.Load() != 1 || reads.Load() < 2 {
				t.Fatalf("lost admission after wait: code=%d output=%s", code, stdout.String())
			}
		})
	}
}

func TestInvalidResponsesAndInterruption(t *testing.T) {
	for _, body := range []string{`{"id":"` + token + `"}`, strings.Repeat("x", (1<<20)+1)} {
		flags := fixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, body) }))
		var stdout, stderr bytes.Buffer
		code := cli.Run(t.Context(), append(flags, "operation", "get", uuid.NewString()), &stdout, &stderr)
		var result cli.Result
		if err := json.Unmarshal(stdout.Bytes(), &result); err != nil || code != 1 || result.Error == nil || result.Error.Code != "invalid_response" || strings.Contains(stdout.String()+stderr.String(), token) {
			t.Fatal("invalid response was not bounded and redacted")
		}
	}
	flags := fixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { t.Error("cancelled context reached server") }))
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	var stdout, stderr bytes.Buffer
	if code := cli.Run(ctx, append(flags, "operation", "wait", uuid.NewString()), &stdout, &stderr); code != 130 || !strings.Contains(stdout.String(), `"code":"interrupted"`) {
		t.Fatalf("interruption outcome: %d %s", code, stdout.String())
	}
}

// Approving a node is the step between enrolling it and it being able to hold work.
func TestNodeApproveSendsTheDeclaredPolicy(t *testing.T) {
	var body []byte
	var ifMatch, method, path string
	flags := fixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		method, path, ifMatch = r.Method, r.URL.Path, r.Header.Get("If-Match")
		body, _ = io.ReadAll(r.Body)
		json.NewEncoder(w).Encode(map[string]any{"node_id": uuid.New(), "revision": 1})
	}))
	node := uuid.NewString()
	var stdout, stderr bytes.Buffer
	code := cli.Run(t.Context(), append(flags, "node", "approve", "--node-id", node,
		"--architecture", "arm64", "--runtime-version", "27.1.1", "--storage-id", "pool-a",
		"--engine", "postgres:17.11", "--headroom-cpu-millis", "500",
		"--headroom-memory-bytes", "268435456", "--headroom-disk-bytes", "1073741824"), &stdout, &stderr)
	if code != 0 {
		t.Fatalf("code=%d stdout=%s stderr=%s", code, stdout.String(), stderr.String())
	}
	if method != "PUT" || path != "/v1/nodes/"+node+"/placement-policy" {
		t.Fatalf("%s %s, want the node policy route", method, path)
	}
	// A first write expects revision 0, and the expectation travels.
	if ifMatch != `"0"` {
		t.Fatalf("if-match=%q, want the expected revision", ifMatch)
	}
	var sent map[string]any
	if err := json.Unmarshal(body, &sent); err != nil {
		t.Fatal(err)
	}
	if sent["architecture"] != "arm64" || sent["storage_id"] != "pool-a" {
		t.Fatalf("body=%s, want what was declared", body)
	}
	engines, ok := sent["engines"].([]any)
	if !ok || len(engines) != 1 {
		t.Fatalf("engines=%v, want the declared engine", sent["engines"])
	}
}

// An engine declared without a version is refused before anything is sent.
func TestNodeApproveRefusesAMalformedEngine(t *testing.T) {
	var reached bool
	flags := fixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { reached = true }))
	var stdout, stderr bytes.Buffer
	code := cli.Run(t.Context(), append(flags, "node", "approve", "--node-id", uuid.NewString(),
		"--architecture", "arm64", "--runtime-version", "27.1.1", "--storage-id", "pool-a",
		"--engine", "postgres"), &stdout, &stderr)
	if code == 0 {
		t.Fatal("an engine with no version was accepted")
	}
	if reached {
		t.Fatal("a malformed declaration was sent to the API")
	}
}

// dropTimeout bounds one test database teardown. It exists to stop a hang, not to
// assert how fast PostgreSQL drops a database under load.
const dropTimeout = 60 * time.Second

// A cluster that can be created and never removed holds its reservations, including its
// host port, for as long as the installation lives. Each action reaches its own route
// and carries the retry key the operator can repeat with.
func TestClusterActionsReachTheirRoutes(t *testing.T) {
	for _, action := range []string{"stop", "start", "delete"} {
		t.Run(action, func(t *testing.T) {
			var method, path, key, ifMatch string
			id := uuid.New()
			planToken := uuid.NewString()
			flags := fixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				method, path = r.Method, r.URL.Path
				key, ifMatch = r.Header.Get("Idempotency-Key"), r.Header.Get("If-Match")
				w.WriteHeader(http.StatusAccepted)
				json.NewEncoder(w).Encode(map[string]any{
					"cluster_id": id, "operation_id": uuid.New(), "desired_revision": 4,
				})
			}))
			var stdout, stderr bytes.Buffer
			args := append(flags, "db", action, id.String(), "--idempotency-key", "retry-me", "--expected-revision", "3")
			if action == "delete" {
				args = append(args, "--yes", "--plan-token", planToken)
			}
			code := cli.Run(t.Context(), args, &stdout, &stderr)
			if code != 0 {
				t.Fatalf("code=%d stdout=%s stderr=%s", code, stdout.String(), stderr.String())
			}
			wantMethod, wantPath := "POST", "/v1/clusters/"+id.String()+"/actions/"+action
			if action == "delete" {
				wantMethod, wantPath = "DELETE", "/v1/clusters/"+id.String()
			}
			if method != wantMethod || path != wantPath {
				t.Fatalf("%s %s, want the %s route", method, path, action)
			}
			if key != "retry-me" || ifMatch != `"3"` {
				t.Fatalf("key=%q if-match=%q, want the key and expectation to travel", key, ifMatch)
			}
			// The key is disclosed before the request, so an uncertain outcome is
			// retryable by hand.
			if !strings.Contains(stderr.String(), "retry-me") {
				t.Fatalf("stderr=%q, want the retry key disclosed", stderr.String())
			}
		})
	}
}

// An action needs a cluster to act on. A malformed one is refused before anything is sent.
func TestClusterActionRefusesAMalformedID(t *testing.T) {
	var reached bool
	flags := fixture(t, http.HandlerFunc(func(http.ResponseWriter, *http.Request) { reached = true }))
	var stdout, stderr bytes.Buffer
	if code := cli.Run(t.Context(), append(flags, "db", "delete", "not-a-uuid"), &stdout, &stderr); code == 0 {
		t.Fatalf("a malformed cluster ID was accepted: %s", stdout.String())
	}
	if reached {
		t.Fatal("a malformed cluster ID reached the API")
	}
}

// A read command that prints nothing reads as "this cluster has no members". The
// default output is what an operator sees, so it must render what the API returned.
func TestMembersRenderWithoutJSON(t *testing.T) {
	id, member := uuid.New(), uuid.New()
	flags := fixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]any{
			"members": []map[string]string{{"id": member.String(), "role": "primary", "state": "active"}},
		})
	}))
	var human []string
	for _, flag := range flags {
		if flag != "--json" {
			human = append(human, flag)
		}
	}
	var stdout, stderr bytes.Buffer
	if code := cli.Run(t.Context(), append(human, "db", "members", id.String()), &stdout, &stderr); code != 0 {
		t.Fatalf("code=%d stderr=%s", code, stderr.String())
	}
	for _, want := range []string{member.String(), "primary", "active"} {
		if !strings.Contains(stdout.String(), want) {
			t.Fatalf("stdout=%q, want it to name %q", stdout.String(), want)
		}
	}
}
