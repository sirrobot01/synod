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
	"github.com/sirrobot01/synod/internal/operation"
	"github.com/sirrobot01/synod/internal/store/postgres"
)

const token = "cli-fixture-private-token-never-print-this-value"

// Each client test needs a real TLS listener and private token file.
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
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
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
	handler, err := api.NewServer(store, verifier, installation, map[cluster.Engine][]string{cluster.PostgreSQL: {"17.11"}, cluster.Redis: {"8.2.7"}}, slog.New(slog.NewTextHandler(io.Discard, nil)))
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
