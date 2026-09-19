package cli_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sirrobot01/synod/internal/agent/enrollment"
	"github.com/sirrobot01/synod/internal/api"
	"github.com/sirrobot01/synod/internal/auth"
	"github.com/sirrobot01/synod/internal/cli"
	"github.com/sirrobot01/synod/internal/client"
	"github.com/sirrobot01/synod/internal/pki"
	"github.com/sirrobot01/synod/internal/store/postgres"
)

type savedRegistration struct {
	Version               int    `json:"version"`
	Origin                string `json:"origin"`
	AdministratorVerifier []byte `json:"administrator_verifier"`
	Request               struct {
		NodeID     uuid.UUID `json:"node_id"`
		Token      string    `json:"token"`
		TTLSeconds int       `json:"ttl_seconds"`
	} `json:"request"`
	IdempotencyKey uuid.UUID               `json:"idempotency_key"`
	Body           []byte                  `json:"body"`
	Response       *client.EnrollmentToken `json:"response"`
}

func readRegistration(t *testing.T, directory string) savedRegistration {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(directory, "request.json"))
	if err != nil {
		t.Fatal(err)
	}
	var value savedRegistration
	if err := json.Unmarshal(data, &value); err != nil {
		t.Fatal("invalid request checkpoint")
	}
	decoded, err := base64.RawURLEncoding.Strict().DecodeString(value.Request.Token)
	if err != nil || len(decoded) != 32 || len(value.Request.Token) != 43 {
		t.Fatal("invalid generated token")
	}
	export, err := os.ReadFile(filepath.Join(directory, "enrollment-token"))
	if err != nil || string(export) != value.Request.Token+"\n" {
		t.Fatal("missing or changed token export")
	}
	for _, name := range []string{"request.json", "enrollment-token", "lock"} {
		info, err := os.Lstat(filepath.Join(directory, name))
		if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 {
			t.Fatal("unsafe state file")
		}
	}
	if bytes.Contains(data, []byte(token)) {
		t.Fatal("administrator secret copied into request state")
	}
	return value
}

func runRegistration(t *testing.T, flags []string, directory string, extra ...string) (int, cli.Result) {
	t.Helper()
	args := append(append([]string{}, flags...), "enrollment-token", "register", "--request-dir", directory)
	args = append(args, extra...)
	var stdout, stderr bytes.Buffer
	code := cli.Run(t.Context(), args, &stdout, &stderr)
	if strings.Contains(stdout.String()+stderr.String(), token) {
		t.Fatal("administrator token leaked")
	}
	if data, err := os.ReadFile(filepath.Join(directory, "enrollment-token")); err == nil && bytes.Contains(append(stdout.Bytes(), stderr.Bytes()...), bytes.TrimSpace(data)) {
		t.Fatal("enrollment token leaked")
	}
	var result cli.Result
	if err := json.Unmarshal(stdout.Bytes(), &result); err != nil {
		t.Fatal("invalid command JSON")
	}
	return code, result
}

func TestEnrollmentTokenDurableReplay(t *testing.T) {
	directory := filepath.Join(t.TempDir(), "request")
	node, registration := uuid.New(), uuid.New()
	var calls atomic.Int32
	var mutex sync.Mutex
	var bodies, keys []string
	flags := fixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		saved := readRegistration(t, directory)
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Error("read request failed")
		}
		if r.TLS == nil || r.Method != "POST" || r.URL.Path != "/v1/enrollment-tokens" || r.Header.Get("Authorization") != "Bearer "+token || !bytes.Equal(body, saved.Body) || r.Header.Get("Idempotency-Key") != saved.IdempotencyKey.String() {
			t.Error("request differs from durable authenticated intent")
		}
		mutex.Lock()
		bodies = append(bodies, string(body))
		keys = append(keys, r.Header.Get("Idempotency-Key"))
		mutex.Unlock()
		if calls.Add(1) == 1 {
			connection, _, err := w.(http.Hijacker).Hijack()
			if err != nil {
				t.Error(err)
				return
			}
			connection.Close()
			return
		}
		w.WriteHeader(201)
		json.NewEncoder(w).Encode(client.EnrollmentToken{ID: registration, NodeID: node, ExpiresAt: time.Now().Add(10 * time.Minute).UTC(), Replayed: true, TokenFile: "server-controlled-path"})
	}))
	flags = append(flags, "--attempts", "1")
	code, _ := runRegistration(t, flags, directory, "--initialize", "--node-id", node.String())
	if code != 1 {
		t.Fatal("lost reply did not remain uncertain")
	}
	before := readRegistration(t, directory)
	if before.Response != nil || before.Request.TTLSeconds != 600 {
		t.Fatal("incorrect pending state")
	}
	code, result := runRegistration(t, flags, directory)
	if code != 0 || result.EnrollmentToken == nil || result.EnrollmentToken.ID != registration || !result.EnrollmentToken.Replayed || result.EnrollmentToken.TokenFile != filepath.Join(directory, "enrollment-token") {
		t.Fatal("saved replay did not complete")
	}
	after := readRegistration(t, directory)
	if after.Response == nil || before.IdempotencyKey != after.IdempotencyKey || !bytes.Equal(before.Body, after.Body) || after.Response.TokenFile != "" {
		t.Fatal("completion changed durable request")
	}
	mutex.Lock()
	if len(bodies) != 2 || bodies[0] != bodies[1] || keys[0] != keys[1] {
		t.Error("request or key changed across reopen")
	}
	mutex.Unlock()
	if err := os.Remove(filepath.Join(directory, "enrollment-token")); err != nil {
		t.Fatal(err)
	}
	code, result = runRegistration(t, flags, directory)
	if code != 0 || !result.EnrollmentToken.Replayed || calls.Load() != 2 {
		t.Fatal("cached completion made another request")
	}
	repaired := readRegistration(t, directory)
	if repaired.Request.Token != before.Request.Token {
		t.Fatal("token export changed on repair")
	}
	var stdout, stderr bytes.Buffer
	args := append(append([]string{}, flags[1:]...), "enrollment-token", "register", "--request-dir", directory)
	if code := cli.Run(t.Context(), args, &stdout, &stderr); code != 0 || !strings.Contains(stdout.String(), "Enrollment registration: "+registration.String()) || strings.Contains(stdout.String()+stderr.String(), before.Request.Token) || strings.Contains(stdout.String()+stderr.String(), token) {
		t.Fatal("unsafe or invalid text output")
	}

}

func TestEnrollmentTokenRejectsUnsafeOrChangedState(t *testing.T) {
	for _, kind := range []string{"node", "ttl", "admin", "origin", "corrupt", "missing-request", "missing-lock", "public-request", "public-directory", "symlink-request", "symlink-token", "symlink-directory", "different-token", "extra-file", "body-mismatch", "unknown-version", "invalid-completion", "oversized-request"} {
		t.Run(kind, func(t *testing.T) {
			directory := filepath.Join(t.TempDir(), "request")
			node := uuid.New()
			var calls atomic.Int32
			flags := fixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				w.WriteHeader(503)
				io.WriteString(w, `{"error":{"code":"internal_error","message":"`+token+`"}}`)
			}))
			flags = append(flags, "--attempts", "1")
			code, _ := runRegistration(t, flags, directory, "--initialize", "--node-id", node.String())
			if code != 1 || calls.Load() != 1 {
				t.Fatal("failed to create pending request")
			}
			saved := readRegistration(t, directory)
			requestFile := filepath.Join(directory, "request.json")
			var extra []string
			var err error
			switch kind {
			case "node":
				extra = []string{"--node-id", uuid.NewString()}
			case "ttl":
				extra = []string{"--ttl", "601s"}
			case "admin":
				err = os.WriteFile(flags[4], []byte(strings.Repeat("changed-secret", 4)), 0o600)
			case "origin":
				flags = append(flags, "--api-url", "https://127.0.0.1:1")
			case "corrupt":
				err = os.WriteFile(requestFile, []byte("{"), 0o600)
			case "missing-request":
				err = os.Remove(requestFile)
			case "missing-lock":
				err = os.Remove(filepath.Join(directory, "lock"))
			case "public-request":
				err = os.Chmod(requestFile, 0o644)
			case "public-directory":
				err = os.Chmod(directory, 0o755)
			case "symlink-request", "symlink-token":
				file := requestFile
				if kind == "symlink-token" {
					file = filepath.Join(directory, "enrollment-token")
				}
				destination := filepath.Join(t.TempDir(), "copy")
				if err := os.Rename(file, destination); err != nil {
					t.Fatal(err)
				}
				err = os.Symlink(destination, file)
			case "symlink-directory":
				destination := directory + "-copy"
				if err := os.Rename(directory, destination); err != nil {
					t.Fatal(err)
				}
				err = os.Symlink(destination, directory)
			case "different-token":
				err = os.WriteFile(filepath.Join(directory, "enrollment-token"), []byte(strings.Repeat("A", 43)+"\n"), 0o600)
			case "extra-file":
				err = os.WriteFile(filepath.Join(directory, "unrelated"), []byte("keep"), 0o600)
			case "body-mismatch", "unknown-version", "invalid-completion":
				switch kind {
				case "body-mismatch":
					saved.Body = []byte(`{}`)
				case "unknown-version":
					saved.Version++
				case "invalid-completion":
					saved.Response = &client.EnrollmentToken{ID: uuid.New(), NodeID: uuid.New(), ExpiresAt: time.Now()}
				}
				data, marshalErr := json.Marshal(saved)
				if marshalErr != nil {
					t.Fatal(marshalErr)
				}
				err = os.WriteFile(requestFile, data, 0o600)
			case "oversized-request":
				err = os.WriteFile(requestFile, bytes.Repeat([]byte(" "), (16<<10)+1), 0o600)
			}
			if err != nil {
				t.Fatal(err)
			}
			code, result := runRegistration(t, flags, directory, extra...)
			if code != 1 || result.Error == nil || result.Error.Code != "registration_state" || calls.Load() != 1 {
				t.Fatalf("unsafe state reached network: code=%d calls=%d error=%v", code, calls.Load(), result.Error)
			}
		})
	}
}

func TestEnrollmentTokenCompletionWriteFailure(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("permission fault requires an unprivileged user")
	}
	directory := filepath.Join(t.TempDir(), "request")
	node, id := uuid.New(), uuid.New()
	var calls atomic.Int32
	flags := fixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			if err := os.Chmod(directory, 0o500); err != nil {
				t.Error(err)
			}
		}
		w.WriteHeader(201)
		json.NewEncoder(w).Encode(client.EnrollmentToken{ID: id, NodeID: node, ExpiresAt: time.Now().Add(10 * time.Minute).UTC(), Replayed: calls.Load() > 1})
	}))
	t.Cleanup(func() { os.Chmod(directory, 0o700) })
	code, result := runRegistration(t, flags, directory, "--initialize", "--node-id", node.String())
	if code != 1 || result.Error == nil || result.Error.Code != "registration_state" {
		t.Fatal("failed completion checkpoint reported success")
	}
	if err := os.Chmod(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	before := readRegistration(t, directory)
	if before.Response != nil {
		t.Fatal("failed completion write changed saved request")
	}
	code, result = runRegistration(t, flags, directory)
	after := readRegistration(t, directory)
	if code != 0 || result.EnrollmentToken.ID != id || !result.EnrollmentToken.Replayed || calls.Load() != 2 || !bytes.Equal(before.Body, after.Body) || before.IdempotencyKey != after.IdempotencyKey {
		t.Fatal("completion recovery changed registration")
	}
}

func TestEnrollmentTokenFlagsAndResponseValidation(t *testing.T) {
	node := uuid.New()
	var calls atomic.Int32
	flags := fixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(201)
		io.WriteString(w, `{"id":"`+uuid.NewString()+`","node_id":"`+node.String()+`","expires_at":"0001-01-01T00:00:00Z"}`)
	}))
	for _, extra := range [][]string{nil, {"--initialize"}, {"--initialize", "--node-id", uuid.Nil.String()}, {"--initialize", "--node-id", node.String(), "--ttl", "1ms"}, {"--initialize", "--node-id", node.String(), "--ttl", "60.5s"}, {"--initialize", "--node-id", node.String(), "--ttl", "3601s"}} {
		directory := filepath.Join(t.TempDir(), "request")
		if code, _ := runRegistration(t, flags, directory, extra...); code != 1 {
			t.Fatal("invalid initialization succeeded")
		}
		if _, err := os.Lstat(directory); !os.IsNotExist(err) {
			t.Fatal("invalid options created state")
		}
	}
	if calls.Load() != 0 {
		t.Fatal("invalid options reached network")
	}
	directory := filepath.Join(t.TempDir(), "request")
	code, result := runRegistration(t, flags, directory, "--initialize", "--node-id", node.String(), "--ttl", "60s")
	if code != 1 || result.Error == nil || result.Error.Code != "invalid_response" || readRegistration(t, directory).Response != nil {
		t.Fatal("invalid response checkpointed")
	}
	var stdout, stderr bytes.Buffer
	if code := cli.Run(t.Context(), []string{"enrollment-token", "register", "--help"}, &stdout, &stderr); code != 0 || !strings.Contains(stdout.String(), "--request-dir") || !strings.Contains(stdout.String(), "--initialize") {
		t.Fatal("missing enrollment help")
	}
}

// The child runs the public command without passing either secret on its arguments.
func TestEnrollmentTokenProcess(t *testing.T) {
	if os.Getenv("SYNOD_TEST_TOKEN_CLI_CHILD") != "1" {
		return
	}
	var args []string
	if err := json.Unmarshal([]byte(os.Getenv("SYNOD_TEST_TOKEN_CLI_ARGS")), &args); err != nil {
		os.Exit(91)
	}
	code := cli.Run(context.Background(), args, os.Stdout, os.Stderr)
	if code == 0 && os.Getenv("SYNOD_TEST_TOKEN_CLI_HOLD") == "1" {
		for {
			time.Sleep(time.Hour)
		}
	}
	os.Exit(code)
}

func TestEnrollmentTokenPostgresProcessRecovery(t *testing.T) {
	dsn := os.Getenv("SYNOD_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set SYNOD_TEST_DATABASE_URL for actual PostgreSQL qualification")
	}
	admin, err := pgxpool.New(t.Context(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	name := "task_046_" + strings.ReplaceAll(uuid.NewString(), "-", "")
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
	verifier, err := auth.NewAdminToken(token, "task-046-admin")
	if err != nil {
		t.Fatal(err)
	}
	installation, node := uuid.New(), uuid.New()
	caPEM, caKey, err := pki.CreateCA(installation, time.Now(), 7*24*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	ca, err := pki.LoadCA(installation, caPEM, caKey)
	if err != nil {
		t.Fatal(err)
	}
	handler, err := api.NewServer(store, verifier, installation, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	handler.EnrollmentAuthority = ca
	directory := filepath.Join(t.TempDir(), "request")
	committed := make(chan client.EnrollmentToken, 1)
	release := make(chan struct{})
	defer close(release)
	var calls atomic.Int32
	var mutex sync.Mutex
	var bodies, keys []string
	flags := fixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/enrollment-tokens" {
			handler.ServeHTTP(w, r)
			return
		}
		saved := readRegistration(t, directory)
		body, err := io.ReadAll(r.Body)
		if err != nil || !bytes.Equal(body, saved.Body) || r.Header.Get("Idempotency-Key") != saved.IdempotencyKey.String() {
			t.Error("POST differs from saved intent")
		}
		mutex.Lock()
		bodies = append(bodies, string(body))
		keys = append(keys, r.Header.Get("Idempotency-Key"))
		mutex.Unlock()
		r.Body = io.NopCloser(bytes.NewReader(body))
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, r)
		if recorder.Code != 201 {
			t.Error("actual registration API rejected request")
		}
		if calls.Add(1) == 1 {
			var value client.EnrollmentToken
			if err := json.Unmarshal(recorder.Body.Bytes(), &value); err != nil {
				t.Error(err)
			}
			committed <- value
			select {
			case <-release:
			case <-r.Context().Done():
			}
			return
		}
		for name, values := range recorder.Header() {
			w.Header()[name] = values
		}
		w.WriteHeader(recorder.Code)
		w.Write(recorder.Body.Bytes())
	}))
	flags = append(flags, "--attempts", "1", "--request-timeout", "30s")
	args := append(append([]string{}, flags...), "enrollment-token", "register", "--request-dir", directory, "--initialize", "--node-id", node.String(), "--ttl", "600s")
	arguments, err := json.Marshal(args)
	if err != nil {
		t.Fatal(err)
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	child := exec.Command(executable, "-test.run=^TestEnrollmentTokenProcess$")
	child.Env = append(os.Environ(), "SYNOD_TEST_TOKEN_CLI_CHILD=1", "SYNOD_TEST_TOKEN_CLI_ARGS="+string(arguments))
	var childOut, childErr bytes.Buffer
	child.Stdout, child.Stderr = &childOut, &childErr
	if err := child.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if child.ProcessState == nil {
			child.Process.Kill()
			child.Wait()
		}
	}()
	var accepted client.EnrollmentToken
	select {
	case accepted = <-committed:
	case <-time.After(15 * time.Second):
		t.Fatal("child did not reach durable server commit")
	}
	// A competing public command must not open or send the retained request.
	code, result := runRegistration(t, flags, directory)
	if code != 1 || result.Error == nil || result.Error.Code != "registration_locked" || calls.Load() != 1 {
		t.Fatal("process lock did not exclude a second command")
	}
	if err := child.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	if err := child.Wait(); err == nil {
		t.Fatal("child was not killed")
	}
	saved := readRegistration(t, directory)
	if saved.Response != nil {
		t.Fatal("lost reply unexpectedly checkpointed")
	}
	if strings.Contains(childOut.String()+childErr.String(), token) || strings.Contains(childOut.String()+childErr.String(), saved.Request.Token) {
		t.Fatal("child output leaked a secret")
	}
	// A fresh process replays the pending request. Kill it after its completion is synced.
	retryArgs := append(append([]string{}, flags...), "enrollment-token", "register", "--request-dir", directory)
	arguments, _ = json.Marshal(retryArgs)
	completeChild := exec.Command(executable, "-test.run=^TestEnrollmentTokenProcess$")
	completeChild.Env = append(os.Environ(), "SYNOD_TEST_TOKEN_CLI_CHILD=1", "SYNOD_TEST_TOKEN_CLI_HOLD=1", "SYNOD_TEST_TOKEN_CLI_ARGS="+string(arguments))
	output, err := completeChild.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := completeChild.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if completeChild.ProcessState == nil {
			completeChild.Process.Kill()
			completeChild.Wait()
		}
	}()
	result = cli.Result{}
	var completedOutput bytes.Buffer
	decoded := make(chan error, 1)
	go func() { decoded <- json.NewDecoder(io.TeeReader(output, &completedOutput)).Decode(&result) }()
	select {
	case err := <-decoded:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("completion child did not respond")
	}
	if err := completeChild.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	completeChild.Wait()
	if strings.Contains(completedOutput.String(), token) || strings.Contains(completedOutput.String(), saved.Request.Token) {
		t.Fatal("completion output leaked a secret")
	}
	if result.EnrollmentToken == nil || result.EnrollmentToken.ID != accepted.ID || !result.EnrollmentToken.ExpiresAt.Equal(accepted.ExpiresAt) || !result.EnrollmentToken.Replayed {
		t.Fatal("process restart did not replay original registration")
	}
	mutex.Lock()
	if len(bodies) != 2 || bodies[0] != bodies[1] || keys[0] != keys[1] {
		t.Error("restart changed body or key")
	}
	mutex.Unlock()
	var count, audits, ttl int
	var storedVerifier []byte
	var storedNode uuid.UUID
	if err := pool.QueryRow(t.Context(), "SELECT count(*) FROM enrollment_tokens").Scan(&count); err != nil || count != 1 {
		t.Fatal("duplicate registration")
	}
	if err := pool.QueryRow(t.Context(), "SELECT count(*) FROM audit_events WHERE action='enrollment.token.registered'").Scan(&audits); err != nil || audits != 1 {
		t.Fatal("duplicate registration audit")
	}
	if err := pool.QueryRow(t.Context(), "SELECT node_id,token_verifier,ttl_seconds FROM enrollment_tokens WHERE id=$1", accepted.ID).Scan(&storedNode, &storedVerifier, &ttl); err != nil {
		t.Fatal(err)
	}
	expectedVerifier := sha256.Sum256([]byte(saved.Request.Token))
	if storedNode != node || ttl != 600 || !bytes.Equal(storedVerifier, expectedVerifier[:]) {
		t.Fatal("Store registration differs from saved request")
	}
	code, result = runRegistration(t, flags, directory)
	if code != 0 || result.EnrollmentToken.ID != accepted.ID || calls.Load() != 2 {
		t.Fatal("durable completion failed after process kill")
	}
	// The exported file must work with the actual agent enrollment client and API issuer.
	state, err := enrollment.Open(filepath.Join(t.TempDir(), "agent"), enrollment.Options{Identity: enrollment.Identity{InstallationID: installation, NodeID: node}, AllowCreate: true})
	if err != nil {
		t.Fatal(err)
	}
	defer state.Close()
	pem, err := os.ReadFile(flags[6])
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(pem) {
		t.Fatal("invalid fixture trust")
	}
	credentials, err := state.Submit(t.Context(), enrollment.Config{Endpoint: flags[2], TokenFile: result.EnrollmentToken.TokenFile, RootCAs: roots, RequestTimeout: time.Second, Attempts: 1})
	if err != nil || credentials.Identity.NodeID != node || credentials.CertificateID == uuid.Nil {
		t.Fatalf("exported token did not enroll the assigned agent: %v", err)
	}
	t.Log("Actual HTTPS/PostgreSQL: killed reply, exclusive process lock, exact restart replay, one registration/audit, completion kill, and agent token handoff passed")
}
