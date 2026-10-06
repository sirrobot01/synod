package cli_test

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/api"
	"github.com/sirrobot01/synod/internal/auth"
	"github.com/sirrobot01/synod/internal/cli"
	"github.com/sirrobot01/synod/internal/store"
)

// identityFlags starts one API on a fresh SQLite database and returns the CLI flags.
func identityFlags(t *testing.T) []string {
	t.Helper()
	metadata, err := store.Open(t.Context(), filepath.Join(t.TempDir(), "synod.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { metadata.Close() })
	verifier, err := auth.NewAdminToken(token, "cli-test-admin")
	if err != nil {
		t.Fatal(err)
	}
	handler, err := api.NewServer(metadata, verifier, uuid.New(), slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	return fixture(t, handler)
}

func run(t *testing.T, flags []string, args ...string) (int, cli.Result, string) {
	t.Helper()
	var stdout, stderr bytes.Buffer
	code := cli.Run(t.Context(), append(append([]string{}, flags...), args...), &stdout, &stderr)
	var result cli.Result
	if stdout.Len() > 0 {
		if err := json.Unmarshal(stdout.Bytes(), &result); err != nil {
			t.Fatalf("output %q: %v", stdout.String(), err)
		}
	}
	return code, result, stderr.String()
}

// A password never reaches a flag, so the command reads a protected file.
func TestUserCommandsReadAProtectedPasswordFile(t *testing.T) {
	flags := identityFlags(t)
	directory := t.TempDir()
	passwordFile := filepath.Join(directory, "password")
	if err := os.WriteFile(passwordFile, []byte("correct horse battery\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	code, result, stderr := run(t, flags, "user", "create", "operator", "--password-file", passwordFile)
	if code != 0 || result.User == nil || result.User.Username != "operator" {
		t.Fatalf("create: %d %+v %s", code, result.User, stderr)
	}
	id := result.User.ID
	// A world-readable file is refused, because a password must stay private.
	loose := filepath.Join(directory, "loose")
	if err := os.WriteFile(loose, []byte("correct horse battery\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if code, _, _ := run(t, flags, "user", "create", "other", "--password-file", loose); code == 0 {
		t.Fatal("a world-readable password file was accepted")
	}
	// There is no password flag at all, so a password cannot reach the process table.
	if code, _, stderr := run(t, flags, "user", "create", "other", "--password", "correct horse battery"); code == 0 || !strings.Contains(stderr, "invalid_command") {
		t.Fatalf("a password flag exists: %d %s", code, stderr)
	}
	next := filepath.Join(directory, "next")
	if err := os.WriteFile(next, []byte("a different password\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if code, _, stderr := run(t, flags, "user", "set-password", id.String(), "--password-file", next); code != 0 {
		t.Fatalf("set-password: %d %s", code, stderr)
	}
	if code, _, stderr := run(t, flags, "user", "disable", id.String()); code != 0 {
		t.Fatalf("disable: %d %s", code, stderr)
	}
	if code, _, _ := run(t, flags, "user", "disable", "not-a-uuid"); code == 0 {
		t.Fatal("an invalid user ID was accepted")
	}
}

// Without --json the command must still print the secret. It is disclosed once.
func TestTokenCreatePrintsTheSecretWithoutJSON(t *testing.T) {
	flags := identityFlags(t)
	var human []string
	for _, flag := range flags {
		if flag != "--json" {
			human = append(human, flag)
		}
	}
	var stdout, stderr bytes.Buffer
	code := cli.Run(t.Context(), append(human, "token", "create", "deploy pipeline"), &stdout, &stderr)
	if code != 0 {
		t.Fatalf("create: %d %s", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), "synod_") {
		t.Fatalf("the secret never reached stdout: %q", stdout.String())
	}
	if !strings.Contains(stdout.String(), "deploy pipeline") {
		t.Fatalf("the token name never reached stdout: %q", stdout.String())
	}
	// The same must hold for an account, which otherwise reports nothing at all.
	stdout.Reset()
	stderr.Reset()
	passwordFile := filepath.Join(t.TempDir(), "password")
	if err := os.WriteFile(passwordFile, []byte("correct horse battery\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if code := cli.Run(t.Context(), append(human, "user", "create", "operator", "--password-file", passwordFile), &stdout, &stderr); code != 0 {
		t.Fatalf("user create: %d %s", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), "operator") {
		t.Fatalf("the account never reached stdout: %q", stdout.String())
	}
}

// An unkeyed write must be attempted once. A retry would mint a second credential
// whose secret the caller never sees.
func TestTokenCreateIsNotRetried(t *testing.T) {
	var mutex sync.Mutex
	attempts := 0
	flags := fixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "POST" && r.URL.Path == "/v1/tokens" {
			mutex.Lock()
			attempts++
			mutex.Unlock()
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		if _, err := io.WriteString(w, `{"code":"dependency_unavailable","message":"unavailable"}`); err != nil {
			t.Error(err)
		}
	}))
	if code, _, _ := run(t, flags, "token", "create", "deploy pipeline"); code == 0 {
		t.Fatal("an unavailable API reported success")
	}
	mutex.Lock()
	defer mutex.Unlock()
	if attempts != 1 {
		t.Fatalf("token creation was attempted %d times, so a lost reply can mint a duplicate", attempts)
	}
}

func TestTokenCommandsDiscloseTheSecretOnce(t *testing.T) {
	flags := identityFlags(t)
	code, created, stderr := run(t, flags, "token", "create", "deploy pipeline")
	if code != 0 || created.Token == nil || !strings.HasPrefix(created.Token.Plaintext, "synod_") {
		t.Fatalf("create: %d %+v %s", code, created.Token, stderr)
	}
	code, listed, stderr := run(t, flags, "token", "list")
	if code != 0 || len(listed.Tokens) != 1 || listed.Tokens[0].ID != created.Token.ID {
		t.Fatalf("list: %d %+v %s", code, listed.Tokens, stderr)
	}
	// The listing never repeats the secret.
	if listed.Tokens[0].Plaintext != "" {
		t.Fatal("a listing repeated the token secret")
	}
	if code, _, stderr := run(t, flags, "token", "revoke", created.Token.ID.String()); code != 0 {
		t.Fatalf("revoke: %d %s", code, stderr)
	}
	code, after, _ := run(t, flags, "token", "list")
	if code != 0 || len(after.Tokens) != 1 || after.Tokens[0].RevokedAt == nil {
		t.Fatalf("a revoked token is not marked: %+v", after.Tokens)
	}
	if code, _, _ := run(t, flags, "token", "create", "bad expiry", "--expires-at", "tomorrow"); code == 0 {
		t.Fatal("an unparsable expiry was accepted")
	}
}
