package cli_test

import (
	"bytes"
	"context"
	"encoding/json"
	"encoding/pem"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/cli"
	"github.com/sirrobot01/synod/internal/client"
	"github.com/sirrobot01/synod/internal/jointoken"
)

const token = "cli-fixture-private-token-never-print-this-value"

// fixture serves handler over TLS and returns the CLI flags that trust it.
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
	return []string{"--json", "--api-url", server.URL, "--token-file", tokenFile, "--ca-file", caFile, "--request-timeout", "1s"}
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
			args := append(append([]string{}, flags...), "node", "list", "--attempts", "1")
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
		args := append(append([]string{}, flags...), "node", "list", "--token-file", path)
		if code := cli.Run(t.Context(), args, &stdout, &stderr); code != 1 || strings.Contains(stdout.String()+stderr.String(), token) {
			t.Fatal("accepted an exposed token file")
		}
	}
	fifo := filepath.Join(t.TempDir(), "token-fifo")
	if err := syscall.Mkfifo(fifo, 0o600); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	if code := cli.Run(t.Context(), append(flags, "node", "list", "--token-file", fifo), &stdout, &stderr); code != 1 {
		t.Fatal("accepted a FIFO token")
	}
}

func TestHelpAndInvalidArguments(t *testing.T) {
	t.Setenv("SYNOD_API_URL", "https://user:"+token+"@localhost:8443")
	for _, command := range [][]string{{"--help"}, {"node", "approve", "--help"}} {
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

func TestInvalidResponsesAndInterruption(t *testing.T) {
	for _, body := range []string{`{"id":"` + token + `"}`, strings.Repeat("x", (1<<20)+1)} {
		flags := fixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, body) }))
		var stdout, stderr bytes.Buffer
		code := cli.Run(t.Context(), append(flags, "node", "list"), &stdout, &stderr)
		var result cli.Result
		if err := json.Unmarshal(stdout.Bytes(), &result); err != nil || code != 1 || result.Error == nil || result.Error.Code != "invalid_response" || strings.Contains(stdout.String()+stderr.String(), token) {
			t.Fatalf("invalid response was not bounded and redacted: %s", stdout.String())
		}
	}
	flags := fixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { t.Error("cancelled context reached server") }))
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	var stdout, stderr bytes.Buffer
	if code := cli.Run(ctx, append(flags, "node", "list"), &stdout, &stderr); code != 130 || !strings.Contains(stdout.String(), `"code":"interrupted"`) {
		t.Fatalf("interruption outcome: %d %s", code, stdout.String())
	}
}

// Approval accepts a device name. The CLI resolves it before it approves the ID.
func TestNodeApproveResolvesAName(t *testing.T) {
	node := client.Node{ID: uuid.New(), Name: "odin", Architecture: "arm64"}
	var requests []string
	flags := fixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests = append(requests, r.Method+" "+r.URL.Path)
		if r.Method == http.MethodPost {
			approved := node
			now := time.Now()
			approved.ApprovedAt = &now
			json.NewEncoder(w).Encode(approved)
			return
		}
		json.NewEncoder(w).Encode(node)
	}))
	var stdout, stderr bytes.Buffer
	var result cli.Result
	if code := cli.Run(t.Context(), append(flags, "node", "approve", "odin"), &stdout, &stderr); code != 0 || json.Unmarshal(stdout.Bytes(), &result) != nil || result.Node == nil || result.Node.ApprovedAt == nil {
		t.Fatalf("code=%d stdout=%s stderr=%s", code, stdout.String(), stderr.String())
	}
	want := []string{"GET /v1/nodes/by-name/odin", "POST /v1/nodes/" + node.ID.String() + "/actions/approve"}
	if !slices.Equal(requests, want) {
		t.Fatalf("requests=%v, want %v", requests, want)
	}
}

// A join token names the address that the controller reports and pins the API CA the CLI trusts.
func TestNodeJoinTokenPinsTheAPICA(t *testing.T) {
	for _, mode := range []string{"new", "recovered"} {
		t.Run(mode, func(t *testing.T) {
			node := client.Node{ID: uuid.Nil}
			var extra []string
			if mode == "recovered" {
				node.ID = uuid.New()
				node.Name = "saved"
				extra = []string{"--device", node.Name}
			}
			installation := client.Installation{InstallationID: uuid.New(), JoinAddress: "192.0.2.20:8443"}
			var registered map[string]any
			flags := fixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.Method + " " + r.URL.Path {
				case "GET /v1/installation":
					json.NewEncoder(w).Encode(installation)
				case "GET /v1/nodes/by-name/saved":
					json.NewEncoder(w).Encode(node)
				case "POST /v1/enrollment-tokens":
					json.NewDecoder(r.Body).Decode(&registered)
					w.WriteHeader(http.StatusCreated)
					json.NewEncoder(w).Encode(client.EnrollmentToken{ID: uuid.New(), NodeID: node.ID, ExpiresAt: time.Now().Add(time.Hour)})
				default:
					http.Error(w, "unexpected request", http.StatusBadRequest)
				}
			}))
			var stdout, stderr bytes.Buffer
			var result cli.Result
			if code := cli.Run(t.Context(), append(append(flags, "node", "join-token", "--ttl", "1h"), extra...), &stdout, &stderr); code != 0 || json.Unmarshal(stdout.Bytes(), &result) != nil {
				t.Fatalf("code=%d stdout=%s stderr=%s", code, stdout.String(), stderr.String())
			}
			joined, err := jointoken.Decode(result.JoinToken)
			if err != nil {
				t.Fatal(err)
			}
			caData, err := os.ReadFile(flags[slices.Index(flags, "--ca-file")+1])
			if err != nil {
				t.Fatal(err)
			}
			block, _ := pem.Decode(caData)
			if joined.NodeID != node.ID || joined.Address != installation.JoinAddress || joined.InstallationID != installation.InstallationID || joined.Fingerprint != jointoken.Fingerprint(block.Bytes) {
				t.Fatalf("token=%+v, want the controller's join address, installation, and API CA pin", joined)
			}
			if registered["token"] != joined.Secret || registered["node_id"] != node.ID.String() || registered["ttl_seconds"] != float64(3600) {
				t.Fatalf("registration=%v, want the token secret with no node", registered)
			}
		})
	}
}

// The node commands work against the real API and store.
func TestNodeCommandsAgainstTheAPI(t *testing.T) {
	flags := identityFlags(t)
	code, result, stderr := run(t, flags, "node", "list")
	if code != 0 || len(result.Nodes) != 0 {
		t.Fatalf("empty list: %d %+v %s", code, result, stderr)
	}
	code, result, _ = run(t, flags, "node", "approve", uuid.NewString())
	if code != 1 || result.Error == nil || result.Error.Code != "not_found" {
		t.Fatalf("unknown node: %d %+v", code, result.Error)
	}
	code, result, _ = run(t, flags, "node", "join-token")
	if code != 0 || result.JoinToken == "" {
		t.Fatalf("join token: %d %+v", code, result.Error)
	}
}
