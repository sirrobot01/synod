package cli

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"

	"github.com/google/uuid"
)

func TestArgumentErrorsNeverEchoInputs(t *testing.T) {
	secret := "private-value-do-not-print"
	for _, args := range [][]string{
		{secret}, {"enroll", "bootstrap", "--unknown=" + secret},
		{"enroll", "retry", "--attempts=" + secret},
		{"enroll", "bootstrap", "--installation-id=" + secret},
		{"enroll", "retry", secret},
	} {
		var stdout, stderr bytes.Buffer
		if Run(t.Context(), args, &stdout, &stderr) != 1 || stdout.Len() != 0 {
			t.Fatalf("unexpected output or status")
		}
		var failure Failure
		if json.Unmarshal(stderr.Bytes(), &failure) != nil || failure.Code != "invalid_configuration" || strings.Contains(stderr.String(), secret) {
			t.Fatalf("unsafe error: %s", stderr.String())
		}
	}
}

func TestTrustFilesAreBoundedRegularAndStrict(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	defer server.Close()
	cert := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})
	dir := t.TempDir()
	good := filepath.Join(dir, "ca")
	if err := os.WriteFile(good, cert, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := readTrust(good); err != nil {
		t.Fatal(err)
	}
	for name, data := range map[string][]byte{"empty": nil, "oversize": bytes.Repeat([]byte("x"), (1<<20)+1), "junk": append([]byte("ignored prefix\n"), cert...), "trailing": append(bytes.Clone(cert), []byte("junk")...), "private": pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: []byte("not a CA")})} {
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, data, 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := readTrust(path); err == nil {
			t.Errorf("accepted %s", name)
		}
	}
	symlink := filepath.Join(dir, "symlink")
	if err := os.Symlink(good, symlink); err != nil {
		t.Fatal(err)
	}
	fifo := filepath.Join(dir, "fifo")
	if err := syscall.Mkfifo(fifo, 0o600); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{dir, symlink, fifo, filepath.Join(dir, "missing")} {
		if _, err := readTrust(path); err == nil {
			t.Error("accepted non-regular trust")
		}
	}
	if err := os.Chmod(good, 0o666); err != nil {
		t.Fatal(err)
	}
	if _, err := readTrust(good); err == nil {
		t.Error("accepted publicly writable trust")
	}
}

func TestRetryCannotCreateAndBootstrapCannotOverwrite(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "state")
	args := []string{"enroll", "retry", "--installation-id", uuid.NewString(), "--node-id", uuid.NewString(), "--state-dir", dir}
	var out, diagnostics bytes.Buffer
	if Run(t.Context(), args, &out, &diagnostics) != 1 {
		t.Fatal("retry created state")
	}
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Fatal("retry mutated absent state")
	}
	if err := os.Mkdir(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	marker := filepath.Join(dir, "marker")
	if err := os.WriteFile(marker, []byte("retained"), 0o600); err != nil {
		t.Fatal(err)
	}
	args[1] = "bootstrap"
	if Run(t.Context(), args, &out, &diagnostics) != 1 {
		t.Fatal("bootstrap accepted existing directory")
	}
	data, err := os.ReadFile(marker)
	if err != nil || string(data) != "retained" {
		t.Fatal("bootstrap changed existing state")
	}
}

func TestCommandRejectsUntrustedHTTPS(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1); w.WriteHeader(500) }))
	defer server.Close()
	server.Config.ErrorLog = log.New(io.Discard, "", 0)
	parent := t.TempDir()
	tokenPath := filepath.Join(parent, "token")
	secret := base64.RawURLEncoding.EncodeToString(make([]byte, 32))
	if err := os.WriteFile(tokenPath, []byte(secret), 0o600); err != nil {
		t.Fatal(err)
	}
	args := []string{"enroll", "bootstrap", "--installation-id", uuid.NewString(), "--node-id", uuid.NewString(), "--state-dir", filepath.Join(parent, "state"), "--token-file", tokenPath, "--api-url", server.URL, "--attempts", "1"}
	var stdout, stderr bytes.Buffer
	if Run(t.Context(), args, &stdout, &stderr) != 1 || calls.Load() != 0 || stdout.Len() != 0 {
		t.Fatal("untrusted HTTPS accepted")
	}
	var failure Failure
	if json.Unmarshal(stderr.Bytes(), &failure) != nil || failure.Code != "transport_failed" || bytes.Contains(stderr.Bytes(), []byte(secret)) {
		t.Fatal("unsafe transport error")
	}
}
