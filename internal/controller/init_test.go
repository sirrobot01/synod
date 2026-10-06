package controller

import (
	"bytes"
	"crypto/sha256"
	"crypto/x509"
	"database/sql"
	"encoding/base64"
	"encoding/pem"
	"os"
	"path/filepath"
	"strings"
	"testing"

	_ "modernc.org/sqlite"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/config"
	"github.com/sirrobot01/synod/internal/jointoken"
	"github.com/sirrobot01/synod/internal/pki"
	"github.com/spf13/pflag"
)

// A controller cannot serve an agent transport without this material, and nothing else
// in the product creates it.
func TestBootstrapWritesWhatAControllerNeeds(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "material")
	var out bytes.Buffer
	if err := writeMaterial(dir, "", &out); err != nil {
		t.Fatal(err)
	}
	installation := strings.TrimSpace(readFile(t, dir, "installation-id"))
	id, err := uuid.Parse(installation)
	if err != nil || id == uuid.Nil {
		t.Fatalf("installation-id=%q, want a UUID", installation)
	}
	// The CA loads under the installation it was created for.
	authority, err := pki.LoadCA(id, []byte(readFile(t, dir, "ca.crt")), []byte(readFile(t, dir, "ca.key")))
	if err != nil {
		t.Fatalf("the CA it wrote does not load: %v", err)
	}
	if authority == nil {
		t.Fatal("no certificate authority")
	}
	// The controller certificate is signed by that CA and carries the controller role.
	block, _ := pem.Decode([]byte(readFile(t, dir, "agent-tls.crt")))
	if block == nil {
		t.Fatal("the controller certificate is not PEM")
	}
	leaf, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM([]byte(readFile(t, dir, "ca.crt"))) {
		t.Fatal("the CA certificate is not usable as a root")
	}
	if _, err := leaf.Verify(x509.VerifyOptions{Roots: roots, KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageAny}}); err != nil {
		t.Fatalf("the controller certificate is not signed by the CA it wrote: %v", err)
	}
	expected, err := (pki.Identity{InstallationID: id, SubjectID: subjectOf(t, leaf), Role: pki.Controller}).URI()
	if err != nil {
		t.Fatal(err)
	}
	if len(leaf.URIs) != 1 || leaf.URIs[0].String() != expected.String() {
		t.Fatalf("uris=%v, want the controller identity %s", leaf.URIs, expected)
	}
	// An agent dials the controller by name, so the certificate must carry one.
	if len(leaf.DNSNames)+len(leaf.IPAddresses) == 0 {
		t.Fatal("the controller certificate names no address an agent could dial")
	}
	// The wrapping key is what provisioning seals credentials with.
	key, err := base64.StdEncoding.DecodeString(strings.TrimSpace(readFile(t, dir, "secret.key")))
	if err != nil || len(key) != 32 {
		t.Fatalf("secret.key is %d bytes, want 32", len(key))
	}
	if _, err := uuid.Parse(strings.TrimSpace(readFile(t, dir, "secret-key-id"))); err != nil {
		t.Fatal(err)
	}
}

// Private material must not be readable by anyone else: one of these files is the
// identity of every agent that will ever enrol.
func TestBootstrapKeepsPrivateMaterialPrivate(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "material")
	if err := writeMaterial(dir, "", &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"ca.key", "agent-tls.key", "secret.key"} {
		info, err := os.Stat(filepath.Join(dir, name))
		if err != nil {
			t.Fatal(err)
		}
		if info.Mode().Perm()&0o077 != 0 {
			t.Fatalf("%s is mode %o, want it unreadable by others", name, info.Mode().Perm())
		}
	}
	if info, err := os.Stat(dir); err != nil || info.Mode().Perm()&0o077 != 0 {
		t.Fatalf("the material directory is mode %o, want it private", info.Mode().Perm())
	}
}

// Replacing a CA locks out every agent already enrolled against it, so a rerun refuses.
func TestBootstrapRefusesToReplaceMaterial(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "material")
	if err := writeMaterial(dir, "", &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	before := readFile(t, dir, "ca.crt")
	if err := writeMaterial(dir, "", &bytes.Buffer{}); err == nil {
		t.Fatal("a rerun replaced installation material")
	}
	if readFile(t, dir, "ca.crt") != before {
		t.Fatal("a refused rerun still changed the CA")
	}
}

// The addresses agents dial are declared, because a certificate without them is one
// nothing can connect to.
func TestBootstrapCertifiesTheNamesAgentsDial(t *testing.T) {
	t.Setenv("SYNOD_CONTROLLER_NAMES", "controller.example, 10.0.0.227 ,")
	dir := filepath.Join(t.TempDir(), "material")
	if err := writeMaterial(dir, "", &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	block, _ := pem.Decode([]byte(readFile(t, dir, "agent-tls.crt")))
	leaf, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	if len(leaf.DNSNames) != 1 || leaf.DNSNames[0] != "controller.example" {
		t.Fatalf("dns=%v, want the declared name", leaf.DNSNames)
	}
	if len(leaf.IPAddresses) != 1 || leaf.IPAddresses[0].String() != "10.0.0.227" {
		t.Fatalf("ips=%v, want the declared address", leaf.IPAddresses)
	}
}

func readFile(t *testing.T, dir, name string) string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(dir, name))
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

// subjectOf reads the subject identity back out of the certificate's own URI.
func subjectOf(t *testing.T, leaf *x509.Certificate) uuid.UUID {
	t.Helper()
	if len(leaf.URIs) != 1 {
		t.Fatalf("uris=%v, want exactly one identity", leaf.URIs)
	}
	parts := strings.Split(leaf.URIs[0].String(), "/")
	id, err := uuid.Parse(parts[len(parts)-1])
	if err != nil {
		t.Fatalf("the identity URI does not end in a subject UUID: %s", leaf.URIs[0])
	}
	return id
}

func TestServerInitCommand(t *testing.T) {
	dir := t.TempDir()
	sysDir := filepath.Join(dir, "systemd")
	if err := os.MkdirAll(sysDir, 0o755); err != nil {
		t.Fatal(err)
	}

	cmd := initCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"--dir", dir,
		"--address", "192.168.1.100",
		"--systemd-dir", sysDir,
	})

	if err := cmd.Execute(); err != nil {
		t.Fatalf("init command failed: %v", err)
	}

	output := out.String()
	if !strings.Contains(output, "Synod installation") {
		t.Errorf("output missing installation notice: %s", output)
	}
	if !strings.Contains(output, "synod agent join synod_join_") {
		t.Errorf("output must print a join token: %s", output)
	}
	if !strings.Contains(output, "export SYNOD_API_URL") {
		t.Errorf("output missing SYNOD_API_URL export: %s", output)
	}

	// Verify file permissions for secret files.
	secretFiles := []string{
		"ca.key", "agent-tls.key", "api-ca.key", "api.key",
		"secret.key", "admin.token", "controller.json",
	}
	for _, name := range secretFiles {
		info, err := os.Stat(filepath.Join(dir, name))
		if err != nil {
			t.Fatalf("missing %s: %v", name, err)
		}
		if perm := info.Mode().Perm(); perm&0o077 != 0 {
			t.Errorf("%s has permissions %o, want private (0600)", name, perm)
		}
	}

	// Verify API TLS cert.
	apiCertBlock, rest := pem.Decode([]byte(readFile(t, dir, "api.crt")))
	if apiCertBlock == nil {
		t.Fatal("api.crt is not valid PEM")
	}
	apiLeaf, err := x509.ParseCertificate(apiCertBlock.Bytes)
	if err != nil {
		t.Fatalf("parse api.crt: %v", err)
	}
	// The API CA follows the leaf so that a joining agent can pin it.
	chainBlock, _ := pem.Decode(rest)
	if chainBlock == nil || !bytes.Equal(chainBlock.Bytes, pemBytes(t, readFile(t, dir, "api-ca.crt"))) {
		t.Fatal("api.crt does not carry the API CA after the leaf")
	}
	apiRoots := x509.NewCertPool()
	if !apiRoots.AppendCertsFromPEM([]byte(readFile(t, dir, "api-ca.crt"))) {
		t.Fatal("api-ca.crt is not usable as a root")
	}
	if _, err := apiLeaf.Verify(x509.VerifyOptions{Roots: apiRoots}); err != nil {
		t.Fatalf("api.crt not verified by api-ca.crt: %v", err)
	}

	// Verify systemd service.
	svcFile := filepath.Join(sysDir, "synod-server.service")
	svcContent := string(readFile(t, sysDir, "synod-server.service"))
	if !strings.Contains(svcContent, "ExecStart=") || !strings.Contains(svcContent, filepath.Join(dir, "controller.json")) {
		t.Errorf("systemd service file %s content incorrect: %s", svcFile, svcContent)
	}

	// Verify controller.json loads with BindController.
	flags := pflag.NewFlagSet("test-init-load", pflag.ContinueOnError)
	load := config.BindController(flags)
	if err := flags.Parse([]string{"--config", filepath.Join(dir, "controller.json")}); err != nil {
		t.Fatalf("parse flags: %v", err)
	}
	cfg, err := load()
	if err != nil {
		t.Fatalf("BindController failed to load generated controller.json: %v", err)
	}
	if cfg.Listen != "192.168.1.100:8443" {
		t.Errorf("cfg.Listen = %q, want 192.168.1.100:8443", cfg.Listen)
	}
	if cfg.AgentListen != "192.168.1.100:8444" {
		t.Errorf("cfg.AgentListen = %q, want 192.168.1.100:8444", cfg.AgentListen)
	}
}

func TestServerInitRefusesReinitialization(t *testing.T) {
	dir := t.TempDir()
	var out bytes.Buffer
	cmd1 := initCommand()
	cmd1.SetOut(&out)
	cmd1.SetErr(&out)
	cmd1.SetArgs([]string{"--dir", dir, "--systemd-dir", filepath.Join(dir, "absent")})
	if err := cmd1.Execute(); err != nil {
		t.Fatalf("first init failed: %v", err)
	}

	cmd2 := initCommand()
	cmd2.SetOut(&out)
	cmd2.SetErr(&out)
	cmd2.SetArgs([]string{"--dir", dir, "--systemd-dir", filepath.Join(dir, "absent")})
	if err := cmd2.Execute(); err == nil {
		t.Fatal("expected second init to fail, but it succeeded")
	}
}

func TestServerInitCustomInstallationID(t *testing.T) {
	dir := t.TempDir()
	customID := uuid.New().String()
	var out bytes.Buffer
	cmd := initCommand()
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"--dir", dir, "--installation-id", customID, "--systemd-dir", filepath.Join(dir, "absent")})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("init with custom id failed: %v", err)
	}

	installed := strings.TrimSpace(readFile(t, dir, "installation-id"))
	if installed != customID {
		t.Errorf("installation-id file = %q, want %q", installed, customID)
	}
}

func pemBytes(t *testing.T, data string) []byte {
	t.Helper()
	block, _ := pem.Decode([]byte(data))
	if block == nil {
		t.Fatal("not PEM")
	}
	return block.Bytes
}

// init registers a one-use join token that binds no node.
func TestServerInitRegistersJoinToken(t *testing.T) {
	dir := t.TempDir()
	var out bytes.Buffer
	cmd := initCommand()
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"--dir", dir, "--address", "192.0.2.10", "--systemd-dir", filepath.Join(dir, "absent")})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("init failed: %v\n%s", err, out.String())
	}
	var encoded string
	for line := range strings.Lines(out.String()) {
		if trimmed := strings.TrimSpace(line); strings.HasPrefix(trimmed, "synod_join_") {
			encoded = trimmed
			break
		}
	}
	token, err := jointoken.Decode(encoded)
	if err != nil {
		t.Fatalf("decode the printed join token %q: %v", encoded, err)
	}
	installation := uuid.MustParse(strings.TrimSpace(readFile(t, dir, "installation-id")))
	if token.Address != "192.0.2.10:8443" || token.InstallationID != installation || token.Fingerprint != jointoken.Fingerprint(pemBytes(t, readFile(t, dir, "api-ca.crt"))) {
		t.Fatalf("token=%+v, want the API origin, installation %s, and the API CA pin", token, installation)
	}
	raw, err := sql.Open("sqlite", "file:"+filepath.Join(dir, "synod.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	verifier := sha256.Sum256([]byte(token.Secret))
	var unbound bool
	if err := raw.QueryRowContext(t.Context(), `SELECT node_id IS NULL AND consumed_at IS NULL FROM enrollment_tokens WHERE token_verifier=?`, verifier[:]).Scan(&unbound); err != nil || !unbound {
		t.Fatalf("stored join token unbound=%v err=%v, want one unconsumed token with no node", unbound, err)
	}
}
