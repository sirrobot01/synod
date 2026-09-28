// Package enginefixture supplies disposable TLS engines for integration tests.
package enginefixture

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/pki"
)

const postgresImage = "postgres@sha256:d74eeac9a635390a49bc21bd49fccd973de707e2a53a76ac49b552b8712ec46f"
const redisImage = "redis@sha256:d30960f73a599496d8b2802c97758bab6b1cd421fd06337f837779c47a57e1f3"

// Docker runs the Docker CLI against the isolated test daemon.
func Docker(t *testing.T, stdin string, args ...string) (string, error) {
	t.Helper()
	command := exec.Command("docker", args...)
	command.Env = append(os.Environ(), "DOCKER_HOST="+os.Getenv("SYNOD_TEST_DOCKER_HOST"))
	command.Stdin = strings.NewReader(stdin)
	var out, errOut bytes.Buffer
	command.Stdout, command.Stderr = &out, &errOut
	if err := command.Run(); err != nil {
		return out.String(), fmt.Errorf("docker %s: %w: %s", args[0], err, errOut.String())
	}
	return out.String(), nil
}

// Exec runs commands in one test container.
func Exec(t *testing.T, name string) func(context.Context, []string, string, ...string) ([]byte, error) {
	return func(ctx context.Context, env []string, stdin string, args ...string) ([]byte, error) {
		command := []string{"exec", "-i"}
		for _, value := range env {
			command = append(command, "-e", value)
		}
		out, err := Docker(t, stdin, append(append(command, name), args...)...)
		return []byte(out), err
	}
}

// TLS writes a product CA and node certificate for engine connections.
func TLS(t *testing.T) string {
	t.Helper()
	dir, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	installation := uuid.New()
	now := time.Now()
	caPEM, caKeyPEM, err := pki.CreateCA(installation, now, 24*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	ca, err := pki.LoadCA(installation, caPEM, caKeyPEM)
	if err != nil {
		t.Fatal(err)
	}
	_, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	csr, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{}, key)
	if err != nil {
		t.Fatal(err)
	}
	leafPEM, err := ca.Issue(csr, pki.Identity{InstallationID: installation, Role: pki.Agent, SubjectID: uuid.New()}, nil, now, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	for name, data := range map[string][]byte{"ca.crt": caPEM, "server.crt": leafPEM, "server.key": pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})} {
		if err := os.WriteFile(filepath.Join(dir, name), data, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

// StartPostgres runs one copy as tier 1 does: the Synod superuser, local trust, TLS, and logical WAL.
func StartPostgres(t *testing.T, network, name, tls, admin string) func(context.Context, []string, string, ...string) ([]byte, error) {
	t.Helper()
	script := `mkdir -p /tls && cp /tls-src/* /tls/ && chown -R postgres /tls && chmod 600 /tls/server.key && exec docker-entrypoint.sh postgres -c wal_level=logical -c max_replication_slots=10 -c max_wal_senders=10 -c ssl=on -c ssl_cert_file=/tls/server.crt -c ssl_key_file=/tls/server.key -c ssl_ca_file=/tls/ca.crt`
	if _, err := Docker(t, "", "run", "-d", "--name", name, "--network", network, "--network-alias", name, "--label", "synod.test=database",
		"-e", "POSTGRES_USER=synod_admin", "-e", "POSTGRES_PASSWORD="+admin, "-e", "POSTGRES_INITDB_ARGS=--auth-local=trust --auth-host=scram-sha-256",
		"-v", tls+":/tls-src:ro", postgresImage, "bash", "-c", script); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = Docker(t, "", "rm", "-f", "-v", name) })
	x := Exec(t, name)
	Wait(t, 60*time.Second, func() error {
		_, err := x(t.Context(), nil, "", "pg_isready", "-U", "synod_admin", "-h", "127.0.0.1")
		return err
	})
	return x
}

func Wait(t *testing.T, limit time.Duration, check func() error) {
	t.Helper()
	deadline := time.Now().Add(limit)
	for {
		err := check()
		if err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("condition not met in %s: %v", limit, err)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

// StartRedis runs one copy as tier 1 does: ACL users from a file, TLS between copies, and a local socket.
func StartRedis(t *testing.T, network, name, tls, admin, replication, socket string) func(context.Context, []string, string, ...string) ([]byte, error) {
	t.Helper()
	acl := fmt.Sprintf("user default on >%s ~* &* +@all\nuser synod_replication on >%s +psync +replconf +ping\n", admin, replication)
	if err := os.WriteFile(filepath.Join(tls, name+".acl"), []byte(acl), 0o644); err != nil {
		t.Fatal(err)
	}
	script := `mkdir -p /tls /run/synod-redis && cp /tls-src/* /tls/ && cp /tls/"$1".acl /tls/users.acl && chown -R redis /tls /run/synod-redis && exec docker-entrypoint.sh redis-server --port 0 --tls-port 6379 --tls-cert-file /tls/server.crt --tls-key-file /tls/server.key --tls-ca-cert-file /tls/ca.crt --tls-replication yes --tls-auth-clients optional --appendonly yes --aclfile /tls/users.acl --unixsocket ` + socket + ` --unixsocketperm 700`
	if _, err := Docker(t, "", "run", "-d", "--name", name, "--network", network, "--network-alias", name, "--label", "synod.test=database", "-v", tls+":/tls-src:ro", redisImage, "sh", "-c", script, "redis", name); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = Docker(t, "", "rm", "-f", "-v", name) })
	x := Exec(t, name)
	Wait(t, 30*time.Second, func() error {
		_, err := x(t.Context(), []string{"REDISCLI_AUTH=" + admin}, "", "redis-cli", "-s", socket, "PING")
		return err
	})
	return x
}
