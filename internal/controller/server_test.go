package controller

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sirrobot01/synod/internal/cluster"
	"github.com/sirrobot01/synod/internal/pki"
	"github.com/sirrobot01/synod/internal/secret"
	"github.com/sirrobot01/synod/internal/store/postgres"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestProtectedFiles(t *testing.T) {
	for _, test := range []struct {
		name  string
		mode  os.FileMode
		data  string
		limit int64
		ok    bool
	}{
		{"private", 0o600, "value", 5, true},
		{"public", 0o644, "value", 5, false},
		{"group readable", 0o640, "value", 5, false},
		{"too large", 0o600, "values", 5, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "secret")
			if err := os.WriteFile(path, []byte(test.data), test.mode); err != nil {
				t.Fatal(err)
			}
			data, err := readStartupFile(path, test.limit, true)
			if (err == nil) != test.ok {
				t.Fatalf("accepted=%v want=%v", err == nil, test.ok)
			}
			if test.ok && string(data) != test.data {
				t.Fatal("file contents changed")
			}
		})
	}
	if _, err := readStartupFile(t.TempDir(), 10, true); err == nil {
		t.Fatal("accepted directory")
	}
}

func TestCredentialKeyringRefusesUnprotectedOrInvalidKey(t *testing.T) {
	for _, test := range []struct {
		name string
		mode os.FileMode
		data string
	}{
		{"public file", 0o644, base64.StdEncoding.EncodeToString(make([]byte, 32))},
		{"short key", 0o600, base64.StdEncoding.EncodeToString([]byte("private-short-key"))},
		{"invalid base64", 0o600, "private-invalid-key"},
		{"oversized file", 0o600, strings.Repeat("private-key", 500)},
	} {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "wrapping-key")
			if err := os.WriteFile(path, []byte(test.data), test.mode); err != nil {
				t.Fatal(err)
			}
			if ring, err := loadKeyring(path, uuid.NewString()); err == nil || ring != nil || strings.Contains(err.Error(), test.data) {
				t.Fatalf("invalid wrapping key was accepted or exposed: %v", err)
			}
		})
	}
}

func TestStartupSpecialAndCertificateFiles(t *testing.T) {
	path := filepath.Join(t.TempDir(), "fifo")
	if err := syscall.Mkfifo(path, 0o600); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { _, err := readStartupFile(path, 1024, true); done <- err }()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("accepted FIFO")
		}
	case <-time.After(time.Second):
		t.Fatal("FIFO blocked startup")
	}
	cert := filepath.Join(t.TempDir(), "cert")
	if err := os.WriteFile(cert, []byte(strings.Repeat("x", (1<<20)+1)), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := readStartupFile(cert, 1<<20, false); err == nil {
		t.Fatal("accepted oversized certificate")
	}
	if err := os.WriteFile(cert, []byte("public-certificate"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := readStartupFile(cert, 1<<20, false); err != nil {
		t.Fatal(err)
	}
}

func TestControllerHTTPSAndShutdown(t *testing.T) {
	dsn := os.Getenv("SYNOD_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("run scripts/test-postgres.sh")
	}
	admin, err := pgxpool.New(t.Context(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	name := "controller_" + strings.ReplaceAll(uuid.NewString(), "-", "")
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
	pgconfig, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	pgconfig.ConnConfig.Database = name
	databaseURL, err := url.Parse(dsn)
	if err != nil {
		t.Fatal(err)
	}
	databaseURL.Path = "/" + name
	databaseURL.RawPath = ""
	tlsFixture := httptest.NewTLSServer(http.NotFoundHandler())
	tlsFixture.Close()
	certificate := tlsFixture.TLS.Certificates[0]
	key, err := x509.MarshalPKCS8PrivateKey(certificate.PrivateKey)
	if err != nil {
		t.Fatal(err)
	}
	directory := t.TempDir()
	certPath, keyPath, tokenPath := filepath.Join(directory, "cert.pem"), filepath.Join(directory, "key.pem"), filepath.Join(directory, "token")
	token := strings.Repeat("controller-secret-", 3)
	for path, data := range map[string][]byte{
		certPath:  pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certificate.Certificate[0]}),
		keyPath:   pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: key}),
		tokenPath: []byte(token + "\n"),
	} {
		if err := os.WriteFile(path, data, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	installation := uuid.New()
	wrappingKeyID := uuid.New()
	wrappingKeyPath := filepath.Join(directory, "wrapping-key")
	wrappingKey := make([]byte, 32)
	if _, err := rand.Read(wrappingKey); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(wrappingKeyPath, []byte(base64.StdEncoding.EncodeToString(wrappingKey)), 0o600); err != nil {
		t.Fatal(err)
	}
	caCert, caKey, err := pki.CreateCA(installation, time.Now(), 7*24*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	caCertPath, caKeyPath := filepath.Join(directory, "ca.pem"), filepath.Join(directory, "ca-key.pem")
	if err := os.WriteFile(caCertPath, caCert, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(caKeyPath, caKey, 0o600); err != nil {
		t.Fatal(err)
	}
	authority, err := pki.LoadCA(installation, caCert, caKey)
	if err != nil {
		t.Fatal(err)
	}
	_, controllerPrivate, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	controllerCSR, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{}, controllerPrivate)
	if err != nil {
		t.Fatal(err)
	}
	controllerCert, err := authority.Issue(controllerCSR, pki.Identity{InstallationID: installation, Role: pki.Controller, SubjectID: uuid.New()}, []string{"127.0.0.1"}, time.Now(), time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	controllerKey, err := x509.MarshalPKCS8PrivateKey(controllerPrivate)
	if err != nil {
		t.Fatal(err)
	}
	agentCertPath, agentKeyPath := filepath.Join(directory, "controller-agent.pem"), filepath.Join(directory, "controller-agent-key.pem")
	if err := os.WriteFile(agentCertPath, controllerCert, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(agentKeyPath, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: controllerKey}), 0o600); err != nil {
		t.Fatal(err)
	}
	logPath := filepath.Join(directory, "controller.log")
	log, err := os.Create(logPath)
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	command := Command()
	command.SetArgs([]string{"--listen", "127.0.0.1:0", "--database-url", databaseURL.String(), "--installation-id", installation.String(), "--ca-cert-file", caCertPath, "--ca-key-file", caKeyPath, "--agent-listen", "127.0.0.1:0", "--agent-tls-cert-file", agentCertPath, "--agent-tls-key-file", agentKeyPath, "--agent-ca-file", caCertPath, "--admin-token-file", tokenPath, "--tls-cert-file", certPath, "--tls-key-file", keyPath, "--secret-key-file", wrappingKeyPath, "--secret-key-id", wrappingKeyID.String(), "--placement-architecture", "arm64", "--placement-runtime-version", "27.1.1", "--placement-storage-id", "fixture"})
	command.SetErr(log)
	go func() { done <- command.ExecuteContext(ctx) }()
	var address, agentAddress string
	deadline := time.NewTimer(15 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for address == "" || agentAddress == "" {
		select {
		case err := <-done:
			t.Fatalf("controller exited before listen: %v", err)
		case <-deadline.C:
			t.Fatal("controller did not start")
		case <-ticker.C:
			data, err := os.ReadFile(logPath)
			if err != nil {
				t.Fatal(err)
			}
			for _, line := range strings.Split(string(data), "\n") {
				var event struct {
					Address      string `json:"address"`
					AgentAddress string `json:"agent_address"`
				}
				if json.Unmarshal([]byte(line), &event) == nil {
					if event.Address != "" {
						address = event.Address
					}
					if event.AgentAddress != "" {
						agentAddress = event.AgentAddress
					}
				}
			}
		}
	}
	request, err := http.NewRequestWithContext(t.Context(), "POST", "https://"+address+"/v1/clusters", strings.NewReader(`{"schema_version":1,"name":"startup","engine":"redis","redis":{"version":"8.2.7","topology":"standalone","persistence":"aof"}}`))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer "+token)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Idempotency-Key", "startup")
	response, err := tlsFixture.Client().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(response.Body)
	response.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != 202 {
		t.Fatalf("admission: %d %s", response.StatusCode, data)
	}
	verification, err := pgxpool.NewWithConfig(t.Context(), pgconfig)
	if err != nil {
		t.Fatal(err)
	}
	defer verification.Close()
	var count int
	if err := verification.QueryRow(t.Context(), "SELECT count(*) FROM clusters WHERE name = $1", "startup").Scan(&count); err != nil || count != 1 {
		t.Fatalf("admission missing from isolated database: count=%d error=%v", count, err)
	}
	var admission postgres.Admission
	if err := json.Unmarshal(data, &admission); err != nil {
		t.Fatal(err)
	}
	// Credential access uses the startup key even without an etcd-backed runner.
	password := "startup-password-private"
	ring, err := secret.NewKeyring(wrappingKeyID, map[uuid.UUID][]byte{wrappingKeyID: wrappingKey})
	if err != nil {
		t.Fatal(err)
	}
	credentialID := uuid.New()
	envelope, err := ring.Seal(secret.Binding{InstallationID: installation, OwnerType: "cluster", OwnerID: admission.ClusterID, CredentialID: credentialID, Version: 1}, []byte(password))
	if err != nil {
		t.Fatal(err)
	}
	if err := postgres.New(verification).RecordClusterIntent(t.Context(), postgres.ClusterIntent{InstallationID: installation, ClusterID: admission.ClusterID, Credentials: []postgres.Credential{{CredentialID: credentialID, Version: 1, Purpose: postgres.ApplicationCredential, RoleName: "synod_app", Envelope: envelope}}}); err != nil {
		t.Fatal(err)
	}
	// Removing the file proves that requests use the configured keyring, not a reload.
	if err := os.Remove(wrappingKeyPath); err != nil {
		t.Fatal(err)
	}
	reveal, err := http.NewRequestWithContext(t.Context(), http.MethodPost, "https://"+address+"/v1/clusters/"+admission.ClusterID.String()+"/credentials/application/reveal", strings.NewReader(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	reveal.Header.Set("Authorization", "Bearer "+token)
	reveal.Header.Set("Content-Type", "application/json")
	revealed, err := tlsFixture.Client().Do(reveal)
	if err != nil {
		t.Fatal(err)
	}
	var value cluster.RevealedCredential
	decodeErr := json.NewDecoder(revealed.Body).Decode(&value)
	revealed.Body.Close()
	if revealed.StatusCode != http.StatusOK || decodeErr != nil || value.Password != password || value.Credential.CredentialID != credentialID || revealed.Header.Get("Cache-Control") != "no-store" {
		t.Fatalf("startup keyring did not serve audited reveal: status=%d decode=%v", revealed.StatusCode, decodeErr)
	}
	// The executable loads the protected CA and serves the complete enrollment path.
	enrollmentToken := base64.RawURLEncoding.EncodeToString(make([]byte, 32))
	node := uuid.New()
	_, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	csr, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{}, private)
	if err != nil {
		t.Fatal(err)
	}
	var enrolled postgres.EnrollmentResult
	for _, endpoint := range []struct {
		path, credential string
		body             any
	}{
		{"/v1/enrollment-tokens", token, map[string]any{"node_id": node, "token": enrollmentToken, "ttl_seconds": 600}},
		{"/v1/enrollments", enrollmentToken, map[string]any{"node_id": node, "request_id": uuid.New(), "csr_der": csr}},
	} {
		body, err := json.Marshal(endpoint.body)
		if err != nil {
			t.Fatal(err)
		}
		req, err := http.NewRequestWithContext(t.Context(), "POST", "https://"+address+endpoint.path, strings.NewReader(string(body)))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Authorization", "Bearer "+endpoint.credential)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", "startup-enroll")
		resp, err := tlsFixture.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		data, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil || resp.StatusCode != 201 {
			t.Fatalf("%s: %d %s %v", endpoint.path, resp.StatusCode, data, err)
		}
		if endpoint.path == "/v1/enrollments" {
			if err := json.Unmarshal(data, &enrolled); err != nil {
				t.Fatal(err)
			}
		}
	}
	agentPrivateDER, err := x509.MarshalPKCS8PrivateKey(private)
	if err != nil {
		t.Fatal(err)
	}
	agentPair, err := tls.X509KeyPair([]byte(enrolled.CertificatePEM), pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: agentPrivateDER}))
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(caCert) {
		t.Fatal("invalid installation trust")
	}
	connection, err := grpc.NewClient(agentAddress, grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots, Certificates: []tls.Certificate{agentPair}})))
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	streamCtx, stopStream := context.WithTimeout(t.Context(), 5*time.Second)
	defer stopStream()
	stream, err := agentv1.NewAgentServiceClient(connection).Connect(streamCtx)
	if err != nil {
		t.Fatal(err)
	}
	hello := &agentv1.Hello{InstallationId: installation.String(), NodeId: node.String(), BootId: uuid.NewString(), JournalGeneration: uuid.NewString(), Protocol: &agentv1.VersionRange{Minimum: 1, Maximum: 1}, Capabilities: []string{"runtime.docker"}, ActivePayloadVersions: []uint32{1}}
	if err := stream.Send(&agentv1.AgentMessage{Message: &agentv1.AgentMessage_Hello{Hello: hello}}); err != nil {
		t.Fatal(err)
	}
	session, err := stream.Recv()
	if err != nil || session.GetSession() == nil {
		t.Fatalf("agent session failed: %v", err)
	}
	presence, err := postgres.New(verification).GetAgentPresence(t.Context(), installation, node)
	if err != nil || presence.SessionID.String() != session.GetSession().SessionId || presence.Hello.JournalGeneration != hello.JournalGeneration {
		t.Fatalf("Hello was not durable before publication: %v", err)
	}
	if err := stream.Send(&agentv1.AgentMessage{Message: &agentv1.AgentMessage_Heartbeat{Heartbeat: &agentv1.Heartbeat{Sequence: 1, ObservedAt: timestamppb.Now()}}}); err != nil {
		t.Fatal(err)
	}
	for presence.Heartbeat == nil {
		select {
		case <-streamCtx.Done():
			t.Fatal("heartbeat was not persisted")
		case <-ticker.C:
		}
		presence, err = postgres.New(verification).GetAgentPresence(t.Context(), installation, node)
		if err != nil {
			t.Fatal(err)
		}
	}
	if presence.Heartbeat.Sequence != 1 {
		t.Fatal("incorrect heartbeat sequence")
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(12 * time.Second):
		t.Fatal("controller did not stop")
	}
	probe, err := net.DialTimeout("tcp", agentAddress, 100*time.Millisecond)
	if err == nil {
		probe.Close()
		t.Fatal("agent listener survived controller shutdown")
	}
	logs, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(logs), token) || strings.Contains(string(logs), dsn) || strings.Contains(string(logs), password) || strings.Contains(string(logs), base64.StdEncoding.EncodeToString(wrappingKey)) {
		t.Fatal("startup logs exposed credentials")
	}
}

func TestServerRefusesPositionalArgumentsWithoutRepeatingThem(t *testing.T) {
	command := Command()
	command.SetArgs([]string{"private-value"})
	err := command.ExecuteContext(t.Context())
	if err == nil || strings.Contains(err.Error(), "private-value") {
		t.Fatalf("error = %v; want a refusal that does not repeat the argument", err)
	}
}
