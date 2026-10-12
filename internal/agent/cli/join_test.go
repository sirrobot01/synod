package cli

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"io"
	"log/slog"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/sirrobot01/synod/internal/agent/daemon"
	"github.com/sirrobot01/synod/internal/agent/enrollment"
	"github.com/sirrobot01/synod/internal/api"
	"github.com/sirrobot01/synod/internal/auth"
	"github.com/sirrobot01/synod/internal/jointoken"
	"github.com/sirrobot01/synod/internal/pki"
	"github.com/sirrobot01/synod/internal/store"
)

// One join token enrolls one node, and a rerun resumes a join that stopped early.
func TestJoinEnrollsAndConfiguresTheAgent(t *testing.T) {
	for _, mode := range []string{"new", "bound"} {
		t.Run(mode, func(t *testing.T) {
			token, installation := enrollmentToken(t, mode == "bound")
			parent := t.TempDir()
			if err := os.Chmod(parent, 0o700); err != nil {
				t.Fatal(err)
			}
			options := joinOptions{dir: filepath.Join(parent, "agent"), dockerSocket: "/var/run/docker.sock", systemdDir: filepath.Join(parent, "absent")}
			var stdout bytes.Buffer

			wrong := token
			wrong.Fingerprint = jointoken.Fingerprint([]byte("another CA"))
			if err := join(t.Context(), jointoken.Encode(wrong), joinOptions{dir: filepath.Join(parent, "wrong")}, &stdout); !errors.Is(err, enrollment.ErrCertificate) {
				t.Fatalf("join with a wrong pin: %v, want a certificate error", err)
			}

			if err := join(t.Context(), jointoken.Encode(token), options, &stdout); err != nil {
				t.Fatalf("join: %v", err)
			}
			joined, err := daemon.Load(filepath.Join(options.dir, "agent.json"))
			if err != nil {
				t.Fatal(err)
			}
			if token.NodeID != uuid.Nil && joined.NodeID != token.NodeID.String() {
				t.Fatal("bound token lost the device ID")
			}
			if joined.InstallationID != installation.String() || len(joined.ControllerEndpoints) != 1 || joined.ControllerEndpoints[0] != "127.0.0.1:8444" {
				t.Fatalf("config=%+v, want the installation and the dialed host with the agent port", joined)
			}
			if !strings.Contains(stdout.String(), "synod node approve "+joined.NodeID) {
				t.Fatalf("output does not name the approval command: %s", stdout.String())
			}
			for name, want := range map[string]bool{"join.token": false, "synod-agent.service": true, "enrollment": true} {
				if _, err := os.Stat(filepath.Join(options.dir, name)); (err == nil) != want {
					t.Fatalf("%s present=%v, want %v", name, err == nil, want)
				}
			}
			if err := join(t.Context(), jointoken.Encode(token), options, &stdout); !errors.Is(err, enrollment.ErrState) {
				t.Fatalf("second join: %v, want a state error", err)
			}
		})
	}
}

// enrollmentToken starts a controller API that accepts one join token.
func enrollmentToken(t *testing.T, bound bool) (jointoken.Token, uuid.UUID) {
	t.Helper()
	metadata, err := store.Open(t.Context(), filepath.Join(t.TempDir(), "synod.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { metadata.Close() })
	installation := uuid.New()
	admin, err := auth.NewAdminToken(strings.Repeat("join-admin-", 4), "test-admin")
	if err != nil {
		t.Fatal(err)
	}
	caCert, caKey, err := pki.CreateCA(installation, time.Now(), 2*store.AgentCertificateLifetime)
	if err != nil {
		t.Fatal(err)
	}
	authority, err := pki.LoadCA(installation, caCert, caKey)
	if err != nil {
		t.Fatal(err)
	}
	server, err := api.NewServer(metadata, admin, installation, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	server.EnrollmentAuthority = authority
	server.AgentListen = "0.0.0.0:8444"
	// The httptest certificate is a self-signed CA, so it is also the pinned API CA.
	https := httptest.NewTLSServer(server)
	t.Cleanup(https.Close)

	token := jointoken.Token{Address: https.Listener.Addr().String(), InstallationID: installation, Fingerprint: jointoken.Fingerprint(https.Certificate().Raw), Secret: jointoken.NewSecret()}
	if bound {
		token.NodeID = uuid.New()
	}
	if _, err := metadata.RegisterEnrollmentToken(t.Context(), store.TokenRegistration{NodeID: token.NodeID, ActorID: "test", RequestID: uuid.NewString(), IdempotencyKey: uuid.NewString(), TokenVerifier: sha256.Sum256([]byte(token.Secret)), TTLSeconds: 600}); err != nil {
		t.Fatal(err)
	}
	return token, installation
}

func TestRunJoinsOnceWithAToken(t *testing.T) {
	token, installation := enrollmentToken(t, false)
	parent := t.TempDir()
	if err := os.Chmod(parent, 0o700); err != nil {
		t.Fatal(err)
	}
	configPath := filepath.Join(parent, "agent", "agent.json")
	if err := joinIfNeeded(t.Context(), configPath, "", "", io.Discard); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Dir(configPath)); !os.IsNotExist(err) {
		t.Fatal("run without a token created agent state")
	}
	var output bytes.Buffer
	if err := joinIfNeeded(t.Context(), configPath, jointoken.Encode(token), "192.0.2.10:8445", &output); err != nil {
		t.Fatal(err)
	}
	joined, err := daemon.Load(configPath)
	if err != nil || joined.InstallationID != installation.String() || joined.DestinationAddress != "192.0.2.10:8445" || joined.DestinationListen != ":8445" {
		t.Fatalf("joined config: %+v, %v", joined, err)
	}
	// A restart keeps the identity. The used token is ignored.
	if err := joinIfNeeded(t.Context(), configPath, "spent-token", "", io.Discard); err != nil {
		t.Fatalf("restart with a spent token: %v", err)
	}
	again, err := daemon.Load(configPath)
	if err != nil || again.NodeID != joined.NodeID {
		t.Fatal("restart changed the device", err)
	}
}
