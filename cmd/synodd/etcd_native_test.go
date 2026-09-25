package main

import (
	"context"
	"crypto/x509"
	"errors"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/config"
	"github.com/sirrobot01/synod/internal/coordinator"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func TestEtcdNativeTLSCoordination(t *testing.T) {
	host, image := os.Getenv("SYNOD_TEST_DOCKER_HOST"), os.Getenv("SYNOD_TEST_ETCD_TLS_IMAGE")
	if host == "" || image == "" {
		t.Skip("set Docker host and an immutable TLS etcd fixture image")
	}
	if !strings.Contains(image, "@sha256:") {
		t.Fatal("TLS etcd fixture requires an immutable image")
	}
	t.Setenv("SYNOD_CONTROLLER_NAMES", "127.0.0.1")
	ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
	defer cancel()
	dir := t.TempDir()
	if err := bootstrap([]string{"-dir", dir}, io.Discard); err != nil {
		t.Fatal(err)
	}
	password := uuid.NewString()
	passwordFile := filepath.Join(dir, "password")
	if err := os.WriteFile(passwordFile, []byte(password), 0o600); err != nil {
		t.Fatal(err)
	}
	name := "synod-etcd-tls-" + uuid.NewString()
	docker := func(args ...string) string {
		t.Helper()
		callCtx, stop := context.WithTimeout(ctx, 30*time.Second)
		defer stop()
		output, err := exec.CommandContext(callCtx, "docker", append([]string{"--host", host}, args...)...).CombinedOutput()
		if err != nil {
			t.Fatalf("TLS fixture Docker command: %v (%d output bytes)", err, len(output))
		}
		return strings.TrimSpace(string(output))
	}
	docker("create", "--name", name, "--label", "io.synod.fixture=etcd-tls", "--memory", "256m", "--cpus", "1", "--pids-limit", "128", "--publish", "127.0.0.1::2379", "--entrypoint", "etcd", image, "--data-dir", "/tmp/etcd", "--listen-client-urls", "https://0.0.0.0:2379", "--advertise-client-urls", "https://127.0.0.1:2379", "--cert-file", "/run/server.crt", "--key-file", "/run/server.key", "--trusted-ca-file", "/run/ca.crt", "--client-cert-auth=true", "--listen-peer-urls", "http://127.0.0.1:2380", "--initial-advertise-peer-urls", "http://127.0.0.1:2380", "--initial-cluster", "default=http://127.0.0.1:2380")
	t.Cleanup(func() {
		cleanupCtx, stop := context.WithTimeout(context.Background(), 30*time.Second)
		defer stop()
		if err := exec.CommandContext(cleanupCtx, "docker", "--host", host, "rm", "--force", "--volumes", name).Run(); err != nil {
			t.Error(err)
		}
	})
	for src, dst := range map[string]string{"agent-tls.crt": "server.crt", "agent-tls.key": "server.key", "ca.crt": "ca.crt"} {
		docker("cp", filepath.Join(dir, src), name+":/run/"+dst)
	}
	docker("start", name)
	endpoint := docker("port", name, "2379/tcp")
	_, publishedPort, err := net.SplitHostPort(endpoint)
	if err != nil {
		t.Fatal(err)
	}
	settings := config.Controller{EtcdEndpoints: endpoint, EtcdCAFile: filepath.Join(dir, "ca.crt"), EtcdTLSCertFile: filepath.Join(dir, "agent-tls.crt"), EtcdTLSKeyFile: filepath.Join(dir, "agent-tls.key"), EtcdUsername: "coordination_" + uuid.NewString(), EtcdPasswordFile: passwordFile}
	connection, err := etcdClientConfig(ctx, settings)
	if err != nil {
		t.Fatal(err)
	}
	initial := connection
	initial.Username, initial.Password = "", ""
	admin, err := clientv3.New(initial)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	rootPassword := uuid.NewString()
	if _, err := admin.Auth.UserAdd(ctx, "root", rootPassword); err != nil {
		t.Fatal("initialize native DCS root", err)
	}
	if _, err := admin.Auth.UserGrantRole(ctx, "root", "root"); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.Auth.RoleAdd(ctx, settings.EtcdUsername); err != nil {
		t.Fatal(err)
	}
	installation, clusterID := uuid.New(), uuid.New()
	prefix := "/synod-task145"
	owned := prefix + "/" + installation.String() + "/"
	if _, err := admin.Auth.RoleGrantPermission(ctx, settings.EtcdUsername, owned, clientv3.GetPrefixRangeEnd(owned), clientv3.PermissionType(clientv3.PermReadWrite)); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.Auth.UserAdd(ctx, settings.EtcdUsername, password); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.Auth.UserGrantRole(ctx, settings.EtcdUsername, settings.EtcdUsername); err != nil {
		t.Fatal(err)
	}
	if unexpected, err := connectEtcd(ctx, settings); !errors.Is(err, rpctypes.ErrAuthNotEnabled) {
		if unexpected != nil {
			unexpected.Close()
		}
		t.Fatal("secure coordination accepted disabled authentication", err)
	}
	if _, err := admin.Auth.AuthEnable(ctx); err != nil {
		t.Fatal(err)
	}
	client, err := connectEtcd(ctx, settings)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	elector, err := coordinator.New(client, prefix)
	if err != nil {
		t.Fatal(err)
	}
	leadership, err := elector.Campaign(ctx, installation, clusterID, "controller", 3)
	if err != nil {
		t.Fatal(err)
	}
	if err := leadership.Verify(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Put(ctx, "/another-installation/probe", "refuse"); !errors.Is(err, rpctypes.ErrPermissionDenied) {
		t.Fatal("coordination credential crossed its namespace", err)
	}
	select {
	case <-time.After(4 * time.Second):
	case <-leadership.Done:
		t.Fatal("TLS election lost its lease during renewal")
	}
	if err := leadership.Verify(ctx); err != nil {
		t.Fatal(err)
	}
	if err := leadership.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err := leadership.Verify(ctx); !errors.Is(err, coordinator.ErrNotLeader) {
		t.Fatal("released lease retained authority", err)
	}
	// Patroni DCS accounts use the root account over the same TLS transport.
	adminPasswordFile := filepath.Join(dir, "admin-password")
	if err := os.WriteFile(adminPasswordFile, []byte(rootPassword), 0o600); err != nil {
		t.Fatal(err)
	}
	patroniSettings := settings
	patroniSettings.InstallationID = installation.String()
	patroniSettings.PatroniDCSAdminUsername, patroniSettings.PatroniDCSAdminPasswordFile = "root", adminPasswordFile
	accounts, adminClient, err := connectPatroniDCSAdmin(ctx, patroniSettings)
	if err != nil {
		t.Fatal(err)
	}
	defer adminClient.Close()
	nonRoot := patroniSettings
	nonRoot.PatroniDCSAdminUsername, nonRoot.PatroniDCSAdminPasswordFile = settings.EtcdUsername, passwordFile
	if unexpected, client, err := connectPatroniDCSAdmin(ctx, nonRoot); err == nil {
		client.Close()
		t.Fatal("a non-root account passed Patroni DCS administration", unexpected)
	}
	patroniCluster := uuid.New()
	name, scope := accounts.PatroniScope(patroniCluster)
	patroniPassword := strings.Repeat("p", 8) + uuid.NewString()
	for range 2 {
		if err := accounts.EnsureAccount(ctx, patroniCluster, patroniPassword); err != nil {
			t.Fatal("ensure Patroni DCS account", err)
		}
	}
	patroniClient := func(password string) (*clientv3.Client, error) {
		account := connection
		account.Username, account.Password = name, password
		return clientv3.New(account)
	}
	member, err := patroniClient(patroniPassword)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := member.Put(ctx, scope+"leader", "member"); err != nil {
		t.Fatal("Patroni account cannot use its scope", err)
	}
	otherName, otherScope := accounts.PatroniScope(uuid.New())
	for _, key := range []string{otherScope + "leader", owned + "probe", "/synod-patroni/" + installation.String() + "/initialize"} {
		if _, err := member.Put(ctx, key, "refuse"); !errors.Is(err, rpctypes.ErrPermissionDenied) {
			t.Fatalf("Patroni account crossed its scope at %s: %v", key, err)
		}
	}
	member.Close()
	if _, err := adminClient.Auth.RoleGrantPermission(ctx, name, otherScope, clientv3.GetPrefixRangeEnd(otherScope), clientv3.PermissionType(clientv3.PermRead)); err != nil {
		t.Fatal(err)
	}
	if err := accounts.EnsureAccount(ctx, patroniCluster, patroniPassword); !errors.Is(err, coordinator.ErrPatroniAccountDrift) {
		t.Fatal("drifted Patroni role accepted", err)
	}
	if _, err := adminClient.Auth.RoleRevokePermission(ctx, name, otherScope, clientv3.GetPrefixRangeEnd(otherScope)); err != nil {
		t.Fatal(err)
	}
	if _, err := adminClient.Auth.RoleAdd(ctx, otherName); err != nil {
		t.Fatal(err)
	}
	if _, err := adminClient.Auth.UserGrantRole(ctx, name, otherName); err != nil {
		t.Fatal(err)
	}
	if err := accounts.EnsureAccount(ctx, patroniCluster, patroniPassword); !errors.Is(err, coordinator.ErrPatroniAccountDrift) {
		t.Fatal("drifted Patroni account accepted", err)
	}
	if _, err := adminClient.Auth.UserRevokeRole(ctx, name, otherName); err != nil {
		t.Fatal(err)
	}
	rotated := strings.Repeat("r", 8) + uuid.NewString()
	if err := accounts.EnsureAccount(ctx, patroniCluster, rotated); err != nil {
		t.Fatal("rotate Patroni DCS password", err)
	}
	if old, err := patroniClient(patroniPassword); err == nil {
		_, err = old.Get(ctx, scope+"leader")
		old.Close()
		if err == nil {
			t.Fatal("old Patroni DCS password still authenticates")
		}
	}
	for range 2 {
		if err := accounts.DeleteAccount(ctx, patroniCluster); err != nil {
			t.Fatal("delete Patroni DCS account", err)
		}
	}
	if deleted, err := patroniClient(rotated); err == nil {
		_, err = deleted.Get(ctx, scope+"leader")
		deleted.Close()
		if err == nil {
			t.Fatal("deleted Patroni DCS account still authenticates")
		}
	}
	for _, test := range []string{"untrusted server", "wrong server identity", "wrong password", "missing client certificate", "plaintext"} {
		t.Run(test, func(t *testing.T) {
			bad := connection
			bad.TLS = connection.TLS.Clone()
			badCtx, stop := context.WithTimeout(t.Context(), 4*time.Second)
			defer stop()
			bad.Context = badCtx
			bad.DialTimeout = 2 * time.Second
			switch test {
			case "untrusted server":
				bad.TLS.RootCAs = x509.NewCertPool()
			case "wrong server identity":
				bad.Endpoints = []string{"https://localhost:" + publishedPort}
			case "wrong password":
				bad.Password = uuid.NewString()
			case "missing client certificate":
				bad.TLS.Certificates = nil
			case "plaintext":
				bad.TLS = nil
				bad.Endpoints = []string{"http://" + endpoint}
				bad.Username, bad.Password = "", ""
			}
			candidate, err := clientv3.New(bad)
			if err != nil {
				return
			}
			defer candidate.Close()
			if _, err := candidate.Get(badCtx, owned+"probe"); err == nil {
				t.Fatal("invalid coordination connection accepted")
			}
		})
	}
}
