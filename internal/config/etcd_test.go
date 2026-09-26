package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestControllerEtcdImageSettings(t *testing.T) {
	for _, name := range []string{"SYNOD_CONFIG", "SYNOD_ETCD_IMAGE"} {
		t.Setenv(name, "")
		if err := os.Unsetenv(name); err != nil {
			t.Fatal(err)
		}
	}
	base := []string{
		"--installation-id", "70c778bc-353c-4159-844b-882a493a1814",
		"--database-url", "postgres://localhost/synod",
		"--admin-token-file", "token",
		"--tls-cert-file", "cert",
		"--tls-key-file", "key",
		"--agent-listen", "127.0.0.1:8444",
		"--agent-tls-cert-file", "cert",
		"--agent-tls-key-file", "key",
		"--agent-ca-file", "ca",
		"--patroni-ca-cert-file", "patroni-ca.crt",
		"--patroni-ca-key-file", "patroni-ca.key",
		"--patroni-image", "synod-patroni@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
	}
	file := filepath.Join(t.TempDir(), "controller.json")
	if err := os.WriteFile(file, []byte(`{"etcd_image":"synod-etcd@sha256:1111111111111111111111111111111111111111111111111111111111111111"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	args := append(base, "--config", file)
	configured, err := parse(args)
	if err != nil || configured.EtcdImage != "synod-etcd@sha256:1111111111111111111111111111111111111111111111111111111111111111" {
		t.Fatalf("file settings lost: %v", err)
	}
	t.Setenv("SYNOD_ETCD_IMAGE", "synod-etcd@sha256:2222222222222222222222222222222222222222222222222222222222222222")
	configured, err = parse(args)
	if err != nil || configured.EtcdImage != "synod-etcd@sha256:2222222222222222222222222222222222222222222222222222222222222222" {
		t.Fatalf("environment settings lost: %v", err)
	}
	args = append(args, "--etcd-image", "synod-etcd@sha256:3333333333333333333333333333333333333333333333333333333333333333")
	configured, err = parse(args)
	if err != nil || configured.EtcdImage != "synod-etcd@sha256:3333333333333333333333333333333333333333333333333333333333333333" {
		t.Fatalf("flag settings lost: %v", err)
	}
}
