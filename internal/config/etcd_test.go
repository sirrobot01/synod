package config

import (
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func TestControllerCoordinationSecurity(t *testing.T) {
	for _, name := range []string{"SYNOD_CONFIG", "SYNOD_ETCD_ENDPOINTS", "SYNOD_ETCD_PREFIX", "SYNOD_ETCD_CA_FILE", "SYNOD_ETCD_TLS_CERT_FILE", "SYNOD_ETCD_TLS_KEY_FILE", "SYNOD_ETCD_USERNAME", "SYNOD_ETCD_PASSWORD_FILE", "SYNOD_ETCD_ALLOW_PLAINTEXT"} {
		t.Setenv(name, "")
		if err := os.Unsetenv(name); err != nil {
			t.Fatal(err)
		}
	}
	base := []string{"--installation-id", "70c778bc-353c-4159-844b-882a493a1814", "--database-url", "postgres://localhost/synod", "--admin-token-file", "token", "--tls-cert-file", "cert", "--tls-key-file", "key", "--agent-listen", "127.0.0.1:8444", "--agent-tls-cert-file", "agent.crt", "--agent-tls-key-file", "agent.key", "--agent-ca-file", "agent-ca.crt", "--etcd-endpoints", "127.0.0.1:2379"}
	for name, extra := range map[string][]string{
		"implicit plaintext": nil,
		"missing password":   {"--etcd-ca-file", "root", "--etcd-username", "controller"},
		"mixed transport":    {"--etcd-allow-plaintext", "--etcd-ca-file", "root"},
		"orphan key":         {"--etcd-ca-file", "root", "--etcd-username", "controller", "--etcd-password-file", "password", "--etcd-tls-key-file", "key"},
		"URL secret":         {"--etcd-endpoints", "https://user:private-value@host:2379", "--etcd-allow-plaintext"},
		"empty host":         {"--etcd-endpoints", ":2379", "--etcd-allow-plaintext"},
		"invalid IPv6":       {"--etcd-endpoints", "[user:private-value]:2379", "--etcd-allow-plaintext"},
		"invalid DNS":        {"--etcd-endpoints", "host..example:2379", "--etcd-allow-plaintext"},
		"duplicate endpoint": {"--etcd-endpoints", "HOST:2379,host.:02379", "--etcd-allow-plaintext"},
		"orphan settings":    {"--etcd-endpoints", "", "--etcd-allow-plaintext"},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := LoadController(append(slices.Clone(base), extra...), io.Discard); err == nil || strings.Contains(err.Error(), "private-value") {
				t.Fatal("invalid coordination settings accepted or exposed")
			}
		})
	}
	for _, extra := range [][]string{
		{"--etcd-allow-plaintext"},
		{"--etcd-ca-file", "root", "--etcd-username", "controller", "--etcd-password-file", "password"},
		{"--etcd-ca-file", "root", "--etcd-username", "controller", "--etcd-password-file", "password", "--etcd-tls-cert-file", "cert", "--etcd-tls-key-file", "key"},
	} {
		if _, err := LoadController(append(slices.Clone(base), extra...), io.Discard); err != nil {
			t.Fatal(err)
		}
	}
	file := filepath.Join(t.TempDir(), "controller.json")
	if err := os.WriteFile(file, []byte(`{"etcd_ca_file":"file-ca","etcd_username":"file-user","etcd_password_file":"file-password","etcd_tls_cert_file":"file-cert","etcd_tls_key_file":"file-key"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	args := append(slices.Clone(base), "--config", file)
	configured, err := LoadController(args, io.Discard)
	if err != nil || configured.EtcdUsername != "file-user" || configured.EtcdTLSKeyFile != "file-key" {
		t.Fatal("file settings lost", err)
	}
	for key, value := range map[string]string{"SYNOD_ETCD_CA_FILE": "env-ca", "SYNOD_ETCD_USERNAME": "env-user", "SYNOD_ETCD_PASSWORD_FILE": "env-password", "SYNOD_ETCD_TLS_CERT_FILE": "env-cert", "SYNOD_ETCD_TLS_KEY_FILE": "env-key", "SYNOD_ETCD_ALLOW_PLAINTEXT": "false"} {
		t.Setenv(key, value)
	}
	configured, err = LoadController(args, io.Discard)
	if err != nil || configured.EtcdCAFile != "env-ca" || configured.EtcdUsername != "env-user" || configured.EtcdPasswordFile != "env-password" || configured.EtcdTLSCertFile != "env-cert" || configured.EtcdTLSKeyFile != "env-key" || configured.EtcdAllowPlaintext {
		t.Fatal("environment settings lost", err)
	}
	args = append(args, "--etcd-ca-file", "flag-ca", "--etcd-username", "flag-user", "--etcd-password-file", "flag-password", "--etcd-tls-cert-file", "flag-cert", "--etcd-tls-key-file", "flag-key")
	configured, err = LoadController(args, io.Discard)
	if err != nil || configured.EtcdCAFile != "flag-ca" || configured.EtcdUsername != "flag-user" || configured.EtcdPasswordFile != "flag-password" || configured.EtcdTLSCertFile != "flag-cert" || configured.EtcdTLSKeyFile != "flag-key" {
		t.Fatal("flag settings lost", err)
	}
	t.Setenv("SYNOD_ETCD_ALLOW_PLAINTEXT", "true")
	if _, err := LoadController(args, io.Discard); err == nil {
		t.Fatal("environment mode not applied")
	}
	if _, err := LoadController(append(args, "--etcd-allow-plaintext=false"), io.Discard); err != nil {
		t.Fatal(err)
	}
	t.Setenv("SYNOD_ETCD_ALLOW_PLAINTEXT", "private-value")
	if _, err := LoadController(args, io.Discard); err == nil || strings.Contains(err.Error(), "private-value") {
		t.Fatal("invalid mode accepted or disclosed")
	}
}
