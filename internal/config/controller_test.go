package config

import (
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestControllerConfigurationPrecedence(t *testing.T) {
	for _, name := range []string{"SYNOD_ETCD_ENDPOINTS", "SYNOD_ETCD_PREFIX", "SYNOD_ETCD_CA_FILE", "SYNOD_ETCD_TLS_CERT_FILE", "SYNOD_ETCD_TLS_KEY_FILE", "SYNOD_ETCD_USERNAME", "SYNOD_ETCD_PASSWORD_FILE", "SYNOD_ETCD_ALLOW_PLAINTEXT", "SYNOD_CONFIG", "SYNOD_INSTALLATION_ID", "SYNOD_LISTEN", "SYNOD_DATABASE_URL", "SYNOD_ADMIN_TOKEN_FILE", "SYNOD_ACTOR_ID", "SYNOD_TLS_CERT_FILE", "SYNOD_TLS_KEY_FILE", "SYNOD_LOG_LEVEL", "SYNOD_CA_CERT_FILE", "SYNOD_CA_KEY_FILE", "SYNOD_AGENT_LISTEN", "SYNOD_AGENT_TLS_CERT_FILE", "SYNOD_AGENT_TLS_KEY_FILE", "SYNOD_AGENT_CA_FILE", "SYNOD_PLACEMENT_ARCHITECTURE", "SYNOD_PLACEMENT_RUNTIME_VERSION", "SYNOD_PLACEMENT_STORAGE_ID", "SYNOD_ENGINE_ENDPOINT_FIRST_PORT", "SYNOD_ENGINE_ENDPOINT_LAST_PORT", "SYNOD_PATRONI_DCS_ADMIN_USERNAME", "SYNOD_PATRONI_DCS_ADMIN_PASSWORD_FILE", "SYNOD_PATRONI_CA_CERT_FILE", "SYNOD_PATRONI_CA_KEY_FILE", "SYNOD_PATRONI_DCS_ENDPOINTS", "SYNOD_PATRONI_IMAGE"} {
		t.Setenv(name, "")
		if err := os.Unsetenv(name); err != nil {
			t.Fatal(err)
		}
	}
	path := filepath.Join(t.TempDir(), "controller.json")
	if err := os.WriteFile(path, []byte(`{"installation_id":"70c778bc-353c-4159-844b-882a493a1814","listen":"127.0.0.1:9443","database_url":"postgres://file.example/synod","admin_token_file":"token","tls_cert_file":"cert","tls_key_file":"key"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	config, err := LoadController([]string{"--config", path}, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if config.Listen != "127.0.0.1:9443" || config.ActorID != "bootstrap-admin" || config.LogLevel != "info" {
		t.Fatalf("file/default precedence failed: listen=%q actor=%q log=%q", config.Listen, config.ActorID, config.LogLevel)
	}
	t.Setenv("SYNOD_CONFIG", path)
	t.Setenv("SYNOD_DATABASE_URL", "postgres://environment.example/synod")
	t.Setenv("SYNOD_LISTEN", "127.0.0.1:10443")
	config, err = LoadController(nil, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if config.Listen != "127.0.0.1:10443" || config.DatabaseURL != "postgres://environment.example/synod" {
		t.Fatal("environment did not override file values")
	}
	config, err = LoadController([]string{"--listen", "127.0.0.1:11443", "--database-url", "postgres://flag.example/synod"}, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if config.Listen != "127.0.0.1:11443" || config.DatabaseURL != "postgres://flag.example/synod" {
		t.Fatal("flags did not override environment values")
	}
	t.Setenv("SYNOD_DATABASE_URL", "")
	if _, err := LoadController(nil, io.Discard); err == nil {
		t.Fatal("explicit empty environment value did not override the file")
	}
}

func TestControllerRejectsInvalidInput(t *testing.T) {
	for _, name := range []string{"SYNOD_ETCD_ENDPOINTS", "SYNOD_ETCD_PREFIX", "SYNOD_ETCD_CA_FILE", "SYNOD_ETCD_TLS_CERT_FILE", "SYNOD_ETCD_TLS_KEY_FILE", "SYNOD_ETCD_USERNAME", "SYNOD_ETCD_PASSWORD_FILE", "SYNOD_ETCD_ALLOW_PLAINTEXT", "SYNOD_CONFIG", "SYNOD_INSTALLATION_ID", "SYNOD_LISTEN", "SYNOD_DATABASE_URL", "SYNOD_ADMIN_TOKEN_FILE", "SYNOD_ACTOR_ID", "SYNOD_TLS_CERT_FILE", "SYNOD_TLS_KEY_FILE", "SYNOD_LOG_LEVEL", "SYNOD_CA_CERT_FILE", "SYNOD_CA_KEY_FILE", "SYNOD_AGENT_LISTEN", "SYNOD_AGENT_TLS_CERT_FILE", "SYNOD_AGENT_TLS_KEY_FILE", "SYNOD_AGENT_CA_FILE", "SYNOD_PLACEMENT_ARCHITECTURE", "SYNOD_PLACEMENT_RUNTIME_VERSION", "SYNOD_PLACEMENT_STORAGE_ID", "SYNOD_ENGINE_ENDPOINT_FIRST_PORT", "SYNOD_ENGINE_ENDPOINT_LAST_PORT"} {
		t.Setenv(name, "")
		if err := os.Unsetenv(name); err != nil {
			t.Fatal(err)
		}
	}
	for _, test := range []struct {
		name string
		data string
		args []string
		want string
	}{
		{name: "unknown field", data: `{"unknown":"private-value"}`, want: "known fields"},
		{name: "trailing object", data: `{} {}`, want: "one JSON object"},
		{name: "trailing text", data: `{} private-value`, want: "one JSON object"},
		{name: "wrong type", data: `{"listen":123}`, want: "known fields"},
		{name: "null object", data: `null`, want: "JSON object"},
		{name: "oversized", data: strings.Repeat(" ", (1<<20)+1), want: "exceeds 1 MiB"},
		{name: "missing settings", data: `{}`, want: "database_url"},
		{name: "invalid database URL", args: []string{"--database-url", "https://user:private-value@example.com/db"}, want: "database_url"},
		{name: "invalid URL escape", args: []string{"--database-url", "postgres://user:private-value@%zz/db"}, want: "database_url"},
		{name: "invalid port", args: []string{"--listen", "localhost:70000"}, want: "listen port"},
		{name: "invalid log level", args: []string{"--log-level", "private-value"}, want: "log_level"},
		{name: "missing installation", args: []string{"--installation-id", ""}, want: "installation_id"},
		{name: "zero installation", args: []string{"--installation-id", "00000000-0000-0000-0000-000000000000"}, want: "installation_id"},
		{name: "empty actor", args: []string{"--actor-id", ""}, want: "actor_id"},
		{name: "missing token file", args: []string{"--admin-token-file", ""}, want: "admin_token_file"},
		{name: "missing agent identity", args: []string{"--agent-listen", "127.0.0.1:8444"}, want: "must be supplied together"},
		{name: "orphan agent trust", args: []string{"--agent-ca-file", "ca"}, want: "must be supplied together"},
		{name: "bad agent listen", args: []string{"--agent-listen", "127.0.0.1:70000", "--agent-tls-cert-file", "cert", "--agent-tls-key-file", "key", "--agent-ca-file", "ca"}, want: "agent_listen port"},
		{name: "missing CA key", args: []string{"--ca-cert-file", "cert"}, want: "supplied together"},
		{name: "missing CA certificate", args: []string{"--ca-key-file", "key"}, want: "supplied together"},
		{name: "missing TLS key", args: []string{"--tls-key-file", ""}, want: "tls_key_file"},
		{name: "positional argument", args: []string{"private-value"}, want: "unexpected controller arguments"},
		{name: "provisioning without a node shape", args: []string{"--secret-key-file", "key", "--secret-key-id", "70c778bc-353c-4159-844b-882a493a1814"}, want: "placement_architecture"},
		{name: "unknown architecture", args: []string{"--secret-key-file", "key", "--secret-key-id", "70c778bc-353c-4159-844b-882a493a1814", "--placement-architecture", "sparc"}, want: "placement_architecture"},
		{name: "repeated architecture", args: []string{"--secret-key-file", "key", "--secret-key-id", "70c778bc-353c-4159-844b-882a493a1814", "--placement-architecture", "amd64,amd64"}, want: "placement_architecture"},
		{name: "empty architecture", args: []string{"--secret-key-file", "key", "--secret-key-id", "70c778bc-353c-4159-844b-882a493a1814", "--placement-architecture", "amd64,"}, want: "placement_architecture"},
		{name: "spaced architectures", args: []string{"--secret-key-file", "key", "--secret-key-id", "70c778bc-353c-4159-844b-882a493a1814", "--placement-architecture", "amd64, arm64"}, want: "placement_architecture"},
		{name: "three architectures", args: []string{"--secret-key-file", "key", "--secret-key-id", "70c778bc-353c-4159-844b-882a493a1814", "--placement-architecture", "amd64,arm64,amd64"}, want: "placement_architecture"},
		{name: "two architectures pass the shape check", args: []string{"--secret-key-file", "key", "--secret-key-id", "70c778bc-353c-4159-844b-882a493a1814", "--placement-architecture", "arm64,amd64", "--placement-runtime-version", "29.7.2"}, want: "placement_storage_id"},
		{name: "provisioning without a storage pool", args: []string{"--secret-key-file", "key", "--secret-key-id", "70c778bc-353c-4159-844b-882a493a1814", "--placement-architecture", "amd64", "--placement-runtime-version", "29.7.2"}, want: "placement_storage_id"},
		{name: "endpoint port out of range", args: []string{"--engine-endpoint-last-port", "70000"}, want: "engine_endpoint_first_port"},
		{name: "endpoint range inverted", args: []string{"--engine-endpoint-first-port", "6000", "--engine-endpoint-last-port", "5000"}, want: "engine_endpoint_first_port"},
		{name: "endpoint port not a number", args: []string{"--engine-endpoint-first-port", "postgres"}, want: "invalid"},
		{name: "patroni admin without password", args: []string{"--patroni-dcs-admin-username", "patroni-admin"}, want: "supplied together"},
		{name: "patroni admin without password file name", args: []string{"--patroni-dcs-admin-password-file", "admin-password"}, want: "supplied together"},
		{name: "patroni CA without key", args: []string{"--patroni-ca-cert-file", "patroni-ca.crt"}, want: "supplied together"},
		{name: "patroni CA without admin", args: []string{"--patroni-ca-cert-file", "patroni-ca.crt", "--patroni-ca-key-file", "patroni-ca.key", "--patroni-dcs-endpoints", "192.0.2.20:2379", "--patroni-image", "synod-patroni@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}, want: "requires both"},
		{name: "patroni admin without CA", args: []string{"--patroni-dcs-admin-username", "patroni-admin", "--patroni-dcs-admin-password-file", "admin-password"}, want: "requires both"},
		{name: "patroni admin without coordination", args: []string{"--patroni-dcs-admin-username", "patroni-admin", "--patroni-dcs-admin-password-file", "admin-password", "--patroni-ca-cert-file", "patroni-ca.crt", "--patroni-ca-key-file", "patroni-ca.key", "--patroni-dcs-endpoints", "192.0.2.20:2379", "--patroni-image", "synod-patroni@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}, want: "secure etcd coordination"},
		{name: "patroni admin with plaintext coordination", args: []string{"--agent-listen", "127.0.0.1:8444", "--agent-tls-cert-file", "cert", "--agent-tls-key-file", "key", "--agent-ca-file", "ca", "--etcd-endpoints", "127.0.0.1:2379", "--etcd-allow-plaintext", "--patroni-dcs-admin-username", "patroni-admin", "--patroni-dcs-admin-password-file", "admin-password", "--patroni-ca-cert-file", "patroni-ca.crt", "--patroni-ca-key-file", "patroni-ca.key", "--patroni-dcs-endpoints", "192.0.2.20:2379", "--patroni-image", "synod-patroni@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}, want: "secure etcd coordination"},
		{name: "patroni admin name with credentials", args: []string{"--agent-listen", "127.0.0.1:8444", "--agent-tls-cert-file", "cert", "--agent-tls-key-file", "key", "--agent-ca-file", "ca", "--etcd-endpoints", "127.0.0.1:2379", "--etcd-ca-file", "ca", "--etcd-username", "coordination", "--etcd-password-file", "password", "--patroni-dcs-admin-username", "root:private-value", "--patroni-dcs-admin-password-file", "admin-password", "--patroni-ca-cert-file", "patroni-ca.crt", "--patroni-ca-key-file", "patroni-ca.key", "--patroni-dcs-endpoints", "192.0.2.20:2379", "--patroni-image", "synod-patroni@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}, want: "bounded account name"},
		{name: "patroni admin reuses coordination account", args: []string{"--agent-listen", "127.0.0.1:8444", "--agent-tls-cert-file", "cert", "--agent-tls-key-file", "key", "--agent-ca-file", "ca", "--etcd-endpoints", "127.0.0.1:2379", "--etcd-ca-file", "ca", "--etcd-username", "coordination", "--etcd-password-file", "password", "--patroni-dcs-admin-username", "coordination", "--patroni-dcs-admin-password-file", "admin-password", "--patroni-ca-cert-file", "patroni-ca.crt", "--patroni-ca-key-file", "patroni-ca.key", "--patroni-dcs-endpoints", "192.0.2.20:2379", "--patroni-image", "synod-patroni@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}, want: "must differ"},
		{name: "patroni image without digest", args: []string{"--agent-listen", "127.0.0.1:8444", "--agent-tls-cert-file", "cert", "--agent-tls-key-file", "key", "--agent-ca-file", "ca", "--etcd-endpoints", "127.0.0.1:2379", "--etcd-ca-file", "ca", "--etcd-username", "coordination", "--etcd-password-file", "password", "--patroni-dcs-admin-username", "patroni-admin", "--patroni-dcs-admin-password-file", "admin-password", "--patroni-ca-cert-file", "patroni-ca.crt", "--patroni-ca-key-file", "patroni-ca.key", "--patroni-dcs-endpoints", "192.0.2.20:2379", "--patroni-image", "synod-patroni:latest"}, want: "patroni_image"},
		{name: "patroni DCS endpoint without port", args: []string{"--agent-listen", "127.0.0.1:8444", "--agent-tls-cert-file", "cert", "--agent-tls-key-file", "key", "--agent-ca-file", "ca", "--etcd-endpoints", "127.0.0.1:2379", "--etcd-ca-file", "ca", "--etcd-username", "coordination", "--etcd-password-file", "password", "--patroni-dcs-admin-username", "patroni-admin", "--patroni-dcs-admin-password-file", "admin-password", "--patroni-ca-cert-file", "patroni-ca.crt", "--patroni-ca-key-file", "patroni-ca.key", "--patroni-dcs-endpoints", "192.0.2.20", "--patroni-image", "synod-patroni@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}, want: "patroni_dcs_endpoints"},
		{name: "patroni without image", args: []string{"--agent-listen", "127.0.0.1:8444", "--agent-tls-cert-file", "cert", "--agent-tls-key-file", "key", "--agent-ca-file", "ca", "--etcd-endpoints", "127.0.0.1:2379", "--etcd-ca-file", "ca", "--etcd-username", "coordination", "--etcd-password-file", "password", "--patroni-dcs-admin-username", "patroni-admin", "--patroni-dcs-admin-password-file", "admin-password", "--patroni-ca-cert-file", "patroni-ca.crt", "--patroni-ca-key-file", "patroni-ca.key", "--patroni-dcs-endpoints", "192.0.2.20:2379"}, want: "requires patroni_dcs_endpoints"},
	} {
		t.Run(test.name, func(t *testing.T) {
			args := []string{"--installation-id", "70c778bc-353c-4159-844b-882a493a1814", "--database-url", "postgres://localhost/synod", "--admin-token-file", "token", "--tls-cert-file", "cert", "--tls-key-file", "key"}
			if test.name == "missing settings" {
				args = nil
			}
			args = append(args, test.args...)
			if test.data != "" {
				path := filepath.Join(t.TempDir(), "controller.json")
				if err := os.WriteFile(path, []byte(test.data), 0o600); err != nil {
					t.Fatal(err)
				}
				args = append([]string{"--config", path}, args...)
			}
			_, err := LoadController(args, io.Discard)
			if err == nil {
				t.Fatal("invalid input was accepted")
			}
			if !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v; want reason %q", err, test.want)
			}
			if strings.Contains(err.Error(), "private-value") {
				t.Fatal("configuration error exposed a supplied value")
			}
		})
	}
}

func TestControllerAcceptsPatroniDCSAdmin(t *testing.T) {
	for _, name := range []string{"SYNOD_CONFIG", "SYNOD_ETCD_ENDPOINTS", "SYNOD_ETCD_ALLOW_PLAINTEXT", "SYNOD_PATRONI_DCS_ADMIN_USERNAME", "SYNOD_PATRONI_DCS_ADMIN_PASSWORD_FILE", "SYNOD_PATRONI_CA_CERT_FILE", "SYNOD_PATRONI_CA_KEY_FILE", "SYNOD_PATRONI_DCS_ENDPOINTS", "SYNOD_PATRONI_IMAGE"} {
		t.Setenv(name, "")
		if err := os.Unsetenv(name); err != nil {
			t.Fatal(err)
		}
	}
	t.Setenv("SYNOD_PATRONI_DCS_ADMIN_PASSWORD_FILE", "admin-password")
	args := []string{"--installation-id", "70c778bc-353c-4159-844b-882a493a1814", "--database-url", "postgres://localhost/synod", "--admin-token-file", "token", "--tls-cert-file", "cert", "--tls-key-file", "key", "--agent-listen", "127.0.0.1:8444", "--agent-tls-cert-file", "cert", "--agent-tls-key-file", "key", "--agent-ca-file", "ca", "--etcd-endpoints", "127.0.0.1:2379", "--etcd-ca-file", "ca", "--etcd-username", "coordination", "--etcd-password-file", "password", "--patroni-dcs-admin-username", "root", "--patroni-ca-cert-file", "patroni-ca.crt", "--patroni-ca-key-file", "patroni-ca.key", "--patroni-dcs-endpoints", "192.0.2.20:2379", "--patroni-image", "synod-patroni@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}
	loaded, err := LoadController(args, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.PatroniDCSAdminUsername != "root" || loaded.PatroniDCSAdminPasswordFile != "admin-password" {
		t.Fatalf("Patroni DCS admin settings = %q %q", loaded.PatroniDCSAdminUsername, loaded.PatroniDCSAdminPasswordFile)
	}
}
