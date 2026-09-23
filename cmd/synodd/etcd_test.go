package main

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/config"
)

func TestEtcdProtectedConfiguration(t *testing.T) {
	t.Setenv("SYNOD_CONTROLLER_NAMES", "127.0.0.1")
	dir := t.TempDir()
	if err := bootstrap([]string{"-dir", dir}, io.Discard); err != nil {
		t.Fatal(err)
	}
	password := uuid.NewString()
	passwordFile := filepath.Join(dir, "password")
	if err := os.WriteFile(passwordFile, []byte(password+"\r\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	settings := config.Controller{EtcdEndpoints: "127.0.0.1:2379, localhost:2380", EtcdCAFile: filepath.Join(dir, "ca.crt"), EtcdTLSCertFile: filepath.Join(dir, "agent-tls.crt"), EtcdTLSKeyFile: filepath.Join(dir, "agent-tls.key"), EtcdUsername: "coordination", EtcdPasswordFile: passwordFile}
	connection, err := etcdClientConfig(t.Context(), settings)
	if err != nil {
		t.Fatal(err)
	}
	if connection.TLS == nil || connection.TLS.MinVersion != tls.VersionTLS12 || connection.TLS.InsecureSkipVerify || connection.TLS.ServerName != "" || len(connection.TLS.Certificates) != 1 || connection.Password != password || connection.Username != settings.EtcdUsername || connection.Endpoints[1] != "https://localhost:2380" {
		t.Fatal("secure connection settings changed")
	}
	plain, err := etcdClientConfig(t.Context(), config.Controller{EtcdEndpoints: "127.0.0.1:2379", EtcdAllowPlaintext: true})
	if err != nil || plain.TLS != nil || plain.Password != "" || plain.Endpoints[0] != "http://127.0.0.1:2379" {
		t.Fatal("explicit development mode changed", err)
	}
	for name, value := range map[string]string{"short": "private-value", "newline": password + "\n\n", "space": password + " ", "oversize": strings.Repeat("x", 257)} {
		t.Run(name, func(t *testing.T) {
			changed := settings
			changed.EtcdPasswordFile = filepath.Join(t.TempDir(), "password")
			if err := os.WriteFile(changed.EtcdPasswordFile, []byte(value), 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := etcdClientConfig(t.Context(), changed); err == nil || strings.Contains(err.Error(), value) {
				t.Fatal("invalid password accepted or disclosed")
			}
		})
	}
	for _, field := range []string{"password", "key"} {
		t.Run("public "+field, func(t *testing.T) {
			changed := settings
			file := filepath.Join(t.TempDir(), field)
			var data []byte
			if field == "password" {
				data = []byte(password)
				changed.EtcdPasswordFile = file
			} else {
				data = []byte(readFile(t, dir, "agent-tls.key"))
				changed.EtcdTLSKeyFile = file
			}
			if err := os.WriteFile(file, data, 0o644); err != nil {
				t.Fatal(err)
			}
			if _, err := etcdClientConfig(t.Context(), changed); err == nil {
				t.Fatal("public secret accepted")
			}
		})
	}
	for name, data := range map[string][]byte{"empty": nil, "invalid": []byte("private-value"), "leaf": []byte(readFile(t, dir, "agent-tls.crt")), "trailing": []byte(readFile(t, dir, "ca.crt") + "private-value")} {
		t.Run("trust "+name, func(t *testing.T) {
			changed := settings
			changed.EtcdCAFile = filepath.Join(t.TempDir(), "ca.crt")
			if err := os.WriteFile(changed.EtcdCAFile, data, 0o644); err != nil {
				t.Fatal(err)
			}
			if _, err := etcdClientConfig(t.Context(), changed); err == nil || strings.Contains(err.Error(), "private-value") {
				t.Fatal("invalid trust accepted or disclosed")
			}
		})
	}
	caBlock, _ := pem.Decode([]byte(readFile(t, dir, "ca.crt")))
	ca, err := x509.ParseCertificate(caBlock.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	caKeyBlock, _ := pem.Decode([]byte(readFile(t, dir, "ca.key")))
	caKey, err := x509.ParsePKCS8PrivateKey(caKeyBlock.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	leafBlock, _ := pem.Decode([]byte(readFile(t, dir, "agent-tls.crt")))
	leaf, err := x509.ParseCertificate(leafBlock.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"expired", "future", "server only", "CA"} {
		t.Run(name, func(t *testing.T) {
			template := *leaf
			switch name {
			case "expired":
				template.NotBefore = time.Now().Add(-time.Hour)
				template.NotAfter = time.Now().Add(-time.Minute)
			case "future":
				template.NotBefore = time.Now().Add(time.Hour)
				template.NotAfter = time.Now().Add(2 * time.Hour)
			case "server only":
				template.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}
			case "CA":
				template.IsCA = true
				template.KeyUsage = x509.KeyUsageCertSign
			}
			der, err := x509.CreateCertificate(rand.Reader, &template, ca, leaf.PublicKey, caKey.(ed25519.PrivateKey))
			if err != nil {
				t.Fatal(err)
			}
			changed := settings
			changed.EtcdTLSCertFile = filepath.Join(t.TempDir(), "leaf.crt")
			if err := os.WriteFile(changed.EtcdTLSCertFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o644); err != nil {
				t.Fatal(err)
			}
			if _, err := etcdClientConfig(t.Context(), changed); err == nil {
				t.Fatal("invalid client identity accepted")
			}
		})
	}
}
