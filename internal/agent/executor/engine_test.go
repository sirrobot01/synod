package executor

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
)

func TestPrepareEngineWritesPrivateTLSFiles(t *testing.T) {
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "node"}, NotBefore: time.Now(), NotAfter: time.Now().Add(time.Hour)}
	der, err := x509.CreateCertificate(rand.Reader, template, template, public, private)
	if err != nil {
		t.Fatal(err)
	}
	apps := t.TempDir()
	e := &Executor{options: Options{AppsDir: apps, EngineTLS: &tls.Certificate{Certificate: [][]byte{der}, PrivateKey: private}, EngineCA: []byte("ca")}}
	app := uuid.NewString()
	mounts := []*agentv1.Mount{{Type: "engine", Source: "data", Target: "/var/lib/postgresql/data"}, {Type: "engine", Source: "tls", Target: "/tls-src", ReadOnly: true}}
	for range 2 {
		if err := e.prepareEngine(app, mounts); err != nil {
			t.Fatal(err)
		}
	}
	root := filepath.Join(apps, app, "engine")
	if info, err := os.Stat(filepath.Join(root, "data")); err != nil || !info.IsDir() || info.Mode().Perm() != 0o700 {
		t.Fatalf("data directory: %v %v", info, err)
	}
	key, err := os.ReadFile(filepath.Join(root, "tls", "server.key"))
	if err != nil {
		t.Fatal(err)
	}
	block, _ := pem.Decode(key)
	parsed, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil || !private.Equal(parsed) {
		t.Fatalf("key: %v", err)
	}
	if info, err := os.Stat(filepath.Join(root, "tls", "server.key")); err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("key mode: %v %v", info, err)
	}
	if ca, err := os.ReadFile(filepath.Join(root, "tls", "ca.crt")); err != nil || string(ca) != "ca" {
		t.Fatalf("ca: %q %v", ca, err)
	}
	e.options.EngineTLS = nil
	if err := e.prepareEngine(app, mounts); err == nil {
		t.Fatal("a copy without a node certificate was prepared")
	}
	if err := e.prepareEngine("../escape", mounts); err == nil {
		t.Fatal("an invalid app ID was accepted")
	}
}
