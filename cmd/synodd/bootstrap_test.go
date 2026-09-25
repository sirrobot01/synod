package main

import (
	"bytes"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/pki"
)

// A controller cannot serve an agent transport without this material, and nothing else
// in the product creates it.
func TestBootstrapWritesWhatAControllerNeeds(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "material")
	var out bytes.Buffer
	if err := bootstrap([]string{"-dir", dir}, &out); err != nil {
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
	// The native Patroni CA loads under the same installation and refuses the control-plane profile.
	if _, err := pki.LoadPatroniCA(id, []byte(readFile(t, dir, "patroni-ca.crt")), []byte(readFile(t, dir, "patroni-ca.key"))); err != nil {
		t.Fatalf("the Patroni CA it wrote does not load: %v", err)
	}
	if _, err := pki.LoadCA(id, []byte(readFile(t, dir, "patroni-ca.crt")), []byte(readFile(t, dir, "patroni-ca.key"))); err == nil {
		t.Fatal("the Patroni CA loaded as the control-plane CA")
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
	if err := bootstrap([]string{"-dir", dir}, &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"ca.key", "patroni-ca.key", "agent-tls.key", "secret.key"} {
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
	if err := bootstrap([]string{"-dir", dir}, &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	before, patroniBefore := readFile(t, dir, "ca.crt"), readFile(t, dir, "patroni-ca.crt")
	if err := bootstrap([]string{"-dir", dir}, &bytes.Buffer{}); err == nil {
		t.Fatal("a rerun replaced installation material")
	}
	if readFile(t, dir, "ca.crt") != before || readFile(t, dir, "patroni-ca.crt") != patroniBefore {
		t.Fatal("a refused rerun still changed a CA")
	}
}

// The addresses agents dial are declared, because a certificate without them is one
// nothing can connect to.
func TestBootstrapCertifiesTheNamesAgentsDial(t *testing.T) {
	t.Setenv("SYNOD_CONTROLLER_NAMES", "controller.example, 10.0.0.227 ,")
	dir := filepath.Join(t.TempDir(), "material")
	if err := bootstrap([]string{"-dir", dir}, &bytes.Buffer{}); err != nil {
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
