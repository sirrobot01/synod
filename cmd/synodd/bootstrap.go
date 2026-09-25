package main

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/pki"
)

// caLifetime is how long an installation CA is valid. Re-issuing it means re-enrolling
// every agent, so it outlives the certificates it signs by a wide margin.
const caLifetime = 10 * 365 * 24 * time.Hour

// controllerCertLifetime is how long the controller's own agent-transport certificate
// lasts. The CA refuses to issue for longer than thirty days, deliberately: these are
// short-lived certificates. Nothing renews it yet, so a controller stops accepting
// agents when it expires and this must be run again against a fresh directory.
const controllerCertLifetime = 30 * 24 * time.Hour

// bootstrap writes the material a controller needs to serve an agent transport.
//
// Nothing else produces it: the CA is created here, and the controller's own transport
// certificate is issued from it. Without both, agent_tls_cert_file and agent_ca_file
// name files that cannot exist, so a controller cannot accept an agent at all.
//
// It writes and exits. It starts no listener and enrols nobody.
func bootstrap(args []string, output io.Writer) error {
	flags := flag.NewFlagSet("synodd bootstrap", flag.ContinueOnError)
	flags.SetOutput(output)
	directory := flags.String("dir", "", "directory to write the installation material into (required)")
	installation := flags.String("installation-id", "", "installation UUID; one is generated when absent")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *directory == "" {
		flags.Usage()
		return errors.New("bootstrap requires -dir")
	}
	id, err := installationID(*installation)
	if err != nil {
		return err
	}
	// The directory is created for this owner alone. It holds two private keys and a
	// wrapping key, and a readable one hands an attacker every agent identity.
	if err := os.MkdirAll(*directory, 0o700); err != nil {
		return fmt.Errorf("create the material directory %s: %w", *directory, err)
	}
	certificatePEM, keyPEM, err := pki.CreateCA(id, time.Now(), caLifetime)
	if err != nil {
		return fmt.Errorf("create the installation CA for %s: %w", id, err)
	}
	authority, err := pki.LoadCA(id, certificatePEM, keyPEM)
	if err != nil {
		return fmt.Errorf("load the new installation CA for %s: %w", id, err)
	}
	controllerCert, controllerKey, err := controllerIdentity(authority, id)
	if err != nil {
		return err
	}
	// Patroni members use a separate P-256 root, because native SCRAM channel binding
	// cannot hash an Ed25519 certificate signature.
	patroniCertificatePEM, patroniKeyPEM, err := pki.CreatePatroniCA(id, time.Now(), caLifetime)
	if err != nil {
		return fmt.Errorf("create the Patroni CA for %s: %w", id, err)
	}
	wrapping := make([]byte, 32)
	if _, err := rand.Read(wrapping); err != nil {
		return fmt.Errorf("generate the credential wrapping key: %w", err)
	}
	wrappingID, err := uuid.NewRandom()
	if err != nil {
		return fmt.Errorf("generate the wrapping key identity: %w", err)
	}
	written := []struct {
		name string
		mode os.FileMode
		data []byte
	}{
		{"ca.crt", 0o644, certificatePEM},
		{"ca.key", 0o600, keyPEM},
		{"patroni-ca.crt", 0o644, patroniCertificatePEM},
		{"patroni-ca.key", 0o600, patroniKeyPEM},
		{"agent-tls.crt", 0o644, controllerCert},
		{"agent-tls.key", 0o600, controllerKey},
		{"secret.key", 0o600, []byte(base64.StdEncoding.EncodeToString(wrapping) + "\n")},
		{"installation-id", 0o644, []byte(id.String() + "\n")},
		{"secret-key-id", 0o644, []byte(wrappingID.String() + "\n")},
	}
	// Nothing is overwritten. Replacing a CA that agents are already enrolled against
	// locks every one of them out, and doing it by accident is the way that happens.
	for _, file := range written {
		if _, err := os.Stat(filepath.Join(*directory, file.name)); err == nil {
			return fmt.Errorf("%s already exists; bootstrap refuses to replace installation material", file.name)
		}
	}
	for _, file := range written {
		if err := os.WriteFile(filepath.Join(*directory, file.name), file.data, file.mode); err != nil {
			return fmt.Errorf("write %s: %w", filepath.Join(*directory, file.name), err)
		}
	}
	// The operator needs both lines. A bootstrap whose report did not reach them
	// looks identical to one that did, and secret_key_id cannot be recovered.
	if _, err := fmt.Fprintf(output, "Wrote installation %s material to %s.\nsecret_key_id is %s. Back up ca.key, patroni-ca.key, and secret.key: none can be recovered.\n", id, *directory, wrappingID); err != nil {
		return fmt.Errorf("report the bootstrap result: %w", err)
	}
	return nil
}

// controllerIdentity mints the controller's own key and has the CA certify it.
//
// The names are what an agent verifies the controller by, so a certificate issued
// without the address agents dial is one nothing can connect to.
func controllerIdentity(authority *pki.CA, installation uuid.UUID) (certificatePEM, keyPEM []byte, err error) {
	_, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, nil, fmt.Errorf("generate the controller key: %w", err)
	}
	csr, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{}, private)
	if err != nil {
		return nil, nil, fmt.Errorf("create the controller certificate request: %w", err)
	}
	subject, err := uuid.NewRandom()
	if err != nil {
		return nil, nil, fmt.Errorf("generate the controller identity: %w", err)
	}
	names, err := controllerNames()
	if err != nil {
		return nil, nil, err
	}
	certificate, err := authority.Issue(csr, pki.Identity{
		InstallationID: installation, SubjectID: subject, Role: pki.Controller,
	}, names, time.Now(), controllerCertLifetime)
	if err != nil {
		return nil, nil, fmt.Errorf("issue the controller certificate for %s: %w", subject, err)
	}
	der, err := x509.MarshalPKCS8PrivateKey(private)
	if err != nil {
		return nil, nil, fmt.Errorf("encode the controller key: %w", err)
	}
	return certificate, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}), nil
}

// controllerNames are the addresses agents may dial. SYNOD_CONTROLLER_NAMES names them
// for a deployment agents reach by something other than loopback.
func controllerNames() ([]string, error) {
	declared := os.Getenv("SYNOD_CONTROLLER_NAMES")
	if declared == "" {
		return []string{"localhost", "127.0.0.1"}, nil
	}
	var names []string
	for name := range strings.SplitSeq(declared, ",") {
		if trimmed := strings.TrimSpace(name); trimmed != "" {
			names = append(names, trimmed)
		}
	}
	if len(names) == 0 {
		return nil, errors.New("SYNOD_CONTROLLER_NAMES is set but names nothing")
	}
	return names, nil
}

func installationID(declared string) (uuid.UUID, error) {
	if declared == "" {
		return uuid.NewRandom()
	}
	id, err := uuid.Parse(declared)
	if err != nil {
		return uuid.Nil, fmt.Errorf("parse installation-id %q: %w", declared, err)
	}
	if id == uuid.Nil {
		return uuid.Nil, errors.New("installation-id must be a nonzero UUID")
	}
	return id, nil
}
