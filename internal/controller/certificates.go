package controller

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/pki"
)

// liveCertificate holds a TLS certificate that renewal replaces while connections run.
type liveCertificate struct {
	current atomic.Pointer[tls.Certificate]
}

func (l *liveCertificate) get(*tls.ClientHelloInfo) (*tls.Certificate, error) {
	return l.current.Load(), nil
}

func (l *liveCertificate) due(now time.Time) bool {
	certificate := l.current.Load()
	return certificate == nil || certificate.Leaf == nil || certificate.Leaf.NotAfter.Sub(now) < pki.RenewalWindow
}

// renewAgentTransport reissues the controller's agent-transport certificate with its key,
// subject, and names. Agents trust the installation CA, so they accept it at once.
func renewAgentTransport(authority *pki.CA, certificateFile, keyFile string, live *liveCertificate, now time.Time) error {
	leaf := live.current.Load().Leaf
	if len(leaf.URIs) != 1 {
		return errors.New("renew the agent-transport certificate: the current certificate has no single identity")
	}
	parts := strings.Split(leaf.URIs[0].Path, "/")
	subject, err := uuid.Parse(parts[len(parts)-1])
	if err != nil {
		return fmt.Errorf("renew the agent-transport certificate: read its subject: %w", err)
	}
	var names []string
	for _, ip := range leaf.IPAddresses {
		names = append(names, ip.String())
	}
	names = append(names, leaf.DNSNames...)
	keyPEM, err := readStartupFile(keyFile, 64<<10, true)
	if err != nil {
		return fmt.Errorf("renew the agent-transport certificate: read its key: %w", err)
	}
	defer clear(keyPEM)
	signer, err := parseSigner(keyPEM)
	if err != nil {
		return fmt.Errorf("renew the agent-transport certificate: %w", err)
	}
	csr, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{}, signer)
	if err != nil {
		return fmt.Errorf("renew the agent-transport certificate: create its request: %w", err)
	}
	certificatePEM, err := authority.Issue(csr, pki.Identity{InstallationID: authority.InstallationID(), SubjectID: subject, Role: pki.Controller}, names, now, controllerCertLifetime)
	if err != nil {
		return fmt.Errorf("renew the agent-transport certificate: %w", err)
	}
	return install(certificateFile, certificatePEM, "", keyPEM, live)
}

// renewAPI issues a new API certificate and key from the API CA with the same names.
// Clients pin the API CA, so they accept the new certificate.
func renewAPI(caCertificateFile, caKeyFile, certificateFile, keyFile string, live *liveCertificate, now time.Time) error {
	caCertificate, err := readStartupFile(caCertificateFile, 64<<10, false)
	if err != nil {
		return fmt.Errorf("renew the API certificate: read the API CA: %w", err)
	}
	caKey, err := readStartupFile(caKeyFile, 64<<10, true)
	if err != nil {
		return fmt.Errorf("renew the API certificate: read the API CA key: %w", err)
	}
	defer clear(caKey)
	leaf := live.current.Load().Leaf
	var names []string
	for _, ip := range leaf.IPAddresses {
		names = append(names, ip.String())
	}
	names = append(names, leaf.DNSNames...)
	certificatePEM, keyPEM, err := issueAPICertificate(caCertificate, caKey, names, now, 365*24*time.Hour)
	if err != nil {
		return fmt.Errorf("renew the API certificate: %w", err)
	}
	defer clear(keyPEM)
	// The chain lets a joining agent pin the API CA from its join token fingerprint.
	return install(certificateFile, append(certificatePEM, caCertificate...), keyFile, keyPEM, live)
}

// install writes a new key before its certificate. Each file is replaced by a rename, so a
// crash leaves the old pair or a new key that the next start pairs with the next renewal.
func install(certificateFile string, certificatePEM []byte, keyFile string, keyPEM []byte, live *liveCertificate) error {
	pair, err := tls.X509KeyPair(certificatePEM, keyPEM)
	if err != nil {
		return fmt.Errorf("load the renewed certificate: %w", err)
	}
	if keyFile != "" {
		if err := writeReplacing(keyFile, keyPEM, 0o600); err != nil {
			return err
		}
	}
	if err := writeReplacing(certificateFile, certificatePEM, 0o644); err != nil {
		return err
	}
	live.current.Store(&pair)
	return nil
}

func writeReplacing(path string, data []byte, mode os.FileMode) error {
	file, err := os.CreateTemp(filepath.Dir(path), ".synod-renew-*")
	if err != nil {
		return fmt.Errorf("replace %s: %w", path, err)
	}
	defer os.Remove(file.Name())
	_, writeErr := file.Write(data)
	if err := errors.Join(writeErr, file.Chmod(mode), file.Sync(), file.Close()); err != nil {
		return fmt.Errorf("replace %s: %w", path, err)
	}
	if err := os.Rename(file.Name(), path); err != nil {
		return fmt.Errorf("replace %s: %w", path, err)
	}
	directory, err := os.Open(filepath.Dir(path))
	if err != nil {
		return fmt.Errorf("replace %s: %w", path, err)
	}
	return errors.Join(directory.Sync(), directory.Close())
}

func parseSigner(keyPEM []byte) (crypto.Signer, error) {
	block, _ := pem.Decode(keyPEM)
	if block == nil {
		return nil, errors.New("the key is not PEM")
	}
	key, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("parse the key: %w", err)
	}
	signer, ok := key.(crypto.Signer)
	if !ok {
		return nil, errors.New("the key cannot sign")
	}
	return signer, nil
}

// keepCertificates renews due certificates at start and then once a day.
// A failed renewal is logged and retried. The old certificate serves until it expires.
func keepCertificates(ctx context.Context, renew []func(time.Time) error, logger *slog.Logger) {
	for {
		for _, step := range renew {
			if err := step(time.Now()); err != nil && ctx.Err() == nil {
				logger.Warn("controller certificate renewal failed; retrying tomorrow", "error", err)
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(24 * time.Hour):
		}
	}
}
