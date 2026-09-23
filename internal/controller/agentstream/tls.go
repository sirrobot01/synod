package agentstream

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/pki"
)

var ErrTLS = errors.New("invalid controller agent-transport identity or trust")

// TLSConfig validates a controller identity and an installation CA overlap bundle.
// The caller must read the private key and trust files through protected descriptors.
func TLSConfig(installationID uuid.UUID, certificatePEM, keyPEM, trustPEM []byte) (*tls.Config, error) {
	if installationID == uuid.Nil || len(certificatePEM) > 64<<10 || len(keyPEM) > 64<<10 || len(trustPEM) > 1<<20 {
		return nil, fmt.Errorf("%w: the installation must be named and the material bounded", ErrTLS)
	}
	pair, err := tls.X509KeyPair(certificatePEM, keyPEM)
	if err != nil {
		return nil, fmt.Errorf("%w: load the controller key pair: %w", ErrTLS, err)
	}
	if pair.Leaf == nil || pair.Leaf.IsCA || len(pair.Leaf.URIs) != 1 || len(pair.Leaf.DNSNames)+len(pair.Leaf.IPAddresses) == 0 {
		return nil, fmt.Errorf("%w: the leaf must be an end-entity certificate with one URI name and at least one DNS or IP name", ErrTLS)
	}
	uri := pair.Leaf.URIs[0]
	parts := strings.Split(uri.Path, "/")
	if len(parts) != 3 {
		return nil, fmt.Errorf("%w: the leaf URI path %q is not /<role>/<subject>", ErrTLS, uri.Path)
	}
	id, err := uuid.Parse(parts[2])
	if err != nil {
		return nil, fmt.Errorf("%w: parse the controller subject in %q: %w", ErrTLS, uri.Path, err)
	}
	expected, err := (pki.Identity{InstallationID: installationID, Role: pki.Controller, SubjectID: id}).URI()
	if err != nil {
		return nil, fmt.Errorf("%w: build the expected controller URI: %w", ErrTLS, err)
	}
	if uri.String() != expected.String() {
		return nil, fmt.Errorf("%w: the leaf names %q and this installation expects %q", ErrTLS, uri, expected)
	}
	roots := x509.NewCertPool()
	count := 0
	for len(bytes.TrimSpace(trustPEM)) > 0 {
		if !bytes.HasPrefix(bytes.TrimSpace(trustPEM), []byte("-----BEGIN CERTIFICATE-----")) {
			return nil, fmt.Errorf("%w: the trust bundle carries content that is not a certificate block after %d certificates", ErrTLS, count)
		}
		block, rest := pem.Decode(trustPEM)
		if block == nil || block.Type != "CERTIFICATE" || len(block.Headers) != 0 {
			return nil, fmt.Errorf("%w: trust bundle entry %d is not a bare CERTIFICATE block", ErrTLS, count+1)
		}
		ca, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			return nil, fmt.Errorf("%w: parse trust bundle entry %d: %w", ErrTLS, count+1, err)
		}
		if !ca.IsCA || !ca.BasicConstraintsValid || ca.KeyUsage&x509.KeyUsageCertSign == 0 || len(ca.URIs) != 1 || ca.URIs[0].String() != "spiffe://"+installationID.String()+".synod/ca" {
			return nil, fmt.Errorf("%w: trust bundle entry %d is not this installation's signing CA", ErrTLS, count+1)
		}
		if err := ca.CheckSignatureFrom(ca); err != nil {
			return nil, fmt.Errorf("%w: trust bundle entry %d is not self-signed: %w", ErrTLS, count+1, err)
		}
		roots.AddCert(ca)
		count++
		trustPEM = rest
	}
	if count == 0 {
		return nil, fmt.Errorf("%w: the trust bundle carries no certificate", ErrTLS)
	}
	intermediates := x509.NewCertPool()
	for _, der := range pair.Certificate[1:] {
		cert, err := x509.ParseCertificate(der)
		if err != nil {
			return nil, fmt.Errorf("%w: parse an intermediate of the controller chain: %w", ErrTLS, err)
		}
		intermediates.AddCert(cert)
	}
	if _, err := pair.Leaf.Verify(x509.VerifyOptions{Roots: roots, Intermediates: intermediates, KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}); err != nil {
		return nil, fmt.Errorf("%w: verify the controller certificate against the installation trust bundle: %w", ErrTLS, err)
	}
	return &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{pair}, ClientCAs: roots}, nil
}
