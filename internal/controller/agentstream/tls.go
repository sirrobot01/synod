package agentstream

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"strings"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/pki"
)

var ErrTLS = errors.New("invalid controller agent-transport identity or trust")

// TLSConfig validates a controller identity and an installation CA overlap bundle.
// The caller must read the private key and trust files through protected descriptors.
func TLSConfig(installationID uuid.UUID, certificatePEM, keyPEM, trustPEM []byte) (*tls.Config, error) {
	if installationID == uuid.Nil || len(certificatePEM) > 64<<10 || len(keyPEM) > 64<<10 || len(trustPEM) > 1<<20 {
		return nil, ErrTLS
	}
	pair, err := tls.X509KeyPair(certificatePEM, keyPEM)
	if err != nil || pair.Leaf == nil || pair.Leaf.IsCA || len(pair.Leaf.URIs) != 1 || len(pair.Leaf.DNSNames)+len(pair.Leaf.IPAddresses) == 0 {
		return nil, ErrTLS
	}
	uri := pair.Leaf.URIs[0]
	parts := strings.Split(uri.Path, "/")
	if len(parts) != 3 {
		return nil, ErrTLS
	}
	id, err := uuid.Parse(parts[2])
	if err != nil {
		return nil, ErrTLS
	}
	expected, err := (pki.Identity{InstallationID: installationID, Role: pki.Controller, SubjectID: id}).URI()
	if err != nil || uri.String() != expected.String() {
		return nil, ErrTLS
	}
	roots := x509.NewCertPool()
	count := 0
	for len(bytes.TrimSpace(trustPEM)) > 0 {
		if !bytes.HasPrefix(bytes.TrimSpace(trustPEM), []byte("-----BEGIN CERTIFICATE-----")) {
			return nil, ErrTLS
		}
		block, rest := pem.Decode(trustPEM)
		if block == nil || block.Type != "CERTIFICATE" || len(block.Headers) != 0 {
			return nil, ErrTLS
		}
		ca, err := x509.ParseCertificate(block.Bytes)
		if err != nil || !ca.IsCA || !ca.BasicConstraintsValid || ca.KeyUsage&x509.KeyUsageCertSign == 0 || ca.CheckSignatureFrom(ca) != nil || len(ca.URIs) != 1 || ca.URIs[0].String() != "spiffe://"+installationID.String()+".synod/ca" {
			return nil, ErrTLS
		}
		roots.AddCert(ca)
		count++
		trustPEM = rest
	}
	if count == 0 {
		return nil, ErrTLS
	}
	intermediates := x509.NewCertPool()
	for _, der := range pair.Certificate[1:] {
		cert, err := x509.ParseCertificate(der)
		if err != nil {
			return nil, ErrTLS
		}
		intermediates.AddCert(cert)
	}
	if _, err := pair.Leaf.Verify(x509.VerifyOptions{Roots: roots, Intermediates: intermediates, KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}); err != nil {
		return nil, ErrTLS
	}
	return &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{pair}, ClientCAs: roots}, nil
}
