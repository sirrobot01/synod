package postgres

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"net/netip"
)

var errReadinessTLS = errors.New("invalid PostgreSQL readiness TLS settings")

// ReadinessTLSConfig validates bounded public trust and requires the declared IP certificate identity.
func ReadinessTLSConfig(caPEM []byte, serverName string) (*tls.Config, error) {
	address, err := netip.ParseAddr(serverName)
	if err != nil || address.String() != serverName || address.Zone() != "" || address.Is4In6() || (!address.IsGlobalUnicast() && !address.IsLoopback()) || len(caPEM) == 0 || len(caPEM) > 128<<10 {
		return nil, errReadinessTLS
	}
	roots := x509.NewCertPool()
	rest := bytes.TrimSpace(caPEM)
	count := 0
	for len(rest) > 0 {
		if count == 8 || !bytes.HasPrefix(rest, []byte("-----BEGIN CERTIFICATE-----")) {
			return nil, errReadinessTLS
		}
		encoded, remaining, found := bytes.Cut(rest, []byte("-----END CERTIFICATE-----"))
		if !found || bytes.Count(encoded, []byte("-----BEGIN")) != 1 {
			return nil, errReadinessTLS
		}
		block, extra := pem.Decode(rest[:len(encoded)+len("-----END CERTIFICATE-----")])
		if block == nil || block.Type != "CERTIFICATE" || len(block.Headers) != 0 || len(bytes.TrimSpace(extra)) != 0 {
			return nil, errReadinessTLS
		}
		certificate, err := x509.ParseCertificate(block.Bytes)
		if err != nil || !certificate.IsCA || !certificate.BasicConstraintsValid || certificate.KeyUsage&x509.KeyUsageCertSign == 0 {
			return nil, errReadinessTLS
		}
		roots.AddCert(certificate)
		rest = bytes.TrimSpace(remaining)
		count++
	}
	if count == 0 {
		return nil, errReadinessTLS
	}
	return &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots, ServerName: serverName}, nil
}
