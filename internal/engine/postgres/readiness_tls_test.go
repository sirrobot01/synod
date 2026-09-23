package postgres

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"math/big"
	"testing"
	"time"
)

func TestReadinessTLSConfigRequiresBoundedPublicCA(t *testing.T) {
	public, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	certificate := &x509.Certificate{SerialNumber: big.NewInt(1), NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), BasicConstraintsValid: true, IsCA: true, KeyUsage: x509.KeyUsageCertSign}
	issue := func() []byte {
		t.Helper()
		der, err := x509.CreateCertificate(rand.Reader, certificate, certificate, public, key)
		if err != nil {
			t.Fatal(err)
		}
		return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	}
	valid := issue()
	for _, address := range []string{"192.0.2.10", "127.0.0.1", "2001:db8::10", "::1"} {
		config, err := ReadinessTLSConfig(valid, address)
		if err != nil || config.ServerName != address || config.MinVersion != tls.VersionTLS12 || config.InsecureSkipVerify || config.RootCAs == nil || len(config.Certificates) != 0 {
			t.Fatal("invalid TLS verification profile", err)
		}
	}
	certificate.IsCA = false
	leaf := issue()
	certificate.IsCA = true
	certificate.KeyUsage = x509.KeyUsageDigitalSignature
	unsigned := issue()
	for name, data := range map[string][]byte{
		"empty": nil, "whitespace": []byte(" \n"), "oversize": bytes.Repeat([]byte("x"), (128<<10)+1),
		"leaf": leaf, "cannot sign": unsigned, "leading data": append([]byte("ignored data\n"), valid...),
		"trailing data":            append(bytes.Clone(valid), []byte("ignored data")...),
		"skipped malformed block":  append([]byte("-----BEGIN CERTIFICATE-----\ninvalid\n"), valid...),
		"trailing malformed block": append(bytes.Clone(valid), []byte("-----BEGIN CERTIFICATE-----\ninvalid")...),
		"too many":                 bytes.Repeat(valid, 9),
		"private key":              pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: []byte("private material")}),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := ReadinessTLSConfig(data, "192.0.2.10"); err == nil {
				t.Fatal("invalid trust accepted")
			}
		})
	}
	if _, err := ReadinessTLSConfig(bytes.Repeat(valid, 8), "192.0.2.10"); err != nil {
		t.Fatal("bounded trust overlap refused", err)
	}
	for _, address := range []string{"", "db.example", "0.0.0.0", "::", "224.0.0.1", "[::1]", "::ffff:192.0.2.10", "fe80::1%en0"} {
		if _, err := ReadinessTLSConfig(valid, address); err == nil {
			t.Fatal("invalid certificate identity accepted", address)
		}
	}
}
