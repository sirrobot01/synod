package postgres

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"net/netip"
	"time"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/pki"
)

// PatroniTLSMaterial contains the protected native trust and identity files.
type PatroniTLSMaterial struct {
	CA          []byte `json:"ca"`
	Certificate []byte `json:"certificate"`
	PrivateKey  []byte `json:"private_key"`
}

// IssuePatroniMemberMaterial creates a member key and has the native CA certify its member identity,
// its IP, and each routed endpoint address.
func IssuePatroniMemberMaterial(ca *pki.CA, identity pki.PatroniIdentity, address netip.Addr, now time.Time, lifetime time.Duration, endpoints ...netip.Addr) (PatroniTLSMaterial, error) {
	if identity.ResourceID == uuid.Nil {
		return PatroniTLSMaterial{}, errors.New("Patroni member material requires a member resource")
	}
	certificate, key, err := issuePatroniLeaf(ca, identity, address, now, lifetime, endpoints...)
	if err != nil {
		return PatroniTLSMaterial{}, err
	}
	return PatroniTLSMaterial{CA: ca.CertificatePEM(), Certificate: certificate, PrivateKey: key}, nil
}

// IssuePatroniDCSMaterial creates the cluster DCS client key and certificate.
// Its trust bundle is the etcd server trust followed by the native CA, so Patroni can verify
// the etcd server and the client leaf chains to a root in the same bundle.
func IssuePatroniDCSMaterial(ca *pki.CA, identity pki.PatroniIdentity, etcdTrust []byte, now time.Time, lifetime time.Duration) (PatroniTLSMaterial, error) {
	trust := bytes.TrimSpace(etcdTrust)
	if identity.ResourceID != uuid.Nil || len(trust) == 0 {
		return PatroniTLSMaterial{}, errors.New("Patroni DCS material requires a cluster identity and the etcd server trust")
	}
	certificate, key, err := issuePatroniLeaf(ca, identity, netip.Addr{}, now, lifetime)
	if err != nil {
		return PatroniTLSMaterial{}, err
	}
	bundle := append(append(bytes.Clone(trust), '\n'), ca.CertificatePEM()...)
	return PatroniTLSMaterial{CA: bundle, Certificate: certificate, PrivateKey: key}, nil
}

// issuePatroniLeaf keeps key generation and the CSR together for both native profiles.
func issuePatroniLeaf(ca *pki.CA, identity pki.PatroniIdentity, address netip.Addr, now time.Time, lifetime time.Duration, endpoints ...netip.Addr) (certificate, key []byte, err error) {
	_, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, nil, fmt.Errorf("generate a Patroni key: %w", err)
	}
	csr, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{}, private)
	if err != nil {
		return nil, nil, fmt.Errorf("sign a Patroni certificate request: %w", err)
	}
	certificate, err = ca.IssuePatroni(csr, identity, address, now, lifetime, endpoints...)
	if err != nil {
		return nil, nil, err
	}
	der, err := x509.MarshalPKCS8PrivateKey(private)
	if err != nil {
		return nil, nil, fmt.Errorf("encode a Patroni key: %w", err)
	}
	return certificate, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}), nil
}

// PatroniFiles validates native identities and returns the seven secret-bearing source files.
func PatroniFiles(settings PatroniSettings, secrets PatroniSecrets, member, dcs PatroniTLSMaterial, now time.Time) (map[string][]byte, error) {
	if now.IsZero() {
		return nil, errors.New("Patroni TLS verification requires the current time")
	}
	config, err := RenderPatroni(settings, secrets)
	if err != nil {
		return nil, err
	}
	for index, material := range []PatroniTLSMaterial{member, dcs} {
		for _, data := range [][]byte{material.CA, material.Certificate, material.PrivateKey} {
			if len(data) == 0 || len(data) > 128<<10 {
				return nil, errors.New("Patroni TLS files must contain one to 128 KiB")
			}
		}
		roots, intermediates := x509.NewCertPool(), x509.NewCertPool()
		for kind, data := range [][]byte{material.CA, material.Certificate} {
			count := 0
			for len(bytes.TrimSpace(data)) != 0 {
				data = bytes.TrimSpace(data)
				if !bytes.HasPrefix(data, []byte("-----BEGIN CERTIFICATE-----")) {
					return nil, errors.New("Patroni TLS requires bare certificate PEM blocks")
				}
				end := bytes.Index(data, []byte("-----END CERTIFICATE-----"))
				if end < 0 {
					return nil, errors.New("Patroni TLS certificate encoding is invalid")
				}
				end += len("-----END CERTIFICATE-----")
				block, rest := pem.Decode(data[:end])
				if block == nil || block.Type != "CERTIFICATE" || len(block.Headers) != 0 || len(bytes.TrimSpace(rest)) != 0 || bytes.Count(data[:end], []byte("-----BEGIN")) != 1 {
					return nil, errors.New("Patroni TLS certificate encoding is invalid")
				}
				certificate, err := x509.ParseCertificate(block.Bytes)
				if err != nil || now.Before(certificate.NotBefore) || !now.Before(certificate.NotAfter) {
					return nil, errors.New("Patroni TLS certificate is invalid or outside its validity period")
				}
				if kind == 0 || count > 0 {
					if !certificate.IsCA || !certificate.BasicConstraintsValid || certificate.KeyUsage&x509.KeyUsageCertSign == 0 {
						return nil, errors.New("Patroni TLS trust and intermediate certificates must be certificate authorities")
					}
					if kind == 0 {
						roots.AddCert(certificate)
					} else {
						intermediates.AddCert(certificate)
					}
				}
				count++
				if count > 8 {
					return nil, errors.New("Patroni TLS certificate chain is too long")
				}
				data = data[end:]
			}
			if count == 0 {
				return nil, errors.New("Patroni TLS certificate chain is empty")
			}
		}
		keyPEM := bytes.TrimSpace(material.PrivateKey)
		key, rest := pem.Decode(keyPEM)
		if !bytes.HasPrefix(keyPEM, []byte("-----BEGIN PRIVATE KEY-----")) || key == nil || key.Type != "PRIVATE KEY" || len(key.Headers) != 0 || len(bytes.TrimSpace(rest)) != 0 || bytes.Count(keyPEM, []byte("-----BEGIN")) != 1 {
			return nil, errors.New("Patroni TLS requires one bare PKCS#8 private key")
		}
		if _, err := x509.ParsePKCS8PrivateKey(key.Bytes); err != nil {
			return nil, errors.New("Patroni TLS private key is invalid")
		}
		pair, err := tls.X509KeyPair(material.Certificate, material.PrivateKey)
		if err != nil || pair.Leaf == nil || pair.Leaf.IsCA || pair.Leaf.KeyUsage&x509.KeyUsageDigitalSignature == 0 {
			return nil, errors.New("Patroni TLS leaf and private key do not form a valid native identity")
		}
		options := x509.VerifyOptions{Roots: roots, Intermediates: intermediates, CurrentTime: now, KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}}
		if _, err := pair.Leaf.Verify(options); err != nil {
			return nil, errors.New("Patroni TLS client identity is not trusted")
		}
		if index == 0 {
			options.KeyUsages = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}
			options.DNSName = settings.Address.String()
			if _, err := pair.Leaf.Verify(options); err != nil {
				return nil, errors.New("Patroni TLS server identity does not match the declared member")
			}
		} else if pair.Leaf.Subject.CommonName != "" {
			return nil, errors.New("Patroni DCS client certificate must have an empty common name")
		}
	}
	return map[string][]byte{
		"config.json": config,
		"ca.crt":      bytes.Clone(member.CA), "member.crt": bytes.Clone(member.Certificate), "member.key": bytes.Clone(member.PrivateKey),
		"dcs-ca.crt": bytes.Clone(dcs.CA), "dcs-client.crt": bytes.Clone(dcs.Certificate), "dcs-client.key": bytes.Clone(dcs.PrivateKey),
	}, nil
}

// Expiry reads the NotAfter time of the leaf certificate.
func (m PatroniTLSMaterial) Expiry() (time.Time, error) {
	block, _ := pem.Decode(m.Certificate)
	if block == nil || block.Type != "CERTIFICATE" {
		return time.Time{}, errors.New("Patroni TLS material holds no certificate")
	}
	certificate, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return time.Time{}, err
	}
	return certificate.NotAfter, nil
}
