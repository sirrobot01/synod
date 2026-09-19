// Package pki issues installation identities for authenticated agent transport.
package pki

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"math/big"
	"net"
	"net/url"
	"strings"
	"time"

	"github.com/google/uuid"
)

// Role names the kind of peer that a certificate identifies.
type Role string

const (
	Agent      Role = "agent"
	Controller Role = "controller"
)

var ErrCertificate = errors.New("invalid installation certificate")
var ErrIdentity = errors.New("invalid peer identity")
var ErrCSR = errors.New("invalid certificate request")

// Identity is assigned by the authorized enrollment record, not by CSR fields.
type Identity struct {
	InstallationID uuid.UUID
	Role           Role
	SubjectID      uuid.UUID
}

// URI returns the SPIFFE URI that encodes the identity.
func (i Identity) URI() (*url.URL, error) {
	if i.InstallationID == uuid.Nil || i.SubjectID == uuid.Nil || (i.Role != Agent && i.Role != Controller) {
		return nil, ErrIdentity
	}
	return &url.URL{Scheme: "spiffe", Host: i.InstallationID.String() + ".synod", Path: "/" + string(i.Role) + "/" + i.SubjectID.String()}, nil
}

// CA signs installation certificates for one installation.
type CA struct {
	installationID uuid.UUID
	certificate    *x509.Certificate
	key            ed25519.PrivateKey
}

// CreateCA creates a self-signed Ed25519 installation CA in PEM form.
func CreateCA(installationID uuid.UUID, now time.Time, lifetime time.Duration) (certificatePEM, privateKeyPEM []byte, err error) {
	if installationID == uuid.Nil || lifetime < time.Hour || lifetime > 10*365*24*time.Hour {
		return nil, nil, ErrCertificate
	}
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, nil, ErrCertificate
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return nil, nil, ErrCertificate
	}
	if serial.Sign() == 0 {
		serial.SetInt64(1)
	}
	template := &x509.Certificate{
		SerialNumber: serial, Subject: pkix.Name{CommonName: "Synod installation CA"},
		NotBefore: now.Add(-5 * time.Minute), NotAfter: now.Add(lifetime),
		IsCA: true, BasicConstraintsValid: true, MaxPathLenZero: true,
		KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		URIs:     []*url.URL{{Scheme: "spiffe", Host: installationID.String() + ".synod", Path: "/ca"}},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, public, private)
	if err != nil {
		return nil, nil, ErrCertificate
	}
	key, err := x509.MarshalPKCS8PrivateKey(private)
	if err != nil {
		return nil, nil, ErrCertificate
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: key}), nil
}

// LoadCA parses a stored installation CA and checks that it can sign.
func LoadCA(installationID uuid.UUID, certificatePEM, privateKeyPEM []byte) (*CA, error) {
	if installationID == uuid.Nil || len(certificatePEM) > 64<<10 || len(privateKeyPEM) > 64<<10 {
		return nil, ErrCertificate
	}
	certBlock, rest := pem.Decode(certificatePEM)
	if certBlock == nil || certBlock.Type != "CERTIFICATE" || len(bytes.TrimSpace(rest)) != 0 {
		return nil, ErrCertificate
	}
	cert, err := x509.ParseCertificate(certBlock.Bytes)
	if err != nil || !cert.IsCA || !cert.BasicConstraintsValid || cert.KeyUsage&x509.KeyUsageCertSign == 0 || cert.CheckSignatureFrom(cert) != nil {
		return nil, ErrCertificate
	}
	expected := (&url.URL{Scheme: "spiffe", Host: installationID.String() + ".synod", Path: "/ca"}).String()
	if len(cert.URIs) != 1 || cert.URIs[0].String() != expected {
		return nil, ErrCertificate
	}
	keyBlock, rest := pem.Decode(privateKeyPEM)
	if keyBlock == nil || keyBlock.Type != "PRIVATE KEY" || len(bytes.TrimSpace(rest)) != 0 {
		return nil, ErrCertificate
	}
	parsed, err := x509.ParsePKCS8PrivateKey(keyBlock.Bytes)
	if err != nil {
		return nil, ErrCertificate
	}
	key, ok := parsed.(ed25519.PrivateKey)
	if !ok || !key.Public().(ed25519.PublicKey).Equal(cert.PublicKey) {
		return nil, ErrCertificate
	}
	return &CA{installationID: installationID, certificate: cert, key: key}, nil
}

// Issue signs a CSR public key under the supplied authorized identity.
// Server names apply only to controllers. Requested CSR names are never copied.
func (ca *CA) Issue(csrDER []byte, identity Identity, serverNames []string, now time.Time, lifetime time.Duration) ([]byte, error) {
	if ca == nil || ca.certificate == nil || len(ca.key) != ed25519.PrivateKeySize || identity.InstallationID != ca.installationID || lifetime < 5*time.Minute || lifetime > 30*24*time.Hour || now.Before(ca.certificate.NotBefore) || now.Add(lifetime).After(ca.certificate.NotAfter) {
		return nil, ErrCertificate
	}
	uri, err := identity.URI()
	if err != nil {
		return nil, err
	}
	if len(csrDER) > 64<<10 {
		return nil, ErrCSR
	}
	csr, err := x509.ParseCertificateRequest(csrDER)
	if err != nil || csr.CheckSignature() != nil {
		return nil, ErrCSR
	}
	if _, ok := csr.PublicKey.(ed25519.PublicKey); !ok {
		return nil, ErrCSR
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return nil, ErrCertificate
	}
	if serial.Sign() == 0 {
		serial.SetInt64(1)
	}
	template := &x509.Certificate{
		SerialNumber: serial, NotBefore: now.Add(-5 * time.Minute), NotAfter: now.Add(lifetime),
		BasicConstraintsValid: true, KeyUsage: x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}, URIs: []*url.URL{uri},
	}
	if template.NotBefore.Before(ca.certificate.NotBefore) {
		template.NotBefore = ca.certificate.NotBefore
	}
	if identity.Role == Agent && len(serverNames) != 0 {
		return nil, ErrIdentity
	}
	if identity.Role == Controller {
		if len(serverNames) == 0 || len(serverNames) > 32 {
			return nil, ErrIdentity
		}
		template.ExtKeyUsage = append(template.ExtKeyUsage, x509.ExtKeyUsageServerAuth)
		for _, name := range serverNames {
			if ip := net.ParseIP(name); ip != nil {
				template.IPAddresses = append(template.IPAddresses, ip)
				continue
			}
			if len(name) == 0 || len(name) > 253 || strings.HasSuffix(name, ".") {
				return nil, ErrIdentity
			}
			for label := range strings.SplitSeq(name, ".") {
				if len(label) == 0 || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' || strings.ContainsFunc(label, func(r rune) bool {
					return !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '-')
				}) {
					return nil, ErrIdentity
				}
			}
			template.DNSNames = append(template.DNSNames, name)
		}
	}
	der, err := x509.CreateCertificate(rand.Reader, template, ca.certificate, csr.PublicKey, ca.key)
	if err != nil {
		return nil, ErrCertificate
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), nil
}

// PeerIdentity requires normal TLS chain verification before identity authorization.
// The session owner must also check current enrollment and revocation state.
func PeerIdentity(state tls.ConnectionState, installationID uuid.UUID, role Role) (Identity, error) {
	if !state.HandshakeComplete || len(state.VerifiedChains) == 0 || len(state.VerifiedChains[0]) == 0 || len(state.PeerCertificates) == 0 {
		return Identity{}, ErrIdentity
	}
	leaf := state.VerifiedChains[0][0]
	if !bytes.Equal(leaf.Raw, state.PeerCertificates[0].Raw) || leaf.IsCA || len(leaf.URIs) != 1 {
		return Identity{}, ErrIdentity
	}
	uri := leaf.URIs[0]
	parts := strings.Split(uri.Path, "/")
	if len(parts) != 3 {
		return Identity{}, ErrIdentity
	}
	subjectID, err := uuid.Parse(parts[2])
	if err != nil {
		return Identity{}, ErrIdentity
	}
	identity := Identity{installationID, role, subjectID}
	expected, err := identity.URI()
	if err != nil || uri.String() != expected.String() {
		return Identity{}, ErrIdentity
	}
	return identity, nil
}

// CertificatePEM returns a fresh copy of the public CA certificate for trust delivery.
func (ca *CA) CertificatePEM() []byte {
	if ca == nil || ca.certificate == nil {
		return nil
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: ca.certificate.Raw})
}
