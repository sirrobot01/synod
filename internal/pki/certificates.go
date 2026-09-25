// Package pki issues installation identities for authenticated agent transport.
package pki

import (
	"bytes"
	"crypto"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
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
		return nil, fmt.Errorf("%w: installation %s, role %q, subject %s", ErrIdentity, i.InstallationID, i.Role, i.SubjectID)
	}
	return &url.URL{Scheme: "spiffe", Host: i.InstallationID.String() + ".synod", Path: "/" + string(i.Role) + "/" + i.SubjectID.String()}, nil
}

// CA signs installation certificates for one installation.
type CA struct {
	installationID uuid.UUID
	certificate    *x509.Certificate
	key            crypto.Signer
	native         bool
}

// CreateCA creates a self-signed Ed25519 installation CA in PEM form.
func CreateCA(installationID uuid.UUID, now time.Time, lifetime time.Duration) (certificatePEM, privateKeyPEM []byte, err error) {
	return createCA(installationID, now, lifetime, false)
}

func createCA(installationID uuid.UUID, now time.Time, lifetime time.Duration, native bool) ([]byte, []byte, error) {
	if installationID == uuid.Nil || lifetime < time.Hour || lifetime > 10*365*24*time.Hour {
		return nil, nil, fmt.Errorf("%w: create a CA for installation %s with a lifetime of %s", ErrCertificate, installationID, lifetime)
	}
	var private crypto.Signer
	var err error
	path, name := "/ca", "Synod installation CA"
	if native {
		private, err = ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		path, name = "/patroni-ca", "Synod Patroni CA"
	} else {
		_, private, err = ed25519.GenerateKey(rand.Reader)
	}
	if err != nil {
		return nil, nil, fmt.Errorf("%w: generate the CA key: %w", ErrCertificate, err)
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return nil, nil, fmt.Errorf("%w: draw the CA serial: %w", ErrCertificate, err)
	}
	if serial.Sign() == 0 {
		serial.SetInt64(1)
	}
	template := &x509.Certificate{
		SerialNumber: serial, Subject: pkix.Name{CommonName: name},
		NotBefore: now.Add(-5 * time.Minute), NotAfter: now.Add(lifetime),
		IsCA: true, BasicConstraintsValid: true, MaxPathLenZero: true,
		KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		URIs:     []*url.URL{{Scheme: "spiffe", Host: installationID.String() + ".synod", Path: path}},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, private.Public(), private)
	if err != nil {
		return nil, nil, fmt.Errorf("%w: sign the CA certificate: %w", ErrCertificate, err)
	}
	key, err := x509.MarshalPKCS8PrivateKey(private)
	if err != nil {
		return nil, nil, fmt.Errorf("%w: encode the CA key: %w", ErrCertificate, err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: key}), nil
}

// LoadCA parses a stored installation CA and checks that it can sign.
func LoadCA(installationID uuid.UUID, certificatePEM, privateKeyPEM []byte) (*CA, error) {
	return loadCA(installationID, certificatePEM, privateKeyPEM, false)
}

func loadCA(installationID uuid.UUID, certificatePEM, privateKeyPEM []byte, native bool) (*CA, error) {
	if installationID == uuid.Nil || len(certificatePEM) > 64<<10 || len(privateKeyPEM) > 64<<10 {
		return nil, fmt.Errorf("%w: the installation must be named and the CA material bounded", ErrCertificate)
	}
	certBlock, rest := pem.Decode(certificatePEM)
	if certBlock == nil || certBlock.Type != "CERTIFICATE" || len(bytes.TrimSpace(rest)) != 0 {
		return nil, fmt.Errorf("%w: the CA material is not one bare CERTIFICATE block", ErrCertificate)
	}
	cert, err := x509.ParseCertificate(certBlock.Bytes)
	if err != nil {
		return nil, fmt.Errorf("%w: parse the CA certificate: %w", ErrCertificate, err)
	}
	if !cert.IsCA || !cert.BasicConstraintsValid || cert.KeyUsage&x509.KeyUsageCertSign == 0 {
		return nil, fmt.Errorf("%w: the stored certificate cannot sign", ErrCertificate)
	}
	if err := cert.CheckSignatureFrom(cert); err != nil {
		return nil, fmt.Errorf("%w: the CA certificate is not self-signed: %w", ErrCertificate, err)
	}
	path := "/ca"
	if native {
		path = "/patroni-ca"
	}
	expected := (&url.URL{Scheme: "spiffe", Host: installationID.String() + ".synod", Path: path}).String()
	if len(cert.URIs) != 1 || cert.URIs[0].String() != expected {
		return nil, fmt.Errorf("%w: the CA names %v and installation %s expects %q", ErrCertificate, cert.URIs, installationID, expected)
	}
	keyBlock, rest := pem.Decode(privateKeyPEM)
	if keyBlock == nil || keyBlock.Type != "PRIVATE KEY" || len(bytes.TrimSpace(rest)) != 0 {
		return nil, fmt.Errorf("%w: the CA key material is not one bare PRIVATE KEY block", ErrCertificate)
	}
	parsed, err := x509.ParsePKCS8PrivateKey(keyBlock.Bytes)
	if err != nil {
		return nil, fmt.Errorf("%w: parse the CA key: %w", ErrCertificate, err)
	}
	key, ok := parsed.(crypto.Signer)
	if !ok {
		return nil, ErrCertificate
	}
	if native {
		p256, ok := key.(*ecdsa.PrivateKey)
		if !ok || p256.Curve != elliptic.P256() || cert.SignatureAlgorithm != x509.ECDSAWithSHA256 {
			return nil, ErrCertificate
		}
	} else if _, ok := key.(ed25519.PrivateKey); !ok {
		return nil, ErrCertificate
	}
	public, err := x509.MarshalPKIXPublicKey(key.Public())
	if err != nil || !bytes.Equal(public, cert.RawSubjectPublicKeyInfo) {
		return nil, fmt.Errorf("%w: the stored key does not match the CA certificate", ErrCertificate)
	}
	return &CA{installationID: installationID, certificate: cert, key: key, native: native}, nil
}

// Issue signs a CSR public key under the supplied authorized identity.
// Server names apply only to controllers. Requested CSR names are never copied.
func (ca *CA) Issue(csrDER []byte, identity Identity, serverNames []string, now time.Time, lifetime time.Duration) ([]byte, error) {
	if ca == nil || ca.certificate == nil || ca.key == nil || identity.InstallationID != ca.installationID {
		return nil, ErrCertificate
	}
	if ca.native {
		return nil, ErrIdentity
	}
	uri, err := identity.URI()
	if err != nil {
		return nil, err
	}
	template := &x509.Certificate{
		BasicConstraintsValid: true, KeyUsage: x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}, URIs: []*url.URL{uri},
	}
	if identity.Role == Agent && len(serverNames) != 0 {
		return nil, fmt.Errorf("%w: an agent certificate carries no server name and %d were requested", ErrIdentity, len(serverNames))
	}
	if identity.Role == Controller {
		if len(serverNames) == 0 || len(serverNames) > 32 {
			return nil, fmt.Errorf("%w: a controller certificate needs 1 to 32 server names and %d were requested", ErrIdentity, len(serverNames))
		}
		template.ExtKeyUsage = append(template.ExtKeyUsage, x509.ExtKeyUsageServerAuth)
		for _, name := range serverNames {
			if ip := net.ParseIP(name); ip != nil {
				template.IPAddresses = append(template.IPAddresses, ip)
				continue
			}
			if len(name) == 0 || len(name) > 253 || strings.HasSuffix(name, ".") {
				return nil, fmt.Errorf("%w: server name %q is not a bounded relative DNS name", ErrIdentity, name)
			}
			for label := range strings.SplitSeq(name, ".") {
				if len(label) == 0 || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' || strings.ContainsFunc(label, func(r rune) bool {
					return !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '-')
				}) {
					return nil, fmt.Errorf("%w: server name %q carries a label that is not an LDH label", ErrIdentity, name)
				}
			}
			template.DNSNames = append(template.DNSNames, name)
		}
	}
	return ca.issueLeaf(csrDER, template, now, lifetime)
}

// issueLeaf keeps validity and proof-of-possession checks common to all profiles.
func (ca *CA) issueLeaf(csrDER []byte, template *x509.Certificate, now time.Time, lifetime time.Duration) ([]byte, error) {
	if ca == nil || ca.certificate == nil || ca.key == nil || lifetime < 5*time.Minute || lifetime > 30*24*time.Hour || now.Before(ca.certificate.NotBefore) || now.Add(lifetime).After(ca.certificate.NotAfter) {
		return nil, fmt.Errorf("%w: CA validity or leaf lifetime does not permit issuance", ErrCertificate)
	}
	if len(csrDER) > 64<<10 {
		return nil, fmt.Errorf("%w: the request is %d bytes", ErrCSR, len(csrDER))
	}
	csr, err := x509.ParseCertificateRequest(csrDER)
	if err != nil {
		return nil, fmt.Errorf("%w: parse the request: %w", ErrCSR, err)
	}
	if err := csr.CheckSignature(); err != nil {
		return nil, fmt.Errorf("%w: check the request signature: %w", ErrCSR, err)
	}
	if _, ok := csr.PublicKey.(ed25519.PublicKey); !ok {
		return nil, fmt.Errorf("%w: the request carries a %T public key and an Ed25519 key is required", ErrCSR, csr.PublicKey)
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return nil, fmt.Errorf("%w: draw the certificate serial: %w", ErrCertificate, err)
	}
	if serial.Sign() == 0 {
		serial.SetInt64(1)
	}
	template.SerialNumber = serial
	template.NotBefore = now.Add(-5 * time.Minute)
	template.NotAfter = now.Add(lifetime)
	if template.NotBefore.Before(ca.certificate.NotBefore) {
		template.NotBefore = ca.certificate.NotBefore
	}
	der, err := x509.CreateCertificate(rand.Reader, template, ca.certificate, csr.PublicKey, ca.key)
	if err != nil {
		return nil, fmt.Errorf("%w: sign leaf certificate: %w", ErrCertificate, err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), nil
}

// PeerIdentity requires normal TLS chain verification before identity authorization.
// The session owner must also check current enrollment and revocation state.
func PeerIdentity(state tls.ConnectionState, installationID uuid.UUID, role Role) (Identity, error) {
	if !state.HandshakeComplete || len(state.VerifiedChains) == 0 || len(state.VerifiedChains[0]) == 0 || len(state.PeerCertificates) == 0 {
		return Identity{}, fmt.Errorf("%w: the peer completed no verified handshake", ErrIdentity)
	}
	leaf := state.VerifiedChains[0][0]
	if !bytes.Equal(leaf.Raw, state.PeerCertificates[0].Raw) || leaf.IsCA || len(leaf.URIs) != 1 {
		return Identity{}, fmt.Errorf("%w: the verified leaf is not the end-entity certificate the peer presented, or it carries %d URI names", ErrIdentity, len(leaf.URIs))
	}
	uri := leaf.URIs[0]
	parts := strings.Split(uri.Path, "/")
	if len(parts) != 3 {
		return Identity{}, fmt.Errorf("%w: the peer URI path %q is not /<role>/<subject>", ErrIdentity, uri.Path)
	}
	subjectID, err := uuid.Parse(parts[2])
	if err != nil {
		return Identity{}, fmt.Errorf("%w: parse the peer subject in %q: %w", ErrIdentity, uri.Path, err)
	}
	identity := Identity{installationID, role, subjectID}
	expected, err := identity.URI()
	if err != nil {
		return Identity{}, fmt.Errorf("%w: build the expected %s URI: %w", ErrIdentity, role, err)
	}
	if uri.String() != expected.String() {
		return Identity{}, fmt.Errorf("%w: the peer names %q and this installation expects %q", ErrIdentity, uri, expected)
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
