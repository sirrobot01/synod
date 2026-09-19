package agentstream

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/pki"
)

func TestControllerTransportIdentityAndTrust(t *testing.T) {
	installation := uuid.New()
	root, key, err := pki.CreateCA(installation, time.Now().Add(-3*time.Hour), 7*24*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	ca, err := pki.LoadCA(installation, root, key)
	if err != nil {
		t.Fatal(err)
	}
	_, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	csr, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{}, private)
	if err != nil {
		t.Fatal(err)
	}
	privateDER, err := x509.MarshalPKCS8PrivateKey(private)
	if err != nil {
		t.Fatal(err)
	}
	privatePEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: privateDER})
	identity := pki.Identity{InstallationID: installation, Role: pki.Controller, SubjectID: uuid.New()}
	leaf, err := ca.Issue(csr, identity, []string{"controller.example"}, time.Now(), time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	config, err := TLSConfig(installation, leaf, privatePEM, root)
	if err != nil || config.ClientCAs == nil || len(config.Certificates) != 1 {
		t.Fatalf("valid controller identity rejected: %v", err)
	}
	otherRoot, _, err := pki.CreateCA(uuid.New(), time.Now(), time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	overlap, _, err := pki.CreateCA(installation, time.Now(), time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := TLSConfig(installation, leaf, privatePEM, append(append([]byte{}, root...), overlap...)); err != nil {
		t.Fatalf("installation trust overlap rejected: %v", err)
	}
	for _, test := range []struct {
		name             string
		installation     uuid.UUID
		cert, key, trust []byte
	}{
		{"wrong installation", uuid.New(), leaf, privatePEM, root},
		{"wrong trust", installation, leaf, privatePEM, otherRoot},
		{"mixed installation trust", installation, leaf, privatePEM, append(append([]byte{}, root...), otherRoot...)},
		{"missing trust", installation, leaf, privatePEM, nil},
		{"malformed trailing trust", installation, leaf, privatePEM, append(append([]byte{}, root...), []byte("bad")...)},
		{"CA as leaf", installation, root, key, root},
		{"wrong private key", installation, leaf, key, root},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := TLSConfig(test.installation, test.cert, test.key, test.trust); err == nil {
				t.Fatal("invalid identity or trust accepted")
			}
		})
	}
	identity.Role = pki.Agent
	agent, err := ca.Issue(csr, identity, nil, time.Now(), time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := TLSConfig(installation, agent, privatePEM, root); err == nil {
		t.Fatal("agent identity served as controller")
	}
	identity.Role = pki.Controller
	expired, err := ca.Issue(csr, identity, []string{"controller.example"}, time.Now().Add(-2*time.Hour), time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := TLSConfig(installation, expired, privatePEM, root); err == nil {
		t.Fatal("expired controller identity accepted")
	}
}
