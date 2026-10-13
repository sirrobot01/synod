package store

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/secret"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
)

func TestAgentRenewsItsCertificateOverItsSession(t *testing.T) {
	s, ca := open(t), authority(t)
	_, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	request := func(signer ed25519.PrivateKey) []byte {
		t.Helper()
		der, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{}, signer)
		if err != nil {
			t.Fatal(err)
		}
		return der
	}
	verifier := sha256.Sum256([]byte("renewal join secret"))
	if _, err := s.RegisterEnrollmentToken(t.Context(), TokenRegistration{ActorID: "admin", RequestID: "r1", IdempotencyKey: "k1", TokenVerifier: verifier, TTLSeconds: 3600}); err != nil {
		t.Fatal(err)
	}
	node := uuid.New()
	enrolled, err := s.EnrollAgent(t.Context(), EnrollmentRequest{NodeID: node, TokenVerifier: verifier, RequestID: uuid.New(), CSRDER: request(key)}, ca)
	if err != nil {
		t.Fatal(err)
	}
	session := uuid.New()
	if err := s.RecordAgentSession(t.Context(), node, session, &agentv1.Hello{NodeId: node.String(), Architecture: "arm64", DestinationAddress: "192.168.0.10:8445"}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.RenewAgentCertificate(t.Context(), node, session, request(key), ca); !errors.Is(err, ErrStaleSession) {
		t.Fatalf("an unapproved device renewed: %v", err)
	}
	if _, err := s.ApproveNode(t.Context(), node, "admin", "approve"); err != nil {
		t.Fatal(err)
	}
	// A device inside the renewal window gets a new certificate for the same key.
	now := time.Now().Add(25 * 24 * time.Hour)
	s.now = func() time.Time { return now }
	if _, err := s.RenewAgentCertificate(t.Context(), node, uuid.New(), request(key), ca); !errors.Is(err, ErrStaleSession) {
		t.Fatalf("another session renewed: %v", err)
	}
	_, otherKey, _ := ed25519.GenerateKey(rand.Reader)
	if _, err := s.RenewAgentCertificate(t.Context(), node, session, request(otherKey), ca); code(err) != EnrollmentUnauthorized {
		t.Fatalf("another key renewed the device: %v %v", err, errors.Unwrap(err))
	}
	listing, err := s.ListDestinations(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	renewed, err := s.RenewAgentCertificate(t.Context(), node, session, request(key), ca)
	if err != nil {
		t.Fatal(err, errors.Unwrap(err))
	}
	if renewed.CertificateID == enrolled.CertificateID {
		t.Fatal("renewal returned the old certificate")
	}
	block, _ := pem.Decode([]byte(renewed.CertificatePEM))
	leaf, err := x509.ParseCertificate(block.Bytes)
	if err != nil || !leaf.PublicKey.(ed25519.PublicKey).Equal(key.Public()) || !leaf.NotAfter.After(now.Add(29*24*time.Hour)) {
		t.Fatal("the renewed certificate does not carry the enrolled key or a full lifetime", err)
	}
	if err := s.CheckAgentCertificate(t.Context(), node, leaf); err != nil {
		t.Fatalf("the controller does not accept the renewed certificate: %v", err)
	}
	after, err := s.ListDestinations(t.Context())
	if err != nil || after.Revision != listing.Revision+1 {
		t.Fatal("renewal did not start a new plan revision for peers", err)
	}
	repeated, err := s.RenewAgentCertificate(t.Context(), node, session, request(key), ca)
	if err != nil || repeated.CertificateID != renewed.CertificateID {
		t.Fatal("a repeated request issued another certificate", err)
	}

	// Peers pin both certificates until the old one ends, then only the new one.
	key2 := uuid.New()
	ring, err := secret.NewKeyring(key2, map[uuid.UUID][]byte{key2: bytes.Repeat([]byte{71}, 32)})
	if err != nil {
		t.Fatal(err)
	}
	pins := func() []string {
		t.Helper()
		plan, err := s.DestinationPlan(t.Context(), ca.InstallationID(), node, session, ring)
		if err != nil {
			t.Fatal(err)
		}
		var hashes []string
		for _, peer := range plan.Peers {
			if peer.NodeID == node {
				hashes = append(hashes, peer.CertificateSHA256)
			}
		}
		return hashes
	}
	newHash := sha256.Sum256(leaf.Raw)
	if hashes := pins(); len(hashes) != 2 {
		t.Fatalf("peers pin %d certificates during the overlap, want 2", len(hashes))
	}
	now = now.Add(10 * 24 * time.Hour)
	if hashes := pins(); len(hashes) != 1 || hashes[0] != hex.EncodeToString(newHash[:]) {
		t.Fatalf("peers still pin an expired certificate: %v", hashes)
	}
}

func TestDeviceReclaimsItsIdentityWithABoundToken(t *testing.T) {
	s, ca := open(t), authority(t)
	request := func(signer ed25519.PrivateKey) []byte {
		t.Helper()
		der, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{}, signer)
		if err != nil {
			t.Fatal(err)
		}
		return der
	}
	leaf := func(certificatePEM string) *x509.Certificate {
		t.Helper()
		block, _ := pem.Decode([]byte(certificatePEM))
		certificate, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			t.Fatal(err)
		}
		return certificate
	}
	register := func(name string, node uuid.UUID) [32]byte {
		t.Helper()
		verifier := sha256.Sum256([]byte(name))
		if _, err := s.RegisterEnrollmentToken(t.Context(), TokenRegistration{NodeID: node, ActorID: "admin", RequestID: name, IdempotencyKey: name, TokenVerifier: verifier, TTLSeconds: 3600}); err != nil {
			t.Fatal(err)
		}
		return verifier
	}
	_, oldKey, _ := ed25519.GenerateKey(rand.Reader)
	node := uuid.New()
	first, err := s.EnrollAgent(t.Context(), EnrollmentRequest{NodeID: node, TokenVerifier: register("first", uuid.Nil), RequestID: uuid.New(), CSRDER: request(oldKey)}, ca)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.RecordAgentSession(t.Context(), node, uuid.New(), &agentv1.Hello{NodeId: node.String(), Architecture: "amd64"}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ApproveNode(t.Context(), node, "admin", "approve"); err != nil {
		t.Fatal(err)
	}
	// An unbound token never takes an existing device.
	_, newKey, _ := ed25519.GenerateKey(rand.Reader)
	if _, err := s.EnrollAgent(t.Context(), EnrollmentRequest{NodeID: node, TokenVerifier: register("unbound", uuid.Nil), RequestID: uuid.New(), CSRDER: request(newKey)}, ca); code(err) != EnrollmentConflict {
		t.Fatalf("an unbound token took the device: %v", err)
	}
	listing, err := s.ListDestinations(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	// The device lost its state. An owner-issued device token reclaims its identity with a new key.
	reclaimed, err := s.EnrollAgent(t.Context(), EnrollmentRequest{NodeID: node, TokenVerifier: register("reclaim", node), RequestID: uuid.New(), CSRDER: request(newKey)}, ca)
	if err != nil {
		t.Fatalf("reclaim: %v: %v", err, errors.Unwrap(err))
	}
	var unapproved bool
	if err := s.db.QueryRowContext(t.Context(), `SELECT approved_at IS NULL FROM nodes WHERE id=?`, node).Scan(&unapproved); err != nil || !unapproved {
		t.Fatal("a reclaimed device kept its approval", err)
	}
	if err := s.CheckAgentCertificate(t.Context(), node, leaf(first.CertificatePEM)); code(err) != EnrollmentUnauthorized {
		t.Fatalf("the old key still works: %v", err)
	}
	if err := s.CheckAgentCertificate(t.Context(), node, leaf(reclaimed.CertificatePEM)); err != nil {
		t.Fatalf("the new key does not work: %v", err)
	}
	if after, err := s.ListDestinations(t.Context()); err != nil || after.Revision != listing.Revision+1 {
		t.Fatal("peers did not get a new plan revision for the new key", err)
	}
}
