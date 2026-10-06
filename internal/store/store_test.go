package store

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/auth"
	"github.com/sirrobot01/synod/internal/pki"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
)

func open(t *testing.T) *Store {
	t.Helper()
	s, err := Open(t.Context(), filepath.Join(t.TempDir(), "synod.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func code(err error) ErrorCode {
	if failure, ok := errors.AsType[*Error](err); ok {
		return failure.Code
	}
	return ""
}

func TestOpenAppliesMigrationsOnce(t *testing.T) {
	path := filepath.Join(t.TempDir(), "synod.db")
	for range 2 {
		s, err := Open(t.Context(), path)
		if err != nil {
			t.Fatal(err)
		}
		var applied int
		if err := s.db.QueryRowContext(t.Context(), `SELECT count(*) FROM schema_migrations`).Scan(&applied); err != nil {
			t.Fatal(err)
		}
		if applied != 5 {
			t.Fatalf("applied %d migrations", applied)
		}
		s.Close()
	}
}

func TestSignInLocksAfterRepeatedFailures(t *testing.T) {
	s := open(t)
	hash, err := auth.HashPassword("correct horse battery")
	if err != nil {
		t.Fatal(err)
	}
	user, err := s.CreateUser(t.Context(), "alice", hash, "admin")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.CreateUser(t.Context(), "alice", hash, "admin"); !errors.Is(err, ErrUsernameTaken) {
		t.Fatalf("duplicate user: %v", err)
	}
	if got, err := s.SignIn(t.Context(), "alice", "correct horse battery"); err != nil || got.ID != user.ID {
		t.Fatalf("sign in: %+v %v", got, err)
	}
	for range auth.MaxFailedAttempts {
		if _, err := s.SignIn(t.Context(), "alice", "wrong"); !errors.Is(err, ErrCredentials) {
			t.Fatalf("wrong password: %v", err)
		}
	}
	if _, err := s.SignIn(t.Context(), "alice", "correct horse battery"); !errors.Is(err, ErrCredentials) {
		t.Fatalf("locked account signed in: %v", err)
	}
	s.now = func() time.Time { return time.Now().Add(auth.LockoutDuration + time.Minute) }
	if _, err := s.SignIn(t.Context(), "alice", "correct horse battery"); err != nil {
		t.Fatalf("lock did not expire: %v", err)
	}
	if err := s.DisableUser(t.Context(), user.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.SignIn(t.Context(), "alice", "correct horse battery"); !errors.Is(err, ErrAccountDisabled) {
		t.Fatalf("disabled account: %v", err)
	}
}

func TestSessionLimitsAndPasswordChange(t *testing.T) {
	s := open(t)
	user, err := s.CreateUser(t.Context(), "bob", "hash", "admin")
	if err != nil {
		t.Fatal(err)
	}
	kept, other := sha256.Sum256([]byte("kept")), sha256.Sum256([]byte("other"))
	session, err := s.CreateSession(t.Context(), user.ID, kept)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.CreateSession(t.Context(), user.ID, other); err != nil {
		t.Fatal(err)
	}
	if got, _, err := s.AuthenticateSession(t.Context(), kept); err != nil || got.ID != user.ID {
		t.Fatalf("authenticate: %v", err)
	}
	if err := s.SetUserPassword(t.Context(), user.ID, "new hash", session.ID); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.AuthenticateSession(t.Context(), other); !errors.Is(err, ErrCredentials) {
		t.Fatalf("other session survived a password change: %v", err)
	}
	s.now = func() time.Time { return time.Now().Add(auth.SessionIdleTimeout + time.Minute) }
	if _, _, err := s.AuthenticateSession(t.Context(), kept); !errors.Is(err, ErrCredentials) {
		t.Fatalf("idle session accepted: %v", err)
	}
}

func TestAPITokens(t *testing.T) {
	s := open(t)
	id, verifier := uuid.New(), sha256.Sum256([]byte("secret"))
	expires := time.Now().Add(time.Hour)
	if _, err := s.CreateAPIToken(t.Context(), id, "alice", "deploy", "admin", verifier, &expires); err != nil {
		t.Fatal(err)
	}
	if _, err := s.AuthenticateAPIToken(t.Context(), id, sha256.Sum256([]byte("guess"))); !errors.Is(err, ErrCredentials) {
		t.Fatalf("wrong verifier: %v", err)
	}
	token, err := s.AuthenticateAPIToken(t.Context(), id, verifier)
	if err != nil || token.Name != "deploy" {
		t.Fatalf("authenticate: %+v %v", token, err)
	}
	tokens, err := s.ListAPITokens(t.Context())
	if err != nil || len(tokens) != 1 || tokens[0].LastUsedAt == nil {
		t.Fatalf("list: %+v %v", tokens, err)
	}
	if err := s.RevokeAPIToken(t.Context(), id); err != nil {
		t.Fatal(err)
	}
	if _, err := s.AuthenticateAPIToken(t.Context(), id, verifier); !errors.Is(err, ErrCredentials) {
		t.Fatalf("revoked token: %v", err)
	}
	if err := s.RevokeAPIToken(t.Context(), uuid.New()); code(err) != NotFound {
		t.Fatalf("unknown token: %v", err)
	}
}

func authority(t *testing.T) *pki.CA {
	t.Helper()
	installation := uuid.New()
	certificate, key, err := pki.CreateCA(installation, time.Now(), 2*AgentCertificateLifetime)
	if err != nil {
		t.Fatal(err)
	}
	ca, err := pki.LoadCA(installation, certificate, key)
	if err != nil {
		t.Fatal(err)
	}
	return ca
}

func csr(t *testing.T) []byte {
	t.Helper()
	_, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	der, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{}, private)
	if err != nil {
		t.Fatal(err)
	}
	return der
}

func join(t *testing.T, s *Store, ca *pki.CA) (uuid.UUID, *x509.Certificate) {
	t.Helper()
	verifier := sha256.Sum256([]byte(uuid.NewString()))
	if _, err := s.RegisterEnrollmentToken(t.Context(), TokenRegistration{ActorID: "admin", RequestID: "r1", IdempotencyKey: uuid.NewString(), TokenVerifier: verifier, TTLSeconds: 3600}); err != nil {
		t.Fatal(err)
	}
	node := uuid.New()
	result, err := s.EnrollAgent(t.Context(), EnrollmentRequest{NodeID: node, TokenVerifier: verifier, RequestID: uuid.New(), CSRDER: csr(t)}, ca)
	if err != nil {
		t.Fatal(err)
	}
	block, _ := pem.Decode([]byte(result.CertificatePEM))
	certificate, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	return node, certificate
}

func TestEnrollmentIsOneUseAndReplayable(t *testing.T) {
	s, ca := open(t), authority(t)
	verifier := sha256.Sum256([]byte("join secret"))
	registration := TokenRegistration{ActorID: "admin", RequestID: "r1", IdempotencyKey: "k1", TokenVerifier: verifier, TTLSeconds: 3600}
	first, err := s.RegisterEnrollmentToken(t.Context(), registration)
	if err != nil {
		t.Fatal(err)
	}
	replay, err := s.RegisterEnrollmentToken(t.Context(), registration)
	if err != nil || !replay.Replayed || replay.ID != first.ID {
		t.Fatalf("registration replay: %+v %v", replay, err)
	}
	registration.TTLSeconds = 60
	if _, err := s.RegisterEnrollmentToken(t.Context(), registration); code(err) != IdempotencyConflict {
		t.Fatalf("changed registration: %v", err)
	}
	request := EnrollmentRequest{NodeID: uuid.New(), TokenVerifier: verifier, RequestID: uuid.New(), CSRDER: csr(t)}
	enrolled, err := s.EnrollAgent(t.Context(), request, ca)
	if err != nil {
		t.Fatal(err)
	}
	again, err := s.EnrollAgent(t.Context(), request, ca)
	if err != nil || !again.Replayed || again.CertificatePEM != enrolled.CertificatePEM {
		t.Fatalf("enrollment replay: %v", err)
	}
	other := EnrollmentRequest{NodeID: uuid.New(), TokenVerifier: verifier, RequestID: uuid.New(), CSRDER: csr(t)}
	if _, err := s.EnrollAgent(t.Context(), other, ca); code(err) != EnrollmentUnauthorized {
		t.Fatalf("used token enrolled a second node: %v", err)
	}
	unknown := EnrollmentRequest{NodeID: uuid.New(), TokenVerifier: sha256.Sum256([]byte("guess")), RequestID: uuid.New(), CSRDER: csr(t)}
	if _, err := s.EnrollAgent(t.Context(), unknown, ca); code(err) != EnrollmentUnauthorized {
		t.Fatalf("unknown token: %v", err)
	}
}

func TestEnrollmentRefusesAnExpiredToken(t *testing.T) {
	s, ca := open(t), authority(t)
	verifier := sha256.Sum256([]byte("short"))
	if _, err := s.RegisterEnrollmentToken(t.Context(), TokenRegistration{ActorID: "admin", RequestID: "r1", IdempotencyKey: "k1", TokenVerifier: verifier, TTLSeconds: 60}); err != nil {
		t.Fatal(err)
	}
	s.now = func() time.Time { return time.Now().Add(2 * time.Minute) }
	if _, err := s.EnrollAgent(t.Context(), EnrollmentRequest{NodeID: uuid.New(), TokenVerifier: verifier, RequestID: uuid.New(), CSRDER: csr(t)}, ca); code(err) != EnrollmentUnauthorized {
		t.Fatalf("expired token: %v", err)
	}
}

func TestCertificateCheckAndRevocation(t *testing.T) {
	s, ca := open(t), authority(t)
	node, certificate := join(t, s, ca)
	if err := s.CheckAgentCertificate(t.Context(), node, certificate); err != nil {
		t.Fatal(err)
	}
	if err := s.CheckAgentCertificate(t.Context(), uuid.New(), certificate); code(err) != EnrollmentUnauthorized {
		t.Fatalf("certificate accepted for another node: %v", err)
	}
	var certificateID uuid.UUID
	if err := s.db.QueryRowContext(t.Context(), `SELECT id FROM node_certificates WHERE node_id=?`, node).Scan(&certificateID); err != nil {
		t.Fatal(err)
	}
	if err := s.RevokeAgentCertificate(t.Context(), node, certificateID, "admin", "r2"); err != nil {
		t.Fatal(err)
	}
	if err := s.CheckAgentCertificate(t.Context(), node, certificate); code(err) != EnrollmentUnauthorized {
		t.Fatalf("revoked certificate accepted: %v", err)
	}
}

func TestPresenceApprovalAndNames(t *testing.T) {
	s, ca := open(t), authority(t)
	node, _ := join(t, s, ca)
	if _, err := s.ApproveNode(t.Context(), node, "admin", "r1"); code(err) != NodeFactsUnavailable {
		t.Fatalf("unseen node approved: %v", err)
	}
	first, second := uuid.New(), uuid.New()
	hello := &agentv1.Hello{NodeId: node.String(), Architecture: "arm64", RuntimeVersion: "29.7.2", Hostname: "odin"}
	if err := s.RecordAgentSession(t.Context(), node, first, hello); err != nil {
		t.Fatal(err)
	}
	if err := s.RecordAgentHeartbeat(t.Context(), node, first, &agentv1.Heartbeat{Sequence: 1}); err != nil {
		t.Fatal(err)
	}
	if err := s.RecordAgentSession(t.Context(), node, second, hello); err != nil {
		t.Fatal(err)
	}
	if err := s.RecordAgentHeartbeat(t.Context(), node, first, &agentv1.Heartbeat{Sequence: 2}); !errors.Is(err, ErrStaleSession) {
		t.Fatalf("replaced session heartbeat: %v", err)
	}
	approved, err := s.ApproveNode(t.Context(), node, "admin", "r2")
	if err != nil || approved.ApprovedAt == nil || approved.Architecture != "arm64" || approved.Name != "odin" {
		t.Fatalf("approve: %+v %v", approved, err)
	}
	if id, err := s.ResolveNodeName(t.Context(), "odin"); err != nil || id != node {
		t.Fatalf("resolve: %v %v", id, err)
	}
	twin, _ := join(t, s, ca)
	if err := s.RecordAgentSession(t.Context(), twin, uuid.New(), &agentv1.Hello{NodeId: twin.String(), Architecture: "amd64", Hostname: "odin"}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ResolveNodeName(t.Context(), "odin"); code(err) != Ambiguous {
		t.Fatalf("ambiguous name: %v", err)
	}
	nodes, err := s.ListNodes(t.Context())
	if err != nil || len(nodes) != 2 {
		t.Fatalf("list: %+v %v", nodes, err)
	}
}
