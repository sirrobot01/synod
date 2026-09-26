package postgres

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sirrobot01/synod/internal/pki"
)

func enrollmentFixture(t *testing.T) (*Store, TokenRegistration, EnrollmentRequest, *pki.CA) {
	t.Helper()
	store := testStore(t)
	installation, node := uuid.New(), uuid.New()
	cert, key, err := pki.CreateCA(installation, time.Now(), 7*24*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	ca, err := pki.LoadCA(installation, cert, key)
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
	rawToken := make([]byte, 32)
	rand.Read(rawToken)
	verifier := sha256.Sum256([]byte(base64.RawURLEncoding.EncodeToString(rawToken)))
	registration := TokenRegistration{InstallationID: installation, NodeID: node, ActorID: "administrator", RequestID: uuid.NewString(), IdempotencyKey: uuid.NewString(), TokenVerifier: verifier, TTLSeconds: 3600}
	request := EnrollmentRequest{InstallationID: installation, NodeID: node, TokenVerifier: verifier, RequestID: uuid.New(), CSRDER: csr}
	return store, registration, request, ca
}

func TestEnrollmentTokenRegistration(t *testing.T) {
	s, r, _, _ := enrollmentFixture(t)
	first, err := s.RegisterEnrollmentToken(t.Context(), r)
	if err != nil {
		t.Fatalf("%v: %v", err, errors.Unwrap(err))
	}
	if first.ID == uuid.Nil || first.Replayed || first.NodeID != r.NodeID {
		t.Fatalf("invalid token metadata: %+v", first)
	}
	r.RequestID = uuid.NewString()
	replay, err := s.RegisterEnrollmentToken(t.Context(), r)
	if err != nil || !replay.Replayed || replay.ID != first.ID || !replay.ExpiresAt.Equal(first.ExpiresAt) {
		t.Fatalf("registration replay: %+v %v", replay, err)
	}
	changed := r
	changed.TTLSeconds = 60
	_, err = s.RegisterEnrollmentToken(t.Context(), changed)
	requireCode(t, err, IdempotencyConflict)
	changed = r
	changed.IdempotencyKey = uuid.NewString()
	_, err = s.RegisterEnrollmentToken(t.Context(), changed)
	requireCode(t, err, EnrollmentConflict)
	for _, table := range []string{"enrollment_tokens", "audit_events"} {
		var count int
		if err := s.pool.QueryRow(t.Context(), "SELECT count(*) FROM "+pgx.Identifier{table}.Sanitize()).Scan(&count); err != nil || count != 1 {
			t.Fatalf("%s count=%d error=%v", table, count, err)
		}
	}
}

func TestConcurrentTokenRegistration(t *testing.T) {
	s, r, _, _ := enrollmentFixture(t)
	results := make(chan EnrollmentToken, 16)
	var group sync.WaitGroup
	for range 16 {
		group.Go(func() {
			value, err := s.RegisterEnrollmentToken(t.Context(), r)
			if err != nil {
				t.Error(err)
				return
			}
			results <- value
		})
	}
	group.Wait()
	close(results)
	var winner uuid.UUID
	count, originals := 0, 0
	for result := range results {
		if winner == uuid.Nil {
			winner = result.ID
		}
		if result.ID != winner {
			t.Fatal("registration returned different IDs")
		}
		count++
		if !result.Replayed {
			originals++
		}
	}
	if count != 16 || originals != 1 {
		t.Fatalf("count=%d original=%d", count, originals)
	}
}

func TestConcurrentEnrollmentAndReplay(t *testing.T) {
	s, registration, request, ca := enrollmentFixture(t)
	if _, err := s.RegisterEnrollmentToken(t.Context(), registration); err != nil {
		t.Fatal(err)
	}
	results := make(chan EnrollmentResult, 16)
	var group sync.WaitGroup
	for range 16 {
		group.Go(func() {
			value, err := s.EnrollAgent(t.Context(), request, ca)
			if err != nil {
				t.Error(err)
				return
			}
			results <- value
		})
	}
	group.Wait()
	close(results)
	var winner EnrollmentResult
	count, originals := 0, 0
	for result := range results {
		if winner.CertificateID == uuid.Nil {
			winner = result
		}
		if result.CertificateID != winner.CertificateID || result.CertificatePEM != winner.CertificatePEM || result.NodeID != request.NodeID || !result.AcceptedAt.Equal(winner.AcceptedAt) {
			t.Fatal("enrollment returned different results")
		}
		count++
		if !result.Replayed {
			originals++
		}
	}
	if count != 16 || originals != 1 {
		t.Fatalf("count=%d original=%d", count, originals)
	}
	for _, table := range []string{"nodes", "node_certificates"} {
		var count int
		if err := s.pool.QueryRow(t.Context(), "SELECT count(*) FROM "+pgx.Identifier{table}.Sanitize()).Scan(&count); err != nil || count != 1 {
			t.Fatalf("%s count=%d error=%v", table, count, err)
		}
	}
	if winner.CACertificatePEM != string(ca.CertificatePEM()) {
		t.Fatal("issuing CA was not retained")
	}
	rotatedCert, rotatedKey, err := pki.CreateCA(request.InstallationID, time.Now(), 7*24*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	rotatedCA, err := pki.LoadCA(request.InstallationID, rotatedCert, rotatedKey)
	if err != nil {
		t.Fatal(err)
	}
	rotatedReplay, err := s.EnrollAgent(t.Context(), request, rotatedCA)
	if err != nil || rotatedReplay.CertificatePEM != winner.CertificatePEM || rotatedReplay.CACertificatePEM != winner.CACertificatePEM || rotatedReplay.CACertificatePEM == string(rotatedCert) {
		t.Fatalf("CA rotation changed enrollment result: %v", err)
	}
	changed := request
	changed.RequestID = uuid.New()
	_, err = s.EnrollAgent(t.Context(), changed, ca)
	requireCode(t, err, EnrollmentConflict)
	changed = request
	changed.CSRDER = append(bytes.Clone(request.CSRDER), 0)
	_, err = s.EnrollAgent(t.Context(), changed, ca)
	requireCode(t, err, EnrollmentConflict)
	changed = request
	changed.NodeID = uuid.New()
	_, err = s.EnrollAgent(t.Context(), changed, ca)
	requireCode(t, err, EnrollmentUnauthorized)
	if _, err := s.pool.Exec(t.Context(), `UPDATE enrollment_tokens SET created_at=clock_timestamp()-interval '2 hours',expires_at=clock_timestamp()-interval '1 hour'`); err != nil {
		t.Fatal(err)
	}
	replay, err := s.EnrollAgent(t.Context(), request, nil)
	if err != nil || !replay.Replayed || replay.CertificatePEM != winner.CertificatePEM {
		t.Fatalf("expired consumed token issued another certificate: %v", err)
	}
}

func TestEnrollmentAuthorizationAndRevocation(t *testing.T) {
	s, registration, request, ca := enrollmentFixture(t)
	_, err := s.EnrollAgent(t.Context(), request, nil)
	requireCode(t, err, EnrollmentUnauthorized)
	if _, err := s.RegisterEnrollmentToken(t.Context(), registration); err != nil {
		t.Fatal(err)
	}
	changed := request
	changed.InstallationID = uuid.New()
	_, err = s.EnrollAgent(t.Context(), changed, ca)
	requireCode(t, err, EnrollmentUnauthorized)
	changed = request
	changed.NodeID = uuid.New()
	_, err = s.EnrollAgent(t.Context(), changed, ca)
	requireCode(t, err, EnrollmentUnauthorized)
	changed = request
	changed.CSRDER = []byte("private-invalid-csr")
	_, err = s.EnrollAgent(t.Context(), changed, ca)
	requireCode(t, err, InvalidRequest)
	if strings.Contains(err.Error(), "private-invalid-csr") {
		t.Fatal("CSR input leaked")
	}
	_, err = s.EnrollAgent(t.Context(), request, nil)
	requireCode(t, err, DependencyUnavailable)
	result, err := s.EnrollAgent(t.Context(), request, ca)
	if err != nil {
		t.Fatal(err)
	}
	block, _ := pem.Decode([]byte(result.CertificatePEM))
	certificate, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.CheckAgentCertificate(t.Context(), request.InstallationID, request.NodeID, certificate); err != nil {
		t.Fatal(err)
	}
	requireCode(t, s.CheckAgentCertificate(t.Context(), uuid.New(), request.NodeID, certificate), EnrollmentUnauthorized)
	requireCode(t, s.CheckAgentCertificate(t.Context(), request.InstallationID, uuid.New(), certificate), EnrollmentUnauthorized)
	forged := *certificate
	forged.Raw = append(bytes.Clone(certificate.Raw), 0)
	requireCode(t, s.CheckAgentCertificate(t.Context(), request.InstallationID, request.NodeID, &forged), EnrollmentUnauthorized)
	requireCode(t, s.RevokeAgentCertificate(t.Context(), uuid.New(), request.NodeID, result.CertificateID, "administrator", uuid.NewString()), NotFound)
	if err := s.RevokeAgentCertificate(t.Context(), request.InstallationID, request.NodeID, result.CertificateID, "administrator", uuid.NewString()); err != nil {
		t.Fatal(err)
	}
	if err := s.RevokeAgentCertificate(t.Context(), request.InstallationID, request.NodeID, result.CertificateID, "administrator", uuid.NewString()); err != nil {
		t.Fatal(err)
	}
	requireCode(t, s.CheckAgentCertificate(t.Context(), request.InstallationID, request.NodeID, certificate), EnrollmentUnauthorized)
	var count int
	if err := s.pool.QueryRow(t.Context(), `SELECT count(*) FROM audit_events WHERE action='node.certificate.revoked'`).Scan(&count); err != nil || count != 1 {
		t.Fatalf("revocation audit count=%d error=%v", count, err)
	}
	again := registration
	again.IdempotencyKey = uuid.NewString()
	again.TokenVerifier = sha256.Sum256([]byte("another verifier"))
	_, err = s.RegisterEnrollmentToken(t.Context(), again)
	requireCode(t, err, EnrollmentConflict)
}

func TestExpiredEnrollmentToken(t *testing.T) {
	s, registration, request, ca := enrollmentFixture(t)
	if _, err := s.RegisterEnrollmentToken(t.Context(), registration); err != nil {
		t.Fatal(err)
	}
	if _, err := s.pool.Exec(t.Context(), `UPDATE enrollment_tokens SET created_at=clock_timestamp()-interval '2 hours',expires_at=clock_timestamp()-interval '1 hour'`); err != nil {
		t.Fatal(err)
	}
	_, err := s.EnrollAgent(t.Context(), request, ca)
	requireCode(t, err, EnrollmentUnauthorized)
	var nodes int
	if err := s.pool.QueryRow(t.Context(), `SELECT count(*) FROM nodes`).Scan(&nodes); err != nil || nodes != 0 {
		t.Fatalf("expired token created node: %d %v", nodes, err)
	}
}

func TestEnrollmentRollbackBoundaries(t *testing.T) {
	for _, table := range []string{"nodes", "node_certificates", "enrollment_tokens", "audit_events"} {
		t.Run(table, func(t *testing.T) {
			s, registration, request, ca := enrollmentFixture(t)
			if _, err := s.RegisterEnrollmentToken(t.Context(), registration); err != nil {
				t.Fatal(err)
			}
			if _, err := s.pool.Exec(t.Context(), `CREATE FUNCTION fail_enrollment() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected write failure'; END $$`); err != nil {
				t.Fatal(err)
			}
			if _, err := s.pool.Exec(t.Context(), "CREATE TRIGGER fail_enrollment AFTER INSERT OR UPDATE ON "+pgx.Identifier{table}.Sanitize()+" FOR EACH ROW EXECUTE FUNCTION fail_enrollment()"); err != nil {
				t.Fatal(err)
			}
			if result, err := s.EnrollAgent(t.Context(), request, ca); err == nil || result.CertificateID != uuid.Nil {
				t.Fatal("returned successful partial enrollment")
			}
			for _, query := range []string{`SELECT count(*) FROM nodes`, `SELECT count(*) FROM node_certificates`, `SELECT count(*) FROM enrollment_tokens WHERE consumed_at IS NOT NULL`, `SELECT count(*) FROM audit_events WHERE action='node.enrolled'`} {
				var count int
				if err := s.pool.QueryRow(t.Context(), query).Scan(&count); err != nil || count != 0 {
					t.Fatalf("partial enrollment rows=%d error=%v", count, err)
				}
			}
		})
	}
}

func TestEnrollmentUnknownCommitRecovery(t *testing.T) {
	s, registration, request, ca := enrollmentFixture(t)
	s.commitTx = func(ctx context.Context, tx pgx.Tx) error {
		if err := tx.Commit(ctx); err != nil {
			return err
		}
		return io.ErrUnexpectedEOF
	}
	first, err := s.RegisterEnrollmentToken(t.Context(), registration)
	requireCode(t, err, OutcomeUnknown)
	if first.ID != uuid.Nil {
		t.Fatal("returned unconfirmed token metadata")
	}
	s.commitTx = nil
	token, err := s.RegisterEnrollmentToken(t.Context(), registration)
	if err != nil || !token.Replayed {
		t.Fatalf("token registration recovery: %v", err)
	}
	s.commitTx = func(ctx context.Context, tx pgx.Tx) error {
		if err := tx.Commit(ctx); err != nil {
			return err
		}
		return io.ErrUnexpectedEOF
	}
	result, err := s.EnrollAgent(t.Context(), request, ca)
	requireCode(t, err, OutcomeUnknown)
	if result.CertificateID != uuid.Nil || result.CertificatePEM != "" {
		t.Fatal("returned unconfirmed certificate")
	}
	s.commitTx = nil
	config := s.pool.Config()
	s.pool.Close()
	_, err = s.EnrollAgent(t.Context(), request, nil)
	requireCode(t, err, DependencyUnavailable)
	pool, err := pgxpool.NewWithConfig(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	s = New(pool)
	recovered, err := s.EnrollAgent(t.Context(), request, nil)
	if err != nil || !recovered.Replayed || recovered.CertificateID == uuid.Nil {
		t.Fatalf("enrollment recovery: %v", err)
	}
	var count int
	if err := s.pool.QueryRow(t.Context(), `SELECT count(*) FROM node_certificates`).Scan(&count); err != nil || count != 1 {
		t.Fatalf("certificate rows=%d error=%v", count, err)
	}
}

func TestEnrollmentExpiryAfterLockWait(t *testing.T) {
	s, registration, request, ca := enrollmentFixture(t)
	if _, err := s.RegisterEnrollmentToken(t.Context(), registration); err != nil {
		t.Fatal(err)
	}
	locker, err := s.pool.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer locker.Rollback(t.Context())
	if _, err := locker.Exec(t.Context(), `UPDATE enrollment_tokens SET expires_at=clock_timestamp()+interval '200 milliseconds'`); err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() { _, err := s.EnrollAgent(t.Context(), request, ca); result <- err }()
	waitForKeyWait(t, s)
	if _, err := locker.Exec(t.Context(), `SELECT pg_sleep(0.25)`); err != nil {
		t.Fatal(err)
	}
	if err := locker.Commit(t.Context()); err != nil {
		t.Fatal(err)
	}
	requireCode(t, <-result, EnrollmentUnauthorized)
}

func TestTokenRegistrationAuditRollback(t *testing.T) {
	s, registration, _, _ := enrollmentFixture(t)
	if _, err := s.pool.Exec(t.Context(), `CREATE FUNCTION fail_registration() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected registration failure'; END $$;
 CREATE TRIGGER fail_registration AFTER INSERT ON audit_events FOR EACH ROW EXECUTE FUNCTION fail_registration()`); err != nil {
		t.Fatal(err)
	}
	if result, err := s.RegisterEnrollmentToken(t.Context(), registration); err == nil || result.ID != uuid.Nil {
		t.Fatal("registration returned success after audit failure")
	}
	var count int
	if err := s.pool.QueryRow(t.Context(), `SELECT count(*) FROM enrollment_tokens`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("registration retained token: %d %v", count, err)
	}
	if _, err := s.pool.Exec(t.Context(), `DROP TRIGGER fail_registration ON audit_events`); err != nil {
		t.Fatal(err)
	}
	if _, err := s.RegisterEnrollmentToken(t.Context(), registration); err != nil {
		t.Fatal(err)
	}
}

// A join token binds no node until an agent enrolls with the node ID it chose.
func TestJoinTokenBindsTheFirstEnrolledNode(t *testing.T) {
	s, r, request, ca := enrollmentFixture(t)
	r.NodeID = uuid.Nil
	r.TTLSeconds = 7 * 24 * 3600
	token, err := s.RegisterEnrollmentToken(t.Context(), r)
	if err != nil || token.NodeID != uuid.Nil {
		t.Fatalf("join token registration: %+v %v", token, err)
	}
	_, err = s.EnrollAgent(t.Context(), request, ca)
	requireCode(t, err, DependencyUnavailable)
	generation := uuid.New()
	if err := s.InitializeGeneration(t.Context(), r.InstallationID, generation); err != nil {
		t.Fatal(err)
	}
	enrolled, err := s.EnrollAgent(t.Context(), request, ca)
	if err != nil || enrolled.NodeID != request.NodeID || enrolled.RecoveryGeneration != generation {
		t.Fatalf("enrollment: %+v %v", enrolled, err)
	}
	var bound uuid.UUID
	if err := s.pool.QueryRow(t.Context(), `SELECT node_id FROM enrollment_tokens WHERE id=$1`, token.ID).Scan(&bound); err != nil || bound != request.NodeID {
		t.Fatalf("token node=%s err=%v, want %s", bound, err, request.NodeID)
	}
	replay, err := s.EnrollAgent(t.Context(), request, ca)
	if err != nil || !replay.Replayed || replay.CertificateID != enrolled.CertificateID || replay.RecoveryGeneration != generation {
		t.Fatalf("replay: %+v %v", replay, err)
	}
	// The consumed token now belongs to the first node.
	other := request
	other.NodeID, other.RequestID = uuid.New(), uuid.New()
	_, err = s.EnrollAgent(t.Context(), other, ca)
	requireCode(t, err, EnrollmentUnauthorized)

	// A second join token cannot take over an enrolled node.
	second := r
	second.IdempotencyKey, second.RequestID = uuid.NewString(), uuid.NewString()
	raw := make([]byte, 32)
	rand.Read(raw)
	second.TokenVerifier = sha256.Sum256([]byte(base64.RawURLEncoding.EncodeToString(raw)))
	if _, err := s.RegisterEnrollmentToken(t.Context(), second); err != nil {
		t.Fatal(err)
	}
	takeover := request
	takeover.TokenVerifier, takeover.RequestID = second.TokenVerifier, uuid.New()
	_, err = s.EnrollAgent(t.Context(), takeover, ca)
	requireCode(t, err, EnrollmentConflict)
}
