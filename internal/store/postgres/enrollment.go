package postgres

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/sirrobot01/synod/internal/pki"
)

const EnrollmentUnauthorized ErrorCode = "enrollment_unauthorized"
const EnrollmentConflict ErrorCode = "enrollment_conflict"

// TokenRegistration registers the verifier of a client-generated enrollment token.
type TokenRegistration struct {
	InstallationID uuid.UUID
	NodeID         uuid.UUID
	ActorID        string
	RequestID      string
	IdempotencyKey string
	TokenVerifier  [32]byte
	TTLSeconds     int
}

// EnrollmentToken is the stored identity of one registered enrollment token.
type EnrollmentToken struct {
	ID        uuid.UUID `json:"id"`
	NodeID    uuid.UUID `json:"node_id"`
	ExpiresAt time.Time `json:"expires_at"`
	Replayed  bool      `json:"replayed"`
}

// RegisterEnrollmentToken stores only the verifier of a client-generated token.
func (s *Store) RegisterEnrollmentToken(ctx context.Context, r TokenRegistration) (EnrollmentToken, error) {
	if r.InstallationID == uuid.Nil || r.TokenVerifier == [32]byte{} || r.TTLSeconds < 60 || r.TTLSeconds > 604800 || len(r.ActorID) < 1 || len(r.ActorID) > 256 || len(r.RequestID) < 1 || len(r.RequestID) > 128 || len(r.IdempotencyKey) < 1 || len(r.IdempotencyKey) > 128 {
		return EnrollmentToken{}, &AdmissionError{Code: InvalidRequest}
	}
	for _, value := range []string{r.ActorID, r.RequestID, r.IdempotencyKey} {
		if strings.ContainsFunc(value, func(c rune) bool { return c < '!' || c > '~' }) {
			return EnrollmentToken{}, &AdmissionError{Code: InvalidRequest}
		}
	}
	// Keep this v1 registration hash representation stable across retries and upgrades.
	body, err := json.Marshal(struct {
		NodeID        uuid.UUID
		TokenVerifier [32]byte
		TTLSeconds    int
	}{r.NodeID, r.TokenVerifier, r.TTLSeconds})
	if err != nil {
		return EnrollmentToken{}, &AdmissionError{Code: InvalidRequest}
	}
	hash := sha256.Sum256(body)
	tokenID, err := uuid.NewRandom()
	if err != nil {
		return EnrollmentToken{}, admissionError(err)
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return EnrollmentToken{}, admissionError(err)
	}
	defer tx.Rollback(ctx)
	result := EnrollmentToken{ID: tokenID, NodeID: r.NodeID}
	var nodeIDParam *uuid.UUID
	if r.NodeID != uuid.Nil {
		nodeIDParam = &r.NodeID
	}
	err = tx.QueryRow(ctx, `INSERT INTO enrollment_tokens (id,installation_id,node_id,actor_id,idempotency_key,request_hash,token_verifier,ttl_seconds,created_at,expires_at)
  VALUES ($1,$2,$3,$4,$5,$6,$7,$8::integer,transaction_timestamp(),transaction_timestamp()+$8::integer*interval '1 second')
  ON CONFLICT (installation_id,actor_id,idempotency_key) DO NOTHING RETURNING expires_at`, tokenID, r.InstallationID, nodeIDParam, r.ActorID, r.IdempotencyKey, hash[:], r.TokenVerifier[:], r.TTLSeconds).Scan(&result.ExpiresAt)
	if errors.Is(err, pgx.ErrNoRows) {
		var storedHash []byte
		var storedNodeID *uuid.UUID
		if err := tx.QueryRow(ctx, `SELECT id,node_id,expires_at,request_hash FROM enrollment_tokens WHERE installation_id=$1 AND actor_id=$2 AND idempotency_key=$3`, r.InstallationID, r.ActorID, r.IdempotencyKey).Scan(&result.ID, &storedNodeID, &result.ExpiresAt, &storedHash); err != nil {
			return EnrollmentToken{}, admissionError(err)
		}
		if storedNodeID != nil {
			result.NodeID = *storedNodeID
		}
		if !bytes.Equal(storedHash, hash[:]) {
			return EnrollmentToken{}, &AdmissionError{Code: IdempotencyConflict}
		}
		result.Replayed = true
		return result, nil
	}
	if err != nil {
		return EnrollmentToken{}, admissionError(err)
	}
	if r.NodeID != uuid.Nil {
		var enrolled bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM nodes WHERE installation_id=$1 AND id=$2)`, r.InstallationID, r.NodeID).Scan(&enrolled); err != nil {
			return EnrollmentToken{}, admissionError(err)
		}
		if enrolled {
			return EnrollmentToken{}, &AdmissionError{Code: EnrollmentConflict}
		}
	}
	auditID, err := uuid.NewRandom()
	if err != nil {
		return EnrollmentToken{}, admissionError(err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO audit_events (id,installation_id,enrollment_token_id,actor_id,request_id,action,outcome,created_at)
  VALUES ($1,$2,$3,$4,$5,'enrollment.token.registered','accepted',transaction_timestamp())`, auditID, r.InstallationID, result.ID, r.ActorID, r.RequestID); err != nil {
		return EnrollmentToken{}, admissionError(err)
	}
	if err := s.commitMetadata(ctx, tx); err != nil {
		return EnrollmentToken{}, err
	}
	return result, nil
}

// EnrollmentRequest is one agent request to exchange a token for a certificate.
type EnrollmentRequest struct {
	InstallationID uuid.UUID
	NodeID         uuid.UUID
	TokenVerifier  [32]byte
	RequestID      uuid.UUID
	CSRDER         []byte
}

// EnrollmentResult is the issued certificate and its identity.
type EnrollmentResult struct {
	NodeID             uuid.UUID `json:"node_id"`
	CertificateID      uuid.UUID `json:"certificate_id"`
	CertificatePEM     string    `json:"certificate_pem"`
	CACertificatePEM   string    `json:"ca_certificate_pem"`
	AcceptedAt         time.Time `json:"accepted_at"`
	Replayed           bool      `json:"replayed"`
	RecoveryGeneration uuid.UUID `json:"recovery_generation,omitzero"`
	// AgentEndpoint is the controller agent transport address. The API sets it.
	AgentEndpoint string `json:"agent_endpoint,omitempty"`
}

// EnrollAgent authenticates the token before CSR signing and commits its result once.
// The CA performs local cryptography only. No external effect occurs inside the transaction.
func (s *Store) EnrollAgent(ctx context.Context, r EnrollmentRequest, ca *pki.CA) (EnrollmentResult, error) {
	if r.InstallationID == uuid.Nil || r.NodeID == uuid.Nil || r.RequestID == uuid.Nil || r.TokenVerifier == [32]byte{} || len(r.CSRDER) == 0 || len(r.CSRDER) > 64<<10 {
		return EnrollmentResult{}, &AdmissionError{Code: InvalidRequest}
	}
	hash := sha256.Sum256(r.CSRDER)
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return EnrollmentResult{}, admissionError(err)
	}
	defer tx.Rollback(ctx)
	var tokenID uuid.UUID
	var tokenNodeID *uuid.UUID
	var expiry time.Time
	var revoked, consumed *time.Time
	var priorRequest, certificateID *uuid.UUID
	var storedHash []byte
	err = tx.QueryRow(ctx, `SELECT id,node_id,expires_at,revoked_at,consumed_at,enrollment_request_id,csr_hash,certificate_id
  FROM enrollment_tokens WHERE installation_id=$1 AND token_verifier=$2 FOR UPDATE`, r.InstallationID, r.TokenVerifier[:]).Scan(&tokenID, &tokenNodeID, &expiry, &revoked, &consumed, &priorRequest, &storedHash, &certificateID)
	if errors.Is(err, pgx.ErrNoRows) {
		return EnrollmentResult{}, &AdmissionError{Code: EnrollmentUnauthorized}
	}
	if err != nil {
		return EnrollmentResult{}, admissionError(err)
	}
	if (tokenNodeID != nil && *tokenNodeID != r.NodeID) || revoked != nil {
		return EnrollmentResult{}, &AdmissionError{Code: EnrollmentUnauthorized}
	}
	nodeID := r.NodeID
	// An agent needs the generation for its configuration. It is nil until a controller runner records one.
	var generation uuid.UUID
	if err := tx.QueryRow(ctx, `SELECT recovery_generation FROM installation_generations WHERE installation_id=$1`, r.InstallationID).Scan(&generation); err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return EnrollmentResult{}, admissionError(err)
	}
	// A joining agent cannot write its configuration without the generation, so a join
	// token stays unconsumed until a controller runner records one.
	if tokenNodeID == nil && generation == uuid.Nil {
		return EnrollmentResult{}, &AdmissionError{Code: DependencyUnavailable}
	}
	if consumed != nil {
		if priorRequest == nil || *priorRequest != r.RequestID || !bytes.Equal(storedHash, hash[:]) || certificateID == nil {
			return EnrollmentResult{}, &AdmissionError{Code: EnrollmentConflict}
		}
		result := EnrollmentResult{NodeID: nodeID, CertificateID: *certificateID, AcceptedAt: *consumed, Replayed: true, RecoveryGeneration: generation}
		var certificateBytes, caBytes []byte
		if err := tx.QueryRow(ctx, `SELECT certificate_pem,ca_certificate_pem FROM node_certificates WHERE installation_id=$1 AND node_id=$2 AND id=$3`, r.InstallationID, nodeID, *certificateID).Scan(&certificateBytes, &caBytes); err != nil {
			return EnrollmentResult{}, admissionError(err)
		}
		result.CertificatePEM = string(certificateBytes)
		result.CACertificatePEM = string(caBytes)
		return result, nil
	}
	var now time.Time
	if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&now); err != nil {
		return EnrollmentResult{}, admissionError(err)
	}
	if !expiry.After(now) {
		return EnrollmentResult{}, &AdmissionError{Code: EnrollmentUnauthorized}
	}
	var enrolled bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM nodes WHERE id=$1)`, nodeID).Scan(&enrolled); err != nil {
		return EnrollmentResult{}, admissionError(err)
	}
	if enrolled {
		return EnrollmentResult{}, &AdmissionError{Code: EnrollmentConflict}
	}
	certificatePEM, err := ca.Issue(r.CSRDER, pki.Identity{InstallationID: r.InstallationID, Role: pki.Agent, SubjectID: nodeID}, nil, now, 24*time.Hour)
	if err != nil {
		code := DependencyUnavailable
		if errors.Is(err, pki.ErrCSR) || errors.Is(err, pki.ErrIdentity) {
			code = InvalidRequest
		}
		return EnrollmentResult{}, &AdmissionError{Code: code, Cause: err}
	}
	block, _ := pem.Decode(certificatePEM)
	if block == nil {
		return EnrollmentResult{}, &AdmissionError{Code: InvalidRequest}
	}
	certificate, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return EnrollmentResult{}, &AdmissionError{Code: InvalidRequest, Cause: err}
	}
	publicKey, err := x509.MarshalPKIXPublicKey(certificate.PublicKey)
	if err != nil {
		return EnrollmentResult{}, &AdmissionError{Code: InvalidRequest, Cause: err}
	}
	keyHash := sha256.Sum256(publicKey)
	derHash := sha256.Sum256(certificate.Raw)
	id, err := uuid.NewRandom()
	if err != nil {
		return EnrollmentResult{}, admissionError(err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO nodes (id,installation_id,public_key_hash,enrolled_at) VALUES ($1,$2,$3,$4)`, nodeID, r.InstallationID, keyHash[:], now); err != nil {
		return EnrollmentResult{}, admissionError(err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO node_certificates (id,installation_id,node_id,serial,der_hash,certificate_pem,ca_certificate_pem,not_before,not_after)
  VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`, id, r.InstallationID, nodeID, certificate.SerialNumber.Text(16), derHash[:], certificatePEM, ca.CertificatePEM(), certificate.NotBefore, certificate.NotAfter); err != nil {
		return EnrollmentResult{}, admissionError(err)
	}
	if _, err := tx.Exec(ctx, `UPDATE enrollment_tokens SET node_id=$1,consumed_at=$2,enrollment_request_id=$3,csr_hash=$4,certificate_id=$5 WHERE id=$6`, nodeID, now, r.RequestID, hash[:], id, tokenID); err != nil {
		return EnrollmentResult{}, admissionError(err)
	}
	auditID, err := uuid.NewRandom()
	if err != nil {
		return EnrollmentResult{}, admissionError(err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO audit_events (id,installation_id,node_id,node_certificate_id,actor_id,request_id,action,outcome,created_at)
  VALUES ($1,$2,$3,$4,$5,$6,'node.enrolled','accepted',$7)`, auditID, r.InstallationID, nodeID, id, "enrollment:"+tokenID.String(), r.RequestID.String(), now); err != nil {
		return EnrollmentResult{}, admissionError(err)
	}
	if err := s.commitMetadata(ctx, tx); err != nil {
		return EnrollmentResult{}, err
	}
	return EnrollmentResult{NodeID: nodeID, CertificateID: id, CertificatePEM: string(certificatePEM), CACertificatePEM: string(ca.CertificatePEM()), AcceptedAt: now, RecoveryGeneration: generation}, nil
}

// CheckAgentCertificate authorizes a verified peer against current metadata state.
func (s *Store) CheckAgentCertificate(ctx context.Context, installationID, nodeID uuid.UUID, certificate *x509.Certificate) error {
	if installationID == uuid.Nil || nodeID == uuid.Nil || certificate == nil || certificate.SerialNumber == nil {
		return &AdmissionError{Code: EnrollmentUnauthorized}
	}
	hash := sha256.Sum256(certificate.Raw)
	var allowed bool
	err := s.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM node_certificates c JOIN nodes n ON n.id=c.node_id AND n.installation_id=c.installation_id
  WHERE c.installation_id=$1 AND c.node_id=$2 AND c.serial=$3 AND c.der_hash=$4 AND c.revoked_at IS NULL AND n.revoked_at IS NULL
  AND c.not_before<=clock_timestamp() AND c.not_after>clock_timestamp())`, installationID, nodeID, certificate.SerialNumber.Text(16), hash[:]).Scan(&allowed)
	if err != nil {
		return admissionError(err)
	}
	if !allowed {
		return &AdmissionError{Code: EnrollmentUnauthorized}
	}
	return nil
}

// RevokeAgentCertificate revokes one issued agent certificate and audits the change.
func (s *Store) RevokeAgentCertificate(ctx context.Context, installationID, nodeID, certificateID uuid.UUID, actorID, requestID string) error {
	if installationID == uuid.Nil || nodeID == uuid.Nil || certificateID == uuid.Nil || len(actorID) < 1 || len(actorID) > 256 || len(requestID) < 1 || len(requestID) > 128 {
		return &AdmissionError{Code: InvalidRequest}
	}
	for _, value := range []string{actorID, requestID} {
		if strings.ContainsFunc(value, func(c rune) bool { return c < '!' || c > '~' }) {
			return &AdmissionError{Code: InvalidRequest}
		}
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return admissionError(err)
	}
	defer tx.Rollback(ctx)
	var revoked *time.Time
	if err := tx.QueryRow(ctx, `SELECT revoked_at FROM node_certificates WHERE installation_id=$1 AND node_id=$2 AND id=$3 FOR UPDATE`, installationID, nodeID, certificateID).Scan(&revoked); err != nil {
		return admissionError(err)
	}
	if revoked != nil {
		return nil
	}
	if _, err := tx.Exec(ctx, `UPDATE node_certificates SET revoked_at=clock_timestamp() WHERE id=$1`, certificateID); err != nil {
		return admissionError(err)
	}
	id, err := uuid.NewRandom()
	if err != nil {
		return admissionError(err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO audit_events (id,installation_id,node_id,node_certificate_id,actor_id,request_id,action,outcome,created_at)
  VALUES ($1,$2,$3,$4,$5,$6,'node.certificate.revoked','accepted',clock_timestamp())`, id, installationID, nodeID, certificateID, actorID, requestID); err != nil {
		return admissionError(err)
	}
	return s.commitMetadata(ctx, tx)
}
