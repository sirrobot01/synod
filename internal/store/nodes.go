package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/x509"
	"database/sql"
	"encoding/json"
	"encoding/pem"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/destination"
	"github.com/sirrobot01/synod/internal/pki"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
)

// AgentCertificateLifetime is how long an issued agent certificate stays valid. Agents renew before it ends.
const AgentCertificateLifetime = 30 * 24 * time.Hour

// ErrStaleSession reports a message from a session that a newer one replaced.
var ErrStaleSession = errors.New("stale agent session")

// TokenRegistration registers the verifier of a client-generated enrollment token.
type TokenRegistration struct {
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
// A token with no node ID is a join token: the joining agent chooses its own ID.
func (s *Store) RegisterEnrollmentToken(ctx context.Context, r TokenRegistration) (EnrollmentToken, error) {
	if r.TokenVerifier == [32]byte{} || r.TTLSeconds < 60 || r.TTLSeconds > 604800 || !printableWithin(r.ActorID, 256) || !printableWithin(r.RequestID, 128) || !printableWithin(r.IdempotencyKey, 128) {
		return EnrollmentToken{}, &Error{Code: InvalidRequest}
	}
	// Keep this registration hash stable across retries.
	body, err := json.Marshal(struct {
		NodeID        uuid.UUID
		TokenVerifier [32]byte
		TTLSeconds    int
	}{r.NodeID, r.TokenVerifier, r.TTLSeconds})
	if err != nil {
		return EnrollmentToken{}, &Error{Code: InvalidRequest, Cause: err}
	}
	hash := sha256.Sum256(body)
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return EnrollmentToken{}, storeError(err)
	}
	defer tx.Rollback()
	result := EnrollmentToken{NodeID: r.NodeID}
	var storedNode sql.Null[uuid.UUID]
	var storedHash []byte
	err = tx.QueryRowContext(ctx, `SELECT id,node_id,expires_at,request_hash FROM enrollment_tokens WHERE actor_id=? AND idempotency_key=?`, r.ActorID, r.IdempotencyKey).
		Scan(&result.ID, &storedNode, millis{&result.ExpiresAt}, &storedHash)
	if err == nil {
		if !bytes.Equal(storedHash, hash[:]) {
			return EnrollmentToken{}, &Error{Code: IdempotencyConflict}
		}
		result.NodeID, result.Replayed = storedNode.V, true
		return result, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return EnrollmentToken{}, storeError(err)
	}
	if r.NodeID != uuid.Nil {
		var enrolled bool
		if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM nodes WHERE id=? AND (public_key_hash IS NOT NULL OR revoked_at IS NOT NULL))`, r.NodeID).Scan(&enrolled); err != nil {
			return EnrollmentToken{}, storeError(err)
		}
		if enrolled {
			return EnrollmentToken{}, &Error{Code: EnrollmentConflict}
		}
	}
	now := s.now()
	result.ID = uuid.New()
	result.ExpiresAt = now.Add(time.Duration(r.TTLSeconds) * time.Second).Truncate(time.Millisecond).UTC()
	node := sql.Null[uuid.UUID]{V: r.NodeID, Valid: r.NodeID != uuid.Nil}
	if _, err := tx.ExecContext(ctx, `INSERT INTO enrollment_tokens (id,node_id,actor_id,idempotency_key,request_hash,token_verifier,created_at,expires_at) VALUES (?,?,?,?,?,?,?,?)`,
		result.ID, node, r.ActorID, r.IdempotencyKey, hash[:], r.TokenVerifier[:], now.UnixMilli(), result.ExpiresAt.UnixMilli()); err != nil {
		if isUnique(err) {
			return EnrollmentToken{}, &Error{Code: EnrollmentConflict, Cause: err}
		}
		return EnrollmentToken{}, storeError(err)
	}
	if err := audit(ctx, tx, r.ActorID, r.RequestID, "enrollment.token.registered", result.ID, now); err != nil {
		return EnrollmentToken{}, err
	}
	if err := storeCommit(tx); err != nil {
		return EnrollmentToken{}, err
	}
	return result, nil
}

// EnrollmentRequest is one agent request to exchange a token for a certificate.
type EnrollmentRequest struct {
	NodeID        uuid.UUID
	TokenVerifier [32]byte
	RequestID     uuid.UUID
	CSRDER        []byte
}

// EnrollmentResult is the issued certificate and its identity.
type EnrollmentResult struct {
	NodeID           uuid.UUID `json:"node_id"`
	CertificateID    uuid.UUID `json:"certificate_id"`
	CertificatePEM   string    `json:"certificate_pem"`
	CACertificatePEM string    `json:"ca_certificate_pem"`
	AcceptedAt       time.Time `json:"accepted_at"`
	Replayed         bool      `json:"replayed"`
	// AgentEndpoint is the controller agent transport address. The API sets it.
	AgentEndpoint string `json:"agent_endpoint,omitempty"`
}

// EnrollAgent authenticates the token, signs the CSR, and commits the result once.
// A retry with the same request and CSR returns the same certificate.
func (s *Store) EnrollAgent(ctx context.Context, r EnrollmentRequest, ca *pki.CA) (EnrollmentResult, error) {
	if r.NodeID == uuid.Nil || r.RequestID == uuid.Nil || r.TokenVerifier == [32]byte{} || len(r.CSRDER) == 0 || len(r.CSRDER) > 64<<10 {
		return EnrollmentResult{}, &Error{Code: InvalidRequest}
	}
	hash := sha256.Sum256(r.CSRDER)
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return EnrollmentResult{}, storeError(err)
	}
	defer tx.Rollback()
	var tokenID uuid.UUID
	var tokenNode, priorRequest, certificateID sql.Null[uuid.UUID]
	var expiry time.Time
	var revoked, consumed *time.Time
	var storedHash []byte
	err = tx.QueryRowContext(ctx, `SELECT id,node_id,expires_at,revoked_at,consumed_at,enrollment_request_id,csr_hash,certificate_id
 FROM enrollment_tokens WHERE token_verifier=?`, r.TokenVerifier[:]).Scan(&tokenID, &tokenNode, millis{&expiry}, optionalMillis{&revoked}, optionalMillis{&consumed}, &priorRequest, &storedHash, &certificateID)
	if errors.Is(err, sql.ErrNoRows) {
		return EnrollmentResult{}, &Error{Code: EnrollmentUnauthorized}
	}
	if err != nil {
		return EnrollmentResult{}, storeError(err)
	}
	if (tokenNode.Valid && tokenNode.V != r.NodeID) || revoked != nil {
		return EnrollmentResult{}, &Error{Code: EnrollmentUnauthorized}
	}
	if consumed != nil {
		if priorRequest.V != r.RequestID || !bytes.Equal(storedHash, hash[:]) || !certificateID.Valid {
			return EnrollmentResult{}, &Error{Code: EnrollmentConflict}
		}
		result := EnrollmentResult{NodeID: r.NodeID, CertificateID: certificateID.V, AcceptedAt: *consumed, Replayed: true}
		if err := tx.QueryRowContext(ctx, `SELECT certificate_pem,ca_certificate_pem FROM node_certificates WHERE node_id=? AND id=?`, r.NodeID, certificateID.V).Scan(&result.CertificatePEM, &result.CACertificatePEM); err != nil {
			return EnrollmentResult{}, storeError(err)
		}
		return result, nil
	}
	now := s.now().Truncate(time.Millisecond).UTC()
	if !expiry.After(now) {
		return EnrollmentResult{}, &Error{Code: EnrollmentUnauthorized}
	}
	var exists, enrolled bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM nodes WHERE id=?),EXISTS(SELECT 1 FROM nodes WHERE id=? AND (public_key_hash IS NOT NULL OR revoked_at IS NOT NULL))`, r.NodeID, r.NodeID).Scan(&exists, &enrolled); err != nil {
		return EnrollmentResult{}, storeError(err)
	}
	if enrolled || (exists && !tokenNode.Valid) {
		return EnrollmentResult{}, &Error{Code: EnrollmentConflict}
	}
	// A controller with no CA issues nothing. A replay above still answers without one.
	if ca == nil {
		return EnrollmentResult{}, &Error{Code: Unavailable}
	}
	certificatePEM, err := ca.Issue(r.CSRDER, pki.Identity{InstallationID: ca.InstallationID(), Role: pki.Agent, SubjectID: r.NodeID}, nil, now, AgentCertificateLifetime)
	if err != nil {
		code := Unavailable
		if errors.Is(err, pki.ErrCSR) || errors.Is(err, pki.ErrIdentity) {
			code = InvalidRequest
		}
		return EnrollmentResult{}, &Error{Code: code, Cause: err}
	}
	block, _ := pem.Decode(certificatePEM)
	if block == nil {
		return EnrollmentResult{}, &Error{Code: InvalidRequest}
	}
	certificate, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return EnrollmentResult{}, &Error{Code: InvalidRequest, Cause: err}
	}
	publicKey, err := x509.MarshalPKIXPublicKey(certificate.PublicKey)
	if err != nil {
		return EnrollmentResult{}, &Error{Code: InvalidRequest, Cause: err}
	}
	keyHash := sha256.Sum256(publicKey)
	derHash := sha256.Sum256(certificate.Raw)
	id := uuid.New()
	if _, err := tx.ExecContext(ctx, `INSERT INTO nodes (id,public_key_hash,enrolled_at) VALUES (?,?,?) ON CONFLICT(id) DO UPDATE SET public_key_hash=excluded.public_key_hash,enrolled_at=excluded.enrolled_at`, r.NodeID, keyHash[:], now.UnixMilli()); err != nil {
		return EnrollmentResult{}, storeError(err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO node_certificates (id,node_id,serial,der_hash,certificate_pem,ca_certificate_pem,not_before,not_after) VALUES (?,?,?,?,?,?,?,?)`,
		id, r.NodeID, certificate.SerialNumber.Text(16), derHash[:], string(certificatePEM), string(ca.CertificatePEM()), certificate.NotBefore.UnixMilli(), certificate.NotAfter.UnixMilli()); err != nil {
		return EnrollmentResult{}, storeError(err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE enrollment_tokens SET node_id=?,consumed_at=?,enrollment_request_id=?,csr_hash=?,certificate_id=? WHERE id=?`, r.NodeID, now.UnixMilli(), r.RequestID, hash[:], id, tokenID); err != nil {
		return EnrollmentResult{}, storeError(err)
	}
	if err := audit(ctx, tx, "enrollment:"+tokenID.String(), r.RequestID.String(), "node.enrolled", r.NodeID, now); err != nil {
		return EnrollmentResult{}, err
	}
	if err := storeCommit(tx); err != nil {
		return EnrollmentResult{}, err
	}
	return EnrollmentResult{NodeID: r.NodeID, CertificateID: id, CertificatePEM: string(certificatePEM), CACertificatePEM: string(ca.CertificatePEM()), AcceptedAt: now}, nil
}

// CheckAgentCertificate authorizes a verified peer certificate against current state.
func (s *Store) CheckAgentCertificate(ctx context.Context, nodeID uuid.UUID, certificate *x509.Certificate) error {
	if nodeID == uuid.Nil || certificate == nil || certificate.SerialNumber == nil {
		return &Error{Code: EnrollmentUnauthorized}
	}
	hash := sha256.Sum256(certificate.Raw)
	now := s.now().UnixMilli()
	var allowed bool
	err := s.db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM node_certificates c JOIN nodes n ON n.id=c.node_id
 WHERE c.node_id=? AND c.serial=? AND c.der_hash=? AND c.revoked_at IS NULL AND n.revoked_at IS NULL AND c.not_before<=? AND c.not_after>?)`,
		nodeID, certificate.SerialNumber.Text(16), hash[:], now, now).Scan(&allowed)
	if err != nil {
		return storeError(err)
	}
	if !allowed {
		return &Error{Code: EnrollmentUnauthorized}
	}
	return nil
}

// RevokeAgentCertificate revokes one issued agent certificate and audits the change.
func (s *Store) RevokeAgentCertificate(ctx context.Context, nodeID, certificateID uuid.UUID, actorID, requestID string) error {
	if nodeID == uuid.Nil || certificateID == uuid.Nil || !printableWithin(actorID, 256) || !printableWithin(requestID, 128) {
		return &Error{Code: InvalidRequest}
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return storeError(err)
	}
	defer tx.Rollback()
	var revoked *time.Time
	if err := tx.QueryRowContext(ctx, `SELECT revoked_at FROM node_certificates WHERE node_id=? AND id=?`, nodeID, certificateID).Scan(optionalMillis{&revoked}); err != nil {
		return storeError(err)
	}
	if revoked != nil {
		return nil
	}
	now := s.now()
	if _, err := tx.ExecContext(ctx, `UPDATE node_certificates SET revoked_at=? WHERE id=?`, now.UnixMilli(), certificateID); err != nil {
		return storeError(err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE destination_revision SET revision=revision+1`); err != nil {
		return storeError(err)
	}
	if err := audit(ctx, tx, actorID, requestID, "node.certificate.revoked", certificateID, now); err != nil {
		return err
	}
	return storeCommit(tx)
}

// RecordAgentSession records a new live session and the facts its Hello reports.
// The controller transport assigns the session identity, never the agent.
func (s *Store) RecordAgentSession(ctx context.Context, nodeID, sessionID uuid.UUID, hello *agentv1.Hello) error {
	if nodeID == uuid.Nil || sessionID == uuid.Nil || hello == nil || hello.NodeId != nodeID.String() || (hello.DestinationAddress != "" && !destination.ValidAddress(hello.DestinationAddress, false)) {
		return &Error{Code: InvalidRequest}
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return storeError(err)
	}
	defer tx.Rollback()
	var previous, previousArchitecture, previousName string
	var approved bool
	if err := tx.QueryRowContext(ctx, `SELECT destination_address,architecture,name,approved_at IS NOT NULL FROM nodes WHERE id=? AND revoked_at IS NULL`, nodeID).Scan(&previous, &previousArchitecture, &previousName, &approved); err != nil {
		return storeError(err)
	}
	now := s.now().UnixMilli()
	if _, err := tx.ExecContext(ctx, `UPDATE nodes SET session_id=?,connected_at=?,last_seen_at=?,heartbeat_sequence=0,
 architecture=?,runtime_version=?,agent_version=?,destination_address=?,name=CASE WHEN name='' THEN ? ELSE name END
 WHERE id=?`, sessionID, now, now, hello.Architecture, hello.RuntimeVersion, hello.AgentVersion, hello.DestinationAddress, hello.Hostname, nodeID); err != nil {
		return storeError(err)
	}
	if approved && (previous != hello.DestinationAddress || previousArchitecture != hello.Architecture || (previousName == "" && hello.Hostname != "")) {
		if _, err := tx.ExecContext(ctx, `UPDATE destination_revision SET revision=revision+1`); err != nil {
			return storeError(err)
		}
	}
	return storeCommit(tx)
}

// RecordAgentHeartbeat refreshes presence for the current session only.
// A repeated or older sequence from that session is ignored.
func (s *Store) RecordAgentHeartbeat(ctx context.Context, nodeID, sessionID uuid.UUID, heartbeat *agentv1.Heartbeat) error {
	if nodeID == uuid.Nil || sessionID == uuid.Nil || heartbeat == nil || heartbeat.Sequence == 0 || heartbeat.Sequence > 1<<62 {
		return &Error{Code: InvalidRequest}
	}
	result, err := s.db.ExecContext(ctx, `UPDATE nodes SET heartbeat_sequence=?,last_seen_at=? WHERE id=? AND session_id=? AND heartbeat_sequence<?`,
		int64(heartbeat.Sequence), s.now().UnixMilli(), nodeID, sessionID, int64(heartbeat.Sequence))
	if err != nil {
		return storeError(err)
	}
	if changed, err := result.RowsAffected(); err != nil {
		return storeError(err)
	} else if changed == 0 {
		var current sql.Null[uuid.UUID]
		if err := s.db.QueryRowContext(ctx, `SELECT session_id FROM nodes WHERE id=?`, nodeID).Scan(&current); err != nil {
			return storeError(err)
		}
		if current.V != sessionID {
			return ErrStaleSession
		}
	}
	return nil
}

// Node is one enrolled device and its last known presence.
type Node struct {
	ID                 uuid.UUID  `json:"id"`
	Name               string     `json:"name"`
	Architecture       string     `json:"architecture"`
	RuntimeVersion     string     `json:"runtime_version"`
	AgentVersion       string     `json:"agent_version"`
	DestinationAddress string     `json:"destination_address,omitempty"`
	EnrolledAt         time.Time  `json:"enrolled_at"`
	ApprovedAt         *time.Time `json:"approved_at"`
	ConnectedAt        *time.Time `json:"connected_at"`
	LastSeenAt         *time.Time `json:"last_seen_at"`
}

const nodeColumns = `id,name,architecture,runtime_version,agent_version,destination_address,enrolled_at,approved_at,connected_at,last_seen_at`

func (n *Node) fields() []any {
	return []any{&n.ID, &n.Name, &n.Architecture, &n.RuntimeVersion, &n.AgentVersion, &n.DestinationAddress, millis{&n.EnrolledAt}, optionalMillis{&n.ApprovedAt}, optionalMillis{&n.ConnectedAt}, optionalMillis{&n.LastSeenAt}}
}

// ListNodes reports every enrolled node that is not revoked, oldest first.
func (s *Store) ListNodes(ctx context.Context) ([]Node, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT `+nodeColumns+` FROM nodes WHERE revoked_at IS NULL ORDER BY enrolled_at, id`)
	if err != nil {
		return nil, storeError(err)
	}
	defer rows.Close()
	nodes := make([]Node, 0)
	for rows.Next() {
		var node Node
		if err := rows.Scan(node.fields()...); err != nil {
			return nil, storeError(err)
		}
		nodes = append(nodes, node)
	}
	if err := rows.Err(); err != nil {
		return nil, storeError(err)
	}
	return nodes, nil
}

// GetNode reads one enrolled node.
func (s *Store) GetNode(ctx context.Context, nodeID uuid.UUID) (Node, error) {
	var node Node
	if err := s.db.QueryRowContext(ctx, `SELECT `+nodeColumns+` FROM nodes WHERE id=? AND revoked_at IS NULL`, nodeID).Scan(node.fields()...); err != nil {
		return Node{}, storeError(err)
	}
	return node, nil
}

// ApproveNode marks an enrolled node as trusted to hold work. Repeat approval keeps the first.
// The node must have reported its facts, so an operator never approves an unseen device.
func (s *Store) ApproveNode(ctx context.Context, nodeID uuid.UUID, actorID, requestID string) (Node, error) {
	if nodeID == uuid.Nil || !printableWithin(actorID, 256) || !printableWithin(requestID, 128) {
		return Node{}, &Error{Code: InvalidRequest}
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Node{}, storeError(err)
	}
	defer tx.Rollback()
	var node Node
	if err := tx.QueryRowContext(ctx, `SELECT `+nodeColumns+` FROM nodes WHERE id=? AND revoked_at IS NULL`, nodeID).Scan(node.fields()...); err != nil {
		return Node{}, storeError(err)
	}
	if node.ApprovedAt != nil {
		return node, nil
	}
	if node.Architecture == "" || node.ConnectedAt == nil {
		return Node{}, &Error{Code: NodeFactsUnavailable}
	}
	now := s.now().Truncate(time.Millisecond).UTC()
	if _, err := tx.ExecContext(ctx, `UPDATE nodes SET approved_at=?,approved_by=? WHERE id=?`, now.UnixMilli(), actorID, nodeID); err != nil {
		return Node{}, storeError(err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE destination_revision SET revision=revision+1`); err != nil {
		return Node{}, storeError(err)
	}
	if err := audit(ctx, tx, actorID, requestID, "node.approved", nodeID, now); err != nil {
		return Node{}, err
	}
	if err := storeCommit(tx); err != nil {
		return Node{}, err
	}
	node.ApprovedAt = &now
	return node, nil
}

// NodeFactsUnavailable reports an approval before the agent ever connected.
const NodeFactsUnavailable ErrorCode = "node_facts_unavailable"

// ResolveNodeName finds the one node with a name. Two nodes with one name are ambiguous.
func (s *Store) ResolveNodeName(ctx context.Context, name string) (uuid.UUID, error) {
	if len(name) == 0 || len(name) > 253 {
		return uuid.Nil, &Error{Code: InvalidRequest}
	}
	rows, err := s.db.QueryContext(ctx, `SELECT id FROM nodes WHERE name=? AND revoked_at IS NULL LIMIT 2`, name)
	if err != nil {
		return uuid.Nil, storeError(err)
	}
	defer rows.Close()
	var ids []uuid.UUID
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			return uuid.Nil, storeError(err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return uuid.Nil, storeError(err)
	}
	switch len(ids) {
	case 0:
		return uuid.Nil, &Error{Code: NotFound}
	case 1:
		return ids[0], nil
	default:
		return uuid.Nil, &Error{Code: Ambiguous}
	}
}

func audit(ctx context.Context, tx *sql.Tx, actorID, requestID, action string, subject uuid.UUID, at time.Time) error {
	if _, err := tx.ExecContext(ctx, `INSERT INTO audit_events (id,actor_id,request_id,action,subject_id,created_at) VALUES (?,?,?,?,?,?)`,
		uuid.New(), actorID, requestID, action, subject, at.UnixMilli()); err != nil {
		return storeError(err)
	}
	return nil
}
