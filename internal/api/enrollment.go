package api

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net"
	"net/http"
	"strings"

	"github.com/go-chi/chi/v5"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/store/postgres"
)

// decodeJSON applies the same bounded wire contract to all JSON mutations.
func (s *Server) decodeJSON(w http.ResponseWriter, r *http.Request, target any) bool {
	mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" {
		s.writeError(w, r, http.StatusUnsupportedMediaType, "unsupported_media_type", "Use application/json.", uuid.Nil)
		return false
	}
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 64<<10))
	decoder.DisallowUnknownFields()
	err = decoder.Decode(target)
	if err == nil {
		err = decoder.Decode(new(any))
		if errors.Is(err, io.EOF) {
			return true
		}
	}
	if _, large := errors.AsType[*http.MaxBytesError](err); large {
		s.writeError(w, r, http.StatusRequestEntityTooLarge, "request_too_large", "The request exceeds 64 KiB.", uuid.Nil)
		return false
	}
	s.writeError(w, r, http.StatusBadRequest, "invalid_request", "Use one valid JSON object with known fields.", uuid.Nil)
	return false
}

func enrollmentVerifier(token string) ([32]byte, bool) {
	if len(token) != 43 {
		return [32]byte{}, false
	}
	decoded, err := base64.RawURLEncoding.Strict().DecodeString(token)
	if err != nil || len(decoded) != 32 || base64.RawURLEncoding.EncodeToString(decoded) != token {
		return [32]byte{}, false
	}
	return sha256.Sum256([]byte(token)), true
}

func (s *Server) registerEnrollmentToken(w http.ResponseWriter, r *http.Request) {
	identity := r.Context().Value(identityKey{}).(requestIdentity)
	keys := r.Header.Values("Idempotency-Key")
	if len(keys) != 1 || keys[0] == "" {
		s.writeError(w, r, http.StatusBadRequest, "invalid_request", "One Idempotency-Key header is required.", uuid.Nil)
		return
	}
	var input struct {
		NodeID     uuid.UUID `json:"node_id"`
		Token      string    `json:"token"`
		TTLSeconds int       `json:"ttl_seconds"`
	}
	if !s.decodeJSON(w, r, &input) {
		return
	}
	verifier, valid := enrollmentVerifier(input.Token)
	if !valid {
		s.writeError(w, r, http.StatusBadRequest, "invalid_request", "Use a canonical base64url token with 32 random bytes.", uuid.Nil)
		return
	}
	result, err := s.store.RegisterEnrollmentToken(r.Context(), postgres.TokenRegistration{InstallationID: s.installationID, NodeID: input.NodeID, ActorID: identity.actorID, RequestID: identity.requestID, IdempotencyKey: keys[0], TokenVerifier: verifier, TTLSeconds: input.TTLSeconds})
	if err != nil {
		s.storeError(w, r, err)
		return
	}
	w.WriteHeader(http.StatusCreated)
	if err := json.NewEncoder(w).Encode(result); err != nil {
		s.logger.Warn("write enrollment token response failed", "request_id", identity.requestID, "error", err)
	}
}

func (s *Server) enrollAgent(w http.ResponseWriter, r *http.Request) {
	headers := r.Header.Values("Authorization")
	var verifier [32]byte
	valid := false
	if len(headers) == 1 {
		scheme, token, found := strings.Cut(headers[0], " ")
		if found && strings.EqualFold(scheme, "Bearer") {
			verifier, valid = enrollmentVerifier(token)
		}
	}
	if !valid {
		w.Header().Set("WWW-Authenticate", "Bearer")
		s.writeError(w, r, http.StatusUnauthorized, "enrollment_unauthorized", "The enrollment credential is invalid.", uuid.Nil)
		return
	}
	var input struct {
		NodeID    uuid.UUID `json:"node_id"`
		RequestID uuid.UUID `json:"request_id"`
		CSRDER    []byte    `json:"csr_der"`
	}
	if !s.decodeJSON(w, r, &input) {
		return
	}
	result, err := s.store.EnrollAgent(r.Context(), postgres.EnrollmentRequest{InstallationID: s.installationID, NodeID: input.NodeID, RequestID: input.RequestID, TokenVerifier: verifier, CSRDER: input.CSRDER}, s.EnrollmentAuthority)
	if err != nil {
		s.storeError(w, r, err)
		return
	}
	if host, port, err := net.SplitHostPort(s.AgentListen); err == nil {
		if ip := net.ParseIP(host); host == "" || (ip != nil && ip.IsUnspecified()) {
			host = r.Host
			if dialed, _, err := net.SplitHostPort(r.Host); err == nil {
				host = dialed
			}
		}
		result.AgentEndpoint = net.JoinHostPort(host, port)
	}
	w.WriteHeader(http.StatusCreated)
	if err := json.NewEncoder(w).Encode(result); err != nil {
		s.logger.Warn("write enrollment response failed", "request_id", r.Context().Value(identityKey{}).(requestIdentity).requestID, "error", err)
	}
}

func (s *Server) revokeAgentCertificate(w http.ResponseWriter, r *http.Request) {
	nodeID, nodeErr := uuid.Parse(chi.URLParam(r, "id"))
	certificateID, certificateErr := uuid.Parse(chi.URLParam(r, "certificate_id"))
	if nodeErr != nil || certificateErr != nil || nodeID == uuid.Nil || certificateID == uuid.Nil {
		s.writeError(w, r, http.StatusBadRequest, "invalid_request", "Use valid node and certificate IDs.", uuid.Nil)
		return
	}
	// An empty object makes future unsupported mutation fields explicit errors.
	var input *struct{}
	if !s.decodeJSON(w, r, &input) {
		return
	}
	if input == nil {
		s.writeError(w, r, http.StatusBadRequest, "invalid_request", "Use an empty JSON object.", uuid.Nil)
		return
	}
	identity := r.Context().Value(identityKey{}).(requestIdentity)
	if err := s.store.RevokeAgentCertificate(r.Context(), s.installationID, nodeID, certificateID, identity.actorID, identity.requestID); err != nil {
		s.storeError(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
