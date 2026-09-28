package api

import (
	"errors"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/app"
	"github.com/sirrobot01/synod/internal/secret"
	"github.com/sirrobot01/synod/internal/store/postgres"
)

func (s *Server) limitCredentialReveal(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !s.credentialRevealLimit.Allow() {
			w.Header().Set("Retry-After", "1")
			s.writeError(w, r, http.StatusTooManyRequests, "rate_limited", "Retry after the indicated delay.", uuid.Nil)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// revealAppCredential returns the client credential of a tier 1 app after the store
// commits an audit event for the access.
func (s *Server) revealAppCredential(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil || id == uuid.Nil {
		s.writeError(w, r, http.StatusBadRequest, "invalid_request", "Use a valid app ID.", uuid.Nil)
		return
	}
	var input *struct{}
	if !s.decodeJSON(w, r, &input) {
		return
	}
	if input == nil {
		s.writeError(w, r, http.StatusBadRequest, "invalid_request", "Use an empty JSON object.", uuid.Nil)
		return
	}
	if s.SecretKeyring == nil {
		s.writeError(w, r, http.StatusServiceUnavailable, "dependency_unavailable", "Credential access is unavailable.", uuid.Nil)
		return
	}
	identity := r.Context().Value(identityKey{}).(requestIdentity)
	credential, username, err := s.store.AuthorizeAppCredentialReveal(r.Context(), s.installationID, id, identity.actorID, identity.requestID)
	if err != nil {
		if failure, ok := errors.AsType[*postgres.AdmissionError](err); ok && failure.Code == postgres.OutcomeUnknown {
			s.writeError(w, r, http.StatusServiceUnavailable, "outcome_unknown", "Credential access authorization is uncertain. No credential was returned.", uuid.Nil)
			return
		}
		s.storeError(w, r, err)
		return
	}
	password, err := s.SecretKeyring.Open(secret.Binding{
		InstallationID: s.installationID, OwnerType: "app", OwnerID: id,
		CredentialID: credential.CredentialID, Version: credential.Version,
	}, credential.Envelope)
	if err != nil {
		s.logger.Error("credential unwrap failed", "app_id", id, "credential_id", credential.CredentialID, "version", credential.Version, "error", err)
		s.writeError(w, r, http.StatusServiceUnavailable, "dependency_unavailable", "Credential access is unavailable.", uuid.Nil)
		return
	}
	defer clear(password)
	s.writeJSON(w, r, app.Credential{AppID: id, CredentialID: credential.CredentialID, Version: credential.Version, Username: username, Password: string(password)})
}
