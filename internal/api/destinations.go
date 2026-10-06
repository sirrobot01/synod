package api

import (
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/destination"
)

func (s *Server) addDestination(w http.ResponseWriter, r *http.Request) {
	var entry destination.Entry
	if !s.decodeJSON(w, r, &entry) {
		return
	}
	identity := r.Context().Value(identityKey{}).(requestIdentity)
	result, err := s.store.AddDestination(r.Context(), s.installationID, s.CredentialKeyring, entry, identity.actorID, identity.requestID)
	if err != nil {
		s.storeError(w, r, err)
		return
	}
	s.writeJSON(w, r, result)
}

func (s *Server) listDestinations(w http.ResponseWriter, r *http.Request) {
	result, err := s.store.ListDestinations(r.Context())
	if err != nil {
		s.storeError(w, r, err)
		return
	}
	s.writeJSON(w, r, result)
}

func (s *Server) removeDestination(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil || id == uuid.Nil {
		s.writeError(w, r, http.StatusBadRequest, "invalid_request", "Use a nonzero destination UUID.")
		return
	}
	identity := r.Context().Value(identityKey{}).(requestIdentity)
	if err := s.store.RemoveDestination(r.Context(), id, identity.actorID, identity.requestID); err != nil {
		s.storeError(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
