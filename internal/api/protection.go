package api

import (
	"net/http"

	"github.com/sirrobot01/synod/internal/backup"
)

func (s *Server) getProtection(w http.ResponseWriter, r *http.Request) {
	plan, err := s.store.ProtectionPlan(r.Context())
	if err != nil {
		s.storeError(w, r, err)
		return
	}
	s.writeJSON(w, r, plan)
}

func (s *Server) updateProtection(w http.ResponseWriter, r *http.Request) {
	var input backup.ProtectionUpdate
	if !s.decodeJSON(w, r, &input) {
		return
	}
	identity := r.Context().Value(identityKey{}).(requestIdentity)
	revision, err := s.store.UpdateProtection(r.Context(), s.installationID, input, identity.actorID, identity.requestID)
	if err != nil {
		s.storeError(w, r, err)
		return
	}
	s.writeJSON(w, r, backup.ProtectionConfirmation{ID: input.ID, Revision: revision})
}
