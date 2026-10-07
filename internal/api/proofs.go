package api

import (
	"github.com/sirrobot01/synod/internal/proof"
	"net/http"
)

func (s *Server) createProof(w http.ResponseWriter, r *http.Request) {
	var input proof.Submission
	if !s.decodeJSON(w, r, &input) {
		return
	}
	identity := r.Context().Value(identityKey{}).(requestIdentity)
	result, err := s.store.CreateProof(r.Context(), s.installationID, input, identity.actorID, identity.requestID)
	if err != nil {
		s.storeError(w, r, err)
		return
	}
	s.writeJSON(w, r, result)
}
