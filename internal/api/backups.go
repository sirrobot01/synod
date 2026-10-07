package api

import (
	"net/http"

	"github.com/sirrobot01/synod/internal/backup"
)

func (s *Server) createBackup(w http.ResponseWriter, r *http.Request) {
	var input backup.Submission
	if !s.decodeJSON(w, r, &input) {
		return
	}
	if input.Method == "" {
		input.Method = backup.Auto
	}
	identity := r.Context().Value(identityKey{}).(requestIdentity)
	result, err := s.store.CreateBackup(r.Context(), s.installationID, input, identity.actorID, identity.requestID)
	if err != nil {
		s.storeError(w, r, err)
		return
	}
	s.writeJSON(w, r, result)
}
