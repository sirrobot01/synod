package api

import (
	"github.com/sirrobot01/synod/internal/notification"
	"net/http"
)

func (s *Server) getNotifications(w http.ResponseWriter, r *http.Request) {
	result, err := s.store.NotificationStatus(r.Context())
	if err != nil {
		s.storeError(w, r, err)
		return
	}
	s.writeJSON(w, r, result)
}
func (s *Server) configureNotifications(w http.ResponseWriter, r *http.Request) {
	var input notification.Update
	if !s.decodeJSON(w, r, &input) {
		return
	}
	identity := r.Context().Value(identityKey{}).(requestIdentity)
	revision, err := s.store.ConfigureNotifications(r.Context(), s.installationID, s.CredentialKeyring, input, identity.actorID, identity.requestID)
	if err != nil {
		s.storeError(w, r, err)
		return
	}
	s.writeJSON(w, r, notification.Confirmation{ID: input.ID, Revision: revision})
}
func (s *Server) testNotification(w http.ResponseWriter, r *http.Request) {
	var input notification.TestRequest
	if !s.decodeJSON(w, r, &input) {
		return
	}
	identity := r.Context().Value(identityKey{}).(requestIdentity)
	if err := s.store.TestNotification(r.Context(), input, identity.actorID, identity.requestID); err != nil {
		s.storeError(w, r, err)
		return
	}
	s.writeJSON(w, r, notification.Confirmation{ID: input.ID, Revision: input.ExpectedRevision})
}
