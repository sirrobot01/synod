package api

import (
	"net/http"

	"github.com/google/uuid"
)

// getInstallation answers the identities an agent configuration needs. The recovery
// generation can change after controller recovery, so a caller reads it when it writes
// a new agent configuration.
func (s *Server) getInstallation(w http.ResponseWriter, r *http.Request) {
	generation, err := s.store.RecoveryGeneration(r.Context(), s.installationID)
	if err != nil {
		s.storeError(w, r, err)
		return
	}
	// A controller runner records the first generation. Before that, no agent can start.
	if generation == uuid.Nil {
		s.writeError(w, r, http.StatusServiceUnavailable, "recovery_generation_unavailable", "No controller runner has recorded a recovery generation. Configure etcd and the agent transport, then start the controller.", uuid.Nil)
		return
	}
	s.writeJSON(w, r, struct {
		InstallationID     uuid.UUID `json:"installation_id"`
		RecoveryGeneration uuid.UUID `json:"recovery_generation"`
	}{s.installationID, generation})
}
