package api

import (
	"net/http"

	"github.com/google/uuid"
)

// getInstallation answers the identity that an agent configuration needs.
func (s *Server) getInstallation(w http.ResponseWriter, r *http.Request) {
	s.writeJSON(w, r, struct {
		InstallationID uuid.UUID `json:"installation_id"`
	}{s.installationID})
}
