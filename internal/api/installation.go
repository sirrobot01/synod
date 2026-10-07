package api

import (
	"net/http"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/buildinfo"
)

// getInstallation identifies this installation and its running controller build.
func (s *Server) getInstallation(w http.ResponseWriter, r *http.Request) {
	s.writeJSON(w, r, struct {
		InstallationID uuid.UUID `json:"installation_id"`
		Version        string    `json:"version"`
		Revision       string    `json:"revision"`
	}{s.installationID, buildinfo.Version, buildinfo.Revision})
}
