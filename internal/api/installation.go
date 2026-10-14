package api

import (
	"net"
	"net/http"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/buildinfo"
)

// getInstallation identifies this installation and its running controller build.
func (s *Server) getInstallation(w http.ResponseWriter, r *http.Request) {
	// A device dials the address that reached this controller when none is configured.
	address := s.JoinAddress
	if address == "" {
		address = r.Host
		if _, _, err := net.SplitHostPort(address); err != nil {
			address = net.JoinHostPort(address, "443")
		}
	}
	s.writeJSON(w, r, struct {
		InstallationID   uuid.UUID `json:"installation_id"`
		Version          string    `json:"version"`
		Revision         string    `json:"revision"`
		JoinAddress      string    `json:"join_address"`
		APICAFingerprint string    `json:"api_ca_fingerprint"`
	}{s.installationID, buildinfo.Version, buildinfo.Revision, address, s.APICAFingerprint})
}
