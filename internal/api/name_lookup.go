package api

import (
	"encoding/json"
	"net/http"

	"github.com/go-chi/chi/v5"
)

func (s *Server) getAppByName(w http.ResponseWriter, r *http.Request) {
	found, err := s.store.GetAppByName(r.Context(), s.installationID, chi.URLParam(r, "name"))
	if err != nil {
		s.storeError(w, r, err)
		return
	}
	item, err := s.store.GetAppStatus(r.Context(), s.installationID, found.ID)
	if err != nil {
		s.storeError(w, r, err)
		return
	}
	if err := json.NewEncoder(w).Encode(item); err != nil {
		s.logger.Warn("write app response failed", "app_id", found.ID, "error", err)
	}
}

func (s *Server) getNodeByName(w http.ResponseWriter, r *http.Request) {
	id, err := s.store.ResolveNodeName(r.Context(), s.installationID, chi.URLParam(r, "name"))
	if err != nil {
		s.storeError(w, r, err)
		return
	}
	if err := json.NewEncoder(w).Encode(map[string]string{"node_id": id.String()}); err != nil {
		s.logger.Warn("write node response failed", "node_id", id, "error", err)
	}
}
