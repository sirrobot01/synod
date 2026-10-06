package api

import "net/http"

func (s *Server) listApps(w http.ResponseWriter, r *http.Request) {
	catalog, err := s.store.ListApps(r.Context())
	if err != nil {
		s.storeError(w, r, err)
		return
	}
	s.writeJSON(w, r, catalog)
}
