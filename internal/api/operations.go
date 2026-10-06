package api

import (
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
)

func (s *Server) listOperations(w http.ResponseWriter, r *http.Request) {
	ops, err := s.store.ListOperations(r.Context())
	if err != nil {
		s.storeError(w, r, err)
		return
	}
	s.writeJSON(w, r, ops)
}

func (s *Server) getOperation(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil || id == uuid.Nil {
		s.writeError(w, r, http.StatusBadRequest, "invalid_request", "Use a nonzero operation UUID.")
		return
	}
	op, err := s.store.GetOperation(r.Context(), id)
	if err != nil {
		s.storeError(w, r, err)
		return
	}
	s.writeJSON(w, r, op)
}
