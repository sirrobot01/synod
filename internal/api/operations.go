package api

import (
	"net/http"
	"strconv"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
)

func (s *Server) listOperations(w http.ResponseWriter, r *http.Request) {
	limit, before := 100, uuid.Nil
	if value := r.URL.Query().Get("limit"); value != "" {
		parsed, err := strconv.Atoi(value)
		if err != nil || parsed < 1 || parsed > 200 {
			s.writeError(w, r, http.StatusBadRequest, "invalid_request", "Use a limit from 1 to 200.")
			return
		}
		limit = parsed
	}
	if value := r.URL.Query().Get("before"); value != "" {
		parsed, err := uuid.Parse(value)
		if err != nil || parsed == uuid.Nil {
			s.writeError(w, r, http.StatusBadRequest, "invalid_request", "Use an operation UUID for before.")
			return
		}
		before = parsed
	}
	ops, err := s.store.ListOperations(r.Context(), limit, before)
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

// removeOperation hides one finished operation from the activity list.
func (s *Server) removeOperation(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil || id == uuid.Nil {
		s.writeError(w, r, http.StatusBadRequest, "invalid_request", "Use a nonzero operation UUID.")
		return
	}
	if err := s.store.RemoveOperation(r.Context(), id); err != nil {
		s.storeError(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// clearOperations hides every finished operation from the activity list.
func (s *Server) clearOperations(w http.ResponseWriter, r *http.Request) {
	var body struct{}
	if !s.decodeJSON(w, r, &body) {
		return
	}
	removed, err := s.store.ClearOperations(r.Context())
	if err != nil {
		s.storeError(w, r, err)
		return
	}
	s.writeJSON(w, r, map[string]int64{"removed": removed})
}
