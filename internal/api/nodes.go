package api

import (
	"errors"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/store"
)

func (s *Server) listNodes(w http.ResponseWriter, r *http.Request) {
	nodes, err := s.store.ListNodes(r.Context())
	if err != nil {
		s.storeError(w, r, err)
		return
	}
	s.writeJSON(w, r, nodes)
}

func (s *Server) getNode(w http.ResponseWriter, r *http.Request) {
	id, ok := s.nodeID(w, r)
	if !ok {
		return
	}
	node, err := s.store.GetNode(r.Context(), id)
	if err != nil {
		s.storeError(w, r, err)
		return
	}
	s.writeJSON(w, r, node)
}

func (s *Server) getNodeByName(w http.ResponseWriter, r *http.Request) {
	id, err := s.store.ResolveNodeName(r.Context(), chi.URLParam(r, "name"))
	if err != nil {
		s.storeError(w, r, err)
		return
	}
	node, err := s.store.GetNode(r.Context(), id)
	if err != nil {
		s.storeError(w, r, err)
		return
	}
	s.writeJSON(w, r, node)
}

// approveNode trusts an enrolled node that has reported its facts. Repeat approval is a no-op.
func (s *Server) approveNode(w http.ResponseWriter, r *http.Request) {
	id, ok := s.nodeID(w, r)
	if !ok {
		return
	}
	identity := r.Context().Value(identityKey{}).(requestIdentity)
	node, err := s.store.ApproveNode(r.Context(), id, identity.actorID, identity.requestID)
	if err != nil {
		s.storeError(w, r, err)
		return
	}
	s.writeJSON(w, r, node)
}

func (s *Server) nodeID(w http.ResponseWriter, r *http.Request) (uuid.UUID, bool) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil || id == uuid.Nil {
		s.writeError(w, r, http.StatusBadRequest, "invalid_request", "The node ID is not a UUID.")
		return uuid.Nil, false
	}
	return id, true
}

// renameNode gives a device a unique name. The agent's host name no longer replaces it.
func (s *Server) renameNode(w http.ResponseWriter, r *http.Request) {
	id, ok := s.nodeID(w, r)
	if !ok {
		return
	}
	var body struct {
		Name string `json:"name"`
	}
	if !s.decodeJSON(w, r, &body) {
		return
	}
	node, err := s.store.RenameNode(r.Context(), id, body.Name)
	if failure, ok := errors.AsType[*store.Error](err); ok && failure.Code == store.InvalidRequest {
		s.writeError(w, r, http.StatusBadRequest, "invalid_request", "Use 1 to 63 letters, digits, hyphens, underscores, or dots. Do not start with a hyphen.")
		return
	}
	if err != nil {
		s.storeError(w, r, err)
		return
	}
	s.writeJSON(w, r, node)
}
