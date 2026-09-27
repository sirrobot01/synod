package api

import (
	"encoding/json"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/app"
	"github.com/sirrobot01/synod/internal/store/postgres"
)

func (s *Server) createApp(w http.ResponseWriter, r *http.Request) {
	identity := r.Context().Value(identityKey{}).(requestIdentity)
	keys := r.Header.Values("Idempotency-Key")
	if len(keys) != 1 || keys[0] == "" {
		s.writeError(w, r, http.StatusBadRequest, "invalid_request", "One Idempotency-Key header is required.", uuid.Nil)
		return
	}
	var spec app.AppSpec
	if !s.decodeJSON(w, r, &spec) {
		return
	}
	result, err := s.store.AdmitApp(r.Context(), postgres.CreateAppRequest{
		Identity:  postgres.RequestIdentity{InstallationID: s.installationID, ActorID: identity.actorID, APIOperation: postgres.CreateAppAPIOperation, IdempotencyKey: keys[0]},
		RequestID: identity.requestID, Spec: spec,
	})
	if err != nil {
		s.storeError(w, r, err)
		return
	}
	w.Header().Set("Location", "/v1/apps/"+result.AppID.String())
	w.WriteHeader(http.StatusAccepted)
	if err := json.NewEncoder(w).Encode(result); err != nil {
		s.logger.Warn("write app admission failed", "request_id", identity.requestID, "error", err)
	}
}

func (s *Server) getApp(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil || id == uuid.Nil {
		s.writeError(w, r, http.StatusBadRequest, "invalid_request", "Use a valid app ID.", uuid.Nil)
		return
	}
	item, err := s.store.GetAppStatus(r.Context(), s.installationID, id)
	if err != nil {
		s.storeError(w, r, err)
		return
	}
	if err := json.NewEncoder(w).Encode(item); err != nil {
		s.logger.Warn("write app response failed", "app_id", id, "error", err)
	}
}

func (s *Server) appAction(action string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		identity := r.Context().Value(identityKey{}).(requestIdentity)
		id, err := uuid.Parse(chi.URLParam(r, "id"))
		if err != nil || id == uuid.Nil {
			s.writeError(w, r, http.StatusBadRequest, "invalid_request", "Use a valid app ID.", uuid.Nil)
			return
		}
		keys := r.Header.Values("Idempotency-Key")
		expected, ok := expectedRevision(r)
		if len(keys) != 1 || keys[0] == "" || !ok || expected < 1 {
			s.writeError(w, r, http.StatusBadRequest, "invalid_request", "Use one Idempotency-Key and one positive If-Match revision.", uuid.Nil)
			return
		}
		var input *struct{}
		if !s.decodeJSON(w, r, &input) {
			return
		}
		if input == nil {
			s.writeError(w, r, http.StatusBadRequest, "invalid_request", "Use an empty JSON object.", uuid.Nil)
			return
		}
		result, err := s.store.AdmitAppAction(r.Context(), postgres.AppActionRequest{Identity: postgres.RequestIdentity{InstallationID: s.installationID, ActorID: identity.actorID, APIOperation: action, IdempotencyKey: keys[0]}, RequestID: identity.requestID, AppID: id, ExpectedRevision: expected})
		if err != nil {
			s.storeError(w, r, err)
			return
		}
		w.Header().Set("Location", "/v1/operations/"+result.OperationID.String())
		w.WriteHeader(http.StatusAccepted)
		if err := json.NewEncoder(w).Encode(result); err != nil {
			s.logger.Warn("write app action failed", "request_id", identity.requestID, "error", err)
		}
	}
}
