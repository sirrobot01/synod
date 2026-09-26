package api

import (
	"encoding/json"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/cluster"
	"github.com/sirrobot01/synod/internal/store/postgres"
)

// clusterAction returns durable admission, not proof that the action has finished.
func (s *Server) clusterAction(apiOperation string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		identity := r.Context().Value(identityKey{}).(requestIdentity)
		clusterID, err := uuid.Parse(chi.URLParam(r, "id"))
		if err != nil || clusterID == uuid.Nil {
			s.writeError(w, r, http.StatusBadRequest, "invalid_request", "Use a valid cluster ID.", uuid.Nil)
			return
		}
		keys := r.Header.Values("Idempotency-Key")
		if len(keys) != 1 || keys[0] == "" {
			s.writeError(w, r, http.StatusBadRequest, "invalid_request", "One Idempotency-Key header is required.", uuid.Nil)
			return
		}
		expected, ok := expectedRevision(r)
		if !ok || expected < 1 {
			s.writeError(w, r, http.StatusBadRequest, "invalid_request", "One positive If-Match revision is required.", uuid.Nil)
			return
		}
		var confirmation *cluster.ActionConfirmation
		var replicas *int
		var memberID uuid.UUID
		switch apiOperation {
		case postgres.DeleteClusterAPIOperation, postgres.RecoverSetupAPIOperation, postgres.RebuildMemberAPIOperation:
			if !s.decodeJSON(w, r, &confirmation) {
				return
			}
			if confirmation == nil {
				s.writeError(w, r, http.StatusBadRequest, "invalid_request", "Use an action confirmation object.", uuid.Nil)
				return
			}
		case postgres.ScaleClusterAPIOperation:
			// Removing a member is destructive, so a scale down carries the plan the
			// operator reviewed in the same body as the count.
			var input *struct {
				Replicas *int `json:"replicas"`
				*cluster.ActionConfirmation
			}
			if !s.decodeJSON(w, r, &input) {
				return
			}
			// An absent count is not a request for zero. The caller must say what it
			// wants, because the two mean different things to a running cluster.
			if input == nil || input.Replicas == nil {
				s.writeError(w, r, http.StatusBadRequest, "invalid_request", "Name the number of replicas.", uuid.Nil)
				return
			}
			replicas, confirmation = input.Replicas, input.ActionConfirmation
		case postgres.SwitchoverAPIOperation:
			// Patroni elects its leader, so the operator names the member that takes over.
			var input *struct {
				MemberID uuid.UUID `json:"member_id"`
			}
			if !s.decodeJSON(w, r, &input) {
				return
			}
			if input == nil || input.MemberID == uuid.Nil {
				s.writeError(w, r, http.StatusBadRequest, "invalid_request", "Name the member_id that becomes the leader.", uuid.Nil)
				return
			}
			memberID = input.MemberID
		default:
			var input *struct{}
			if !s.decodeJSON(w, r, &input) {
				return
			}
			if input == nil {
				s.writeError(w, r, http.StatusBadRequest, "invalid_request", "Use an empty JSON object.", uuid.Nil)
				return
			}
		}
		admission, err := s.store.AdmitClusterAction(r.Context(), postgres.ClusterActionRequest{
			Identity: postgres.RequestIdentity{
				InstallationID: s.installationID, ActorID: identity.actorID,
				APIOperation: apiOperation, IdempotencyKey: keys[0],
			},
			RequestID:        identity.requestID,
			ClusterID:        clusterID,
			ExpectedRevision: expected,
			Confirmation:     confirmation,
			Replicas:         replicas,
			MemberID:         memberID,
		})
		if err != nil {
			s.storeError(w, r, err)
			return
		}
		w.Header().Set("Location", "/v1/operations/"+admission.OperationID.String())
		w.WriteHeader(http.StatusAccepted)
		if err := json.NewEncoder(w).Encode(admission); err != nil {
			s.logger.Warn("write admission response failed", "request_id", identity.requestID, "error", err)
		}
		s.logger.Info("cluster action accepted", "request_id", identity.requestID, "actor_id", identity.actorID,
			"api_operation", apiOperation, "cluster_id", admission.ClusterID, "operation_id", admission.OperationID, "replayed", admission.Replayed)
	}
}

func (s *Server) planClusterAction(w http.ResponseWriter, r *http.Request) {
	identity := r.Context().Value(identityKey{}).(requestIdentity)
	clusterID, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil || clusterID == uuid.Nil {
		s.writeError(w, r, http.StatusBadRequest, "invalid_request", "Use a valid cluster ID.", uuid.Nil)
		return
	}
	var input *struct {
		Action   string    `json:"action"`
		MemberID uuid.UUID `json:"member_id"`
	}
	if !s.decodeJSON(w, r, &input) {
		return
	}
	if input == nil || (input.Action != "delete" && input.Action != "scale_down" && input.Action != "recover_setup" && input.Action != "rebuild_member") {
		s.writeError(w, r, http.StatusBadRequest, "invalid_request", "Use action delete, scale_down, recover_setup, or rebuild_member.", uuid.Nil)
		return
	}
	// Patroni elects its primary, so only the operator can name the failed member or the
	// member a Patroni scale removes. Redis names its own replica.
	if input.Action == "rebuild_member" && input.MemberID == uuid.Nil || input.Action != "rebuild_member" && input.Action != "scale_down" && input.MemberID != uuid.Nil {
		s.writeError(w, r, http.StatusBadRequest, "invalid_request", "Use member_id with action rebuild_member, or with scale_down for a Patroni cluster.", uuid.Nil)
		return
	}
	if input.Action == "scale_down" && input.MemberID != uuid.Nil {
		plan, err := s.store.PreviewMemberRemoval(r.Context(), s.installationID, clusterID, input.MemberID, identity.actorID)
		if err != nil {
			s.storeError(w, r, err)
			return
		}
		s.writeJSON(w, r, plan)
		return
	}
	if input.Action == "rebuild_member" {
		plan, err := s.store.PreviewMemberRebuild(r.Context(), s.installationID, clusterID, input.MemberID, identity.actorID)
		if err != nil {
			s.storeError(w, r, err)
			return
		}
		s.writeJSON(w, r, plan)
		return
	}
	preview := s.store.PreviewClusterDelete
	if input.Action == "scale_down" {
		preview = s.store.PreviewClusterScaleDown
	} else if input.Action == "recover_setup" {
		preview = s.store.PreviewSetupRecovery
	}
	plan, err := preview(r.Context(), s.installationID, clusterID, identity.actorID)
	if err != nil {
		s.storeError(w, r, err)
		return
	}
	s.writeJSON(w, r, plan)
}
