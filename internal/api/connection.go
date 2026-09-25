package api

import (
	"errors"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/cluster"
	"github.com/sirrobot01/synod/internal/secret"
	"github.com/sirrobot01/synod/internal/store/postgres"
)

func (s *Server) getClusterEndpoints(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil || id == uuid.Nil {
		s.writeError(w, r, http.StatusBadRequest, "invalid_request", "Use a valid cluster ID.", uuid.Nil)
		return
	}
	inspection, err := s.store.InspectClusterEndpoints(r.Context(), s.installationID, id)
	if err != nil {
		s.storeError(w, r, err)
		return
	}
	// A Patroni endpoint requires verify-full TLS, so clients receive the public native CA with it.
	if inspection.WriterURL != "" {
		inspection.CACertificatePEM = string(s.PatroniCA)
	}
	s.writeJSON(w, r, inspection)
}

func (s *Server) getClusterCredentials(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil || id == uuid.Nil {
		s.writeError(w, r, http.StatusBadRequest, "invalid_request", "Use a valid cluster ID.", uuid.Nil)
		return
	}
	inspection, err := s.store.InspectClusterCredentials(r.Context(), s.installationID, id)
	if err != nil {
		s.storeError(w, r, err)
		return
	}
	s.writeJSON(w, r, inspection)
}

func (s *Server) limitCredentialReveal(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !s.credentialRevealLimit.Allow() {
			w.Header().Set("Retry-After", "1")
			s.writeError(w, r, http.StatusTooManyRequests, "rate_limited", "Retry after the indicated delay.", uuid.Nil)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (s *Server) revealClusterCredential(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil || id == uuid.Nil {
		s.writeError(w, r, http.StatusBadRequest, "invalid_request", "Use a valid cluster ID.", uuid.Nil)
		return
	}
	purpose := chi.URLParam(r, "purpose")
	if purpose != "application" && purpose != "superuser" && purpose != "replication" {
		s.writeError(w, r, http.StatusBadRequest, "invalid_request", "Select application, superuser, or replication.", uuid.Nil)
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
	if s.SecretKeyring == nil {
		s.writeError(w, r, http.StatusServiceUnavailable, "dependency_unavailable", "Credential access is unavailable.", uuid.Nil)
		return
	}
	identity := r.Context().Value(identityKey{}).(requestIdentity)
	credential, err := s.store.AuthorizeCredentialReveal(r.Context(), s.installationID, id, purpose, identity.actorID, identity.requestID)
	if err != nil {
		if failure, ok := errors.AsType[*postgres.AdmissionError](err); ok && failure.Code == postgres.OutcomeUnknown {
			s.writeError(w, r, http.StatusServiceUnavailable, "outcome_unknown", "Credential access authorization is uncertain. No credential was returned.", uuid.Nil)
			return
		}
		s.storeError(w, r, err)
		return
	}
	// The Store commits the access audit before ciphertext crosses this boundary.
	password, err := s.SecretKeyring.Open(secret.Binding{
		InstallationID: s.installationID, OwnerType: "cluster", OwnerID: id,
		CredentialID: credential.CredentialID, Version: credential.Version,
	}, credential.Envelope)
	if err != nil {
		s.logger.Error("credential unwrap failed", "cluster_id", id, "credential_id", credential.CredentialID, "version", credential.Version, "error", err)
		s.writeError(w, r, http.StatusServiceUnavailable, "dependency_unavailable", "Credential access is unavailable.", uuid.Nil)
		return
	}
	defer clear(password)
	s.writeJSON(w, r, cluster.RevealedCredential{
		Credential: cluster.CredentialMetadata{
			CredentialID: credential.CredentialID, ClusterID: id, Version: credential.Version,
			Purpose: credential.Purpose, Username: credential.RoleName,
		},
		Password: string(password),
	})
}

// getClusterMembers reports the instances a cluster runs and the role each was named
// with. An operator that asked for a replica needs to see whether it arrived.
func (s *Server) getClusterMembers(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil || id == uuid.Nil {
		s.writeError(w, r, http.StatusBadRequest, "invalid_request", "Use a valid cluster ID.", uuid.Nil)
		return
	}
	// An unknown cluster has no members, and so does a cluster whose create has not
	// been planned yet. Those mean different things to an operator, so prove the
	// cluster exists rather than reporting an empty list for a mistyped identity.
	if _, err := s.store.GetCluster(r.Context(), s.installationID, id); err != nil {
		s.storeError(w, r, err)
		return
	}
	members, err := s.store.ReadClusterMembers(r.Context(), s.installationID, id)
	if err != nil {
		s.storeError(w, r, err)
		return
	}
	// The member identity and its role are what an operator acts on. The node and its client
	// address tell members apart, so an operator can name one for a switchover or a rebuild.
	reported := make([]clusterMember, 0, len(members))
	for _, each := range members {
		reported = append(reported, clusterMember{ID: each.MemberID, Role: each.Role, State: each.State, NodeID: each.NodeID, Address: each.Address})
	}
	s.writeJSON(w, r, struct {
		Members []clusterMember `json:"members"`
	}{reported})
}

type clusterMember struct {
	ID      uuid.UUID `json:"id"`
	Role    string    `json:"role"`
	State   string    `json:"state"`
	NodeID  uuid.UUID `json:"node_id,omitzero"`
	Address string    `json:"address,omitempty"`
}
