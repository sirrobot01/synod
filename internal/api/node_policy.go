package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/placement"
	"github.com/sirrobot01/synod/internal/store/postgres"
)

// policyRequest is what an operator declares about one node. Nothing can infer it
// from the host, and the planner refuses a node with no policy.
type policyRequest struct {
	Architecture   string                `json:"architecture"`
	RuntimeVersion string                `json:"runtime_version"`
	StorageID      string                `json:"storage_id"`
	ClientAddress  string                `json:"client_address"`
	Engines        []policyEngineVersion `json:"engines"`
	Labels         map[string]string     `json:"labels"`
	FailureDomains map[string]string     `json:"failure_domains"`
	Headroom       policyVector          `json:"headroom"`
	Cordoned       bool                  `json:"cordoned"`
	Maintenance    bool                  `json:"maintenance"`
}

type policyEngineVersion struct {
	Engine  string `json:"engine"`
	Version string `json:"version"`
}

type policyVector struct {
	CPUMillis   int64 `json:"cpu_millis"`
	MemoryBytes int64 `json:"memory_bytes"`
	DiskBytes   int64 `json:"disk_bytes"`
}

// putNodePlacementPolicy approves one enrolled node for placement. The expected
// revision comes from If-Match, so two operators cannot silently overwrite each other.
func (s *Server) putNodePlacementPolicy(w http.ResponseWriter, r *http.Request) {
	identity := r.Context().Value(identityKey{}).(requestIdentity)
	nodeID, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil || nodeID == uuid.Nil {
		s.writeError(w, r, http.StatusBadRequest, "invalid_request", "The node ID is not a UUID.", uuid.Nil)
		return
	}
	expected, ok := expectedRevision(r)
	if !ok {
		s.writeError(w, r, http.StatusBadRequest, "invalid_request", "If-Match must carry the revision this update expects.", uuid.Nil)
		return
	}
	var body policyRequest
	if !s.decodeJSON(w, r, &body) {
		return
	}
	policy := placement.NodePolicy{
		InstallationID: s.installationID, NodeID: nodeID,
		Architecture: body.Architecture, RuntimeVersion: body.RuntimeVersion, StorageID: body.StorageID,
		ClientAddress: body.ClientAddress,
		Labels:        body.Labels, FailureDomains: body.FailureDomains,
		Headroom: placement.Vector{
			CPUMillis: body.Headroom.CPUMillis, MemoryBytes: body.Headroom.MemoryBytes, DiskBytes: body.Headroom.DiskBytes,
		},
		Cordoned: body.Cordoned, Maintenance: body.Maintenance,
	}
	for _, declared := range body.Engines {
		policy.Engines = append(policy.Engines, placement.EngineVersion{Engine: declared.Engine, Version: declared.Version})
	}
	// Approving an unserved combination makes a node a candidate for work nothing
	// can plan.
	if s.catalog == nil {
		s.writeError(w, r, http.StatusUnprocessableEntity, "unsupported_spec", "This server serves no engines.", uuid.Nil)
		return
	}
	for _, declared := range policy.Engines {
		if !s.served(declared) {
			s.writeError(w, r, http.StatusUnprocessableEntity, "unsupported_spec", "The node was approved for an engine version this server does not serve.", uuid.Nil)
			return
		}
	}
	revision, err := s.store.PutNodePlacementPolicy(r.Context(), policy, expected)
	if err != nil {
		if errors.Is(err, postgres.ErrPolicyRevision) {
			s.writeError(w, r, http.StatusPreconditionFailed, "revision_conflict", "The node policy revision does not match this request.", uuid.Nil)
			return
		}
		s.storeError(w, r, err)
		return
	}
	w.Header().Set("ETag", strconv.Quote(strconv.FormatInt(revision, 10)))
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(map[string]any{"node_id": nodeID, "revision": revision}); err != nil {
		s.logger.Warn("write node policy response failed", "request_id", identity.requestID, "error", err)
	}
}

// served reports an engine version this installation advertises.
func (s *Server) served(declared placement.EngineVersion) bool {
	for _, advertised := range s.catalog.Advertised() {
		if string(advertised.Combination.Engine) == declared.Engine && advertised.Combination.Version == declared.Version {
			return true
		}
	}
	return false
}

// expectedRevision refuses a missing header rather than reading it as zero: not
// looking and looking and finding nothing are different claims.
func expectedRevision(r *http.Request) (int64, bool) {
	values := r.Header.Values("If-Match")
	if len(values) != 1 || values[0] == "" {
		return 0, false
	}
	raw := values[0]
	if len(raw) >= 2 && raw[0] == '"' && raw[len(raw)-1] == '"' {
		raw = raw[1 : len(raw)-1]
	}
	if raw == "" || (len(raw) > 1 && raw[0] == '0') {
		return 0, false
	}
	for _, digit := range raw {
		if digit < '0' || digit > '9' {
			return 0, false
		}
	}
	revision, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || revision < 0 {
		return 0, false
	}
	return revision, true
}
