package api

import (
	"context"
	"encoding/json"
	"errors"
	"io/fs"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/auth"
	"github.com/sirrobot01/synod/internal/cluster"
	"github.com/sirrobot01/synod/internal/engine"
	"github.com/sirrobot01/synod/internal/pki"
	"github.com/sirrobot01/synod/internal/secret"
	"github.com/sirrobot01/synod/internal/store/postgres"
	"golang.org/x/time/rate"
)

type requestIdentity struct {
	actorID   string
	requestID string
	role      string
	userID    uuid.UUID
	sessionID uuid.UUID
	// cookie reports an ambient credential, which an unsafe method must prove the origin of.
	cookie bool
}

func withIdentity(r *http.Request, identity requestIdentity) *http.Request {
	return r.WithContext(context.WithValue(r.Context(), identityKey{}, identity))
}

type identityKey struct{}

// Server routes and authenticates the administrator HTTP API.
type Server struct {
	// EnrollmentAuthority must be configured before serving requests. Nil disables new issuance.
	EnrollmentAuthority *pki.CA
	// SecretKeyring must be configured before serving. Nil disables credential reveal.
	SecretKeyring *secret.Keyring
	// PatroniCA is the public native Patroni CA certificate. Patroni endpoints return it.
	PatroniCA             []byte
	requestLimit          *rate.Limiter
	enrollmentLimit       *rate.Limiter
	credentialRevealLimit *rate.Limiter
	signInLimit           *rate.Limiter
	signInAttempts        *attemptLimiter
	// assets is the built browser application. A nil value serves the API alone.
	assets fs.FS
	// PublicOrigin is the browser origin of this API. Set it before serving requests.
	// An empty value makes a cookie request rely on Sec-Fetch-Site alone.
	PublicOrigin   string
	store          *postgres.Store
	admin          *auth.AdminToken
	installationID uuid.UUID
	// catalog is what this installation serves. A nil catalog serves nothing, which
	// is what a replay must still survive.
	catalog *engine.Catalog
	logger  *slog.Logger
	router  chi.Router
}

// NewServer builds the API server and registers its routes.
func NewServer(store *postgres.Store, admin *auth.AdminToken, installationID uuid.UUID, catalog *engine.Catalog, logger *slog.Logger) (*Server, error) {
	if store == nil || admin == nil || installationID == uuid.Nil || logger == nil {
		return nil, errors.New("API requires a Store, administrator verifier, installation ID, and logger")
	}
	server := &Server{requestLimit: rate.NewLimiter(100, 200), enrollmentLimit: rate.NewLimiter(10, 20), credentialRevealLimit: rate.NewLimiter(1, 5), signInLimit: rate.NewLimiter(1, 10), signInAttempts: newAttemptLimiter(rate.Every(2*time.Second), 5, 4096), store: store, admin: admin, installationID: installationID, logger: logger, catalog: catalog}
	router := chi.NewRouter()
	router.Use(server.begin)
	router.NotFound(func(w http.ResponseWriter, r *http.Request) {
		// The API answers JSON for its own namespace. Every other path belongs to
		// the browser application, which routes the rest itself.
		if strings.HasPrefix(r.URL.Path, "/v1/") || r.URL.Path == "/v1" {
			server.writeError(w, r, http.StatusNotFound, "not_found", "The endpoint was not found.", uuid.Nil)
			return
		}
		server.serveApp(w, r)
	})
	router.MethodNotAllowed(func(w http.ResponseWriter, r *http.Request) {
		// A custom handler replaces the one that chi gives the allowed set to,
		// so report the set by asking the router which methods match this path.
		var allowed []string
		for _, method := range []string{http.MethodGet, http.MethodHead, http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete} {
			if router.Match(chi.NewRouteContext(), method, r.URL.Path) {
				allowed = append(allowed, method)
			}
		}
		if len(allowed) > 0 {
			w.Header().Set("Allow", strings.Join(allowed, ", "))
		}
		server.writeError(w, r, http.StatusMethodNotAllowed, "method_not_allowed", "The endpoint does not support this method.", uuid.Nil)
	})
	router.Route("/v1", func(v1 chi.Router) {
		// Enrollment presents its own scoped credential, never the administrator token.
		v1.Group(func(enrollment chi.Router) {
			enrollment.Use(server.limitEnrollment)
			enrollment.Post("/enrollments", server.enrollAgent)
		})
		// Sign-in creates the credential, so it cannot require one.
		// It still changes state and sets a cookie, so a cross-site form post
		// could sign a browser in as somebody else. It proves its origin too.
		v1.With(server.requireBrowserOrigin).Post("/sessions", server.createSession)
		v1.Group(func(signed chi.Router) {
			signed.Use(server.authenticateAny, server.requireOrigin)
			signed.Get("/sessions/current", server.readSession)
			signed.Head("/sessions/current", server.readSession)
			signed.Delete("/sessions/current", server.deleteSession)
			// An operator may change its own password, so this route checks the subject itself.
			signed.Post("/users/{id}/actions/set-password", server.setUserPassword)
			signed.Group(func(admin chi.Router) {
				admin.Use(server.requireAdmin)
				admin.Post("/clusters", server.createCluster)
				admin.Get("/installation", server.getInstallation)
				admin.Get("/clusters", server.listClusters)
				admin.Head("/clusters", server.listClusters)
				admin.Get("/clusters/{id}", server.getCluster)
				admin.Get("/clusters/{id}/endpoints", server.getClusterEndpoints)
				admin.Get("/clusters/{id}/members", server.getClusterMembers)
				admin.Get("/clusters/{id}/credentials", server.getClusterCredentials)
				admin.With(server.limitCredentialReveal).Post("/clusters/{id}/credentials/{purpose}/reveal", server.revealClusterCredential)
				admin.Post("/clusters/{id}/actions/stop", server.clusterAction(postgres.StopClusterAPIOperation))
				admin.Post("/clusters/{id}/actions/start", server.clusterAction(postgres.StartClusterAPIOperation))
				admin.Post("/clusters/{id}/actions/recover-setup", server.clusterAction(postgres.RecoverSetupAPIOperation))
				admin.Post("/clusters/{id}/actions/rebuild-replica", server.clusterAction(postgres.RebuildReplicaAPIOperation))
				admin.Post("/clusters/{id}/actions/rebuild-member", server.clusterAction(postgres.RebuildMemberAPIOperation))
				admin.Post("/clusters/{id}/actions/renew-certificates", server.clusterAction(postgres.RenewCertificatesAPIOperation))
				admin.Post("/clusters/{id}/actions/switchover", server.clusterAction(postgres.SwitchoverAPIOperation))
				admin.Post("/clusters/{id}/actions/scale", server.clusterAction(postgres.ScaleClusterAPIOperation))
				admin.Delete("/clusters/{id}", server.clusterAction(postgres.DeleteClusterAPIOperation))
				admin.Post("/clusters/{id}/action-plans", server.planClusterAction)
				admin.Get("/operations/{id}", server.getOperation)
				// net/http discards a HEAD body, so one handler serves both methods.
				admin.Head("/clusters/{id}", server.getCluster)
				admin.Head("/operations/{id}", server.getOperation)
				admin.Post("/enrollment-tokens", server.registerEnrollmentToken)
				admin.Post("/nodes/{id}/certificates/{certificate_id}/actions/revoke", server.revokeAgentCertificate)
				admin.Put("/nodes/{id}/placement-policy", server.putNodePlacementPolicy)
				admin.Post("/users", server.createUser)
				admin.Post("/users/{id}/actions/disable", server.disableUser)
				admin.Post("/tokens", server.createToken)
				admin.Get("/tokens", server.listTokens)
				admin.Head("/tokens", server.listTokens)
				admin.Post("/tokens/{id}/actions/revoke", server.revokeToken)
			})
		})
	})
	server.router = router
	return server, nil
}

// ServeHTTP routes one request. Every stage runs as router middleware.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) { s.router.ServeHTTP(w, r) }

// begin bounds the request, gives it an identity, and applies the shared rate limit.
// It runs before routing, so a rejected request still carries the standard headers.
func (s *Server) begin(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		ctx, cancel := context.WithTimeout(r.Context(), 20*time.Second)
		defer cancel()
		r = r.WithContext(ctx)
		requestID, err := uuid.NewRandom()
		if err != nil {
			s.logger.Error("request identity unavailable", "method", r.Method, "path", r.URL.Path, "error", err)
			http.Error(w, "request identity unavailable", http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("X-Request-ID", requestID.String())
		w.Header().Set("Content-Type", "application/json")
		r = r.WithContext(context.WithValue(r.Context(), identityKey{}, requestIdentity{requestID: requestID.String()}))
		if !s.requestLimit.Allow() {
			w.Header().Set("Retry-After", "1")
			s.writeError(w, r, http.StatusTooManyRequests, "rate_limited", "Retry after the indicated delay.", uuid.Nil)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// limitEnrollment applies the separate budget that the scoped enrollment credential uses.
func (s *Server) limitEnrollment(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !s.enrollmentLimit.Allow() {
			w.Header().Set("Retry-After", "1")
			s.writeError(w, r, http.StatusTooManyRequests, "rate_limited", "Retry after the indicated delay.", uuid.Nil)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (s *Server) createCluster(w http.ResponseWriter, r *http.Request) {
	identity := r.Context().Value(identityKey{}).(requestIdentity)
	keys := r.Header.Values("Idempotency-Key")
	if len(keys) != 1 || keys[0] == "" {
		s.writeError(w, r, http.StatusBadRequest, "invalid_request", "One Idempotency-Key header is required.", uuid.Nil)
		return
	}
	var spec cluster.CreateSpec
	if !s.decodeJSON(w, r, &spec) {
		return
	}
	if err := spec.Validate(); err != nil {
		s.writeError(w, r, http.StatusBadRequest, "invalid_request", "The cluster specification is invalid.", uuid.Nil)
		return
	}
	request := postgres.CreateClusterRequest{
		Identity:  postgres.RequestIdentity{InstallationID: s.installationID, ActorID: identity.actorID, APIOperation: postgres.CreateClusterAPIOperation, IdempotencyKey: keys[0]},
		RequestID: identity.requestID,
		Spec:      spec,
	}
	// Replays preserve accepted work even when the supported catalog changes.
	admission, err := s.store.LookupAdmission(r.Context(), request.Identity, request.Spec)
	if err != nil {
		failure, ok := errors.AsType[*postgres.AdmissionError](err)
		if !ok || failure.Code != postgres.NotFound {
			s.storeError(w, r, err)
			return
		}
		// Capability comes from engine, version, topology, and persistence together,
		// so the refusal happens here rather than when a host is touched.
		if s.catalog == nil {
			s.writeError(w, r, http.StatusUnprocessableEntity, "unsupported_spec", "This server serves no engines.", uuid.Nil)
			return
		}
		if _, err := s.catalog.Resolve(spec); err != nil {
			s.writeError(w, r, http.StatusUnprocessableEntity, "unsupported_spec", "The requested engine, version, topology, and persistence are not supported by this server.", uuid.Nil)
			return
		}
		// Patroni needs the DCS admin account and the native CA. The CA loads only when both exist.
		if spec.PostgreSQL != nil && spec.PostgreSQL.Topology == "patroni" && len(s.PatroniCA) == 0 {
			s.writeError(w, r, http.StatusUnprocessableEntity, "unsupported_spec", "Patroni HA is not configured on this installation.", uuid.Nil)
			return
		}
		admission, err = s.store.CreateCluster(r.Context(), request)
		if err != nil {
			s.storeError(w, r, err)
			return
		}
	}
	w.Header().Set("Location", "/v1/operations/"+admission.OperationID.String())
	w.WriteHeader(http.StatusAccepted)
	if err := json.NewEncoder(w).Encode(admission); err != nil {
		s.logger.Warn("write admission response failed", "request_id", identity.requestID, "error", err)
	}
	s.logger.Info("cluster admission accepted", "request_id", identity.requestID, "actor_id", identity.actorID, "cluster_id", admission.ClusterID, "operation_id", admission.OperationID, "replayed", admission.Replayed)
}

func (s *Server) getCluster(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil || id == uuid.Nil {
		s.writeError(w, r, http.StatusBadRequest, "invalid_request", "Use a valid cluster ID.", uuid.Nil)
		return
	}
	value, err := s.store.GetCluster(r.Context(), s.installationID, id)
	if err != nil {
		s.storeError(w, r, err)
		return
	}
	w.Header().Set("ETag", `"`+strconv.FormatInt(value.DesiredRevision, 10)+`"`)
	if err := json.NewEncoder(w).Encode(value); err != nil {
		s.logger.Warn("write cluster response failed", "cluster_id", id, "error", err)
	}
}

func (s *Server) getOperation(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil || id == uuid.Nil {
		s.writeError(w, r, http.StatusBadRequest, "invalid_request", "Use a valid operation ID.", uuid.Nil)
		return
	}
	value, err := s.store.GetOperation(r.Context(), s.installationID, id)
	if err != nil {
		s.storeError(w, r, err)
		return
	}
	if err := json.NewEncoder(w).Encode(value); err != nil {
		s.logger.Warn("write operation response failed", "operation_id", id, "error", err)
	}
}

func (s *Server) storeError(w http.ResponseWriter, r *http.Request, err error) {
	if failure, ok := errors.AsType[*postgres.AdmissionError](err); ok {
		status, message := http.StatusServiceUnavailable, "The metadata service is unavailable."
		switch failure.Code {
		case postgres.EnrollmentUnauthorized:
			status, message = http.StatusUnauthorized, "The enrollment credential is invalid."
			w.Header().Set("WWW-Authenticate", "Bearer")
		case postgres.EnrollmentConflict:
			status, message = http.StatusConflict, "The enrollment identity conflicts with existing state."
		case postgres.InvalidRequest:
			status, message = http.StatusBadRequest, "The request is invalid."
		case postgres.UnsupportedSpec:
			status, message = http.StatusUnprocessableEntity, "The specification is not supported."
		case postgres.IdempotencyConflict:
			status, message = http.StatusConflict, "The idempotency key belongs to a different request."
		case postgres.NameConflict:
			status, message = http.StatusConflict, "The cluster name is already in use."
		case postgres.OperationConflict:
			status, message = http.StatusConflict, "Active work or changed storage state prevents this action. Review the cluster before retrying."
		case postgres.RevisionConflict:
			status, message = http.StatusPreconditionFailed, "The resource revision does not match this request."
		case postgres.NotFound:
			status, message = http.StatusNotFound, "The resource was not found."
		case postgres.OutcomeUnknown:
			message = "The result is uncertain. Retry with the same request identity and input."
		}
		s.writeError(w, r, status, string(failure.Code), message, failure.OperationID)
		return
	}
	// The response carries no detail, so this log is the only place the cause of a
	// 500 survives.
	s.logger.Error("store request failed", "request_id", r.Context().Value(identityKey{}).(requestIdentity).requestID, "method", r.Method, "path", r.URL.Path, "error", err)
	s.writeError(w, r, http.StatusInternalServerError, "internal_error", "The request could not be completed.", uuid.Nil)
}

func (s *Server) writeError(w http.ResponseWriter, r *http.Request, status int, code, message string, operationID uuid.UUID) {
	identity := r.Context().Value(identityKey{}).(requestIdentity)
	response := struct {
		Code        string    `json:"code"`
		Message     string    `json:"message"`
		RequestID   string    `json:"request_id"`
		OperationID uuid.UUID `json:"operation_id,omitzero"`
	}{code, message, identity.requestID, operationID}
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(response); err != nil {
		s.logger.Warn("write error response failed", "request_id", identity.requestID, "error", err)
	}
	s.logger.Info("API request rejected", "request_id", identity.requestID, "code", code, "status", status)
}

// SetAssets gives the server the built browser application.
// Call it before serving requests. A server with no assets answers the API alone.
func (s *Server) SetAssets(assets fs.FS) { s.assets = assets }
