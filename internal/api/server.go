package api

import (
	"context"
	"encoding/json"
	"errors"
	"io/fs"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/auth"
	"github.com/sirrobot01/synod/internal/pki"
	"github.com/sirrobot01/synod/internal/secret"
	"github.com/sirrobot01/synod/internal/store"
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
	// CredentialKeyring wraps destination secrets. Set it before serving requests.
	CredentialKeyring *secret.Keyring
	requestLimit      *rate.Limiter
	enrollmentLimit   *rate.Limiter
	signInLimit       *rate.Limiter
	signInAttempts    *attemptLimiter
	// assets is the built browser application. A nil value serves the API alone.
	assets fs.FS
	// PublicOrigin is the browser origin of this API. Set it before serving requests.
	// An empty value makes a cookie request rely on Sec-Fetch-Site alone.
	PublicOrigin string
	// AgentListen is the controller agent transport listen address. Enrollment
	// returns it to the agent. An unspecified host becomes the host the agent dialed.
	AgentListen    string
	store          *store.Store
	admin          *auth.AdminToken
	installationID uuid.UUID
	logger         *slog.Logger
	router         chi.Router
}

// NewServer builds the API server and registers its routes.
func NewServer(metadata *store.Store, admin *auth.AdminToken, installationID uuid.UUID, logger *slog.Logger) (*Server, error) {
	if metadata == nil || admin == nil || installationID == uuid.Nil || logger == nil {
		return nil, errors.New("API requires a Store, administrator verifier, installation ID, and logger")
	}
	server := &Server{requestLimit: rate.NewLimiter(100, 200), enrollmentLimit: rate.NewLimiter(10, 20), signInLimit: rate.NewLimiter(1, 10), signInAttempts: newAttemptLimiter(rate.Every(2*time.Second), 5, 4096), store: metadata, admin: admin, installationID: installationID, logger: logger}
	router := chi.NewRouter()
	router.Use(server.begin)
	router.NotFound(func(w http.ResponseWriter, r *http.Request) {
		// The API answers JSON for its own namespace. Every other path belongs to
		// the browser application, which routes the rest itself.
		if strings.HasPrefix(r.URL.Path, "/v1/") || r.URL.Path == "/v1" {
			server.writeError(w, r, http.StatusNotFound, "not_found", "The endpoint was not found.")
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
		server.writeError(w, r, http.StatusMethodNotAllowed, "method_not_allowed", "The endpoint does not support this method.")
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
				admin.Get("/apps", server.listApps)
				admin.Post("/destinations", server.addDestination)
				admin.Get("/destinations", server.listDestinations)
				admin.Delete("/destinations/{id}", server.removeDestination)
				admin.Post("/backups", server.createBackup)
				admin.Get("/protection", server.getProtection)
				admin.Put("/protection", server.updateProtection)
				admin.Get("/operations", server.listOperations)
				admin.Get("/operations/{id}", server.getOperation)
				admin.Get("/nodes", server.listNodes)
				admin.Get("/nodes/by-name/{name}", server.getNodeByName)
				admin.Get("/nodes/{id}", server.getNode)
				admin.Post("/nodes/{id}/actions/approve", server.approveNode)
				admin.Get("/installation", server.getInstallation)
				admin.Post("/enrollment-tokens", server.registerEnrollmentToken)
				admin.Post("/nodes/{id}/certificates/{certificate_id}/actions/revoke", server.revokeAgentCertificate)
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
			s.writeError(w, r, http.StatusTooManyRequests, "rate_limited", "Retry after the indicated delay.")
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
			s.writeError(w, r, http.StatusTooManyRequests, "rate_limited", "Retry after the indicated delay.")
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (s *Server) storeError(w http.ResponseWriter, r *http.Request, err error) {
	if failure, ok := errors.AsType[*store.Error](err); ok {
		status, message := http.StatusServiceUnavailable, "The metadata service is unavailable."
		switch failure.Code {
		case store.EnrollmentUnauthorized:
			status, message = http.StatusUnauthorized, "The enrollment credential is invalid."
			w.Header().Set("WWW-Authenticate", "Bearer")
		case store.EnrollmentConflict:
			status, message = http.StatusConflict, "The enrollment identity conflicts with existing state."
		case store.InvalidRequest:
			status, message = http.StatusBadRequest, "The request is invalid."
		case store.IdempotencyConflict:
			status, message = http.StatusConflict, "The idempotency key belongs to a different request."
		case store.NotFound:
			status, message = http.StatusNotFound, "The resource was not found."
		case store.Ambiguous:
			status, message = http.StatusConflict, "More than one resource has this name. Use its ID."
		case store.BackupInitializing:
			status, message = http.StatusConflict, "A repository is being initialized. Wait for its agent to confirm the connection, then retry the backup."
		case store.PlanConflict:
			status, message = http.StatusConflict, "The protection plan changed. Read the plan and confirm its current revision."
		case store.OperationConflict:
			status, message = http.StatusConflict, "The operation state or device approval conflicts with this request. Read the current state."
		case store.NodeFactsUnavailable:
			status, message = http.StatusConflict, "The node has not connected yet. Start its agent, then approve it."
		}
		if status == http.StatusServiceUnavailable {
			s.logger.Error("store request failed", "request_id", r.Context().Value(identityKey{}).(requestIdentity).requestID, "method", r.Method, "path", r.URL.Path, "error", errors.Unwrap(err))
		}
		s.writeError(w, r, status, string(failure.Code), message)
		return
	}
	// The response carries no detail, so this log is the only place the cause of a
	// 500 survives.
	s.logger.Error("store request failed", "request_id", r.Context().Value(identityKey{}).(requestIdentity).requestID, "method", r.Method, "path", r.URL.Path, "error", err)
	s.writeError(w, r, http.StatusInternalServerError, "internal_error", "The request could not be completed.")
}
func (s *Server) writeError(w http.ResponseWriter, r *http.Request, status int, code, message string) {
	identity := r.Context().Value(identityKey{}).(requestIdentity)
	response := struct {
		Code      string `json:"code"`
		Message   string `json:"message"`
		RequestID string `json:"request_id"`
	}{code, message, identity.requestID}
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(response); err != nil {
		s.logger.Warn("write error response failed", "request_id", identity.requestID, "error", err)
	}
	s.logger.Info("API request rejected", "request_id", identity.requestID, "code", code, "status", status)
}

// SetAssets gives the server the built browser application.
// Call it before serving requests. A server with no assets answers the API alone.
func (s *Server) SetAssets(assets fs.FS) { s.assets = assets }
