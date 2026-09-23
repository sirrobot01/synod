package api

import (
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/auth"
	"github.com/sirrobot01/synod/internal/store/postgres"
)

// SessionCookie is the browser cookie that carries one session.
// The __Host- prefix forbids a Domain attribute, so the cookie is pinned to one origin.
const SessionCookie = "__Host-synod_session"

// authenticateAny accepts a session cookie, an API token, or the bootstrap token.
// It records which credential answered, because a cookie is ambient and needs
// an origin proof before it may change state.
func (s *Server) authenticateAny(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		identity := r.Context().Value(identityKey{}).(requestIdentity)
		if cookie, err := r.Cookie(SessionCookie); err == nil && cookie.Value != "" {
			user, session, sessionErr := s.signedIn(r, cookie.Value)
			if sessionErr == nil {
				identity.actorID, identity.role = user.Username, user.Role
				identity.userID, identity.sessionID, identity.cookie = user.ID, session.ID, true
				next.ServeHTTP(w, withIdentity(r, identity))
				return
			}
			// A stale cookie must not hide a bearer credential that the caller also sent.
			if _, bearerErr := bearer(r); bearerErr != nil {
				s.credentialError(w, r, sessionErr)
				return
			}
		}
		presented, err := bearer(r)
		if err != nil {
			w.Header().Set("WWW-Authenticate", "Bearer")
			s.writeError(w, r, http.StatusUnauthorized, "unauthenticated", "Authentication is required.", uuid.Nil)
			return
		}
		// A Synod token carries its row identity. Anything else is the bootstrap secret.
		if id, verifier, err := auth.ParseAPIToken(presented); err == nil {
			token, err := s.store.AuthenticateAPIToken(r.Context(), s.installationID, id, verifier)
			if err != nil {
				s.credentialError(w, r, err)
				return
			}
			identity.actorID, identity.role = token.ActorID, token.Role
			next.ServeHTTP(w, withIdentity(r, identity))
			return
		}
		actorID, err := s.admin.Authenticate(r)
		if err != nil {
			w.Header().Set("WWW-Authenticate", "Bearer")
			s.writeError(w, r, http.StatusUnauthorized, "unauthenticated", "Authentication is required.", uuid.Nil)
			return
		}
		identity.actorID, identity.role = actorID, "admin"
		next.ServeHTTP(w, withIdentity(r, identity))
	})
}

// requireAdmin separates authentication from authorization.
// A valid credential with an insufficient role is forbidden, not unauthenticated.
func (s *Server) requireAdmin(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Context().Value(identityKey{}).(requestIdentity).role != "admin" {
			s.writeError(w, r, http.StatusForbidden, "forbidden", "The credential may not perform this action.", uuid.Nil)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// requireOrigin stops a cross-site request that rides on an ambient cookie.
// A bearer credential is exempt, because another site cannot attach it.
func (s *Server) requireOrigin(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		identity := r.Context().Value(identityKey{}).(requestIdentity)
		unsafe := !slices.Contains([]string{http.MethodGet, http.MethodHead, http.MethodOptions}, r.Method)
		if !identity.cookie || !unsafe || s.sameOrigin(r) {
			next.ServeHTTP(w, r)
			return
		}
		s.writeError(w, r, http.StatusForbidden, "cross_origin", "A session request must prove its origin.", uuid.Nil)
	})
}

// requireBrowserOrigin demands an origin proof from every unsafe method on a route,
// with or without a credential. Sign-in uses it, because it sets the session cookie.
func (s *Server) requireBrowserOrigin(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if slices.Contains([]string{http.MethodGet, http.MethodHead, http.MethodOptions}, r.Method) || s.sameOrigin(r) {
			next.ServeHTTP(w, r)
			return
		}
		s.writeError(w, r, http.StatusForbidden, "cross_origin", "A sign-in request must prove its origin.", uuid.Nil)
	})
}

// sameOrigin accepts a request that the browser reports as same-origin,
// or one whose Origin equals the configured public origin.
// A request that proves neither is refused. Absence is never a proof.
func (s *Server) sameOrigin(r *http.Request) bool {
	if r.Header.Get("Sec-Fetch-Site") == "same-origin" {
		return true
	}
	origin := r.Header.Get("Origin")
	return origin != "" && s.PublicOrigin != "" && origin == s.PublicOrigin
}

func (s *Server) signedIn(r *http.Request, cookie string) (postgres.User, postgres.Session, error) {
	verifier, err := auth.SessionVerifier(cookie)
	if err != nil {
		return postgres.User{}, postgres.Session{}, postgres.ErrCredentials
	}
	return s.store.AuthenticateSession(r.Context(), s.installationID, verifier)
}

// credentialError keeps 401 for an unusable credential and 403 for a refused account.
func (s *Server) credentialError(w http.ResponseWriter, r *http.Request, err error) {
	if errors.Is(err, postgres.ErrAccountDisabled) {
		s.writeError(w, r, http.StatusForbidden, "account_disabled", "The account cannot sign in.", uuid.Nil)
		return
	}
	if errors.Is(err, postgres.ErrCredentials) {
		s.clearSession(w)
		w.Header().Set("WWW-Authenticate", "Bearer")
		s.writeError(w, r, http.StatusUnauthorized, "unauthenticated", "Authentication is required.", uuid.Nil)
		return
	}
	s.storeError(w, r, err)
}

func bearer(r *http.Request) (string, error) {
	values := r.Header.Values("Authorization")
	if len(values) != 1 {
		return "", errors.New("one Authorization header is required")
	}
	value, found := strings.CutPrefix(values[0], "Bearer ")
	if !found || value == "" {
		return "", errors.New("the Authorization header must present a bearer credential")
	}
	return value, nil
}

func (s *Server) setSession(w http.ResponseWriter, value string) {
	http.SetCookie(w, &http.Cookie{
		Name: SessionCookie, Value: value, Path: "/",
		HttpOnly: true, Secure: true, SameSite: http.SameSiteStrictMode,
	})
}

func (s *Server) clearSession(w http.ResponseWriter) {
	http.SetCookie(w, &http.Cookie{
		Name: SessionCookie, Value: "", Path: "/", MaxAge: -1,
		HttpOnly: true, Secure: true, SameSite: http.SameSiteStrictMode,
	})
}

// createSession signs one operator in and issues a new session identity.
// The client never chooses the identity, so a fixed value cannot be planted.
func (s *Server) createSession(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Username string `json:"username"`
		Password string `json:"password"`
	}
	if !s.decodeJSON(w, r, &body) {
		return
	}
	if !s.signInAttempts.allow(strings.ToLower(body.Username), clientAddress(r)) || !s.signInLimit.Allow() {
		w.Header().Set("Retry-After", "1")
		s.writeError(w, r, http.StatusTooManyRequests, "rate_limited", "Retry after the indicated delay.", uuid.Nil)
		return
	}
	user, err := s.store.SignIn(r.Context(), s.installationID, strings.ToLower(body.Username), body.Password)
	if err != nil {
		s.credentialError(w, r, err)
		return
	}
	secret, err := auth.NewSessionSecret()
	if err != nil {
		s.logger.Error("session secret unavailable", "user_id", user.ID, "error", err)
		s.writeError(w, r, http.StatusServiceUnavailable, "dependency_unavailable", "The session could not be created.", uuid.Nil)
		return
	}
	if _, err := s.store.CreateSession(r.Context(), s.installationID, user.ID, secret.Verifier); err != nil {
		s.storeError(w, r, err)
		return
	}
	s.setSession(w, secret.Cookie)
	w.WriteHeader(http.StatusCreated)
	s.writeJSON(w, r, user)
	s.logger.Info("operator signed in", "actor_id", user.Username, "user_id", user.ID)
}

func (s *Server) readSession(w http.ResponseWriter, r *http.Request) {
	identity := r.Context().Value(identityKey{}).(requestIdentity)
	s.writeJSON(w, r, map[string]any{"actor_id": identity.actorID, "role": identity.role, "user_id": identity.userID})
}

func (s *Server) deleteSession(w http.ResponseWriter, r *http.Request) {
	identity := r.Context().Value(identityKey{}).(requestIdentity)
	if identity.sessionID != uuid.Nil {
		if err := s.store.RevokeSession(r.Context(), s.installationID, identity.sessionID); err != nil {
			s.storeError(w, r, err)
			return
		}
	}
	s.clearSession(w)
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) createUser(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Username string `json:"username"`
		Password string `json:"password"`
	}
	if !s.decodeJSON(w, r, &body) {
		return
	}
	username := strings.ToLower(body.Username)
	if !validUsername(username) {
		s.writeError(w, r, http.StatusBadRequest, "invalid_request", "Use 1 to 64 lowercase letters, digits, hyphen, underscore, or dot.", uuid.Nil)
		return
	}
	hash, err := s.hashPassword(w, r, body.Password)
	if err != nil {
		return
	}
	user, err := s.store.CreateUser(r.Context(), s.installationID, username, hash, "admin")
	if errors.Is(err, postgres.ErrUsernameTaken) {
		s.writeError(w, r, http.StatusConflict, "username_taken", "The username is already in use.", uuid.Nil)
		return
	}
	if err != nil {
		s.storeError(w, r, err)
		return
	}
	w.WriteHeader(http.StatusCreated)
	s.writeJSON(w, r, user)
	s.logger.Info("operator account created", "actor_id", r.Context().Value(identityKey{}).(requestIdentity).actorID, "username", username)
}

func (s *Server) setUserPassword(w http.ResponseWriter, r *http.Request) {
	identity := r.Context().Value(identityKey{}).(requestIdentity)
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil || id == uuid.Nil {
		s.writeError(w, r, http.StatusBadRequest, "invalid_request", "Use a valid user ID.", uuid.Nil)
		return
	}
	// An operator may change its own password. Any other account needs the admin role.
	if id != identity.userID && identity.role != "admin" {
		s.writeError(w, r, http.StatusForbidden, "forbidden", "The credential may not change this password.", uuid.Nil)
		return
	}
	var body struct {
		CurrentPassword string `json:"current_password"`
		Password        string `json:"password"`
	}
	if !s.decodeJSON(w, r, &body) {
		return
	}
	// A stolen session must not be enough to take an account over, and this call
	// evicts every other session, so self-service needs the current password.
	// An administrator resetting another account has no password to supply.
	if id == identity.userID {
		if _, err := s.store.SignIn(r.Context(), s.installationID, identity.actorID, body.CurrentPassword); err != nil {
			s.writeError(w, r, http.StatusForbidden, "reauthentication_required", "Supply the current password.", uuid.Nil)
			return
		}
	}
	hash, err := s.hashPassword(w, r, body.Password)
	if err != nil {
		return
	}
	if err := s.store.SetUserPassword(r.Context(), s.installationID, id, hash, identity.sessionID); err != nil {
		s.storeError(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
	s.logger.Info("password changed", "actor_id", identity.actorID, "user_id", id)
}

func (s *Server) disableUser(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil || id == uuid.Nil {
		s.writeError(w, r, http.StatusBadRequest, "invalid_request", "Use a valid user ID.", uuid.Nil)
		return
	}
	if err := s.store.DisableUser(r.Context(), s.installationID, id); err != nil {
		s.storeError(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
	s.logger.Info("operator account disabled", "actor_id", r.Context().Value(identityKey{}).(requestIdentity).actorID, "user_id", id)
}

func (s *Server) createToken(w http.ResponseWriter, r *http.Request) {
	identity := r.Context().Value(identityKey{}).(requestIdentity)
	var body struct {
		Name      string     `json:"name"`
		ActorID   string     `json:"actor_id"`
		ExpiresAt *time.Time `json:"expires_at"`
	}
	if !s.decodeJSON(w, r, &body) {
		return
	}
	if body.ActorID == "" {
		body.ActorID = identity.actorID
	}
	if len(body.Name) < 1 || len(body.Name) > 64 {
		s.writeError(w, r, http.StatusBadRequest, "invalid_request", "Use a name of 1 to 64 bytes.", uuid.Nil)
		return
	}
	if len(body.ActorID) > 256 {
		s.writeError(w, r, http.StatusBadRequest, "invalid_request", "Use an actor identity of at most 256 bytes.", uuid.Nil)
		return
	}
	minted, err := auth.NewAPIToken()
	if err != nil {
		s.logger.Error("API token secret unavailable", "actor_id", identity.actorID, "error", err)
		s.writeError(w, r, http.StatusServiceUnavailable, "dependency_unavailable", "The token could not be created.", uuid.Nil)
		return
	}
	token, err := s.store.CreateAPIToken(r.Context(), s.installationID, minted.ID, body.ActorID, body.Name, "admin", minted.Verifier, body.ExpiresAt)
	if err != nil {
		s.storeError(w, r, err)
		return
	}
	w.WriteHeader(http.StatusCreated)
	// The plaintext appears here once and is never stored or logged.
	s.writeJSON(w, r, struct {
		postgres.APIToken
		Token string `json:"token"`
	}{token, minted.Plaintext})
	s.logger.Info("API token created", "actor_id", identity.actorID, "token_id", token.ID, "token_actor_id", token.ActorID)
}

func (s *Server) listTokens(w http.ResponseWriter, r *http.Request) {
	tokens, err := s.store.ListAPITokens(r.Context(), s.installationID)
	if err != nil {
		s.storeError(w, r, err)
		return
	}
	s.writeJSON(w, r, tokens)
}

func (s *Server) revokeToken(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil || id == uuid.Nil {
		s.writeError(w, r, http.StatusBadRequest, "invalid_request", "Use a valid token ID.", uuid.Nil)
		return
	}
	if err := s.store.RevokeAPIToken(r.Context(), s.installationID, id); err != nil {
		s.storeError(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
	s.logger.Info("API token revoked", "actor_id", r.Context().Value(identityKey{}).(requestIdentity).actorID, "token_id", id)
}

// hashPassword separates a rejected password from a failure of this server.
// Reporting a randomness failure as a bad password sends an operator chasing nothing.
func (s *Server) hashPassword(w http.ResponseWriter, r *http.Request, password string) (string, error) {
	hash, err := auth.HashPassword(password)
	if errors.Is(err, auth.ErrPasswordLength) {
		s.writeError(w, r, http.StatusBadRequest, "invalid_request", "Use a password of at most 1024 bytes.", uuid.Nil)
		return "", err
	}
	if err != nil {
		s.writeError(w, r, http.StatusServiceUnavailable, "dependency_unavailable", "The password could not be stored.", uuid.Nil)
		return "", err
	}
	return hash, nil
}

// clientAddress is the peer address of one request.
// It is the direct peer. A deployment behind a proxy needs a reviewed forwarding contract.
func clientAddress(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

func validUsername(value string) bool {
	if len(value) < 1 || len(value) > 64 {
		return false
	}
	for _, r := range value {
		if (r < 'a' || r > 'z') && (r < '0' || r > '9') && r != '.' && r != '_' && r != '-' {
			return false
		}
	}
	return true
}

func (s *Server) writeJSON(w http.ResponseWriter, r *http.Request, value any) {
	if err := json.NewEncoder(w).Encode(value); err != nil {
		s.logger.Warn("write response failed", "request_id", r.Context().Value(identityKey{}).(requestIdentity).requestID, "error", err)
	}
}
