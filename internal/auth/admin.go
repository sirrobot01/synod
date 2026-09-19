package auth

import (
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"net/http"
	"strings"
)

// ErrUnauthenticated reports a missing, malformed, or wrong administrator token.
// It never separates those cases, so a caller cannot probe for a valid token.
var ErrUnauthenticated = errors.New("authentication required")

// AdminToken authenticates the bootstrap administrator and retains only the digest.
type AdminToken struct {
	digest  [sha256.Size]byte
	actorID string
}

// NewAdminToken stores the digest of the administrator token and its actor ID.
func NewAdminToken(token, actorID string) (*AdminToken, error) {
	if len(token) < 32 || len(token) > 256 || strings.ContainsFunc(token, func(r rune) bool {
		return r < '!' || r > '~'
	}) {
		return nil, errors.New("admin token must contain 32 to 256 visible ASCII bytes")
	}
	if len(actorID) == 0 || len(actorID) > 256 || strings.ContainsFunc(actorID, func(r rune) bool {
		return r < '!' || r > '~'
	}) {
		return nil, errors.New("admin actor ID must contain 1 to 256 visible ASCII bytes")
	}
	return &AdminToken{digest: sha256.Sum256([]byte(token)), actorID: actorID}, nil
}

// Authenticate checks the Authorization bearer header and returns the actor ID.
// It compares digests in constant time.
func (a *AdminToken) Authenticate(request *http.Request) (string, error) {
	headers := request.Header.Values("Authorization")
	if len(headers) != 1 {
		return "", ErrUnauthenticated
	}
	scheme, token, ok := strings.Cut(headers[0], " ")
	if !ok || !strings.EqualFold(scheme, "Bearer") || len(token) < 32 || len(token) > 256 {
		return "", ErrUnauthenticated
	}
	digest := sha256.Sum256([]byte(token))
	if subtle.ConstantTimeCompare(a.digest[:], digest[:]) != 1 {
		return "", ErrUnauthenticated
	}
	return a.actorID, nil
}
