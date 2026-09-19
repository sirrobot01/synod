package auth

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"strings"
	"time"

	"github.com/google/uuid"
)

// ErrCredential reports a malformed token or session value.
var ErrCredential = errors.New("credential_malformed")

// tokenPrefix marks a Synod API token in a log or a secret scanner.
const tokenPrefix = "synod_"

const secretBytes = 32

// APIToken is one newly created token. Plaintext exists only in this value.
type APIToken struct {
	ID uuid.UUID
	// Plaintext is disclosed once, to its creator, and is never stored.
	Plaintext string
	Verifier  [32]byte
}

// NewAPIToken mints one token. The wire form carries its row identity,
// so verification reads exactly one row instead of scanning the table.
func NewAPIToken() (APIToken, error) {
	id, err := uuid.NewRandom()
	if err != nil {
		return APIToken{}, err
	}
	secret := make([]byte, secretBytes)
	if _, err := rand.Read(secret); err != nil {
		return APIToken{}, err
	}
	encoded := base64.RawURLEncoding.EncodeToString(secret)
	return APIToken{
		ID:        id,
		Plaintext: tokenPrefix + strings.ReplaceAll(id.String(), "-", "") + "_" + encoded,
		Verifier:  sha256.Sum256(secret),
	}, nil
}

// ParseAPIToken splits a presented token into its row identity and its verifier.
// It never reports whether the identity exists. Only the Store can answer that.
func ParseAPIToken(presented string) (uuid.UUID, [32]byte, error) {
	rest, found := strings.CutPrefix(presented, tokenPrefix)
	if !found {
		return uuid.Nil, [32]byte{}, ErrCredential
	}
	rawID, encoded, found := strings.Cut(rest, "_")
	if !found || len(rawID) != 32 {
		return uuid.Nil, [32]byte{}, ErrCredential
	}
	id, err := uuid.Parse(rawID)
	if err != nil || id == uuid.Nil {
		return uuid.Nil, [32]byte{}, ErrCredential
	}
	secret, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil || len(secret) != secretBytes {
		return uuid.Nil, [32]byte{}, ErrCredential
	}
	return id, sha256.Sum256(secret), nil
}

// SessionSecret is one newly minted browser session value.
type SessionSecret struct {
	// Cookie is the value that the browser holds. It is never stored.
	Cookie   string
	Verifier [32]byte
}

// NewSessionSecret mints one session value. The Store keeps only its verifier.
func NewSessionSecret() (SessionSecret, error) {
	secret := make([]byte, secretBytes)
	if _, err := rand.Read(secret); err != nil {
		return SessionSecret{}, err
	}
	return SessionSecret{Cookie: base64.RawURLEncoding.EncodeToString(secret), Verifier: sha256.Sum256(secret)}, nil
}

// SessionVerifier converts a presented cookie value into its stored verifier.
func SessionVerifier(cookie string) ([32]byte, error) {
	secret, err := base64.RawURLEncoding.DecodeString(cookie)
	if err != nil || len(secret) != secretBytes {
		return [32]byte{}, ErrCredential
	}
	return sha256.Sum256(secret), nil
}

// Session and account limits. The Store enforces them with database time.
const (
	// SessionIdleTimeout ends a session that nobody has used.
	SessionIdleTimeout = 30 * time.Minute
	// SessionLifetime ends a session at a fixed age. Use does not extend it.
	SessionLifetime = 12 * time.Hour
	// MaxFailedAttempts is the consecutive failure count that locks an account.
	MaxFailedAttempts = 10
	// LockoutDuration is how long a locked account refuses a correct password.
	LockoutDuration = 15 * time.Minute
)
