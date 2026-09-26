// Package jointoken encodes the one string an agent needs to join an installation.
package jointoken

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strings"

	"github.com/google/uuid"
)

const prefix = "synod_join_"

// Token names the API origin, the API CA pin, and the one-use enrollment secret.
type Token struct {
	// Address is the API host and port that serves POST /v1/enrollments.
	Address        string    `json:"address"`
	InstallationID uuid.UUID `json:"installation_id"`
	// Fingerprint is Fingerprint of the API CA certificate.
	Fingerprint string `json:"fingerprint"`
	// Secret is 43 characters of raw URL base64 over 32 random bytes.
	Secret string `json:"secret"`
}

// NewSecret returns a secret in the form the enrollment API accepts.
func NewSecret() string {
	secret := make([]byte, 32)
	rand.Read(secret) // crypto/rand.Read never returns an error.
	return base64.RawURLEncoding.EncodeToString(secret)
}

// Fingerprint returns sha256:<hex> over one DER certificate.
func Fingerprint(der []byte) string {
	sum := sha256.Sum256(der)
	return "sha256:" + hex.EncodeToString(sum[:])
}

// Encode formats a token as one string.
func Encode(token Token) string {
	data, _ := json.Marshal(token)
	return prefix + base64.RawURLEncoding.EncodeToString(data)
}

// Decode parses and validates an encoded token.
func Decode(encoded string) (Token, error) {
	raw, found := strings.CutPrefix(strings.TrimSpace(encoded), prefix)
	if !found {
		return Token{}, errors.New("the join token does not start with " + prefix)
	}
	data, err := base64.RawURLEncoding.DecodeString(raw)
	if err != nil {
		return Token{}, fmt.Errorf("decode the join token: %w", err)
	}
	var token Token
	if err := json.Unmarshal(data, &token); err != nil {
		return Token{}, fmt.Errorf("decode the join token: %w", err)
	}
	if host, port, err := net.SplitHostPort(token.Address); err != nil || host == "" || port == "" {
		return Token{}, errors.New("the join token address must be host:port")
	}
	if token.InstallationID == uuid.Nil {
		return Token{}, errors.New("the join token has no installation ID")
	}
	digest, found := strings.CutPrefix(token.Fingerprint, "sha256:")
	if decoded, err := hex.DecodeString(digest); !found || err != nil || len(decoded) != sha256.Size {
		return Token{}, errors.New("the join token fingerprint must be sha256:<64 hex digits>")
	}
	if secret, err := base64.RawURLEncoding.Strict().DecodeString(token.Secret); err != nil || len(secret) != 32 {
		return Token{}, errors.New("the join token secret must be raw URL base64 over 32 bytes")
	}
	return token, nil
}
