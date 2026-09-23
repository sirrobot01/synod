package secret

import (
	"crypto/rand"
	"encoding/base64"
)

// PasswordBytes is the entropy behind one generated password.
const PasswordBytes = 32

// NewPassword returns a fresh password for a database role. The alphabet is base64url
// because the value passes through a container environment, an engine's own parser,
// and a terminal, and a quote or a backslash in any of those is an escaping bug.
//
// It returns an error rather than a short password when the reader fails: a weak
// credential that looks strong is worse than none.
func NewPassword() (string, error) {
	raw := make([]byte, PasswordBytes)
	if _, err := rand.Read(raw); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(raw), nil
}
