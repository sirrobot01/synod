package auth

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"

	"golang.org/x/crypto/argon2"
)

// ErrPassword reports a password that does not match its stored hash.
var ErrPassword = errors.New("password_mismatch")

// ErrPasswordLength reports a password longer than the accepted length.
var ErrPasswordLength = errors.New("password_length")

// MaxPasswordBytes bounds the hashing cost of one request.
// There is no lower bound. The strength of a password is the operator's choice,
// and this bound exists to cap work, not to enforce a policy.
const MaxPasswordBytes = 1024

// PasswordParameters are the Argon2id cost settings of one hash.
type PasswordParameters struct {
	Memory      uint32
	Iterations  uint32
	Parallelism uint8
}

// CurrentPasswordParameters are the settings that a new hash uses.
var CurrentPasswordParameters = PasswordParameters{Memory: 19456, Iterations: 2, Parallelism: 1}

const saltBytes = 16
const keyBytes = 32

// HashPassword returns one PHC-encoded Argon2id hash of the password.
// The encoding carries its own parameters, so a later change can still read it.
func HashPassword(password string) (string, error) {
	if len(password) > MaxPasswordBytes {
		return "", ErrPasswordLength
	}
	salt := make([]byte, saltBytes)
	if _, err := rand.Read(salt); err != nil {
		return "", err
	}
	return encodePassword(CurrentPasswordParameters, salt, derive(password, salt, CurrentPasswordParameters)), nil
}

// CheckPassword compares a password against its stored hash.
// It reports whether the stored hash uses settings other than the current ones.
// A caller that holds a correct password should then store a fresh hash.
func CheckPassword(encoded, password string) (stale bool, err error) {
	parameters, salt, want, err := decodePassword(encoded)
	if err != nil {
		return false, err
	}
	if len(password) > MaxPasswordBytes {
		return false, ErrPassword
	}
	if subtle.ConstantTimeCompare(derive(password, salt, parameters), want) != 1 {
		return false, ErrPassword
	}
	return parameters != CurrentPasswordParameters, nil
}

func derive(password string, salt []byte, p PasswordParameters) []byte {
	return argon2.IDKey([]byte(password), salt, p.Iterations, p.Memory, p.Parallelism, keyBytes)
}

func encodePassword(p PasswordParameters, salt, key []byte) string {
	raw := base64.RawStdEncoding
	return fmt.Sprintf("$argon2id$v=%d$m=%d,t=%d,p=%d$%s$%s", argon2.Version, p.Memory, p.Iterations, p.Parallelism, raw.EncodeToString(salt), raw.EncodeToString(key))
}

func decodePassword(encoded string) (PasswordParameters, []byte, []byte, error) {
	var p PasswordParameters
	fields := strings.Split(encoded, "$")
	if len(fields) != 6 || fields[0] != "" || fields[1] != "argon2id" {
		return p, nil, nil, ErrPassword
	}
	var version int
	if _, err := fmt.Sscanf(fields[2], "v=%d", &version); err != nil || version != argon2.Version {
		return p, nil, nil, ErrPassword
	}
	if _, err := fmt.Sscanf(fields[3], "m=%d,t=%d,p=%d", &p.Memory, &p.Iterations, &p.Parallelism); err != nil {
		return p, nil, nil, ErrPassword
	}
	if p.Memory == 0 || p.Iterations == 0 || p.Parallelism == 0 {
		return p, nil, nil, ErrPassword
	}
	raw := base64.RawStdEncoding
	salt, saltErr := raw.DecodeString(fields[4])
	key, keyErr := raw.DecodeString(fields[5])
	if saltErr != nil || keyErr != nil || len(salt) == 0 || len(key) == 0 {
		return p, nil, nil, ErrPassword
	}
	return p, salt, key, nil
}

// decoyHash answers an unknown username with the same work as a wrong password.
// Response timing must not report whether an account exists.
var decoyHash = func() string {
	salt := make([]byte, saltBytes)
	if _, err := rand.Read(salt); err != nil {
		panic("auth: no randomness for the decoy hash")
	}
	return encodePassword(CurrentPasswordParameters, salt, derive(strings.Repeat("decoy", 8), salt, CurrentPasswordParameters))
}()

// SpendUnknownUser performs the work that a real check would perform.
// Call it when no account matches, then report the same failure as a wrong password.
func SpendUnknownUser(password string) {
	_, _ = CheckPassword(decoyHash, password)
}
