package auth

import (
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"strings"
	"testing"

	"github.com/google/uuid"
)

func TestAPITokenCarriesItsIdentity(t *testing.T) {
	token, err := NewAPIToken()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(token.Plaintext, "synod_") {
		t.Fatalf("plaintext %q has no scanner prefix", token.Plaintext)
	}
	id, verifier, err := ParseAPIToken(token.Plaintext)
	if err != nil {
		t.Fatal(err)
	}
	if id != token.ID || verifier != token.Verifier {
		t.Fatal("a parsed token does not match the minted one")
	}
	// Two tokens never share a secret or an identity.
	other, err := NewAPIToken()
	if err != nil {
		t.Fatal(err)
	}
	if other.ID == token.ID || other.Verifier == token.Verifier {
		t.Fatal("two tokens collided")
	}
	// The plaintext secret is not recoverable from the stored verifier.
	if strings.Contains(token.Plaintext, base64.RawURLEncoding.EncodeToString(token.Verifier[:])) {
		t.Fatal("the plaintext carries its own verifier")
	}
}

func TestAPITokenRejectsMalformedInput(t *testing.T) {
	token, err := NewAPIToken()
	if err != nil {
		t.Fatal(err)
	}
	body := strings.TrimPrefix(token.Plaintext, "synod_")
	rawID, secret, _ := strings.Cut(body, "_")
	for name, presented := range map[string]string{
		"empty":         "",
		"no prefix":     body,
		"other prefix":  "github_" + body,
		"no separator":  "synod_" + rawID + secret,
		"short id":      "synod_" + rawID[:31] + "_" + secret,
		"unparsable id": "synod_" + strings.Repeat("z", 32) + "_" + secret,
		"nil id":        "synod_" + strings.Repeat("0", 32) + "_" + secret,
		"short secret":  "synod_" + rawID + "_" + secret[:10],
		"broken secret": "synod_" + rawID + "_" + strings.Repeat("!", len(secret)),
	} {
		if _, _, err := ParseAPIToken(presented); !errors.Is(err, ErrCredential) {
			t.Fatalf("%s: accepted", name)
		}
	}
	// A correct identity with another secret parses, and its verifier differs.
	_, verifier, err := ParseAPIToken("synod_" + rawID + "_" + base64.RawURLEncoding.EncodeToString(make([]byte, 32)))
	if err != nil {
		t.Fatal(err)
	}
	if verifier == token.Verifier {
		t.Fatal("another secret produced the same verifier")
	}
}

func TestSessionSecretStoresOnlyItsVerifier(t *testing.T) {
	secret, err := NewSessionSecret()
	if err != nil {
		t.Fatal(err)
	}
	verifier, err := SessionVerifier(secret.Cookie)
	if err != nil || verifier != secret.Verifier {
		t.Fatalf("verifier %v: %v", verifier, err)
	}
	raw, err := base64.RawURLEncoding.DecodeString(secret.Cookie)
	if err != nil || len(raw) != 32 {
		t.Fatalf("cookie is not 256 bits: %v", err)
	}
	if sha256.Sum256(raw) != secret.Verifier {
		t.Fatal("the verifier is not the hash of the cookie value")
	}
	for name, cookie := range map[string]string{
		"empty":  "",
		"short":  secret.Cookie[:10],
		"broken": strings.Repeat("!", 43),
	} {
		if _, err := SessionVerifier(cookie); !errors.Is(err, ErrCredential) {
			t.Fatalf("%s: accepted", name)
		}
	}
	other, err := NewSessionSecret()
	if err != nil {
		t.Fatal(err)
	}
	if other.Cookie == secret.Cookie {
		t.Fatal("two sessions share one value")
	}
	_ = uuid.Nil
}
