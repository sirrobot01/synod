package auth

import (
	"errors"
	"strings"
	"testing"
	"time"
)

func TestPasswordRoundTripAndMismatch(t *testing.T) {
	encoded, err := HashPassword("correct horse battery")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(encoded, "$argon2id$v=19$m=19456,t=2,p=1$") {
		t.Fatalf("encoding %q", encoded)
	}
	stale, err := CheckPassword(encoded, "correct horse battery")
	if err != nil || stale {
		t.Fatalf("stale=%v: %v", stale, err)
	}
	if _, err := CheckPassword(encoded, "correct horse batterz"); !errors.Is(err, ErrPassword) {
		t.Fatalf("a wrong password was accepted: %v", err)
	}
	// Two hashes of one password differ, because each one carries a fresh salt.
	other, err := HashPassword("correct horse battery")
	if err != nil {
		t.Fatal(err)
	}
	if other == encoded {
		t.Fatal("two hashes of one password are identical, so the salt is not random")
	}
}

// A short password is accepted. The server applies no minimum length, so the
// strength of a password is the operator's choice.
func TestShortPasswordIsAccepted(t *testing.T) {
	for _, password := range []string{"", "a", "short"} {
		encoded, err := HashPassword(password)
		if err != nil {
			t.Fatalf("a password of %d bytes was refused: %v", len(password), err)
		}
		if _, err := CheckPassword(encoded, password); err != nil {
			t.Fatalf("a password of %d bytes did not verify: %v", len(password), err)
		}
		// A short password still has to be the right one.
		if _, err := CheckPassword(encoded, password+"x"); !errors.Is(err, ErrPassword) {
			t.Fatalf("a wrong password of %d bytes verified", len(password)+1)
		}
	}
}

func TestPasswordLengthBounds(t *testing.T) {
	if _, err := HashPassword(strings.Repeat("a", MaxPasswordBytes+1)); !errors.Is(err, ErrPasswordLength) {
		t.Fatal("an unbounded password was hashed")
	}
	encoded, err := HashPassword(strings.Repeat("a", MaxPasswordBytes))
	if err != nil {
		t.Fatal(err)
	}
	// An oversized candidate must be refused before it reaches the hash function.
	// A mismatch alone proves nothing, because an unbounded candidate also mismatches.
	// The bound exists to cap the work that one request can demand, so measure that.
	oversized := strings.Repeat("a", MaxPasswordBytes+1)
	if _, err := CheckPassword(encoded, oversized); !errors.Is(err, ErrPassword) {
		t.Fatal("an unbounded candidate was accepted")
	}
	start := time.Now()
	_, _ = CheckPassword(encoded, oversized)
	refused := time.Since(start)
	start = time.Now()
	_, _ = CheckPassword(encoded, "wrong password here")
	hashed := time.Since(start)
	if refused*5 > hashed {
		t.Fatalf("refusing an oversized candidate took %v against %v for a hashed one", refused, hashed)
	}
}

// A stored hash carries its own settings, so a parameter change stays readable.
func TestPasswordReportsStaleParameters(t *testing.T) {
	previous := CurrentPasswordParameters
	CurrentPasswordParameters = PasswordParameters{Memory: 8192, Iterations: 1, Parallelism: 1}
	old, err := HashPassword("correct horse battery")
	if err != nil {
		t.Fatal(err)
	}
	CurrentPasswordParameters = previous
	stale, err := CheckPassword(old, "correct horse battery")
	if err != nil {
		t.Fatalf("a hash with older settings became unreadable: %v", err)
	}
	if !stale {
		t.Fatal("a hash with older settings was not reported stale")
	}
	fresh, err := HashPassword("correct horse battery")
	if err != nil {
		t.Fatal(err)
	}
	if stale, err := CheckPassword(fresh, "correct horse battery"); err != nil || stale {
		t.Fatalf("a current hash was reported stale: %v", err)
	}
}

func TestPasswordRejectsMalformedEncoding(t *testing.T) {
	valid, err := HashPassword("correct horse battery")
	if err != nil {
		t.Fatal(err)
	}
	for name, encoded := range map[string]string{
		"empty":           "",
		"not phc":         "argon2id-hash",
		"other algorithm": strings.Replace(valid, "argon2id", "argon2i", 1),
		"other version":   strings.Replace(valid, "v=19", "v=18", 1),
		"no parameters":   strings.Replace(valid, "m=19456,t=2,p=1", "", 1),
		"zero memory":     strings.Replace(valid, "m=19456", "m=0", 1),
		"broken salt":     strings.Replace(valid, "$argon2id$", "$argon2id$", 1)[:strings.LastIndex(valid, "$")] + "$!!!",
	} {
		if _, err := CheckPassword(encoded, "correct horse battery"); !errors.Is(err, ErrPassword) {
			t.Fatalf("%s: accepted", name)
		}
	}
}

// An unknown username must cost what a wrong password costs.
// A faster answer tells an attacker which accounts exist.
func TestUnknownUserSpendsComparableWork(t *testing.T) {
	encoded, err := HashPassword("correct horse battery")
	if err != nil {
		t.Fatal(err)
	}
	known := func() time.Duration {
		start := time.Now()
		_, _ = CheckPassword(encoded, "wrong password here")
		return time.Since(start)
	}
	unknown := func() time.Duration {
		start := time.Now()
		SpendUnknownUser("wrong password here")
		return time.Since(start)
	}
	// Warm both paths so the first allocation does not skew the comparison.
	known()
	unknown()
	var withUser, withoutUser time.Duration
	for range 5 {
		withUser += known()
		withoutUser += unknown()
	}
	ratio := float64(withUser) / float64(withoutUser)
	if ratio < 0.5 || ratio > 2 {
		t.Fatalf("known %v against unknown %v is a usable timing signal", withUser, withoutUser)
	}
}
