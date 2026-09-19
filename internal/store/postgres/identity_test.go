package postgres

import (
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/auth"
)

const testPassword = "correct horse battery"

func identityFixture(t *testing.T) (*Store, uuid.UUID, User) {
	t.Helper()
	s := testStore(t)
	installation := uuid.New()
	hash, err := auth.HashPassword(testPassword)
	if err != nil {
		t.Fatal(err)
	}
	user, err := s.CreateUser(t.Context(), installation, "operator", hash, "admin")
	if err != nil {
		t.Fatal(err)
	}
	return s, installation, user
}

func TestSignInAcceptsOneCorrectPassword(t *testing.T) {
	s, installation, user := identityFixture(t)
	signed, err := s.SignIn(t.Context(), installation, "operator", testPassword)
	if err != nil || signed.ID != user.ID || signed.Role != "admin" {
		t.Fatalf("sign-in %+v: %v", signed, err)
	}
	for name, attempt := range map[string]struct{ username, password string }{
		"wrong password":   {"operator", "wrong password here"},
		"unknown username": {"nobody", testPassword},
		"empty username":   {"", testPassword},
		"other case":       {"OPERATOR", testPassword},
	} {
		if _, err := s.SignIn(t.Context(), installation, attempt.username, attempt.password); !errors.Is(err, ErrCredentials) {
			t.Fatalf("%s: %v", name, err)
		}
	}
	// Another installation cannot sign in as this account.
	if _, err := s.SignIn(t.Context(), uuid.New(), "operator", testPassword); !errors.Is(err, ErrCredentials) {
		t.Fatal("an account crossed its installation")
	}
}

func TestUsernameIsUniqueForOneInstallation(t *testing.T) {
	s, installation, _ := identityFixture(t)
	hash, err := auth.HashPassword(testPassword)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.CreateUser(t.Context(), installation, "operator", hash, "admin"); !errors.Is(err, ErrUsernameTaken) {
		t.Fatalf("a repeated username was accepted: %v", err)
	}
	// Another installation may use the same name.
	if _, err := s.CreateUser(t.Context(), uuid.New(), "operator", hash, "admin"); err != nil {
		t.Fatal(err)
	}
	for name, role := range map[string]string{"empty": "", "unknown": "superuser"} {
		if _, err := s.CreateUser(t.Context(), uuid.New(), "other", hash, role); err == nil {
			t.Fatalf("%s role accepted", name)
		}
	}
}

// A lock answers exactly as a wrong password answers, and it releases on time.
func TestSignInLocksAfterRepeatedFailures(t *testing.T) {
	s, installation, user := identityFixture(t)
	for range auth.MaxFailedAttempts {
		if _, err := s.SignIn(t.Context(), installation, "operator", "wrong password here"); !errors.Is(err, ErrCredentials) {
			t.Fatal(err)
		}
	}
	// The correct password now fails, and the failure is indistinguishable.
	if _, err := s.SignIn(t.Context(), installation, "operator", testPassword); !errors.Is(err, ErrCredentials) {
		t.Fatalf("a locked account revealed itself: %v", err)
	}
	var locked *time.Time
	if err := s.pool.QueryRow(t.Context(), `SELECT locked_until FROM users WHERE id=$1`, user.ID).Scan(&locked); err != nil || locked == nil {
		t.Fatalf("no lock was recorded: %v", err)
	}
	// Releasing the lock restores the account without an administrator.
	if _, err := s.pool.Exec(t.Context(), `UPDATE users SET locked_until=clock_timestamp()-interval '1 second' WHERE id=$1`, user.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.SignIn(t.Context(), installation, "operator", testPassword); err != nil {
		t.Fatalf("a released lock still refused: %v", err)
	}
	// A success clears the counter, so the next failure starts from zero.
	var attempts int
	if err := s.pool.QueryRow(t.Context(), `SELECT failed_attempts FROM users WHERE id=$1`, user.ID).Scan(&attempts); err != nil || attempts != 0 {
		t.Fatalf("attempts=%d after a success: %v", attempts, err)
	}
}

func TestDisabledAccountCannotSignIn(t *testing.T) {
	s, installation, user := identityFixture(t)
	secret, err := auth.NewSessionSecret()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.CreateSession(t.Context(), installation, user.ID, secret.Verifier); err != nil {
		t.Fatal(err)
	}
	if err := s.DisableUser(t.Context(), installation, user.ID); err != nil {
		t.Fatal(err)
	}
	// A correct password reports the disabled account. A wrong one still says nothing.
	if _, err := s.SignIn(t.Context(), installation, "operator", testPassword); !errors.Is(err, ErrAccountDisabled) {
		t.Fatalf("disabled sign-in: %v", err)
	}
	if _, err := s.SignIn(t.Context(), installation, "operator", "wrong password here"); !errors.Is(err, ErrCredentials) {
		t.Fatalf("a disabled account revealed itself to a wrong password: %v", err)
	}
	// Disabling ends every live session at once.
	if _, _, err := s.AuthenticateSession(t.Context(), installation, secret.Verifier); !errors.Is(err, ErrCredentials) {
		t.Fatalf("a disabled account kept its session: %v", err)
	}
	if err := s.DisableUser(t.Context(), installation, uuid.New()); err == nil {
		t.Fatal("an unknown account was disabled")
	}
}

func TestSessionAppliesIdleAndAbsoluteLimits(t *testing.T) {
	s, installation, user := identityFixture(t)
	secret, err := auth.NewSessionSecret()
	if err != nil {
		t.Fatal(err)
	}
	session, err := s.CreateSession(t.Context(), installation, user.ID, secret.Verifier)
	if err != nil {
		t.Fatal(err)
	}
	if !session.AbsoluteExpiresAt.After(session.CreatedAt) {
		t.Fatalf("session %+v has no lifetime", session)
	}
	// The cookie value never reaches the database.
	var stored []byte
	if err := s.pool.QueryRow(t.Context(), `SELECT verifier FROM sessions WHERE id=$1`, session.ID).Scan(&stored); err != nil {
		t.Fatal(err)
	}
	if string(stored) == secret.Cookie {
		t.Fatal("the database holds the cookie value")
	}
	signed, live, err := s.AuthenticateSession(t.Context(), installation, secret.Verifier)
	if err != nil || signed.ID != user.ID || live.ID != session.ID {
		t.Fatalf("session authentication: %v", err)
	}
	// An unknown verifier is refused.
	other, err := auth.NewSessionSecret()
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.AuthenticateSession(t.Context(), installation, other.Verifier); !errors.Is(err, ErrCredentials) {
		t.Fatalf("an unknown session was accepted: %v", err)
	}
	// Idle expiry ends the session and the row records it.
	if _, err := s.pool.Exec(t.Context(), `UPDATE sessions SET last_used_at=clock_timestamp()-$1*interval '1 microsecond' WHERE id=$2`, (auth.SessionIdleTimeout + time.Minute).Microseconds(), session.ID); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.AuthenticateSession(t.Context(), installation, secret.Verifier); !errors.Is(err, ErrCredentials) {
		t.Fatalf("an idle session survived: %v", err)
	}
	var revoked *time.Time
	if err := s.pool.QueryRow(t.Context(), `SELECT revoked_at FROM sessions WHERE id=$1`, session.ID).Scan(&revoked); err != nil || revoked == nil {
		t.Fatal("an expired session was not ended")
	}
}

// Use never extends the absolute limit.
func TestSessionAbsoluteLimitIgnoresUse(t *testing.T) {
	s, installation, user := identityFixture(t)
	secret, err := auth.NewSessionSecret()
	if err != nil {
		t.Fatal(err)
	}
	session, err := s.CreateSession(t.Context(), installation, user.ID, secret.Verifier)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.pool.Exec(t.Context(), `UPDATE sessions SET absolute_expires_at=clock_timestamp()-interval '1 second',last_used_at=clock_timestamp() WHERE id=$1`, session.ID); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.AuthenticateSession(t.Context(), installation, secret.Verifier); !errors.Is(err, ErrCredentials) {
		t.Fatalf("a recently used session outlived its absolute limit: %v", err)
	}
}

func TestPasswordChangeEndsOtherSessions(t *testing.T) {
	s, installation, user := identityFixture(t)
	keep, err := auth.NewSessionSecret()
	if err != nil {
		t.Fatal(err)
	}
	kept, err := s.CreateSession(t.Context(), installation, user.ID, keep.Verifier)
	if err != nil {
		t.Fatal(err)
	}
	other, err := auth.NewSessionSecret()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.CreateSession(t.Context(), installation, user.ID, other.Verifier); err != nil {
		t.Fatal(err)
	}
	hash, err := auth.HashPassword("a different password")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.SetUserPassword(t.Context(), installation, user.ID, hash, kept.ID); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.AuthenticateSession(t.Context(), installation, other.Verifier); !errors.Is(err, ErrCredentials) {
		t.Fatalf("a password change kept another session: %v", err)
	}
	if _, _, err := s.AuthenticateSession(t.Context(), installation, keep.Verifier); err != nil {
		t.Fatalf("a password change ended the caller's own session: %v", err)
	}
	if _, err := s.SignIn(t.Context(), installation, "operator", "a different password"); err != nil {
		t.Fatalf("the new password was refused: %v", err)
	}
	if _, err := s.SignIn(t.Context(), installation, "operator", testPassword); !errors.Is(err, ErrCredentials) {
		t.Fatal("the old password still works")
	}
	if err := s.SetUserPassword(t.Context(), installation, uuid.New(), hash, uuid.Nil); err == nil {
		t.Fatal("an unknown account changed its password")
	}
}

// A sign-in with older hash settings stores a fresh hash without a password change.
func TestSignInRehashesStaleParameters(t *testing.T) {
	s, installation, user := identityFixture(t)
	previous := auth.CurrentPasswordParameters
	auth.CurrentPasswordParameters = auth.PasswordParameters{Memory: 8192, Iterations: 1, Parallelism: 1}
	old, err := auth.HashPassword(testPassword)
	if err != nil {
		t.Fatal(err)
	}
	auth.CurrentPasswordParameters = previous
	if _, err := s.pool.Exec(t.Context(), `UPDATE users SET password_hash=$1 WHERE id=$2`, old, user.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.SignIn(t.Context(), installation, "operator", testPassword); err != nil {
		t.Fatal(err)
	}
	var stored string
	if err := s.pool.QueryRow(t.Context(), `SELECT password_hash FROM users WHERE id=$1`, user.ID).Scan(&stored); err != nil {
		t.Fatal(err)
	}
	if stored == old {
		t.Fatal("a stale hash survived a successful sign-in")
	}
	if stale, err := auth.CheckPassword(stored, testPassword); err != nil || stale {
		t.Fatalf("the fresh hash is wrong: stale=%v %v", stale, err)
	}
}

func TestAPITokenVerificationAndRevocation(t *testing.T) {
	s, installation, _ := identityFixture(t)
	minted, err := auth.NewAPIToken()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.CreateAPIToken(t.Context(), installation, minted.ID, "ci", "deploy pipeline", "admin", minted.Verifier, nil); err != nil {
		t.Fatal(err)
	}
	id, verifier, err := auth.ParseAPIToken(minted.Plaintext)
	if err != nil {
		t.Fatal(err)
	}
	token, err := s.AuthenticateAPIToken(t.Context(), installation, id, verifier)
	if err != nil || token.ActorID != "ci" {
		t.Fatalf("token %+v: %v", token, err)
	}
	// The plaintext is never stored.
	var count int
	if err := s.pool.QueryRow(t.Context(), `SELECT count(*) FROM api_tokens WHERE encode(verifier,'hex')=$1`, minted.Plaintext).Scan(&count); err != nil || count != 0 {
		t.Fatalf("the plaintext reached the database: %d %v", count, err)
	}
	// Another secret for the same identity is refused.
	other, err := auth.NewAPIToken()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.AuthenticateAPIToken(t.Context(), installation, id, other.Verifier); !errors.Is(err, ErrCredentials) {
		t.Fatalf("another secret was accepted: %v", err)
	}
	// An unknown identity and another installation are both refused.
	if _, err := s.AuthenticateAPIToken(t.Context(), installation, other.ID, other.Verifier); !errors.Is(err, ErrCredentials) {
		t.Fatal("an unknown token was accepted")
	}
	if _, err := s.AuthenticateAPIToken(t.Context(), uuid.New(), id, verifier); !errors.Is(err, ErrCredentials) {
		t.Fatal("a token crossed its installation")
	}
	tokens, err := s.ListAPITokens(t.Context(), installation)
	if err != nil || len(tokens) != 1 || tokens[0].ID != minted.ID {
		t.Fatalf("list %+v: %v", tokens, err)
	}
	if err := s.RevokeAPIToken(t.Context(), installation, minted.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.AuthenticateAPIToken(t.Context(), installation, id, verifier); !errors.Is(err, ErrCredentials) {
		t.Fatalf("a revoked token was accepted: %v", err)
	}
	// Revocation is idempotent, and an unknown token cannot be revoked.
	if err := s.RevokeAPIToken(t.Context(), installation, minted.ID); err != nil {
		t.Fatalf("repeat revocation: %v", err)
	}
	if err := s.RevokeAPIToken(t.Context(), installation, uuid.New()); err == nil {
		t.Fatal("an unknown token was revoked")
	}
}

func TestAPITokenExpiry(t *testing.T) {
	s, installation, _ := identityFixture(t)
	minted, err := auth.NewAPIToken()
	if err != nil {
		t.Fatal(err)
	}
	past := time.Now().Add(-time.Minute)
	if _, err := s.CreateAPIToken(t.Context(), installation, minted.ID, "ci", "short lived", "admin", minted.Verifier, &past); err != nil {
		t.Fatal(err)
	}
	if _, err := s.AuthenticateAPIToken(t.Context(), installation, minted.ID, minted.Verifier); !errors.Is(err, ErrCredentials) {
		t.Fatalf("an expired token was accepted: %v", err)
	}
	future := time.Now().Add(time.Hour)
	live, err := auth.NewAPIToken()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.CreateAPIToken(t.Context(), installation, live.ID, "ci", "still valid", "admin", live.Verifier, &future); err != nil {
		t.Fatal(err)
	}
	if _, err := s.AuthenticateAPIToken(t.Context(), installation, live.ID, live.Verifier); err != nil {
		t.Fatalf("an unexpired token was refused: %v", err)
	}
}

func TestCountUsersReportsBootstrapState(t *testing.T) {
	s := testStore(t)
	installation := uuid.New()
	total, err := s.CountUsers(t.Context(), installation)
	if err != nil || total != 0 {
		t.Fatalf("empty installation reports %d: %v", total, err)
	}
	hash, err := auth.HashPassword(testPassword)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.CreateUser(t.Context(), installation, "operator", hash, "admin"); err != nil {
		t.Fatal(err)
	}
	if total, err := s.CountUsers(t.Context(), installation); err != nil || total != 1 {
		t.Fatalf("one account reports %d: %v", total, err)
	}
}
