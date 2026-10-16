package store

import (
	"context"
	"crypto/subtle"
	"database/sql"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/auth"
)

// ErrCredentials reports a failed sign-in. It never says which part failed.
// An unknown account, a wrong password, and a locked account share this answer.
var ErrCredentials = errors.New("invalid_credentials")

// ErrAccountDisabled reports a correct credential for an account that cannot sign in.
var ErrAccountDisabled = errors.New("account_disabled")

// ErrUsernameTaken reports a username that the installation already uses.
var ErrUsernameTaken = errors.New("username_taken")

// ErrSetupComplete reports that an operator account already exists.
var ErrSetupComplete = errors.New("setup_complete")

// User is one operator account.
type User struct {
	ID                uuid.UUID `json:"id"`
	Username          string    `json:"username"`
	Role              string    `json:"role"`
	Disabled          bool      `json:"disabled"`
	PasswordChangedAt time.Time `json:"password_changed_at"`
	CreatedAt         time.Time `json:"created_at"`
}

// Session is one live browser session.
type Session struct {
	ID                uuid.UUID `json:"id"`
	UserID            uuid.UUID `json:"user_id"`
	CreatedAt         time.Time `json:"created_at"`
	LastUsedAt        time.Time `json:"last_used_at"`
	AbsoluteExpiresAt time.Time `json:"absolute_expires_at"`
}

// APIToken is one stored token without its secret.
type APIToken struct {
	ID         uuid.UUID  `json:"id"`
	ActorID    string     `json:"actor_id"`
	Name       string     `json:"name"`
	Role       string     `json:"role"`
	CreatedAt  time.Time  `json:"created_at"`
	LastUsedAt *time.Time `json:"last_used_at"`
	ExpiresAt  *time.Time `json:"expires_at"`
	RevokedAt  *time.Time `json:"revoked_at"`
}

const userColumns = `id,username,role,disabled_at IS NOT NULL,password_changed_at,created_at`

// ListUsers returns all operator accounts in username order, including disabled accounts.
func (s *Store) ListUsers(ctx context.Context) ([]User, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT `+userColumns+` FROM users ORDER BY username,id`)
	if err != nil {
		return nil, storeError(err)
	}
	defer rows.Close()
	users := make([]User, 0)
	for rows.Next() {
		var user User
		if err := rows.Scan(user.fields()...); err != nil {
			return nil, storeError(err)
		}
		users = append(users, user)
	}
	if err := rows.Err(); err != nil {
		return nil, storeError(err)
	}
	return users, nil
}

func (u *User) fields() []any {
	return []any{&u.ID, &u.Username, &u.Role, &u.Disabled, millis{&u.PasswordChangedAt}, millis{&u.CreatedAt}}
}

const tokenColumns = `id,actor_id,name,role,created_at,last_used_at,expires_at,revoked_at`

func (t *APIToken) fields() []any {
	return []any{&t.ID, &t.ActorID, &t.Name, &t.Role, millis{&t.CreatedAt}, optionalMillis{&t.LastUsedAt}, optionalMillis{&t.ExpiresAt}, optionalMillis{&t.RevokedAt}}
}

// CreateUser stores one operator account. The caller supplies an encoded password hash.
func (s *Store) CreateUser(ctx context.Context, username, passwordHash, role string) (User, error) {
	if username == "" || passwordHash == "" || role != "admin" {
		return User{}, &Error{Code: InvalidRequest}
	}
	now := s.now().UnixMilli()
	var user User
	err := s.db.QueryRowContext(ctx, `INSERT INTO users (id,username,password_hash,role,password_changed_at,created_at,updated_at)
 VALUES (?,?,?,?,?,?,?) RETURNING `+userColumns, uuid.New(), username, passwordHash, role, now, now, now).Scan(user.fields()...)
	if isUnique(err) {
		return User{}, ErrUsernameTaken
	}
	if err != nil {
		return User{}, storeError(err)
	}
	return user, nil
}

// SetupRequired reports whether no operator account exists yet.
func (s *Store) SetupRequired(ctx context.Context) (bool, error) {
	var required bool
	if err := s.db.QueryRowContext(ctx, `SELECT NOT EXISTS (SELECT 1 FROM users)`).Scan(&required); err != nil {
		return false, storeError(err)
	}
	return required, nil
}

// CreateFirstUser creates the first operator account with the admin role.
// It refuses with ErrSetupComplete once any account exists, so only one first-run setup succeeds.
func (s *Store) CreateFirstUser(ctx context.Context, username, passwordHash string) (User, error) {
	if username == "" || passwordHash == "" {
		return User{}, &Error{Code: InvalidRequest}
	}
	now := s.now().UnixMilli()
	var user User
	err := s.db.QueryRowContext(ctx, `INSERT INTO users (id,username,password_hash,role,password_changed_at,created_at,updated_at)
 SELECT ?,?,?,'admin',?,?,? WHERE NOT EXISTS (SELECT 1 FROM users) RETURNING `+userColumns, uuid.New(), username, passwordHash, now, now, now).Scan(user.fields()...)
	if errors.Is(err, sql.ErrNoRows) {
		return User{}, ErrSetupComplete
	}
	if err != nil {
		return User{}, storeError(err)
	}
	return user, nil
}

// SignIn verifies a password and keeps the lockout counter in the same transaction.
// It spends the work of a real check when no account matches, so response timing
// never reports which usernames exist. A locked account answers as a wrong password.
func (s *Store) SignIn(ctx context.Context, username, password string) (User, error) {
	if username == "" {
		auth.SpendUnknownUser(password)
		return User{}, ErrCredentials
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return User{}, storeError(err)
	}
	defer tx.Rollback()
	var user User
	var hash string
	var attempts int
	var lockedUntil *time.Time
	err = tx.QueryRowContext(ctx, `SELECT `+userColumns+`,password_hash,failed_attempts,locked_until FROM users WHERE username=?`, username).
		Scan(append(user.fields(), &hash, &attempts, optionalMillis{&lockedUntil})...)
	if errors.Is(err, sql.ErrNoRows) {
		auth.SpendUnknownUser(password)
		return User{}, ErrCredentials
	}
	if err != nil {
		return User{}, storeError(err)
	}
	now := s.now()
	if lockedUntil != nil && lockedUntil.After(now) {
		auth.SpendUnknownUser(password)
		return User{}, ErrCredentials
	}
	stale, err := auth.CheckPassword(hash, password)
	if err != nil {
		attempts++
		locked := lockedUntil
		if attempts >= auth.MaxFailedAttempts {
			until := now.Add(auth.LockoutDuration)
			locked, attempts = &until, 0
		}
		if _, err := tx.ExecContext(ctx, `UPDATE users SET failed_attempts=?,locked_until=?,updated_at=? WHERE id=?`, attempts, unixMillis(locked), now.UnixMilli(), user.ID); err != nil {
			return User{}, storeError(err)
		}
		if err := tx.Commit(); err != nil {
			return User{}, storeError(err)
		}
		return User{}, ErrCredentials
	}
	// A correct password clears the counter even when the account cannot sign in.
	if _, err := tx.ExecContext(ctx, `UPDATE users SET failed_attempts=0,locked_until=NULL,updated_at=? WHERE id=?`, now.UnixMilli(), user.ID); err != nil {
		return User{}, storeError(err)
	}
	if stale {
		// The stored settings are older than the current ones, so store a fresh hash.
		if fresh, err := auth.HashPassword(password); err == nil {
			if _, err := tx.ExecContext(ctx, `UPDATE users SET password_hash=? WHERE id=?`, fresh, user.ID); err != nil {
				return User{}, storeError(err)
			}
		}
	}
	if err := tx.Commit(); err != nil {
		return User{}, storeError(err)
	}
	if user.Disabled {
		return User{}, ErrAccountDisabled
	}
	return user, nil
}

// SetUserPassword replaces a password and ends every other session of that account.
// The caller keeps its own session only when it supplies that session identity.
func (s *Store) SetUserPassword(ctx context.Context, userID uuid.UUID, passwordHash string, keep uuid.UUID) error {
	if userID == uuid.Nil || passwordHash == "" {
		return &Error{Code: InvalidRequest}
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return storeError(err)
	}
	defer tx.Rollback()
	now := s.now().UnixMilli()
	result, err := tx.ExecContext(ctx, `UPDATE users SET password_hash=?,password_changed_at=?,updated_at=?,failed_attempts=0,locked_until=NULL WHERE id=?`, passwordHash, now, now, userID)
	if err != nil {
		return storeError(err)
	}
	if changed, err := result.RowsAffected(); err != nil || changed == 0 {
		return &Error{Code: NotFound, Cause: err}
	}
	if _, err := tx.ExecContext(ctx, `UPDATE sessions SET revoked_at=? WHERE user_id=? AND revoked_at IS NULL AND id<>?`, now, userID, keep); err != nil {
		return storeError(err)
	}
	return storeCommit(tx)
}

// DisableUser stops new sign-ins and ends every live session of the account.
func (s *Store) DisableUser(ctx context.Context, userID uuid.UUID) error {
	if userID == uuid.Nil {
		return &Error{Code: InvalidRequest}
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return storeError(err)
	}
	defer tx.Rollback()
	now := s.now().UnixMilli()
	result, err := tx.ExecContext(ctx, `UPDATE users SET disabled_at=COALESCE(disabled_at,?),updated_at=? WHERE id=?`, now, now, userID)
	if err != nil {
		return storeError(err)
	}
	if changed, err := result.RowsAffected(); err != nil || changed == 0 {
		return &Error{Code: NotFound, Cause: err}
	}
	if _, err := tx.ExecContext(ctx, `UPDATE sessions SET revoked_at=? WHERE user_id=? AND revoked_at IS NULL`, now, userID); err != nil {
		return storeError(err)
	}
	return storeCommit(tx)
}

// CountUsers reports how many accounts the installation has.
func (s *Store) CountUsers(ctx context.Context) (int, error) {
	var total int
	if err := s.db.QueryRowContext(ctx, `SELECT count(*) FROM users`).Scan(&total); err != nil {
		return 0, storeError(err)
	}
	return total, nil
}

// CreateSession stores one session verifier. The cookie value never reaches the database.
func (s *Store) CreateSession(ctx context.Context, userID uuid.UUID, verifier [32]byte) (Session, error) {
	if userID == uuid.Nil {
		return Session{}, &Error{Code: InvalidRequest}
	}
	now := s.now()
	session := Session{ID: uuid.New(), UserID: userID, CreatedAt: now.Truncate(time.Millisecond).UTC(), LastUsedAt: now.Truncate(time.Millisecond).UTC(), AbsoluteExpiresAt: now.Add(auth.SessionLifetime).Truncate(time.Millisecond).UTC()}
	if _, err := s.db.ExecContext(ctx, `INSERT INTO sessions (id,user_id,verifier,created_at,last_used_at,absolute_expires_at) VALUES (?,?,?,?,?,?)`,
		session.ID, userID, verifier[:], session.CreatedAt.UnixMilli(), session.LastUsedAt.UnixMilli(), session.AbsoluteExpiresAt.UnixMilli()); err != nil {
		return Session{}, storeError(err)
	}
	return session, nil
}

// AuthenticateSession resolves a presented cookie verifier to its account.
// It applies the idle and the absolute limit, then records use. Neither limit extends the other.
func (s *Store) AuthenticateSession(ctx context.Context, verifier [32]byte) (User, Session, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return User{}, Session{}, storeError(err)
	}
	defer tx.Rollback()
	var user User
	var session Session
	err = tx.QueryRowContext(ctx, `SELECT s.id,s.user_id,s.created_at,s.last_used_at,s.absolute_expires_at,
 u.id,u.username,u.role,u.disabled_at IS NOT NULL,u.password_changed_at,u.created_at
 FROM sessions s JOIN users u ON u.id=s.user_id WHERE s.verifier=? AND s.revoked_at IS NULL`, verifier[:]).
		Scan(append([]any{&session.ID, &session.UserID, millis{&session.CreatedAt}, millis{&session.LastUsedAt}, millis{&session.AbsoluteExpiresAt}}, user.fields()...)...)
	if errors.Is(err, sql.ErrNoRows) {
		return User{}, Session{}, ErrCredentials
	}
	if err != nil {
		return User{}, Session{}, storeError(err)
	}
	now := s.now()
	if !now.Before(session.AbsoluteExpiresAt) || !now.Before(session.LastUsedAt.Add(auth.SessionIdleTimeout)) {
		if _, err := tx.ExecContext(ctx, `UPDATE sessions SET revoked_at=? WHERE id=?`, now.UnixMilli(), session.ID); err != nil {
			return User{}, Session{}, storeError(err)
		}
		if err := storeCommit(tx); err != nil {
			return User{}, Session{}, err
		}
		return User{}, Session{}, ErrCredentials
	}
	if _, err := tx.ExecContext(ctx, `UPDATE sessions SET last_used_at=? WHERE id=?`, now.UnixMilli(), session.ID); err != nil {
		return User{}, Session{}, storeError(err)
	}
	if err := storeCommit(tx); err != nil {
		return User{}, Session{}, err
	}
	if user.Disabled {
		return User{}, Session{}, ErrAccountDisabled
	}
	return user, session, nil
}

// RevokeSession ends one session. Repeat revocation is idempotent.
func (s *Store) RevokeSession(ctx context.Context, sessionID uuid.UUID) error {
	if _, err := s.db.ExecContext(ctx, `UPDATE sessions SET revoked_at=? WHERE id=? AND revoked_at IS NULL`, s.now().UnixMilli(), sessionID); err != nil {
		return storeError(err)
	}
	return nil
}

// CreateAPIToken stores one token verifier and its audit identity.
func (s *Store) CreateAPIToken(ctx context.Context, id uuid.UUID, actorID, name, role string, verifier [32]byte, expires *time.Time) (APIToken, error) {
	if id == uuid.Nil || actorID == "" || name == "" || role != "admin" {
		return APIToken{}, &Error{Code: InvalidRequest}
	}
	var token APIToken
	err := s.db.QueryRowContext(ctx, `INSERT INTO api_tokens (id,actor_id,name,verifier,role,created_at,expires_at)
 VALUES (?,?,?,?,?,?,?) RETURNING `+tokenColumns, id, actorID, name, verifier[:], role, s.now().UnixMilli(), unixMillis(expires)).Scan(token.fields()...)
	if err != nil {
		return APIToken{}, storeError(err)
	}
	return token, nil
}

// AuthenticateAPIToken reads the one row that the presented identity names,
// then compares the verifier in constant time. No request scans the table.
func (s *Store) AuthenticateAPIToken(ctx context.Context, id uuid.UUID, verifier [32]byte) (APIToken, error) {
	if id == uuid.Nil {
		return APIToken{}, ErrCredentials
	}
	var token APIToken
	var stored []byte
	err := s.db.QueryRowContext(ctx, `SELECT `+tokenColumns+`,verifier FROM api_tokens WHERE id=?`, id).Scan(append(token.fields(), &stored)...)
	if errors.Is(err, sql.ErrNoRows) {
		return APIToken{}, ErrCredentials
	}
	if err != nil {
		return APIToken{}, storeError(err)
	}
	if subtle.ConstantTimeCompare(stored, verifier[:]) != 1 {
		return APIToken{}, ErrCredentials
	}
	now := s.now()
	if token.RevokedAt != nil || (token.ExpiresAt != nil && !now.Before(*token.ExpiresAt)) {
		return APIToken{}, ErrCredentials
	}
	if _, err := s.db.ExecContext(ctx, `UPDATE api_tokens SET last_used_at=? WHERE id=?`, now.UnixMilli(), id); err != nil {
		return APIToken{}, storeError(err)
	}
	return token, nil
}

// ListAPITokens reports the newest tokens without any secret. The listing is bounded.
func (s *Store) ListAPITokens(ctx context.Context) ([]APIToken, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT `+tokenColumns+` FROM api_tokens ORDER BY created_at DESC, id DESC LIMIT 1000`)
	if err != nil {
		return nil, storeError(err)
	}
	defer rows.Close()
	tokens := make([]APIToken, 0)
	for rows.Next() {
		var token APIToken
		if err := rows.Scan(token.fields()...); err != nil {
			return nil, storeError(err)
		}
		tokens = append(tokens, token)
	}
	if err := rows.Err(); err != nil {
		return nil, storeError(err)
	}
	return tokens, nil
}

// RevokeAPIToken ends one token. Repeat revocation is idempotent.
func (s *Store) RevokeAPIToken(ctx context.Context, id uuid.UUID) error {
	result, err := s.db.ExecContext(ctx, `UPDATE api_tokens SET revoked_at=COALESCE(revoked_at,?) WHERE id=?`, s.now().UnixMilli(), id)
	if err != nil {
		return storeError(err)
	}
	if changed, err := result.RowsAffected(); err != nil || changed == 0 {
		return &Error{Code: NotFound, Cause: err}
	}
	return nil
}

// storeCommit commits tx and reports a failure with a stable code.
func storeCommit(tx *sql.Tx) error {
	if err := tx.Commit(); err != nil {
		return storeError(err)
	}
	return nil
}
