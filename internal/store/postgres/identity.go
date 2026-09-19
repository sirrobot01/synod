package postgres

import (
	"context"
	"crypto/subtle"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/sirrobot01/synod/internal/auth"
)

// ErrCredentials reports a failed sign-in. It never says which part failed.
// An unknown account, a wrong password, and a locked account share this answer.
var ErrCredentials = errors.New("invalid_credentials")

// ErrAccountDisabled reports a correct credential for an account that cannot sign in.
var ErrAccountDisabled = errors.New("account_disabled")

// ErrUsernameTaken reports a username that the installation already uses.
var ErrUsernameTaken = errors.New("username_taken")

// User is one operator account.
type User struct {
	ID                uuid.UUID `json:"id"`
	InstallationID    uuid.UUID `json:"installation_id"`
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

const userColumns = `id,installation_id,username,role,disabled_at IS NOT NULL,password_changed_at,created_at`

// CreateUser stores one operator account. The caller supplies an encoded password hash.
func (s *Store) CreateUser(ctx context.Context, installationID uuid.UUID, username, passwordHash, role string) (User, error) {
	if installationID == uuid.Nil || username == "" || passwordHash == "" || role != "admin" {
		return User{}, &AdmissionError{Code: InvalidRequest}
	}
	var user User
	err := s.pool.QueryRow(ctx, `INSERT INTO users (id,installation_id,username,password_hash,role)
 VALUES ($1,$2,$3,$4,$5) RETURNING `+userColumns, uuid.New(), installationID, username, passwordHash, role).
		Scan(&user.ID, &user.InstallationID, &user.Username, &user.Role, &user.Disabled, &user.PasswordChangedAt, &user.CreatedAt)
	if err != nil {
		if pgErr, ok := errors.AsType[*pgconn.PgError](err); ok && pgErr.Code == "23505" {
			return User{}, ErrUsernameTaken
		}
		return User{}, admissionError(err)
	}
	return user, nil
}

// SignIn verifies a password and keeps the lockout counter in the same transaction.
// It spends the work of a real check when no account matches, so response timing
// never reports which usernames exist.
// A locked account answers exactly as a wrong password answers.
func (s *Store) SignIn(ctx context.Context, installationID uuid.UUID, username, password string) (User, error) {
	if installationID == uuid.Nil || username == "" {
		auth.SpendUnknownUser(password)
		return User{}, ErrCredentials
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return User{}, admissionError(err)
	}
	defer tx.Rollback(ctx)
	var user User
	var hash string
	var attempts int
	var lockedUntil *time.Time
	err = tx.QueryRow(ctx, `SELECT `+userColumns+`,password_hash,failed_attempts,locked_until
 FROM users WHERE installation_id=$1 AND username=$2 FOR UPDATE`, installationID, username).
		Scan(&user.ID, &user.InstallationID, &user.Username, &user.Role, &user.Disabled, &user.PasswordChangedAt, &user.CreatedAt, &hash, &attempts, &lockedUntil)
	if errors.Is(err, pgx.ErrNoRows) {
		auth.SpendUnknownUser(password)
		return User{}, ErrCredentials
	}
	if err != nil {
		return User{}, admissionError(err)
	}
	var now time.Time
	if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&now); err != nil {
		return User{}, admissionError(err)
	}
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
		if _, err := tx.Exec(ctx, `UPDATE users SET failed_attempts=$1,locked_until=$2,updated_at=clock_timestamp() WHERE id=$3`, attempts, locked, user.ID); err != nil {
			return User{}, admissionError(err)
		}
		if err := s.commitMetadata(ctx, tx); err != nil {
			return User{}, err
		}
		return User{}, ErrCredentials
	}
	// A correct password clears the counter even when the account cannot sign in.
	if _, err := tx.Exec(ctx, `UPDATE users SET failed_attempts=0,locked_until=NULL,updated_at=clock_timestamp() WHERE id=$1`, user.ID); err != nil {
		return User{}, admissionError(err)
	}
	if stale {
		// The stored settings are older than the current ones, so store a fresh hash.
		if fresh, err := auth.HashPassword(password); err == nil {
			if _, err := tx.Exec(ctx, `UPDATE users SET password_hash=$1 WHERE id=$2`, fresh, user.ID); err != nil {
				return User{}, admissionError(err)
			}
		}
	}
	if err := s.commitMetadata(ctx, tx); err != nil {
		return User{}, err
	}
	if user.Disabled {
		return User{}, ErrAccountDisabled
	}
	return user, nil
}

// SetUserPassword replaces a password and ends every other session of that account.
// The caller keeps its own session only when it supplies that session identity.
func (s *Store) SetUserPassword(ctx context.Context, installationID, userID uuid.UUID, passwordHash string, keep uuid.UUID) error {
	if installationID == uuid.Nil || userID == uuid.Nil || passwordHash == "" {
		return &AdmissionError{Code: InvalidRequest}
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return admissionError(err)
	}
	defer tx.Rollback(ctx)
	tag, err := tx.Exec(ctx, `UPDATE users SET password_hash=$1,password_changed_at=clock_timestamp(),updated_at=clock_timestamp(),
 failed_attempts=0,locked_until=NULL WHERE installation_id=$2 AND id=$3`, passwordHash, installationID, userID)
	if err != nil {
		return admissionError(err)
	}
	if tag.RowsAffected() == 0 {
		return &AdmissionError{Code: NotFound}
	}
	if _, err := tx.Exec(ctx, `UPDATE sessions SET revoked_at=clock_timestamp()
 WHERE installation_id=$1 AND user_id=$2 AND revoked_at IS NULL AND id<>$3`, installationID, userID, keep); err != nil {
		return admissionError(err)
	}
	return s.commitMetadata(ctx, tx)
}

// DisableUser stops new sign-ins and ends every live session of the account.
func (s *Store) DisableUser(ctx context.Context, installationID, userID uuid.UUID) error {
	if installationID == uuid.Nil || userID == uuid.Nil {
		return &AdmissionError{Code: InvalidRequest}
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return admissionError(err)
	}
	defer tx.Rollback(ctx)
	tag, err := tx.Exec(ctx, `UPDATE users SET disabled_at=COALESCE(disabled_at,clock_timestamp()),updated_at=clock_timestamp()
 WHERE installation_id=$1 AND id=$2`, installationID, userID)
	if err != nil {
		return admissionError(err)
	}
	if tag.RowsAffected() == 0 {
		return &AdmissionError{Code: NotFound}
	}
	if _, err := tx.Exec(ctx, `UPDATE sessions SET revoked_at=clock_timestamp()
 WHERE installation_id=$1 AND user_id=$2 AND revoked_at IS NULL`, installationID, userID); err != nil {
		return admissionError(err)
	}
	return s.commitMetadata(ctx, tx)
}

// CountUsers reports how many accounts the installation has.
// Startup uses it to decide whether the bootstrap credential still needs to create one.
func (s *Store) CountUsers(ctx context.Context, installationID uuid.UUID) (int, error) {
	var total int
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM users WHERE installation_id=$1`, installationID).Scan(&total); err != nil {
		return 0, admissionError(err)
	}
	return total, nil
}

// CreateSession stores one session verifier. The cookie value never reaches the database.
func (s *Store) CreateSession(ctx context.Context, installationID, userID uuid.UUID, verifier [32]byte) (Session, error) {
	if installationID == uuid.Nil || userID == uuid.Nil {
		return Session{}, &AdmissionError{Code: InvalidRequest}
	}
	var session Session
	err := s.pool.QueryRow(ctx, `INSERT INTO sessions (id,installation_id,user_id,verifier,absolute_expires_at)
 VALUES ($1,$2,$3,$4,clock_timestamp()+$5*interval '1 microsecond')
 RETURNING id,user_id,created_at,last_used_at,absolute_expires_at`,
		uuid.New(), installationID, userID, verifier[:], auth.SessionLifetime.Microseconds()).
		Scan(&session.ID, &session.UserID, &session.CreatedAt, &session.LastUsedAt, &session.AbsoluteExpiresAt)
	if err != nil {
		return Session{}, admissionError(err)
	}
	return session, nil
}

// AuthenticateSession resolves a presented cookie verifier to its account.
// It applies the idle and the absolute limit with database time, then records use.
// Neither limit extends the other.
func (s *Store) AuthenticateSession(ctx context.Context, installationID uuid.UUID, verifier [32]byte) (User, Session, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return User{}, Session{}, admissionError(err)
	}
	defer tx.Rollback(ctx)
	var user User
	var session Session
	err = tx.QueryRow(ctx, `SELECT s.id,s.user_id,s.created_at,s.last_used_at,s.absolute_expires_at,
 u.id,u.installation_id,u.username,u.role,u.disabled_at IS NOT NULL,u.password_changed_at,u.created_at
 FROM sessions s JOIN users u ON u.id=s.user_id
 WHERE s.installation_id=$1 AND s.verifier=$2 AND s.revoked_at IS NULL FOR UPDATE OF s`, installationID, verifier[:]).
		Scan(&session.ID, &session.UserID, &session.CreatedAt, &session.LastUsedAt, &session.AbsoluteExpiresAt,
			&user.ID, &user.InstallationID, &user.Username, &user.Role, &user.Disabled, &user.PasswordChangedAt, &user.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return User{}, Session{}, ErrCredentials
	}
	if err != nil {
		return User{}, Session{}, admissionError(err)
	}
	var now time.Time
	if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&now); err != nil {
		return User{}, Session{}, admissionError(err)
	}
	if !now.Before(session.AbsoluteExpiresAt) || !now.Before(session.LastUsedAt.Add(auth.SessionIdleTimeout)) {
		if _, err := tx.Exec(ctx, `UPDATE sessions SET revoked_at=clock_timestamp() WHERE id=$1`, session.ID); err != nil {
			return User{}, Session{}, admissionError(err)
		}
		if err := s.commitMetadata(ctx, tx); err != nil {
			return User{}, Session{}, err
		}
		return User{}, Session{}, ErrCredentials
	}
	if _, err := tx.Exec(ctx, `UPDATE sessions SET last_used_at=clock_timestamp() WHERE id=$1`, session.ID); err != nil {
		return User{}, Session{}, admissionError(err)
	}
	if err := s.commitMetadata(ctx, tx); err != nil {
		return User{}, Session{}, err
	}
	if user.Disabled {
		return User{}, Session{}, ErrAccountDisabled
	}
	return user, session, nil
}

// RevokeSession ends one session. Repeat revocation is idempotent.
func (s *Store) RevokeSession(ctx context.Context, installationID uuid.UUID, sessionID uuid.UUID) error {
	if _, err := s.pool.Exec(ctx, `UPDATE sessions SET revoked_at=clock_timestamp()
 WHERE installation_id=$1 AND id=$2 AND revoked_at IS NULL`, installationID, sessionID); err != nil {
		return admissionError(err)
	}
	return nil
}

// CreateAPIToken stores one token verifier and its audit identity.
func (s *Store) CreateAPIToken(ctx context.Context, installationID, id uuid.UUID, actorID, name, role string, verifier [32]byte, expires *time.Time) (APIToken, error) {
	if installationID == uuid.Nil || id == uuid.Nil || actorID == "" || name == "" || role != "admin" {
		return APIToken{}, &AdmissionError{Code: InvalidRequest}
	}
	var token APIToken
	err := s.pool.QueryRow(ctx, `INSERT INTO api_tokens (id,installation_id,actor_id,name,verifier,role,expires_at)
 VALUES ($1,$2,$3,$4,$5,$6,$7) RETURNING id,actor_id,name,role,created_at,last_used_at,expires_at,revoked_at`,
		id, installationID, actorID, name, verifier[:], role, expires).
		Scan(&token.ID, &token.ActorID, &token.Name, &token.Role, &token.CreatedAt, &token.LastUsedAt, &token.ExpiresAt, &token.RevokedAt)
	if err != nil {
		return APIToken{}, admissionError(err)
	}
	return token, nil
}

// AuthenticateAPIToken reads the one row that the presented identity names,
// then compares the verifier in constant time. No request scans the table.
func (s *Store) AuthenticateAPIToken(ctx context.Context, installationID, id uuid.UUID, verifier [32]byte) (APIToken, error) {
	if installationID == uuid.Nil || id == uuid.Nil {
		return APIToken{}, ErrCredentials
	}
	var token APIToken
	var stored []byte
	var now time.Time
	err := s.pool.QueryRow(ctx, `SELECT id,actor_id,name,role,created_at,last_used_at,expires_at,revoked_at,verifier,clock_timestamp()
 FROM api_tokens WHERE installation_id=$1 AND id=$2`, installationID, id).
		Scan(&token.ID, &token.ActorID, &token.Name, &token.Role, &token.CreatedAt, &token.LastUsedAt, &token.ExpiresAt, &token.RevokedAt, &stored, &now)
	if errors.Is(err, pgx.ErrNoRows) {
		return APIToken{}, ErrCredentials
	}
	if err != nil {
		return APIToken{}, admissionError(err)
	}
	if subtle.ConstantTimeCompare(stored, verifier[:]) != 1 {
		return APIToken{}, ErrCredentials
	}
	if token.RevokedAt != nil || (token.ExpiresAt != nil && !now.Before(*token.ExpiresAt)) {
		return APIToken{}, ErrCredentials
	}
	if _, err := s.pool.Exec(ctx, `UPDATE api_tokens SET last_used_at=clock_timestamp() WHERE id=$1`, id); err != nil {
		return APIToken{}, admissionError(err)
	}
	return token, nil
}

// ListAPITokens reports the newest tokens of the installation without any secret.
// The listing is bounded, and the newest rows come first so an audit sees recent work.
func (s *Store) ListAPITokens(ctx context.Context, installationID uuid.UUID) ([]APIToken, error) {
	rows, err := s.pool.Query(ctx, `SELECT id,actor_id,name,role,created_at,last_used_at,expires_at,revoked_at
 FROM api_tokens WHERE installation_id=$1 ORDER BY created_at DESC, id DESC LIMIT 1000`, installationID)
	if err != nil {
		return nil, admissionError(err)
	}
	defer rows.Close()
	tokens := make([]APIToken, 0)
	for rows.Next() {
		var token APIToken
		if err := rows.Scan(&token.ID, &token.ActorID, &token.Name, &token.Role, &token.CreatedAt, &token.LastUsedAt, &token.ExpiresAt, &token.RevokedAt); err != nil {
			return nil, admissionError(err)
		}
		tokens = append(tokens, token)
	}
	if rows.Err() != nil {
		return nil, admissionError(rows.Err())
	}
	return tokens, nil
}

// RevokeAPIToken ends one token. Repeat revocation is idempotent.
func (s *Store) RevokeAPIToken(ctx context.Context, installationID, id uuid.UUID) error {
	tag, err := s.pool.Exec(ctx, `UPDATE api_tokens SET revoked_at=COALESCE(revoked_at,clock_timestamp())
 WHERE installation_id=$1 AND id=$2`, installationID, id)
	if err != nil {
		return admissionError(err)
	}
	if tag.RowsAffected() == 0 {
		return &AdmissionError{Code: NotFound}
	}
	return nil
}
