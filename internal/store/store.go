// Package store keeps the controller metadata in one embedded SQLite database.
package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"embed"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"
)

//go:embed migrations/*.sql
var migrations embed.FS

// ErrorCode names a stable store failure that a caller can act on.
type ErrorCode string

const (
	InvalidRequest         ErrorCode = "invalid_request"
	IdempotencyConflict    ErrorCode = "idempotency_conflict"
	NotFound               ErrorCode = "not_found"
	Unavailable            ErrorCode = "dependency_unavailable"
	EnrollmentUnauthorized ErrorCode = "enrollment_unauthorized"
	EnrollmentConflict     ErrorCode = "enrollment_conflict"
	Ambiguous              ErrorCode = "ambiguous_name"
)

// Error reports a failed store call with a stable code.
type Error struct {
	Code  ErrorCode
	Cause error
}

// Error omits the cause because a driver message can name files and statements.
func (e *Error) Error() string { return "store: " + string(e.Code) }

func (e *Error) Unwrap() error { return e.Cause }

// Store owns one SQLite database. Close it when the controller stops.
type Store struct {
	db   *sql.DB
	now  func() time.Time
	jobs chan struct{}
}

// JobsAdded receives a value after a new backup, restore, or proof request commits.
func (s *Store) JobsAdded() <-chan struct{} { return s.jobs }

func (s *Store) jobAdded(err error) error {
	if err == nil {
		select {
		case s.jobs <- struct{}{}:
		default:
		}
	}
	return err
}

// Open opens or creates the database file at path and applies pending migrations.
func Open(ctx context.Context, path string) (*Store, error) {
	if path == "" {
		return nil, errors.New("open metadata: the database path is empty")
	}
	path, err := filepath.Abs(path)
	if err != nil {
		return nil, fmt.Errorf("resolve metadata path: %w", err)
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open metadata %s: %w", path, err)
	}
	info, statErr := file.Stat()
	if err := errors.Join(statErr, file.Close()); err != nil {
		return nil, fmt.Errorf("check metadata %s: %w", path, err)
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0o077 != 0 || info.Sys().(*syscall.Stat_t).Uid != uint32(os.Geteuid()) {
		return nil, fmt.Errorf("metadata %s must be a regular file owned by this user; set owner-only permissions", path)
	}
	// IMMEDIATE transactions take the write lock at BEGIN, so two writers queue on
	// the busy timeout instead of failing when a read lock tries to upgrade.
	query := url.Values{}
	for _, pragma := range []string{"busy_timeout(10000)", "journal_mode(WAL)", "foreign_keys(1)", "synchronous(FULL)"} {
		query.Add("_pragma", pragma)
	}
	query.Set("_txlock", "immediate")
	dsn := url.URL{Scheme: "file", Path: path, RawQuery: query.Encode()}
	db, err := sql.Open("sqlite", dsn.String())
	if err != nil {
		return nil, fmt.Errorf("open metadata %s: %w", path, err)
	}
	s := &Store{db: db, now: time.Now, jobs: make(chan struct{}, 1)}
	if err := s.migrate(ctx); err != nil {
		return nil, errors.Join(err, db.Close())
	}
	return s, nil
}

func (s *Store) Close() error { return s.db.Close() }

func (s *Store) migrate(ctx context.Context) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin metadata migration: %w", err)
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (
		version INTEGER PRIMARY KEY, checksum BLOB NOT NULL, applied_at INTEGER NOT NULL)`); err != nil {
		return fmt.Errorf("create migration history: %w", err)
	}
	entries, err := migrations.ReadDir("migrations")
	if err != nil {
		return fmt.Errorf("read migrations: %w", err)
	}
	for _, entry := range entries {
		prefix, _, ok := strings.Cut(entry.Name(), "_")
		if !ok {
			return fmt.Errorf("invalid migration name %q", entry.Name())
		}
		version, err := strconv.Atoi(prefix)
		if err != nil || version <= 0 {
			return fmt.Errorf("invalid migration version %q", prefix)
		}
		body, err := migrations.ReadFile("migrations/" + entry.Name())
		if err != nil {
			return fmt.Errorf("read migration %d: %w", version, err)
		}
		hash := sha256.Sum256(body)
		var stored []byte
		err = tx.QueryRowContext(ctx, `SELECT checksum FROM schema_migrations WHERE version=?`, version).Scan(&stored)
		if err == nil {
			if !bytes.Equal(stored, hash[:]) {
				return fmt.Errorf("migration %d checksum changed; delete the development database and start again", version)
			}
			continue
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("read migration %d history: %w", version, err)
		}
		if _, err := tx.ExecContext(ctx, string(body)); err != nil {
			return fmt.Errorf("apply migration %d: %w", version, err)
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO schema_migrations (version,checksum,applied_at) VALUES (?,?,?)`, version, hash[:], s.now().UnixMilli()); err != nil {
			return fmt.Errorf("record migration %d: %w", version, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit metadata migrations: %w", err)
	}
	return nil
}

// storeError translates driver failures into stable codes without their messages.
func storeError(err error) error {
	if errors.Is(err, sql.ErrNoRows) {
		return &Error{Code: NotFound, Cause: err}
	}
	return &Error{Code: Unavailable, Cause: err}
}

// isUnique reports a UNIQUE constraint failure.
func isUnique(err error) bool {
	failure, ok := errors.AsType[*sqlite.Error](err)
	return ok && failure.Code() == sqlite3.SQLITE_CONSTRAINT_UNIQUE
}

// printableWithin reports visible ASCII text of 1 to limit bytes.
func printableWithin(value string, limit int) bool {
	if len(value) < 1 || len(value) > limit {
		return false
	}
	return !strings.ContainsFunc(value, func(r rune) bool { return r < '!' || r > '~' })
}

// millis scans a Unix-millisecond column into a time. NULL leaves the zero time.
type millis struct{ target *time.Time }

func (m millis) Scan(value any) error {
	switch v := value.(type) {
	case nil:
		*m.target = time.Time{}
	case int64:
		*m.target = time.UnixMilli(v).UTC()
	default:
		return fmt.Errorf("scan %T as Unix milliseconds", value)
	}
	return nil
}

// optionalMillis scans a nullable Unix-millisecond column into a time pointer.
type optionalMillis struct{ target **time.Time }

func (m optionalMillis) Scan(value any) error {
	switch v := value.(type) {
	case nil:
		*m.target = nil
	case int64:
		t := time.UnixMilli(v).UTC()
		*m.target = &t
	default:
		return fmt.Errorf("scan %T as Unix milliseconds", value)
	}
	return nil
}

// unixMillis is the column value of an optional time.
func unixMillis(t *time.Time) any {
	if t == nil {
		return nil
	}
	return t.UnixMilli()
}
