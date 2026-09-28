// Package postgres stores durable product admissions in PostgreSQL.
package postgres

import (
	"bytes"
	"context"
	"crypto/sha256"
	"embed"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed migrations/*.sql
var migrations embed.FS

// ErrorCode names a stable admission failure that a caller can act on.
type ErrorCode string

const (
	InvalidRequest        ErrorCode = "invalid_request"
	UnsupportedSpec       ErrorCode = "unsupported_spec"
	IdempotencyConflict   ErrorCode = "idempotency_conflict"
	NameConflict          ErrorCode = "name_conflict"
	OperationConflict     ErrorCode = "operation_conflict"
	NotFound              ErrorCode = "not_found"
	DependencyUnavailable ErrorCode = "dependency_unavailable"
	OutcomeUnknown        ErrorCode = "outcome_unknown"
	// RevisionConflict reports an action whose expected revision is not the current
	// one, so the caller acted on an app it had not seen.
	RevisionConflict ErrorCode = "revision_conflict"
)

// AdmissionError reports a failed admission with a stable code.
type AdmissionError struct {
	Code        ErrorCode
	OperationID uuid.UUID
	Cause       error
}

// Error omits driver details because they can contain connection information.
func (e *AdmissionError) Error() string { return "postgres admission: " + string(e.Code) }

func (e *AdmissionError) Unwrap() error { return e.Cause }

// RequestIdentity names the installation, actor, and idempotency key of one request.
type RequestIdentity struct {
	InstallationID uuid.UUID
	ActorID        string
	APIOperation   string
	IdempotencyKey string
}

// Store borrows its pool. The caller owns pool shutdown.
// Authorization must precede every method, including replay lookup.
type Store struct {
	pool *pgxpool.Pool
	// Tests replace only the COMMIT reply after a real transaction commits.
	commitTx func(context.Context, pgx.Tx) error
}

// New builds a Store on a connection pool.
func New(pool *pgxpool.Pool) *Store { return &Store{pool: pool} }

// Migrate applies all pending migrations atomically under a database lock.
func (s *Store) Migrate(ctx context.Context) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin metadata migration: %w", err)
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(724359982101)`); err != nil {
		return fmt.Errorf("lock metadata migrations: %w", err)
	}
	if _, err := tx.Exec(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (
		version integer PRIMARY KEY, checksum bytea NOT NULL, applied_at timestamptz NOT NULL DEFAULT transaction_timestamp())`); err != nil {
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
		err = tx.QueryRow(ctx, `SELECT checksum FROM schema_migrations WHERE version=$1`, version).Scan(&stored)
		if err == nil {
			if !bytes.Equal(stored, hash[:]) {
				return fmt.Errorf("migration %d checksum changed", version)
			}
			continue
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("read migration %d history: %w", version, err)
		}
		if _, err := tx.Exec(ctx, string(body)); err != nil {
			return fmt.Errorf("apply migration %d: %w", version, err)
		}
		if _, err := tx.Exec(ctx, `INSERT INTO schema_migrations (version, checksum) VALUES ($1,$2)`, version, hash[:]); err != nil {
			return fmt.Errorf("record migration %d: %w", version, err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit metadata migrations: %w", err)
	}
	return nil
}

// admissionError translates named constraints without exposing PostgreSQL messages.
func admissionError(err error) error {
	if errors.Is(err, pgx.ErrNoRows) {
		return &AdmissionError{Code: NotFound, Cause: err}
	}
	if pgErr, ok := errors.AsType[*pgconn.PgError](err); ok && pgErr.Code == "23505" {
		switch pgErr.ConstraintName {
		case "apps_active_name":
			return &AdmissionError{Code: NameConflict, Cause: err}
		case "nodes_pkey", "enrollment_tokens_installation_id_token_verifier_key", "node_certificates_installation_id_serial_key":
			return &AdmissionError{Code: EnrollmentConflict, Cause: err}
		case "operations_active_app":
			return &AdmissionError{Code: OperationConflict, Cause: err}
		}
	}
	return &AdmissionError{Code: DependencyUnavailable, Cause: err}
}

// printableWithin reports visible ASCII text of 1 to limit bytes.
func printableWithin(value string, limit int) bool {
	if len(value) < 1 || len(value) > limit {
		return false
	}
	return !strings.ContainsFunc(value, func(r rune) bool { return r < '!' || r > '~' })
}
