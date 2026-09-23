// Package postgres stores durable product admissions in PostgreSQL.
package postgres

import (
	"bytes"
	"context"
	"crypto/sha256"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sirrobot01/synod/internal/cluster"
)

//go:embed migrations/*.sql
var migrations embed.FS

const CreateClusterAPIOperation = "clusters.create.v1"

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
	// one, so the caller acted on a cluster it had not seen.
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

// CreateClusterRequest is one admission request for a new cluster.
type CreateClusterRequest struct {
	Identity  RequestIdentity
	RequestID string
	Spec      cluster.CreateSpec
}

// Admission is the committed result of one accepted request.
// Replayed reports that an earlier identical request produced this result.
type Admission struct {
	ClusterID       uuid.UUID `json:"cluster_id"`
	OperationID     uuid.UUID `json:"operation_id"`
	DesiredRevision int64     `json:"desired_revision"`
	AcceptedAt      time.Time `json:"accepted_at"`
	Replayed        bool      `json:"replayed"`
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

// encodeRequest fixes the v1 hash to validated typed input, without transport fields.
func encodeRequest(identity RequestIdentity, spec cluster.CreateSpec) ([]byte, [32]byte, error) {
	if identity.InstallationID == uuid.Nil || len(identity.ActorID) < 1 || len(identity.ActorID) > 256 ||
		!utf8.ValidString(identity.ActorID) || strings.ContainsRune(identity.ActorID, 0) ||
		identity.APIOperation != CreateClusterAPIOperation || len(identity.IdempotencyKey) < 1 || len(identity.IdempotencyKey) > 128 {
		return nil, [32]byte{}, &AdmissionError{Code: InvalidRequest}
	}
	for _, b := range []byte(identity.IdempotencyKey) {
		if b < '!' || b > '~' {
			return nil, [32]byte{}, &AdmissionError{Code: InvalidRequest}
		}
	}
	if err := spec.Validate(); err != nil {
		return nil, [32]byte{}, &AdmissionError{Code: InvalidRequest, Cause: err}
	}
	body, err := json.Marshal(spec)
	if err != nil {
		return nil, [32]byte{}, &AdmissionError{Code: InvalidRequest, Cause: err}
	}
	return body, sha256.Sum256(body), nil
}

// admissionError translates named constraints without exposing PostgreSQL messages.
func admissionError(err error) error {
	if errors.Is(err, pgx.ErrNoRows) {
		return &AdmissionError{Code: NotFound, Cause: err}
	}
	if pgErr, ok := errors.AsType[*pgconn.PgError](err); ok && pgErr.Code == "23505" {
		switch pgErr.ConstraintName {
		case "clusters_active_name":
			return &AdmissionError{Code: NameConflict, Cause: err}
		case "nodes_pkey", "enrollment_tokens_installation_id_token_verifier_key", "node_certificates_installation_id_serial_key":
			return &AdmissionError{Code: EnrollmentConflict, Cause: err}
		case "operations_active_cluster":
			return &AdmissionError{Code: OperationConflict, Cause: err}
		}
	}
	return &AdmissionError{Code: DependencyUnavailable, Cause: err}
}

// CreateCluster commits desired state, pending work, replay identity, and audit together.
// A successful return does not authorize dispatch or report provisioned resources.
func (s *Store) CreateCluster(ctx context.Context, request CreateClusterRequest) (Admission, error) {
	body, hash, err := encodeRequest(request.Identity, request.Spec)
	if err != nil {
		return Admission{}, err
	}
	if len(request.RequestID) < 1 || len(request.RequestID) > 128 {
		return Admission{}, &AdmissionError{Code: InvalidRequest}
	}
	for _, b := range []byte(request.RequestID) {
		if b < '!' || b > '~' {
			return Admission{}, &AdmissionError{Code: InvalidRequest}
		}
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return Admission{}, admissionError(err)
	}
	defer tx.Rollback(ctx)
	clusterID, err := uuid.NewRandom()
	if err != nil {
		return Admission{}, admissionError(err)
	}
	operationID, err := uuid.NewRandom()
	if err != nil {
		return Admission{}, admissionError(err)
	}
	identity := request.Identity
	result := Admission{ClusterID: clusterID, OperationID: operationID, DesiredRevision: 1}
	err = tx.QueryRow(ctx, `INSERT INTO idempotency_results
		(installation_id, actor_id, api_operation, idempotency_key, request_hash, hash_version, cluster_id, operation_id, desired_revision, accepted_at)
		VALUES ($1,$2,$3,$4,$5,1,$6,$7,1,transaction_timestamp())
		ON CONFLICT DO NOTHING RETURNING accepted_at`,
		identity.InstallationID, identity.ActorID, identity.APIOperation, identity.IdempotencyKey, hash[:], clusterID, operationID).Scan(&result.AcceptedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		// A new statement snapshot sees the winner after its unique-key wait.
		var storedHash []byte
		var hashVersion int
		err := tx.QueryRow(ctx, `SELECT cluster_id, operation_id, desired_revision, accepted_at, request_hash, hash_version
			FROM idempotency_results WHERE installation_id=$1 AND actor_id=$2 AND api_operation=$3 AND idempotency_key=$4`,
			identity.InstallationID, identity.ActorID, identity.APIOperation, identity.IdempotencyKey).
			Scan(&result.ClusterID, &result.OperationID, &result.DesiredRevision, &result.AcceptedAt, &storedHash, &hashVersion)
		if err != nil {
			return Admission{}, &AdmissionError{Code: DependencyUnavailable, Cause: err}
		}
		if hashVersion != 1 || !bytes.Equal(storedHash, hash[:]) {
			return Admission{}, &AdmissionError{Code: IdempotencyConflict, OperationID: result.OperationID}
		}
		result.Replayed = true
		return result, nil
	}
	if err != nil {
		return Admission{}, admissionError(err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO clusters
		(id, installation_id, name, engine, spec_version, spec, desired_revision, desired_lifecycle, observed_revision, observed_lifecycle, execution_epoch, created_at, updated_at)
		VALUES ($1,$2,$3,$4,1,$5,1,'running',0,'provisioning',0,transaction_timestamp(),transaction_timestamp())`,
		clusterID, identity.InstallationID, request.Spec.Name, request.Spec.Engine, body); err != nil {
		return Admission{}, admissionError(err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO operations
		(id, installation_id, cluster_id, actor_id, api_operation, idempotency_key, request_hash, hash_version, kind, desired_revision, status, plan_version, execution_epoch, attempts, created_at, updated_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,1,'cluster.create',1,'pending',0,0,0,transaction_timestamp(),transaction_timestamp())`,
		operationID, identity.InstallationID, clusterID, identity.ActorID, identity.APIOperation, identity.IdempotencyKey, hash[:]); err != nil {
		return Admission{}, admissionError(err)
	}
	eventID, err := uuid.NewRandom()
	if err != nil {
		return Admission{}, admissionError(err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO outbox (event_id, installation_id, cluster_id, operation_id, kind, schema_version, created_at)
		VALUES ($1,$2,$3,$4,'operation.ready',1,transaction_timestamp())`,
		eventID, identity.InstallationID, clusterID, operationID); err != nil {
		return Admission{}, admissionError(err)
	}
	auditID, err := uuid.NewRandom()
	if err != nil {
		return Admission{}, admissionError(err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO audit_events (id, installation_id, cluster_id, operation_id, actor_id, request_id, action, outcome, created_at)
		VALUES ($1,$2,$3,$4,$5,$6,'cluster.create.accepted','accepted',transaction_timestamp())`,
		auditID, identity.InstallationID, clusterID, operationID, identity.ActorID, request.RequestID); err != nil {
		return Admission{}, admissionError(err)
	}
	if s.commitTx != nil {
		err = s.commitTx(ctx, tx)
	} else {
		err = tx.Commit(ctx)
	}
	if err != nil {
		if errors.Is(err, pgx.ErrTxCommitRollback) {
			return Admission{}, admissionError(err)
		}
		if pgErr, ok := errors.AsType[*pgconn.PgError](err); ok &&
			(strings.HasPrefix(pgErr.Code, "23") || pgErr.Code == "40001" || pgErr.Code == "40P01") {
			return Admission{}, admissionError(err)
		}
		return Admission{}, &AdmissionError{Code: OutcomeUnknown, Cause: err}
	}
	return result, nil
}

// LookupAdmission resolves a committed result by its original request identity.
// Absence does not prove that an earlier transaction stopped. Retry through CreateCluster.
func (s *Store) LookupAdmission(ctx context.Context, identity RequestIdentity, spec cluster.CreateSpec) (Admission, error) {
	_, hash, err := encodeRequest(identity, spec)
	if err != nil {
		return Admission{}, err
	}
	var result Admission
	var storedHash []byte
	var hashVersion int
	err = s.pool.QueryRow(ctx, `SELECT cluster_id, operation_id, desired_revision, accepted_at, request_hash, hash_version
		FROM idempotency_results WHERE installation_id=$1 AND actor_id=$2 AND api_operation=$3 AND idempotency_key=$4`,
		identity.InstallationID, identity.ActorID, identity.APIOperation, identity.IdempotencyKey).
		Scan(&result.ClusterID, &result.OperationID, &result.DesiredRevision, &result.AcceptedAt, &storedHash, &hashVersion)
	if err != nil {
		return Admission{}, admissionError(err)
	}
	if hashVersion != 1 || !bytes.Equal(storedHash, hash[:]) {
		return Admission{}, &AdmissionError{Code: IdempotencyConflict, OperationID: result.OperationID}
	}
	result.Replayed = true
	return result, nil
}
