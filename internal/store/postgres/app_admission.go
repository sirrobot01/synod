package postgres

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"strings"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/sirrobot01/synod/internal/app"
)

const CreateAppAPIOperation = "app.create"

type CreateAppRequest struct {
	Identity  RequestIdentity
	RequestID string
	Spec      app.AppSpec
}

// AdmitApp commits desired state and its retry identity in one transaction.
func (s *Store) AdmitApp(ctx context.Context, request CreateAppRequest) (app.Admission, error) {
	identity := request.Identity
	if identity.InstallationID == uuid.Nil || identity.APIOperation != CreateAppAPIOperation || len(identity.ActorID) < 1 || len(identity.ActorID) > 256 || !utf8.ValidString(identity.ActorID) || strings.ContainsRune(identity.ActorID, 0) {
		return app.Admission{}, &AdmissionError{Code: InvalidRequest}
	}
	for _, value := range []string{identity.IdempotencyKey, request.RequestID} {
		if len(value) < 1 || len(value) > 128 || strings.ContainsFunc(value, func(r rune) bool { return r < '!' || r > '~' }) {
			return app.Admission{}, &AdmissionError{Code: InvalidRequest}
		}
	}
	if err := request.Spec.Validate(); err != nil {
		return app.Admission{}, &AdmissionError{Code: InvalidRequest, Cause: err}
	}
	if request.Spec.Tier != app.TierParallel && request.Spec.Tier != app.TierSingleWriter {
		return app.Admission{}, &AdmissionError{Code: UnsupportedSpec}
	}
	if len(request.Spec.Service.SupportedArchitectures) != 0 {
		return app.Admission{}, &AdmissionError{Code: InvalidRequest}
	}
	if len(request.Spec.Service.Ports) != 1 || request.Spec.Service.Ports[0].Protocol == "udp" {
		return app.Admission{}, &AdmissionError{Code: UnsupportedSpec}
	}
	body, err := json.Marshal(request.Spec)
	if err != nil {
		return app.Admission{}, &AdmissionError{Code: InvalidRequest, Cause: err}
	}
	hash := sha256.Sum256(body)
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return app.Admission{}, admissionError(err)
	}
	defer tx.Rollback(ctx)
	id, err := uuid.NewRandom()
	if err != nil {
		return app.Admission{}, err
	}
	result := app.Admission{AppID: id, OperationID: uuid.New(), DesiredRevision: 1}
	err = tx.QueryRow(ctx, `INSERT INTO app_admissions (api_operation,installation_id,actor_id,idempotency_key,request_id,request_hash,app_id,operation_id,desired_revision,accepted_at)
 VALUES ('app.create',$1,$2,$3,$4,$5,$6,$7,1,transaction_timestamp()) ON CONFLICT DO NOTHING RETURNING accepted_at`,
		identity.InstallationID, identity.ActorID, identity.IdempotencyKey, request.RequestID, hash[:], id, result.OperationID).Scan(&result.AcceptedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		var storedHash []byte
		err = tx.QueryRow(ctx, `SELECT app_id,operation_id,desired_revision,accepted_at,request_hash FROM app_admissions WHERE installation_id=$1 AND actor_id=$2 AND idempotency_key=$3 AND api_operation='app.create'`, identity.InstallationID, identity.ActorID, identity.IdempotencyKey).
			Scan(&result.AppID, &result.OperationID, &result.DesiredRevision, &result.AcceptedAt, &storedHash)
		if err != nil {
			return app.Admission{}, admissionError(err)
		}
		if !bytes.Equal(storedHash, hash[:]) {
			return app.Admission{}, &AdmissionError{Code: IdempotencyConflict}
		}
		result.Replayed = true
		return result, nil
	}
	if err != nil {
		return app.Admission{}, admissionError(err)
	}
	_, err = tx.Exec(ctx, `INSERT INTO apps (id,installation_id,name,tier,spec_version,spec,desired_revision,desired_lifecycle,observed_revision,observed_lifecycle,created_at,updated_at)
 VALUES ($1,$2,$3,$4,1,$5,1,'running',0,'provisioning',transaction_timestamp(),transaction_timestamp())`, id, identity.InstallationID, request.Spec.Name, request.Spec.Tier, body)
	if err != nil {
		return app.Admission{}, admissionError(err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO operations
 (id,installation_id,cluster_id,actor_id,api_operation,idempotency_key,request_hash,hash_version,kind,desired_revision,status,plan_version,execution_epoch,attempts,created_at,updated_at)
 VALUES ($1,$2,$3,$4,'app.create',$5,$6,1,'app.deploy',1,'pending',0,0,0,transaction_timestamp(),transaction_timestamp())`, result.OperationID, identity.InstallationID, id, identity.ActorID, identity.IdempotencyKey, hash[:]); err != nil {
		return app.Admission{}, admissionError(err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO outbox (event_id,installation_id,cluster_id,operation_id,kind,schema_version,created_at)
 VALUES ($1,$2,$3,$4,'operation.ready',1,transaction_timestamp())`, uuid.New(), identity.InstallationID, id, result.OperationID); err != nil {
		return app.Admission{}, admissionError(err)
	}
	if s.commitTx != nil {
		err = s.commitTx(ctx, tx)
	} else {
		err = tx.Commit(ctx)
	}
	if err != nil {
		if errors.Is(err, pgx.ErrTxCommitRollback) {
			return app.Admission{}, admissionError(err)
		}
		if pgErr, ok := errors.AsType[*pgconn.PgError](err); ok && (strings.HasPrefix(pgErr.Code, "23") || pgErr.Code == "40001" || pgErr.Code == "40P01") {
			return app.Admission{}, admissionError(err)
		}
		return app.Admission{}, &AdmissionError{Code: OutcomeUnknown, Cause: err}
	}
	return result, nil
}
