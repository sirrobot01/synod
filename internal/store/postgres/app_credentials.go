package postgres

import (
	"context"
	"errors"
	"strings"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// RecordAppCredentials writes the sealed credentials of a tier 1 app before any command
// that needs them is sent. A repeat that disagrees is refused rather than merged: a
// credential whose recorded version changed is one nobody can match to what a host holds.
func (s *Store) RecordAppCredentials(ctx context.Context, installation, appID uuid.UUID, credentials []Credential) error {
	if installation == uuid.Nil || appID == uuid.Nil {
		return &AdmissionError{Code: InvalidRequest}
	}
	if err := validCredentials(credentials); err != nil {
		return err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return admissionError(err)
	}
	defer tx.Rollback(ctx)
	// The app row serializes this writer against a planner that dispatches its commands.
	var tier int
	if err := tx.QueryRow(ctx, `SELECT tier FROM apps WHERE installation_id=$1 AND id=$2 FOR UPDATE`, installation, appID).Scan(&tier); err != nil {
		return admissionError(err)
	}
	if tier != 1 {
		return &AdmissionError{Code: InvalidRequest}
	}
	for _, credential := range credentials {
		if _, err := tx.Exec(ctx, `INSERT INTO app_credentials
 (installation_id,app_id,credential_id,version,purpose,role_name,key_id,format_version,sealed)
 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
 ON CONFLICT (installation_id,credential_id,version) DO NOTHING`,
			installation, appID, credential.CredentialID, credential.Version,
			credential.Purpose, credential.RoleName, credential.Envelope.KeyID,
			credential.Envelope.FormatVersion, credential.Envelope.Sealed); err != nil {
			return admissionError(err)
		}
		var same bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM app_credentials
 WHERE installation_id=$1 AND credential_id=$2 AND version=$3 AND app_id=$4 AND purpose=$5 AND role_name=$6
 AND key_id=$7 AND format_version=$8 AND sealed=$9)`,
			installation, credential.CredentialID, credential.Version, appID,
			credential.Purpose, credential.RoleName, credential.Envelope.KeyID,
			credential.Envelope.FormatVersion, credential.Envelope.Sealed).Scan(&same); err != nil {
			return admissionError(err)
		}
		if !same {
			return ErrCredentialConflict
		}
	}
	return s.commitMetadata(ctx, tx)
}

// ReadAppCredential returns the newest sealed credential of one purpose, or nil when none exists.
func (s *Store) ReadAppCredential(ctx context.Context, installation, appID uuid.UUID, purpose string) (*Credential, error) {
	if installation == uuid.Nil || appID == uuid.Nil || !validPurpose(purpose) {
		return nil, &AdmissionError{Code: InvalidRequest}
	}
	var found Credential
	err := s.pool.QueryRow(ctx, `SELECT credential_id,version,purpose,role_name,key_id,format_version,sealed
 FROM app_credentials WHERE installation_id=$1 AND app_id=$2 AND purpose=$3
 ORDER BY version DESC LIMIT 1`, installation, appID, purpose).
		Scan(&found.CredentialID, &found.Version, &found.Purpose, &found.RoleName,
			&found.Envelope.KeyID, &found.Envelope.FormatVersion, &found.Envelope.Sealed)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, admissionError(err)
	}
	return &found, nil
}

// AuthorizeAppCredentialReveal commits an audit event and returns the client credential
// of a tier 1 app with its login name. PostgreSQL clients log in as the application role.
// Redis clients use the default user, which holds the superuser password.
func (s *Store) AuthorizeAppCredentialReveal(ctx context.Context, installationID, appID uuid.UUID, actorID, requestID string) (Credential, string, error) {
	if installationID == uuid.Nil || appID == uuid.Nil || len(actorID) < 1 || len(actorID) > 256 || !utf8.ValidString(actorID) || strings.ContainsRune(actorID, 0) || !printableWithin(requestID, 128) {
		return Credential{}, "", &AdmissionError{Code: InvalidRequest}
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Credential{}, "", admissionError(err)
	}
	defer tx.Rollback(ctx)
	// Keep deletion and credential writes behind the access decision.
	var engine string
	err = tx.QueryRow(ctx, `SELECT spec->'database'->>'engine' FROM apps WHERE installation_id=$1 AND id=$2
 AND tier=1 AND deleted_at IS NULL AND desired_lifecycle<>'deleted' FOR SHARE`, installationID, appID).Scan(&engine)
	if err != nil {
		return Credential{}, "", admissionError(err)
	}
	purpose, username := ApplicationCredential, "app"
	if engine == "redis" {
		purpose, username = SuperuserCredential, "default"
	}
	var found Credential
	err = tx.QueryRow(ctx, `SELECT credential_id,version,purpose,role_name,key_id,format_version,sealed
 FROM app_credentials WHERE installation_id=$1 AND app_id=$2 AND purpose=$3 ORDER BY version DESC LIMIT 1`, installationID, appID, purpose).
		Scan(&found.CredentialID, &found.Version, &found.Purpose, &found.RoleName, &found.Envelope.KeyID, &found.Envelope.FormatVersion, &found.Envelope.Sealed)
	if err != nil {
		return Credential{}, "", admissionError(err)
	}
	if purpose == ApplicationCredential {
		username = found.RoleName
	}
	_, err = tx.Exec(ctx, `INSERT INTO audit_events
 (id,installation_id,app_id,credential_id,credential_version,actor_id,request_id,action,outcome,created_at)
 VALUES ($1,$2,$3,$4,$5,$6,$7,'credential.reveal.accepted','accepted',clock_timestamp())`,
		uuid.New(), installationID, appID, found.CredentialID, found.Version, actorID, requestID)
	if err != nil {
		return Credential{}, "", admissionError(err)
	}
	if err := s.commitMetadata(ctx, tx); err != nil {
		return Credential{}, "", err
	}
	return found, username, nil
}
