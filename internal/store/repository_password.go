package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/secret"
)

func passwordBinding(installationID uuid.UUID) secret.Binding {
	return secret.Binding{InstallationID: installationID, OwnerType: "installation", OwnerID: installationID, CredentialID: uuid.NewSHA1(installationID, []byte("repository-password")), Version: 1}
}

// SetRepositoryPassword saves the recovery kit password, sealed with the credential key ring.
func (s *Store) SetRepositoryPassword(ctx context.Context, installationID uuid.UUID, ring *secret.Keyring, password string) error {
	if installationID == uuid.Nil || ring == nil || len(password) < 16 || len(password) > 1024 {
		return &Error{Code: InvalidRequest}
	}
	plain := []byte(password)
	defer clear(plain)
	envelope, err := ring.Seal(passwordBinding(installationID), plain)
	if err != nil {
		return storeError(err)
	}
	sealed, err := json.Marshal(envelope)
	if err != nil {
		return storeError(err)
	}
	if _, err := s.db.ExecContext(ctx, `INSERT INTO repository_password (singleton,sealed) VALUES (1,?) ON CONFLICT(singleton) DO UPDATE SET sealed=excluded.sealed`, sealed); err != nil {
		return storeError(err)
	}
	return nil
}

// RepositoryPassword returns the recovery kit password, or an empty string when none is saved.
func (s *Store) RepositoryPassword(ctx context.Context, installationID uuid.UUID, ring *secret.Keyring) (string, error) {
	return repositoryPassword(ctx, s.db, installationID, ring)
}

// repositoryPassword returns the recovery kit password, or an empty string before init saves one.
func repositoryPassword(ctx context.Context, db interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, installationID uuid.UUID, ring *secret.Keyring) (string, error) {
	var sealed []byte
	err := db.QueryRowContext(ctx, `SELECT sealed FROM repository_password WHERE singleton=1`).Scan(&sealed)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	if err != nil {
		return "", storeError(err)
	}
	var envelope secret.Envelope
	if err := json.Unmarshal(sealed, &envelope); err != nil {
		return "", &Error{Code: Unavailable}
	}
	plain, err := ring.Open(passwordBinding(installationID), envelope)
	if err != nil {
		return "", storeError(err)
	}
	defer clear(plain)
	return string(plain), nil
}
