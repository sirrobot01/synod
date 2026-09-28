package postgres

import (
	"context"
	"errors"
	"regexp"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/secret"
)

// Credential purposes name the engine account each password belongs to.
const (
	SuperuserCredential   = "superuser"
	ApplicationCredential = "application"
	ReplicationCredential = "replication"
)

// identifierPattern is refused here rather than quoted later.
var identifierPattern = regexp.MustCompile(`^[a-z][a-z0-9_]{0,62}$`)

// Credential is one sealed secret. The caller seals before it calls, so a store that
// never receives a plaintext cannot leak one.
type Credential struct {
	CredentialID uuid.UUID
	Version      int64
	Purpose      string
	RoleName     string
	Envelope     secret.Envelope
}

// ErrCredentialConflict reports a second record that disagrees with the first.
var ErrCredentialConflict = errors.New("credential_conflict")

// ReadCredentialVersion returns the exact sealed reference from its owning app.
func (s *Store) ReadCredentialVersion(ctx context.Context, installationID, appID, credentialID uuid.UUID, version int64, purpose string) (Credential, error) {
	if installationID == uuid.Nil || appID == uuid.Nil || credentialID == uuid.Nil || version < 1 || !validPurpose(purpose) {
		return Credential{}, &AdmissionError{Code: InvalidRequest}
	}
	var found Credential
	err := s.pool.QueryRow(ctx, `SELECT credential_id,version,purpose,role_name,key_id,format_version,sealed
 FROM app_credentials WHERE installation_id=$1 AND app_id=$2 AND credential_id=$3 AND version=$4 AND purpose=$5`,
		installationID, appID, credentialID, version, purpose).
		Scan(&found.CredentialID, &found.Version, &found.Purpose, &found.RoleName,
			&found.Envelope.KeyID, &found.Envelope.FormatVersion, &found.Envelope.Sealed)
	if err != nil {
		return Credential{}, admissionError(err)
	}
	return found, nil
}

func validPurpose(purpose string) bool {
	return purpose == SuperuserCredential || purpose == ApplicationCredential || purpose == ReplicationCredential
}

// maxAppCredentials bounds one record. A database needs a handful, not a list.
const maxAppCredentials = 16

func validCredentials(credentials []Credential) error {
	if len(credentials) == 0 || len(credentials) > maxAppCredentials {
		return &AdmissionError{Code: InvalidRequest}
	}
	purposes := make(map[string]bool, len(credentials))
	identifiers := make(map[uuid.UUID]bool, len(credentials))
	for _, credential := range credentials {
		if credential.CredentialID == uuid.Nil || credential.Version < 1 || !validPurpose(credential.Purpose) || !identifierPattern.MatchString(credential.RoleName) ||
			credential.Envelope.FormatVersion != secret.FormatVersion || credential.Envelope.KeyID == uuid.Nil ||
			len(credential.Envelope.Sealed) == 0 || len(credential.Envelope.Sealed) > secret.MaxPlaintextBytes+28 {
			return &AdmissionError{Code: InvalidRequest}
		}
		if purposes[credential.Purpose] || identifiers[credential.CredentialID] {
			return &AdmissionError{Code: InvalidRequest}
		}
		purposes[credential.Purpose], identifiers[credential.CredentialID] = true, true
	}
	return nil
}
