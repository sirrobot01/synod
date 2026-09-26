package postgres

import (
	"context"
	"errors"
	"regexp"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/sirrobot01/synod/internal/secret"
)

// ErrDispatchAlreadyHappened reports intent recorded after a command was already sent.
// Specification 07 requires it persisted before dispatch, and afterwards it cannot be
// what the dispatch was based on.
var ErrDispatchAlreadyHappened = errors.New("dispatch_already_happened")

// Credential purposes separate SQL access from native HA identities.
const (
	SuperuserCredential        = "superuser"
	ApplicationCredential      = "application"
	ReplicationCredential      = "replication"
	PatroniRewindCredential    = "patroni_rewind"
	PatroniRESTCredential      = "patroni_rest"
	PatroniMemberTLSCredential = "patroni_member_tls"
	PatroniDCSTLSCredential    = "patroni_dcs_tls"
)

// identifierPattern is refused here rather than quoted later.
var identifierPattern = regexp.MustCompile(`^[a-z][a-z0-9_]{0,62}$`)

// Credential is one sealed secret. The caller seals before it calls, so a store that
// never receives a plaintext cannot leak one.
type Credential struct {
	CredentialID uuid.UUID
	MemberID     uuid.UUID
	Version      int64
	Purpose      string
	RoleName     string
	Envelope     secret.Envelope
	// ExpiresAt is the certificate expiry of TLS material. Other purposes have none.
	ExpiresAt time.Time
}

// Database is one logical database this installation intends the cluster to hold.
type Database struct {
	Name      string
	OwnerRole string
}

// ClusterIntent is everything a provision means to create, written before dispatch.
type ClusterIntent struct {
	InstallationID uuid.UUID
	ClusterID      uuid.UUID
	Databases      []Database
	Credentials    []Credential
}

// RecordClusterIntent writes what a provision intends, before any command is sent. A
// repeat that disagrees is refused rather than merged: a credential whose recorded
// version changed underneath is one nobody can match to what a host holds.
func (s *Store) RecordClusterIntent(ctx context.Context, intent ClusterIntent) error {
	if err := intent.validate(); err != nil {
		return err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return admissionError(err)
	}
	defer tx.Rollback(ctx)
	// An EXISTS that finds nothing locks nothing, so the cluster row is the only
	// object that can serialize this against a writer that dispatches a command.
	if _, err := tx.Exec(ctx, `SELECT 1 FROM clusters WHERE installation_id=$1 AND id=$2 FOR UPDATE`,
		intent.InstallationID, intent.ClusterID); err != nil {
		return admissionError(err)
	}
	var dispatched bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM command_dispatches
 WHERE installation_id=$1 AND cluster_id=$2)`, intent.InstallationID, intent.ClusterID).Scan(&dispatched); err != nil {
		return admissionError(err)
	}
	if dispatched {
		return ErrDispatchAlreadyHappened
	}
	if err := recordClusterIntentTx(ctx, tx, intent, true); err != nil {
		return err
	}
	return s.commitMetadata(ctx, tx)
}

// recordClusterIntentTx checks exact stored intent and optionally inserts missing records.
func recordClusterIntentTx(ctx context.Context, tx pgx.Tx, intent ClusterIntent, write bool) error {
	for _, database := range intent.Databases {
		if write {
			if _, err := tx.Exec(ctx, `INSERT INTO cluster_databases (installation_id,cluster_id,name,owner_role)
 VALUES ($1,$2,$3,$4) ON CONFLICT (installation_id,cluster_id,name) DO NOTHING`,
				intent.InstallationID, intent.ClusterID, database.Name, database.OwnerRole); err != nil {
				return admissionError(err)
			}
		}
		var same bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM cluster_databases
 WHERE installation_id=$1 AND cluster_id=$2 AND name=$3 AND owner_role=$4)`,
			intent.InstallationID, intent.ClusterID, database.Name, database.OwnerRole).Scan(&same); err != nil {
			return admissionError(err)
		}
		if !same {
			return ErrCredentialConflict
		}
	}
	for _, credential := range intent.Credentials {
		if write {
			if _, err := tx.Exec(ctx, `INSERT INTO cluster_credentials
 (installation_id,cluster_id,credential_id,version,purpose,role_name,key_id,format_version,sealed,member_id,expires_at)
 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,NULLIF($10::uuid,'00000000-0000-0000-0000-000000000000'),$11)
 ON CONFLICT (installation_id,credential_id,version) DO NOTHING`,
				intent.InstallationID, intent.ClusterID, credential.CredentialID, credential.Version,
				credential.Purpose, credential.RoleName, credential.Envelope.KeyID,
				credential.Envelope.FormatVersion, credential.Envelope.Sealed, credential.MemberID, expiry(credential.ExpiresAt)); err != nil {
				return admissionError(err)
			}
		}
		var same bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM cluster_credentials
 WHERE installation_id=$1 AND credential_id=$2 AND version=$3 AND cluster_id=$4 AND purpose=$5 AND role_name=$6
 AND key_id=$7 AND format_version=$8 AND sealed=$9
 AND member_id IS NOT DISTINCT FROM NULLIF($10::uuid,'00000000-0000-0000-0000-000000000000')
 AND expires_at IS NOT DISTINCT FROM $11)`,
			intent.InstallationID, credential.CredentialID, credential.Version, intent.ClusterID,
			credential.Purpose, credential.RoleName, credential.Envelope.KeyID,
			credential.Envelope.FormatVersion, credential.Envelope.Sealed, credential.MemberID, expiry(credential.ExpiresAt)).Scan(&same); err != nil {
			return admissionError(err)
		}
		if !same {
			return ErrCredentialConflict
		}
	}
	return nil
}

// expiry stores a zero time as NULL.
func expiry(at time.Time) *time.Time {
	if at.IsZero() {
		return nil
	}
	at = at.UTC().Truncate(time.Microsecond)
	return &at
}

// ErrCredentialConflict reports a second record that disagrees with the first.
var ErrCredentialConflict = errors.New("credential_conflict")

// ReadClusterCredential returns the newest sealed SQL credential of one cluster-wide purpose.
func (s *Store) ReadClusterCredential(ctx context.Context, installationID, clusterID uuid.UUID, purpose string) (Credential, error) {
	var found Credential
	if installationID == uuid.Nil || clusterID == uuid.Nil || !validSQLPurpose(purpose) {
		return found, &AdmissionError{Code: InvalidRequest}
	}
	err := s.pool.QueryRow(ctx, `SELECT credential_id,version,purpose,role_name,key_id,format_version,sealed
 FROM cluster_credentials WHERE installation_id=$1 AND cluster_id=$2 AND purpose=$3
 ORDER BY version DESC LIMIT 1`, installationID, clusterID, purpose).
		Scan(&found.CredentialID, &found.Version, &found.Purpose, &found.RoleName,
			&found.Envelope.KeyID, &found.Envelope.FormatVersion, &found.Envelope.Sealed)
	if err != nil {
		return Credential{}, admissionError(err)
	}
	return found, nil
}

// ReadCredentialVersion returns the exact sealed reference from its owning cluster.
func (s *Store) ReadCredentialVersion(ctx context.Context, installationID, clusterID, credentialID uuid.UUID, version int64, purpose string) (Credential, error) {
	if installationID == uuid.Nil || clusterID == uuid.Nil || credentialID == uuid.Nil || version < 1 || !validPurpose(purpose) {
		return Credential{}, &AdmissionError{Code: InvalidRequest}
	}
	var found Credential
	err := s.pool.QueryRow(ctx, `SELECT credential_id,COALESCE(member_id,'00000000-0000-0000-0000-000000000000'),version,purpose,role_name,key_id,format_version,sealed
 FROM cluster_credentials WHERE installation_id=$1 AND cluster_id=$2 AND credential_id=$3 AND version=$4 AND purpose=$5`,
		installationID, clusterID, credentialID, version, purpose).
		Scan(&found.CredentialID, &found.MemberID, &found.Version, &found.Purpose, &found.RoleName,
			&found.Envelope.KeyID, &found.Envelope.FormatVersion, &found.Envelope.Sealed)
	if err != nil {
		return Credential{}, admissionError(err)
	}
	return found, nil
}

// ReadClusterDatabases answers what this installation intends the cluster to hold.
func (s *Store) ReadClusterDatabases(ctx context.Context, installationID, clusterID uuid.UUID) ([]Database, error) {
	if installationID == uuid.Nil || clusterID == uuid.Nil {
		return nil, &AdmissionError{Code: InvalidRequest}
	}
	rows, err := s.pool.Query(ctx, `SELECT name,owner_role FROM cluster_databases
 WHERE installation_id=$1 AND cluster_id=$2 ORDER BY name`, installationID, clusterID)
	if err != nil {
		return nil, admissionError(err)
	}
	defer rows.Close()
	var found []Database
	for rows.Next() {
		var database Database
		if err := rows.Scan(&database.Name, &database.OwnerRole); err != nil {
			return nil, admissionError(err)
		}
		found = append(found, database)
	}
	if rows.Err() != nil {
		return nil, admissionError(rows.Err())
	}
	return found, nil
}

func validPurpose(purpose string) bool {
	switch purpose {
	case PatroniRewindCredential, PatroniRESTCredential, PatroniMemberTLSCredential, PatroniDCSTLSCredential:
		return true
	}
	return validSQLPurpose(purpose)
}

func validSQLPurpose(purpose string) bool {
	return purpose == SuperuserCredential || purpose == ApplicationCredential || purpose == ReplicationCredential
}

// maxClusterCredentials bounds one intent. A cluster needs a handful, not a list.
const maxClusterCredentials = 16

func (c ClusterIntent) validate() error {
	if c.InstallationID == uuid.Nil || c.ClusterID == uuid.Nil ||
		len(c.Credentials) == 0 || len(c.Credentials) > maxClusterCredentials ||
		len(c.Databases) > maxClusterCredentials {
		return &AdmissionError{Code: InvalidRequest}
	}
	roles := make(map[string]bool, len(c.Credentials))
	type scope struct {
		purpose string
		member  uuid.UUID
	}
	purposes := make(map[scope]bool, len(c.Credentials))
	identifiers := make(map[uuid.UUID]bool, len(c.Credentials))
	for _, credential := range c.Credentials {
		if credential.CredentialID == uuid.Nil || credential.Version < 1 || !validPurpose(credential.Purpose) ||
			credential.Envelope.FormatVersion != secret.FormatVersion || credential.Envelope.KeyID == uuid.Nil ||
			len(credential.Envelope.Sealed) == 0 || len(credential.Envelope.Sealed) > secret.MaxPlaintextBytes+28 {
			return &AdmissionError{Code: InvalidRequest}
		}
		validRole := false
		switch credential.Purpose {
		case SuperuserCredential, ApplicationCredential, ReplicationCredential:
			validRole = identifierPattern.MatchString(credential.RoleName)
			roles[credential.RoleName] = true
		case PatroniRewindCredential:
			validRole = credential.RoleName == "synod_rewind"
		case PatroniRESTCredential:
			validRole = credential.RoleName == "synod_patroni"
		case PatroniMemberTLSCredential, PatroniDCSTLSCredential:
			validRole = credential.RoleName == ""
		}
		tls := credential.Purpose == PatroniMemberTLSCredential || credential.Purpose == PatroniDCSTLSCredential
		if !validRole || (credential.Purpose == PatroniMemberTLSCredential) != (credential.MemberID != uuid.Nil) || tls == credential.ExpiresAt.IsZero() {
			return &AdmissionError{Code: InvalidRequest}
		}
		key := scope{credential.Purpose, credential.MemberID}
		if purposes[key] || identifiers[credential.CredentialID] {
			return &AdmissionError{Code: InvalidRequest}
		}
		purposes[key], identifiers[credential.CredentialID] = true, true
	}
	names := make(map[string]bool, len(c.Databases))
	for _, database := range c.Databases {
		if !identifierPattern.MatchString(database.Name) || names[database.Name] {
			return &AdmissionError{Code: InvalidRequest}
		}
		// A database owned by a role this intent does not create is one nothing can
		// log in to.
		if !roles[database.OwnerRole] {
			return &AdmissionError{Code: InvalidRequest}
		}
		names[database.Name] = true
	}
	return nil
}
