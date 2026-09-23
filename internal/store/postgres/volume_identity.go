package postgres

import (
	"context"
	"errors"
	"strings"

	"github.com/google/uuid"
)

// ErrVolumeIdentityConflict reports a second declaration that disagrees with the
// identity already recorded. A volume whose declared identity moved is a volume
// nobody can prove anything about, so the disagreement is surfaced, never merged.
var ErrVolumeIdentityConflict = errors.New("volume_identity_conflict")

// Retention is what this installation intends for a volume's data.
//
// A normal delete retains by default, so a retained volume can outlive the cluster
// that created it. Purge is separate and explicit.
type Retention string

const (
	Retained       Retention = "retained"
	PurgeRequested Retention = "purge_requested"
	Purged         Retention = "purged"
)

// VolumeIdentity is what this installation believes one volume is.
//
// It is a declaration, not an observation. The agent proves the native object
// matches it before an engine starts, because specification 33 states that an empty
// mount path is not proof of a new database volume.
type VolumeIdentity struct {
	InstallationID uuid.UUID
	NodeID         uuid.UUID
	ClusterID      uuid.UUID
	ResourceID     uuid.UUID
	Engine         string
	EngineVersion  string
	// DataPath is where the engine reads inside its container. The host mountpoint
	// belongs to the agent, which verifies it locally and never sends it.
	DataPath             string
	CapacityRequestBytes int64
	Retention            Retention
}

// RecordVolumeIdentity writes the identity of one volume.
//
// The identity of a volume that already exists never changes. A second write with
// different content is a conflict rather than an update, because a volume whose
// declared identity moved is a volume nobody can prove anything about.
func (s *Store) RecordVolumeIdentity(ctx context.Context, identity VolumeIdentity) error {
	if err := identity.validate(); err != nil {
		return err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return admissionError(err)
	}
	defer tx.Rollback(ctx)
	// Retained identities can outlive their cluster. Lock it when it exists.
	if _, err := tx.Exec(ctx, `SELECT id FROM clusters WHERE installation_id=$1 AND id=$2 FOR SHARE`, identity.InstallationID, identity.ClusterID); err != nil {
		return admissionError(err)
	}
	tag, err := tx.Exec(ctx, `INSERT INTO volume_identities
 (installation_id,node_id,cluster_id,resource_id,engine,engine_version,data_path,capacity_request_bytes,retention)
 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
 ON CONFLICT (installation_id,resource_id) DO NOTHING`,
		identity.InstallationID, identity.NodeID, identity.ClusterID, identity.ResourceID,
		identity.Engine, identity.EngineVersion, identity.DataPath,
		identity.CapacityRequestBytes, string(identity.Retention))
	if err != nil {
		return admissionError(err)
	}
	if err := s.commitMetadata(ctx, tx); err != nil {
		return err
	}
	if tag.RowsAffected() == 1 {
		return nil
	}
	// A row already exists. An exact repeat is an ordinary replay; anything else
	// claims the volume is something other than what was recorded.
	stored, err := s.ReadVolumeIdentity(ctx, identity.InstallationID, identity.ResourceID)
	if err != nil {
		return err
	}
	if stored.NodeID != identity.NodeID || stored.ClusterID != identity.ClusterID ||
		stored.Engine != identity.Engine || stored.EngineVersion != identity.EngineVersion ||
		stored.DataPath != identity.DataPath || stored.CapacityRequestBytes != identity.CapacityRequestBytes {
		return ErrVolumeIdentityConflict
	}
	return nil
}

// ReadVolumeIdentity answers what this installation believes one volume is.
func (s *Store) ReadVolumeIdentity(ctx context.Context, installationID, resourceID uuid.UUID) (VolumeIdentity, error) {
	var identity VolumeIdentity
	if installationID == uuid.Nil || resourceID == uuid.Nil {
		return identity, &AdmissionError{Code: InvalidRequest}
	}
	var retention string
	err := s.pool.QueryRow(ctx, `SELECT installation_id,node_id,cluster_id,resource_id,engine,engine_version,
 data_path,capacity_request_bytes,retention FROM volume_identities
 WHERE installation_id=$1 AND resource_id=$2`, installationID, resourceID).Scan(
		&identity.InstallationID, &identity.NodeID, &identity.ClusterID, &identity.ResourceID,
		&identity.Engine, &identity.EngineVersion, &identity.DataPath,
		&identity.CapacityRequestBytes, &retention)
	if err != nil {
		return VolumeIdentity{}, admissionError(err)
	}
	identity.Retention = Retention(retention)
	return identity, nil
}

// ReadNodeVolumeIdentities answers what one node is believed to hold.
//
// A node being drained or removed needs this: retained storage is what stops a node
// from being declared free, and it is not visible from the cluster alone.
func (s *Store) ReadNodeVolumeIdentities(ctx context.Context, installationID, nodeID uuid.UUID) ([]VolumeIdentity, error) {
	if installationID == uuid.Nil || nodeID == uuid.Nil {
		return nil, &AdmissionError{Code: InvalidRequest}
	}
	rows, err := s.pool.Query(ctx, `SELECT installation_id,node_id,cluster_id,resource_id,engine,engine_version,
 data_path,capacity_request_bytes,retention FROM volume_identities
 WHERE installation_id=$1 AND node_id=$2 ORDER BY resource_id LIMIT $3`, installationID, nodeID, maxVolumeIdentities+1)
	if err != nil {
		return nil, admissionError(err)
	}
	defer rows.Close()
	var found []VolumeIdentity
	for rows.Next() {
		var identity VolumeIdentity
		var retention string
		if err := rows.Scan(&identity.InstallationID, &identity.NodeID, &identity.ClusterID, &identity.ResourceID,
			&identity.Engine, &identity.EngineVersion, &identity.DataPath,
			&identity.CapacityRequestBytes, &retention); err != nil {
			return nil, admissionError(err)
		}
		identity.Retention = Retention(retention)
		found = append(found, identity)
	}
	if rows.Err() != nil {
		return nil, admissionError(rows.Err())
	}
	if len(found) > maxVolumeIdentities {
		return nil, ErrSnapshotTooLarge
	}
	return found, nil
}

// SetVolumeRetention records a change of intent for one volume's data.
//
// It moves intent only. Nothing here removes anything: specification 33 keeps purge
// as a separate, explicitly confirmed operation with its own validation.
func (s *Store) SetVolumeRetention(ctx context.Context, installationID, resourceID uuid.UUID, retention Retention) error {
	if installationID == uuid.Nil || resourceID == uuid.Nil || !retention.valid() {
		return &AdmissionError{Code: InvalidRequest}
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return admissionError(err)
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `SELECT c.id FROM clusters c JOIN volume_identities v ON c.installation_id=v.installation_id AND c.id=v.cluster_id WHERE v.installation_id=$1 AND v.resource_id=$2 FOR SHARE OF c`, installationID, resourceID); err != nil {
		return admissionError(err)
	}
	tag, err := tx.Exec(ctx, `UPDATE volume_identities SET retention=$1,updated_at=clock_timestamp()
 WHERE installation_id=$2 AND resource_id=$3`, string(retention), installationID, resourceID)
	if err != nil {
		return admissionError(err)
	}
	if tag.RowsAffected() != 1 {
		return &AdmissionError{Code: NotFound}
	}
	return s.commitMetadata(ctx, tx)
}

// maxVolumeIdentities bounds one node read.
const maxVolumeIdentities = 4096

func (r Retention) valid() bool {
	return r == Retained || r == PurgeRequested || r == Purged
}

func (v VolumeIdentity) validate() error {
	for _, id := range []uuid.UUID{v.InstallationID, v.NodeID, v.ClusterID, v.ResourceID} {
		if id == uuid.Nil {
			return &AdmissionError{Code: InvalidRequest}
		}
	}
	if v.Engine != "postgres" && v.Engine != "redis" {
		return &AdmissionError{Code: InvalidRequest}
	}
	if !boundedIdentityText(v.EngineVersion, 64) || v.CapacityRequestBytes <= 0 || !v.Retention.valid() {
		return &AdmissionError{Code: InvalidRequest}
	}
	// The path is what an engine reads. A relative or unclean one names something
	// other than what was intended, so it is refused rather than normalised.
	if !strings.HasPrefix(v.DataPath, "/") || !boundedIdentityText(v.DataPath, 1024) || strings.Contains(v.DataPath, "..") {
		return &AdmissionError{Code: InvalidRequest}
	}
	return nil
}

// boundedIdentityText reports printable text within a length the schema accepts.
func boundedIdentityText(value string, limit int) bool {
	if value == "" || len(value) > limit {
		return false
	}
	return !strings.ContainsFunc(value, func(r rune) bool { return r < '!' || r > '~' })
}
