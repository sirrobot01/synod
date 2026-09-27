package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/app"
)

// CreateApp creates a new application in PostgreSQL store.
func (s *Store) CreateApp(ctx context.Context, item app.App) (app.App, error) {
	if item.InstallationID == uuid.Nil || item.Name == "" || item.Spec.Validate() != nil {
		return app.App{}, &AdmissionError{Code: InvalidRequest}
	}
	if item.ID == uuid.Nil {
		item.ID = uuid.New()
	}
	if item.DesiredRevision <= 0 {
		item.DesiredRevision = 1
	}
	if item.DesiredLifecycle == "" {
		item.DesiredLifecycle = "running"
	}
	if item.ObservedLifecycle == "" {
		item.ObservedLifecycle = "provisioning"
	}
	now := time.Now().UTC()
	item.CreatedAt = now
	item.UpdatedAt = now

	specJSON, err := json.Marshal(item.Spec)
	if err != nil {
		return app.App{}, fmt.Errorf("marshal app spec: %w", err)
	}

	_, err = s.pool.Exec(ctx, `INSERT INTO apps (
		id, installation_id, name, tier, spec_version, spec,
		desired_revision, desired_lifecycle, observed_revision, observed_lifecycle,
		created_at, updated_at
	) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)`,
		item.ID, item.InstallationID, item.Name, item.Tier, item.Spec.SchemaVersion, specJSON,
		item.DesiredRevision, item.DesiredLifecycle, item.ObservedRevision, item.ObservedLifecycle,
		item.CreatedAt, item.UpdatedAt,
	)
	if err != nil {
		return app.App{}, admissionError(err)
	}
	return item, nil
}

// GetApp fetches an app by ID.
func (s *Store) GetApp(ctx context.Context, installationID, id uuid.UUID) (app.App, error) {
	if installationID == uuid.Nil || id == uuid.Nil {
		return app.App{}, &AdmissionError{Code: InvalidRequest}
	}
	var item app.App
	var specJSON []byte
	err := s.pool.QueryRow(ctx, `SELECT id, installation_id, name, tier, spec,
		desired_revision, desired_lifecycle, observed_revision, observed_lifecycle,
		created_at, updated_at, deleted_at
		FROM apps WHERE installation_id = $1 AND id = $2`, installationID, id).
		Scan(
			&item.ID, &item.InstallationID, &item.Name, &item.Tier, &specJSON,
			&item.DesiredRevision, &item.DesiredLifecycle, &item.ObservedRevision, &item.ObservedLifecycle,
			&item.CreatedAt, &item.UpdatedAt, &item.DeletedAt,
		)
	if err != nil {
		return app.App{}, admissionError(err)
	}
	if err := json.Unmarshal(specJSON, &item.Spec); err != nil {
		return app.App{}, fmt.Errorf("decode app spec: %w", err)
	}
	return item, nil
}

// GetAppByName fetches an app by name.
func (s *Store) GetAppByName(ctx context.Context, installationID uuid.UUID, name string) (app.App, error) {
	if installationID == uuid.Nil || name == "" {
		return app.App{}, &AdmissionError{Code: InvalidRequest}
	}
	var item app.App
	var specJSON []byte
	err := s.pool.QueryRow(ctx, `SELECT id, installation_id, name, tier, spec,
		desired_revision, desired_lifecycle, observed_revision, observed_lifecycle,
		created_at, updated_at, deleted_at
		FROM apps WHERE installation_id = $1 AND name = $2 AND deleted_at IS NULL`, installationID, name).
		Scan(
			&item.ID, &item.InstallationID, &item.Name, &item.Tier, &specJSON,
			&item.DesiredRevision, &item.DesiredLifecycle, &item.ObservedRevision, &item.ObservedLifecycle,
			&item.CreatedAt, &item.UpdatedAt, &item.DeletedAt,
		)
	if err != nil {
		return app.App{}, admissionError(err)
	}
	if err := json.Unmarshal(specJSON, &item.Spec); err != nil {
		return app.App{}, fmt.Errorf("decode app spec: %w", err)
	}
	return item, nil
}

// ListApps returns all active apps for an installation.
func (s *Store) ListApps(ctx context.Context, installationID uuid.UUID) ([]app.App, error) {
	if installationID == uuid.Nil {
		return nil, &AdmissionError{Code: InvalidRequest}
	}
	rows, err := s.pool.Query(ctx, `SELECT id, installation_id, name, tier, spec,
		desired_revision, desired_lifecycle, observed_revision, observed_lifecycle,
		created_at, updated_at, deleted_at
		FROM apps WHERE installation_id = $1 AND deleted_at IS NULL ORDER BY name`, installationID)
	if err != nil {
		return nil, admissionError(err)
	}
	defer rows.Close()

	var apps []app.App
	for rows.Next() {
		var item app.App
		var specJSON []byte
		if err := rows.Scan(
			&item.ID, &item.InstallationID, &item.Name, &item.Tier, &specJSON,
			&item.DesiredRevision, &item.DesiredLifecycle, &item.ObservedRevision, &item.ObservedLifecycle,
			&item.CreatedAt, &item.UpdatedAt, &item.DeletedAt,
		); err != nil {
			return nil, admissionError(err)
		}
		if err := json.Unmarshal(specJSON, &item.Spec); err != nil {
			return nil, fmt.Errorf("decode app spec: %w", err)
		}
		apps = append(apps, item)
	}
	if err := rows.Err(); err != nil {
		return nil, admissionError(err)
	}
	return apps, nil
}

// SaveAppCopies stores or updates the allocated copies for an app.
func (s *Store) SaveAppCopies(ctx context.Context, installationID, appID uuid.UUID, copies []app.Copy) error {
	if installationID == uuid.Nil || appID == uuid.Nil {
		return &AdmissionError{Code: InvalidRequest}
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return admissionError(err)
	}
	defer tx.Rollback(ctx)

	for _, c := range copies {
		if c.ID == uuid.Nil {
			c.ID = uuid.New()
		}
		now := time.Now().UTC()
		_, err := tx.Exec(ctx, `INSERT INTO app_copies (
			id, installation_id, app_id, node_id, copy_index, host_port, observed_state, created_at, updated_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		ON CONFLICT (installation_id, app_id, copy_index) WHERE desired DO UPDATE SET
			node_id = EXCLUDED.node_id,
			host_port = EXCLUDED.host_port,
			observed_state = EXCLUDED.observed_state,
			updated_at = EXCLUDED.updated_at`,
			c.ID, installationID, appID, c.NodeID, c.CopyIndex, c.HostPort, c.ObservedState, now, now,
		)
		if err != nil {
			return admissionError(err)
		}
	}
	return tx.Commit(ctx)
}

// GetAppCopies returns all allocated copies for an app.
func (s *Store) GetAppCopies(ctx context.Context, installationID, appID uuid.UUID) ([]app.Copy, error) {
	if installationID == uuid.Nil || appID == uuid.Nil {
		return nil, &AdmissionError{Code: InvalidRequest}
	}
	rows, err := s.pool.Query(ctx, `SELECT c.id,c.app_id,c.node_id,c.copy_index,c.host_port,c.observed_state,c.created_at,c.updated_at,
 CASE WHEN c.observed_session_id=p.session_id AND c.observed_at>clock_timestamp()-interval '20 seconds'
 AND p.last_seen_at>clock_timestamp()-interval '20 seconds' AND NOT p.recovery_required AND n.revoked_at IS NULL THEN c.health ELSE 'unknown' END,c.observed_at
 FROM app_copies c LEFT JOIN agent_presence p ON p.installation_id=c.installation_id AND p.node_id=c.node_id
 JOIN nodes n ON n.installation_id=c.installation_id AND n.id=c.node_id
 WHERE c.installation_id=$1 AND c.app_id=$2 AND c.desired ORDER BY c.copy_index`, installationID, appID)
	if err != nil {
		return nil, admissionError(err)
	}
	defer rows.Close()

	var copies []app.Copy
	for rows.Next() {
		var c app.Copy
		if err := rows.Scan(&c.ID, &c.AppID, &c.NodeID, &c.CopyIndex, &c.HostPort, &c.ObservedState, &c.CreatedAt, &c.UpdatedAt, &c.Health, &c.ObservedAt); err != nil {
			return nil, admissionError(err)
		}
		copies = append(copies, c)
	}
	if err := rows.Err(); err != nil {
		return nil, admissionError(err)
	}
	return copies, nil
}
