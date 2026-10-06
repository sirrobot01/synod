package store

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/backup"
	"github.com/sirrobot01/synod/internal/destination"
	"github.com/sirrobot01/synod/internal/inventory"
	"github.com/sirrobot01/synod/internal/secret"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/proto"
)

// AddDestination saves settings once. A retry must have the same ID and input.
func (s *Store) AddDestination(ctx context.Context, installationID uuid.UUID, ring *secret.Keyring, entry destination.Entry, actorID, requestID string) (destination.Destination, error) {
	if entry.Validate() != nil || !entry.Destination.CreatedAt.IsZero() || installationID == uuid.Nil || !printableWithin(actorID, 256) || !printableWithin(requestID, 128) {
		return destination.Destination{}, &Error{Code: InvalidRequest}
	}
	input, err := json.Marshal(entry)
	if err != nil {
		return destination.Destination{}, &Error{Code: InvalidRequest}
	}
	defer clear(input)
	d := entry.Destination
	binding := secret.Binding{InstallationID: installationID, OwnerType: "destination", OwnerID: d.ID, CredentialID: d.ID, Version: 1}
	// Encrypt the whole input to bind the credentials to the exact public settings.
	envelope, err := ring.Seal(binding, input)
	if err != nil {
		return destination.Destination{}, storeError(err)
	}
	sealed, err := json.Marshal(envelope)
	if err != nil {
		return destination.Destination{}, storeError(err)
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return destination.Destination{}, storeError(err)
	}
	defer tx.Rollback()
	var previousDefinition, previousSealed []byte
	var removed sql.NullInt64
	err = tx.QueryRowContext(ctx, `SELECT definition,credentials,removed_at FROM destinations WHERE id=?`, d.ID).Scan(&previousDefinition, &previousSealed, &removed)
	if err == nil {
		if removed.Valid {
			return destination.Destination{}, &Error{Code: IdempotencyConflict}
		}
		var previous secret.Envelope
		if err := json.Unmarshal(previousSealed, &previous); err != nil {
			return destination.Destination{}, storeError(err)
		}
		plain, err := ring.Open(binding, previous)
		if err != nil {
			return destination.Destination{}, storeError(err)
		}
		defer clear(plain)
		if !bytes.Equal(plain, input) {
			return destination.Destination{}, &Error{Code: IdempotencyConflict}
		}
		if err := json.Unmarshal(previousDefinition, &d); err != nil {
			return destination.Destination{}, storeError(err)
		}
		return d, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return destination.Destination{}, storeError(err)
	}
	if d.NodeID != uuid.Nil {
		var allowed bool
		if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM nodes WHERE id=? AND approved_at IS NOT NULL AND revoked_at IS NULL AND (?!='device' OR destination_address!=''))`, d.NodeID, d.Location.Kind).Scan(&allowed); err != nil {
			return destination.Destination{}, storeError(err)
		}
		if !allowed {
			return destination.Destination{}, &Error{Code: InvalidRequest}
		}
	}
	var count, size int
	if err := tx.QueryRowContext(ctx, `SELECT count(*),coalesce(sum(input_bytes),0) FROM destinations WHERE removed_at IS NULL`).Scan(&count, &size); err != nil {
		return destination.Destination{}, storeError(err)
	}
	if count >= destination.MaxDestinations || size+len(input) > 384<<10 {
		return destination.Destination{}, &Error{Code: InvalidRequest}
	}
	d.CreatedAt = s.now().UTC()
	definition, err := json.Marshal(d)
	if err != nil {
		return destination.Destination{}, storeError(err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO destinations (id,name,node_id,kind,definition,credentials,input_bytes) VALUES (?,?,?,?,?,?,?)`, d.ID, d.Name, sql.Null[uuid.UUID]{V: d.NodeID, Valid: d.NodeID != uuid.Nil}, d.Location.Kind, definition, sealed, len(input)); err != nil {
		if isUnique(err) {
			return destination.Destination{}, &Error{Code: IdempotencyConflict}
		}
		return destination.Destination{}, storeError(err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE destination_revision SET revision=revision+1`); err != nil {
		return destination.Destination{}, storeError(err)
	}
	if err := audit(ctx, tx, actorID, requestID, "destination.added", d.ID, s.now()); err != nil {
		return destination.Destination{}, err
	}
	return d, storeCommit(tx)
}

// RemoveDestination removes saved credentials. It never deletes repository data.
func (s *Store) RemoveDestination(ctx context.Context, id uuid.UUID, actorID, requestID string) error {
	if id == uuid.Nil || !printableWithin(actorID, 256) || !printableWithin(requestID, 128) {
		return &Error{Code: InvalidRequest}
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return storeError(err)
	}
	defer tx.Rollback()
	var removed sql.NullInt64
	if err := tx.QueryRowContext(ctx, `SELECT removed_at FROM destinations WHERE id=?`, id).Scan(&removed); err != nil {
		return storeError(err)
	}
	if removed.Valid {
		return nil
	}
	var busy bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM operations WHERE kind='backup' AND status NOT IN ('succeeded','failed','cancelled') AND json_extract(CAST(input AS TEXT),'$.destination_id')=?)`, id).Scan(&busy); err != nil {
		return storeError(err)
	}
	if busy {
		return &Error{Code: OperationConflict}
	}
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM app_protection p JOIN json_each(CAST(p.settings AS TEXT),'$.destinations') d WHERE p.enabled=1 AND d.value=?) OR EXISTS(SELECT 1 FROM protection_defaults p JOIN json_each(CAST(p.settings AS TEXT),'$.destinations') d WHERE p.auto_protect=1 AND d.value=?)`, id, id).Scan(&busy); err != nil {
		return storeError(err)
	}
	if busy {
		return &Error{Code: OperationConflict}
	}
	if _, err := tx.ExecContext(ctx, `UPDATE destinations SET credentials=NULL,removed_at=? WHERE id=?`, s.now().UnixMilli(), id); err != nil {
		return storeError(err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE destination_revision SET revision=revision+1`); err != nil {
		return storeError(err)
	}
	if err := audit(ctx, tx, actorID, requestID, "destination.removed", id, s.now()); err != nil {
		return err
	}
	return storeCommit(tx)
}

// ListDestinations includes durable delivery status for each approved device.
func (s *Store) ListDestinations(ctx context.Context) (destination.Listing, error) {
	result := destination.Listing{Destinations: []destination.Destination{}, Delivery: []destination.Delivery{}}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return result, storeError(err)
	}
	defer tx.Rollback()
	if err := tx.QueryRowContext(ctx, `SELECT revision FROM destination_revision`).Scan(&result.Revision); err != nil {
		return result, storeError(err)
	}
	rows, err := tx.QueryContext(ctx, `SELECT definition FROM destinations WHERE removed_at IS NULL ORDER BY name,id`)
	if err != nil {
		return result, storeError(err)
	}
	defer rows.Close()
	for rows.Next() {
		var raw []byte
		var d destination.Destination
		if err := rows.Scan(&raw); err != nil {
			return result, storeError(err)
		}
		if err := json.Unmarshal(raw, &d); err != nil {
			return result, storeError(err)
		}
		result.Destinations = append(result.Destinations, d)
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return result, storeError(err)
	}
	rows, err = tx.QueryContext(ctx, `SELECT n.id,n.name,coalesce(n.session_id,''),coalesce(d.revision,0),coalesce(d.revision=? AND d.session_id=n.session_id,0),d.acknowledged_at FROM nodes n LEFT JOIN destination_deliveries d ON d.node_id=n.id WHERE n.approved_at IS NOT NULL AND n.revoked_at IS NULL ORDER BY n.name,n.id`, result.Revision)
	if err != nil {
		return result, storeError(err)
	}
	defer rows.Close()
	for rows.Next() {
		var d destination.Delivery
		if err := rows.Scan(&d.NodeID, &d.NodeName, &d.SessionID, &d.Revision, &d.Current, optionalMillis{&d.AcknowledgedAt}); err != nil {
			return result, storeError(err)
		}
		result.Delivery = append(result.Delivery, d)
	}
	if err := rows.Err(); err != nil {
		return result, storeError(err)
	}
	return result, storeCommit(tx)
}

// DestinationPlan returns authorized settings and the private recovery catalog.
func (s *Store) DestinationPlan(ctx context.Context, installationID, nodeID, sessionID uuid.UUID, ring *secret.Keyring) (destination.Plan, error) {
	result := destination.Plan{InstallationID: installationID, Destinations: []destination.Entry{}}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return result, storeError(err)
	}
	defer tx.Rollback()
	var allowed bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM nodes WHERE id=? AND session_id=? AND approved_at IS NOT NULL AND revoked_at IS NULL)`, nodeID, sessionID).Scan(&allowed); err != nil {
		return result, storeError(err)
	}
	if !allowed {
		return result, ErrStaleSession
	}
	if err := tx.QueryRowContext(ctx, `SELECT revision FROM destination_revision`).Scan(&result.Revision); err != nil {
		return result, storeError(err)
	}
	catalog, compressed, err := s.recoveryCatalog(ctx, tx, installationID, result.Revision, ring)
	if err != nil {
		return result, err
	}
	result.Catalog = compressed
	for _, target := range catalog.Destinations {
		if target.Location.Kind == "local" && target.NodeID != nodeID {
			continue
		}
		result.Destinations = append(result.Destinations, destination.Entry{Destination: destination.Destination{ID: target.ID, Name: target.Name, NodeID: target.NodeID, Location: target.Location, CreatedAt: target.CreatedAt}, Credentials: target.Credentials})
		if target.Initialized {
			result.Ready = append(result.Ready, target.ID)
		}
	}
	rows, err := tx.QueryContext(ctx, `SELECT n.id,n.destination_address,c.der_hash FROM nodes n JOIN node_certificates c ON c.node_id=n.id WHERE n.approved_at IS NOT NULL AND n.revoked_at IS NULL AND c.revoked_at IS NULL ORDER BY n.id,c.id`)
	if err != nil {
		return result, storeError(err)
	}
	defer rows.Close()
	for rows.Next() {
		var peer destination.Peer
		var hash []byte
		if err := rows.Scan(&peer.NodeID, &peer.Address, &hash); err != nil {
			return result, storeError(err)
		}
		peer.CertificateSHA256 = hex.EncodeToString(hash)
		result.Peers = append(result.Peers, peer)
	}
	if err := rows.Err(); err != nil {
		return result, storeError(err)
	}
	if err := rows.Close(); err != nil {
		return result, storeError(err)
	}
	rows, err = tx.QueryContext(ctx, `SELECT s.definition,a.definition FROM app_protection p JOIN backup_schedules s ON s.id=p.schedule_id JOIN apps a ON a.id=p.app_id WHERE p.enabled=1 AND s.node_id=? AND a.present=1 ORDER BY p.app_id`, nodeID)
	if err != nil {
		return result, storeError(err)
	}
	defer rows.Close()
	for rows.Next() {
		var raw, definition []byte
		if err := rows.Scan(&raw, &definition); err != nil {
			return result, storeError(err)
		}
		var schedule backup.Schedule
		app := inventory.App{Present: true, Definition: new(agentv1.DiscoveredApp)}
		if json.Unmarshal(raw, &schedule) != nil || schedule.InstallationID != installationID || proto.Unmarshal(definition, app.Definition) != nil {
			return result, &Error{Code: Unavailable}
		}
		if backup.BlockedCode(app) == "" {
			result.Schedules = append(result.Schedules, schedule)
		}
	}
	if err := rows.Err(); err != nil {
		return result, storeError(err)
	}
	if result.Validate(nodeID) != nil {
		return destination.Plan{}, &Error{Code: Unavailable}
	}
	return result, storeCommit(tx)
}

// AcknowledgeDestinations accepts a committed plan from the current approved session.
func (s *Store) AcknowledgeDestinations(ctx context.Context, nodeID, sessionID uuid.UUID, revision int64) error {
	if nodeID == uuid.Nil || sessionID == uuid.Nil || revision < 1 {
		return &Error{Code: InvalidRequest}
	}
	result, err := s.db.ExecContext(ctx, `INSERT INTO destination_deliveries (node_id,session_id,revision,acknowledged_at)
 SELECT id,session_id,?,? FROM nodes WHERE id=? AND session_id=? AND approved_at IS NOT NULL AND revoked_at IS NULL AND ?=(SELECT revision FROM destination_revision)
 ON CONFLICT(node_id) DO UPDATE SET session_id=excluded.session_id,revision=excluded.revision,acknowledged_at=excluded.acknowledged_at`, revision, s.now().UnixMilli(), nodeID, sessionID, revision)
	if err != nil {
		return storeError(err)
	}
	if n, err := result.RowsAffected(); err != nil {
		return storeError(err)
	} else if n == 0 {
		// An acknowledgement in flight can name the preceding revision. Resend the current plan.
		var current bool
		if err := s.db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM nodes WHERE id=? AND session_id=? AND approved_at IS NOT NULL AND revoked_at IS NULL)`, nodeID, sessionID).Scan(&current); err != nil {
			return storeError(err)
		}
		if !current {
			return ErrStaleSession
		}
	}
	return nil
}
