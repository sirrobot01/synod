package store

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/backup"
	"github.com/sirrobot01/synod/internal/destination"
	"github.com/sirrobot01/synod/internal/inventory"
	"github.com/sirrobot01/synod/internal/proof"
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
	if _, err := assignMaintenance(ctx, tx, s.now()); err != nil {
		return destination.Destination{}, err
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
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM operations WHERE status NOT IN ('succeeded','failed','cancelled') AND ((kind IN ('backup','integrity_check','repository_maintenance') AND json_extract(CAST(input AS TEXT),'$.destination_id')=?) OR (kind IN ('restore','restore_drill') AND (json_extract(CAST(input AS TEXT),'$.destination_id') IS NULL OR json_extract(CAST(input AS TEXT),'$.destination_id')=?))))`, id, id).Scan(&busy); err != nil {
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

// MaintenanceOwnerMissing is how long a remote destination's maintenance owner can be absent before another device takes over.
const MaintenanceOwnerMissing = 7 * 24 * time.Hour

// assignMaintenance gives each live destination one maintenance device. The caller changes the plan revision when it reports a change.
// A local or device repository belongs to its device. A remote repository moves only when its device is gone or missing.
func assignMaintenance(ctx context.Context, tx *sql.Tx, now time.Time) (bool, error) {
	result, err := tx.ExecContext(ctx, `UPDATE destinations SET maintenance_node_id=node_id WHERE removed_at IS NULL AND node_id IS NOT NULL AND maintenance_node_id IS NOT node_id`)
	if err != nil {
		return false, storeError(err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return false, storeError(err)
	}
	// Prefer the earliest approved device that was seen recently. Without one, a destination with no owner takes any approved device.
	cutoff := now.Add(-MaintenanceOwnerMissing).UnixMilli()
	var owner uuid.UUID
	err = tx.QueryRowContext(ctx, `SELECT id FROM nodes WHERE approved_at IS NOT NULL AND revoked_at IS NULL ORDER BY coalesce(last_seen_at,0)<?,approved_at,id LIMIT 1`, cutoff).Scan(&owner)
	if errors.Is(err, sql.ErrNoRows) {
		return changed > 0, nil
	}
	if err != nil {
		return false, storeError(err)
	}
	result, err = tx.ExecContext(ctx, `UPDATE destinations SET maintenance_node_id=? WHERE removed_at IS NULL AND node_id IS NULL AND maintenance_node_id IS NOT ?
 AND (maintenance_node_id IS NULL OR EXISTS(SELECT 1 FROM nodes WHERE id=? AND last_seen_at>=?))
 AND NOT EXISTS(SELECT 1 FROM nodes n WHERE n.id=maintenance_node_id AND n.approved_at IS NOT NULL AND n.revoked_at IS NULL AND n.last_seen_at>=?)`, owner, owner, owner, cutoff, cutoff)
	if err != nil {
		return false, storeError(err)
	}
	moved, err := result.RowsAffected()
	if err != nil {
		return false, storeError(err)
	}
	return changed+moved > 0, nil
}

// ReassignMaintenance moves remote repository maintenance away from a device that has been missing for MaintenanceOwnerMissing.
func (s *Store) ReassignMaintenance(ctx context.Context) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return storeError(err)
	}
	defer tx.Rollback()
	changed, err := assignMaintenance(ctx, tx, s.now())
	if err != nil || !changed {
		return err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE destination_revision SET revision=revision+1`); err != nil {
		return storeError(err)
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
	if err := rows.Close(); err != nil {
		return result, storeError(err)
	}
	rows, err = tx.QueryContext(ctx, `SELECT id,node_id,status,result,json_extract(CAST(input AS TEXT),'$.destination_id') FROM (
 SELECT *,row_number() OVER (PARTITION BY json_extract(CAST(input AS TEXT),'$.destination_id') ORDER BY coalesce(unixepoch(json_extract(CAST(result AS TEXT),'$.started_at'),'subsec')*1000,created_at) DESC,created_at DESC,id DESC) AS position
 FROM operations WHERE kind='integrity_check') WHERE position=1`)
	if err != nil {
		return result, storeError(err)
	}
	defer rows.Close()
	for rows.Next() {
		var status proof.Status
		var raw []byte
		if err := rows.Scan(&status.ID, &status.NodeID, &status.State, &raw, &status.DestinationID); err != nil {
			return result, storeError(err)
		}
		if len(raw) > 0 && json.Unmarshal(raw, &status.Result) != nil {
			return result, &Error{Code: Unavailable}
		}
		result.Checks = append(result.Checks, status)
	}
	if err := rows.Err(); err != nil {
		return result, storeError(err)
	}
	if err := rows.Close(); err != nil {
		return result, storeError(err)
	}
	rows, err = tx.QueryContext(ctx, `SELECT d.id,coalesce(d.maintenance_node_id,''),o.id,o.node_id,o.status,o.result FROM destinations d LEFT JOIN (
 SELECT *,json_extract(CAST(input AS TEXT),'$.destination_id') AS destination_id,row_number() OVER (PARTITION BY json_extract(CAST(input AS TEXT),'$.destination_id') ORDER BY created_at DESC,id DESC) AS position
 FROM operations WHERE kind='repository_maintenance') o ON o.destination_id=d.id AND o.position=1 WHERE d.removed_at IS NULL ORDER BY d.name,d.id`)
	if err != nil {
		return result, storeError(err)
	}
	defer rows.Close()
	result.Maintenance = []destination.Maintenance{}
	for rows.Next() {
		var m destination.Maintenance
		var owner string
		var id, node, state sql.NullString
		var raw []byte
		if err := rows.Scan(&m.DestinationID, &owner, &id, &node, &state, &raw); err != nil {
			return result, storeError(err)
		}
		if owner != "" {
			if m.NodeID, err = uuid.Parse(owner); err != nil {
				return result, &Error{Code: Unavailable}
			}
		}
		if id.Valid {
			m.Last = &proof.Status{DestinationID: m.DestinationID, State: state.String}
			m.Last.ID, err = uuid.Parse(id.String)
			if err == nil {
				m.Last.NodeID, err = uuid.Parse(node.String)
			}
			if err != nil || (len(raw) > 0 && json.Unmarshal(raw, &m.Last.Result) != nil) {
				return result, &Error{Code: Unavailable}
			}
		}
		result.Maintenance = append(result.Maintenance, m)
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return result, storeError(err)
	}
	rows, err = tx.QueryContext(ctx, `SELECT d.id,h.observed_at,coalesce(h.total_bytes,0),coalesce(h.available_bytes,0) FROM destinations d LEFT JOIN destination_health h ON h.destination_id=d.id WHERE d.removed_at IS NULL ORDER BY d.name,d.id`)
	if err != nil {
		return result, storeError(err)
	}
	defer rows.Close()
	result.Space = []destination.Space{}
	for rows.Next() {
		var space destination.Space
		if err := rows.Scan(&space.DestinationID, millis{&space.ObservedAt}, &space.TotalBytes, &space.AvailableBytes); err != nil {
			return result, storeError(err)
		}
		space.Known = !space.ObservedAt.IsZero() && s.now().Sub(space.ObservedAt) <= 5*time.Minute
		result.Space = append(result.Space, space)
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
	if err := rows.Close(); err != nil {
		return result, storeError(err)
	}
	for _, schedule := range result.Schedules {
		result.Proofs = append(result.Proofs, proof.Schedule{ID: uuid.NewSHA1(schedule.ID, []byte("restore_drill")), Kind: proof.Drill, InstallationID: installationID, NodeID: nodeID, AppID: schedule.AppID, CreatedAt: schedule.CreatedAt})
	}
	owned := make(map[uuid.UUID]bool)
	rows, err = tx.QueryContext(ctx, `SELECT id FROM destinations WHERE removed_at IS NULL AND maintenance_node_id=?`, nodeID)
	if err != nil {
		return result, storeError(err)
	}
	defer rows.Close()
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			return result, storeError(err)
		}
		owned[id] = true
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return result, storeError(err)
	}
	for _, entry := range result.Destinations {
		d := entry.Destination
		result.Proofs = append(result.Proofs, proof.Schedule{ID: uuid.NewSHA1(d.ID, nodeID[:]), Kind: proof.Integrity, InstallationID: installationID, NodeID: nodeID, DestinationID: d.ID, CreatedAt: d.CreatedAt})
		if owned[d.ID] {
			result.Proofs = append(result.Proofs, proof.Schedule{ID: uuid.NewSHA1(d.ID, append([]byte(proof.Maintenance), nodeID[:]...)), Kind: proof.Maintenance, InstallationID: installationID, NodeID: nodeID, DestinationID: d.ID, CreatedAt: d.CreatedAt})
		}
	}
	for _, schedule := range result.Proofs {
		raw, _ := json.Marshal(schedule)
		if _, err := tx.ExecContext(ctx, `INSERT INTO proof_schedules (id,node_id,definition) VALUES (?,?,?) ON CONFLICT(id) DO NOTHING`, schedule.ID, nodeID, raw); err != nil {
			return result, storeError(err)
		}
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
