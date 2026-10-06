package store

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/backup"
	"github.com/sirrobot01/synod/internal/inventory"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/proto"
)

// RecordInventory accepts a complete scan from the current authenticated session.
// A scan failure keeps the last successful app inventory.
func (s *Store) RecordInventory(ctx context.Context, nodeID, sessionID uuid.UUID, report *agentv1.AppInventory) error {
	if nodeID == uuid.Nil || sessionID == uuid.Nil || inventory.Validate(report) != nil {
		return &Error{Code: InvalidRequest}
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return storeError(err)
	}
	defer tx.Rollback()
	var current string
	var approved bool
	if err := tx.QueryRowContext(ctx, `SELECT coalesce(session_id,''),approved_at IS NOT NULL FROM nodes WHERE id=? AND revoked_at IS NULL`, nodeID).Scan(&current, &approved); err != nil {
		return storeError(err)
	}
	if current != sessionID.String() {
		return ErrStaleSession
	}
	observed := report.ObservedAt.AsTime().UnixMilli()
	var previous int64
	err = tx.QueryRowContext(ctx, `SELECT observed_at FROM app_scans WHERE node_id=? AND session_id=?`, nodeID, sessionID).Scan(&previous)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return storeError(err)
	}
	if err == nil && observed <= previous {
		return nil
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO app_scans (node_id,session_id,observed_at,received_at,failure_code) VALUES (?,?,?,?,?)
ON CONFLICT(node_id) DO UPDATE SET session_id=excluded.session_id,observed_at=excluded.observed_at,received_at=excluded.received_at,failure_code=excluded.failure_code`, nodeID, sessionID, observed, s.now().UnixMilli(), report.FailureCode); err != nil {
		return storeError(err)
	}
	if report.FailureCode == "" {
		type savedApp struct {
			present    bool
			definition []byte
		}
		prior := make(map[uuid.UUID]savedApp)
		rows, err := tx.QueryContext(ctx, `SELECT id,present,definition FROM apps WHERE node_id=?`, nodeID)
		if err != nil {
			return storeError(err)
		}
		for rows.Next() {
			var id uuid.UUID
			var saved savedApp
			if err := rows.Scan(&id, &saved.present, &saved.definition); err != nil {
				rows.Close()
				return storeError(err)
			}
			prior[id] = saved
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return storeError(err)
		}
		if err := rows.Close(); err != nil {
			return storeError(err)
		}
		var auto bool
		var installation sql.Null[uuid.UUID]
		var raw []byte
		var defaults backup.Settings
		if err := tx.QueryRowContext(ctx, `SELECT auto_protect,settings,installation_id FROM protection_defaults`).Scan(&auto, &raw, &installation); err != nil {
			return storeError(err)
		}
		if json.Unmarshal(raw, &defaults) != nil {
			return &Error{Code: Unavailable}
		}
		changed := false
		if _, err := tx.ExecContext(ctx, `UPDATE apps SET present=0 WHERE node_id=?`, nodeID); err != nil {
			return storeError(err)
		}
		for _, app := range report.Apps {
			definition, err := proto.Marshal(app)
			if err != nil {
				return &Error{Code: InvalidRequest, Cause: err}
			}
			id := uuid.NewSHA1(nodeID, []byte(app.Key))
			if saved, ok := prior[id]; !ok || !saved.present || !bytes.Equal(saved.definition, definition) {
				changed = true
			}
			delete(prior, id)
			if _, err := tx.ExecContext(ctx, `INSERT INTO apps (id,node_id,source_key,observed_at,present,definition) VALUES (?,?,?,?,1,?)
ON CONFLICT(node_id,source_key) DO UPDATE SET observed_at=excluded.observed_at,present=1,definition=excluded.definition`, id, nodeID, app.Key, observed, definition); err != nil {
				return storeError(err)
			}
			if auto && approved {
				var selected bool
				if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM app_protection WHERE app_id=?)`, id).Scan(&selected); err != nil {
					return storeError(err)
				}
				if !selected {
					if err := setAppProtection(ctx, tx, installation.V, id, nodeID, app.Key, true, defaults, s.now()); err != nil {
						return err
					}
					changed = true
				}
			}
		}
		for _, saved := range prior {
			changed = changed || saved.present
		}
		if changed {
			if _, err := tx.ExecContext(ctx, `UPDATE destination_revision SET revision=revision+1`); err != nil {
				return storeError(err)
			}
		}
	}
	return storeCommit(tx)
}

// ListApps returns saved apps and scan state, including devices that have not scanned.
func (s *Store) ListApps(ctx context.Context) (inventory.Catalog, error) {
	catalog := inventory.Catalog{Apps: []inventory.App{}, Devices: []inventory.Device{}}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return catalog, storeError(err)
	}
	defer tx.Rollback()
	rows, err := tx.QueryContext(ctx, `SELECT a.id,a.node_id,n.name,a.observed_at,a.present,a.definition FROM apps a JOIN nodes n ON n.id=a.node_id ORDER BY n.name,a.source_key,a.id`)
	if err != nil {
		return catalog, storeError(err)
	}
	defer rows.Close()
	for rows.Next() {
		var app inventory.App
		var raw []byte
		if err := rows.Scan(&app.ID, &app.NodeID, &app.NodeName, millis{&app.ObservedAt}, &app.Present, &raw); err != nil {
			return catalog, storeError(err)
		}
		app.Definition = new(agentv1.DiscoveredApp)
		if err := proto.Unmarshal(raw, app.Definition); err != nil {
			return catalog, storeError(err)
		}
		catalog.Apps = append(catalog.Apps, app)
	}
	if err := rows.Err(); err != nil {
		return catalog, storeError(err)
	}
	if err := rows.Close(); err != nil {
		return catalog, storeError(err)
	}
	rows, err = tx.QueryContext(ctx, `SELECT n.id,n.name,s.observed_at,s.received_at,coalesce(s.failure_code,'') FROM nodes n LEFT JOIN app_scans s ON s.node_id=n.id ORDER BY n.name,n.id`)
	if err != nil {
		return catalog, storeError(err)
	}
	defer rows.Close()
	for rows.Next() {
		var device inventory.Device
		if err := rows.Scan(&device.NodeID, &device.NodeName, optionalMillis{&device.ObservedAt}, optionalMillis{&device.ReceivedAt}, &device.FailureCode); err != nil {
			return catalog, storeError(err)
		}
		catalog.Devices = append(catalog.Devices, device)
	}
	if err := rows.Err(); err != nil {
		return catalog, storeError(err)
	}
	return catalog, storeCommit(tx)
}
