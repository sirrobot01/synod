package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"math"
	"time"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/backup"
	"github.com/sirrobot01/synod/internal/destination"
	"github.com/sirrobot01/synod/internal/inventory"
	"github.com/sirrobot01/synod/internal/repository"
	"github.com/sirrobot01/synod/internal/secret"
	"google.golang.org/protobuf/proto"
)

// ImportCatalog rebuilds empty metadata. Devices must enroll and receive approval again.
func (s *Store) ImportCatalog(ctx context.Context, catalog repository.Catalog, ring *secret.Keyring) error {
	raw, err := repository.EncodeCatalog(catalog)
	clear(raw)
	if err != nil || catalog.Generation >= math.MaxInt64 || ring == nil {
		return &Error{Code: InvalidRequest}
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return storeError(err)
	}
	defer tx.Rollback()
	var used bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM nodes) OR EXISTS(SELECT 1 FROM users) OR EXISTS(SELECT 1 FROM api_tokens) OR EXISTS(SELECT 1 FROM destinations) OR EXISTS(SELECT 1 FROM enrollment_tokens) OR EXISTS(SELECT 1 FROM recovery_catalog) OR EXISTS(SELECT 1 FROM protection_defaults WHERE installation_id IS NOT NULL) OR EXISTS(SELECT 1 FROM destination_revision WHERE revision!=1)`).Scan(&used); err != nil {
		return storeError(err)
	}
	if used {
		return &Error{Code: OperationConflict}
	}
	now := s.now().UTC().Truncate(time.Millisecond)
	for _, device := range catalog.Devices {
		if !printableWithin(device.Name, 253) {
			return &Error{Code: InvalidRequest}
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO nodes (id,name,architecture,enrolled_at) VALUES (?,?,?,?)`, device.ID, device.Name, device.Architecture, now.UnixMilli()); err != nil {
			return storeError(err)
		}
	}
	apps := make(map[uuid.UUID]inventory.App, len(catalog.Apps))
	for _, app := range catalog.Apps {
		definition, err := proto.Marshal(app.Definition)
		if err != nil {
			return &Error{Code: InvalidRequest}
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO apps (id,node_id,source_key,observed_at,present,definition) VALUES (?,?,?,?,0,?)`, app.ID, app.NodeID, app.Definition.Key, app.ObservedAt.UnixMilli(), definition); err != nil {
			return storeError(err)
		}
		apps[app.ID] = app
	}
	var totalBytes int
	for _, d := range catalog.Destinations {
		entry := destination.Entry{Destination: destination.Destination{ID: d.ID, Name: d.Name, NodeID: d.NodeID, Location: d.Location}, Credentials: d.Credentials}
		if entry.Validate() != nil {
			return &Error{Code: InvalidRequest}
		}
		input, _ := json.Marshal(entry)
		totalBytes += len(input)
		if totalBytes > 384<<10 {
			clear(input)
			return &Error{Code: InvalidRequest}
		}
		envelope, err := ring.Seal(secret.Binding{InstallationID: catalog.InstallationID, OwnerType: "destination", OwnerID: d.ID, CredentialID: d.ID, Version: 1}, input)
		clear(input)
		if err != nil {
			return storeError(err)
		}
		sealed, _ := json.Marshal(envelope)
		entry.Destination.CreatedAt = d.CreatedAt
		definition, _ := json.Marshal(entry.Destination)
		if _, err := tx.ExecContext(ctx, `INSERT INTO destinations (id,name,node_id,kind,definition,credentials,input_bytes,initialized_at) VALUES (?,?,?,?,?,?,?,?)`, d.ID, d.Name, sql.Null[uuid.UUID]{V: d.NodeID, Valid: d.NodeID != uuid.Nil}, d.Location.Kind, definition, sealed, len(input), sql.NullInt64{Int64: now.UnixMilli(), Valid: d.Initialized}); err != nil {
			return storeError(err)
		}
	}
	defaults := backup.DefaultSettings()
	if catalog.Defaults != nil {
		defaults = *catalog.Defaults
	}
	settings, _ := json.Marshal(defaults)
	if _, err := tx.ExecContext(ctx, `UPDATE protection_defaults SET installation_id=?,auto_protect=?,settings=?`, catalog.InstallationID, catalog.AutoProtect, settings); err != nil {
		return storeError(err)
	}
	for _, selection := range catalog.Protection {
		app := apps[selection.AppID]
		if selection.Enabled {
			if err := setAppProtection(ctx, tx, catalog.InstallationID, app.ID, app.NodeID, app.Definition.Key, true, selection.Settings, now); err != nil {
				return err
			}
			continue
		}
		settings, _ := json.Marshal(selection.Settings)
		if _, err := tx.ExecContext(ctx, `INSERT INTO app_protection (app_id,enabled,settings,changed_at) VALUES (?,0,?,?)`, app.ID, settings, now.UnixMilli()); err != nil {
			return storeError(err)
		}
	}
	if _, err := tx.ExecContext(ctx, `UPDATE destination_revision SET revision=?`, int64(catalog.Generation)+1); err != nil {
		return storeError(err)
	}
	// Freeze the new catalog before commit. This also checks the complete imported state.
	_, compressed, err := s.recoveryCatalog(ctx, tx, catalog.InstallationID, int64(catalog.Generation)+1, ring)
	clear(compressed)
	if err != nil {
		return err
	}
	return storeCommit(tx)
}
