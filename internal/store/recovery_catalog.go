package store

import (
	"bytes"
	"cmp"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/backup"
	"github.com/sirrobot01/synod/internal/destination"
	"github.com/sirrobot01/synod/internal/inventory"
	"github.com/sirrobot01/synod/internal/repository"
	"github.com/sirrobot01/synod/internal/secret"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/proto"
)

// recoveryCatalog freezes one private catalog per revision so all agents write identical bytes.
func (s *Store) recoveryCatalog(ctx context.Context, tx *sql.Tx, installationID uuid.UUID, revision int64, ring *secret.Keyring) (repository.Catalog, []byte, error) {
	binding := secret.Binding{InstallationID: installationID, OwnerType: "recovery_catalog", OwnerID: installationID, CredentialID: installationID, Version: revision}
	var savedRevision int64
	var sealed []byte
	err := tx.QueryRowContext(ctx, `SELECT revision,envelope FROM recovery_catalog WHERE singleton=1`).Scan(&savedRevision, &sealed)
	if err == nil && savedRevision == revision {
		var envelope secret.Envelope
		if json.Unmarshal(sealed, &envelope) != nil {
			return repository.Catalog{}, nil, &Error{Code: Unavailable}
		}
		raw, err := ring.Open(binding, envelope)
		if err != nil {
			return repository.Catalog{}, nil, storeError(err)
		}
		catalog, err := repository.DecodeCatalog(raw)
		if err != nil || catalog.InstallationID != installationID || catalog.Generation != uint64(revision) {
			clear(raw)
			return repository.Catalog{}, nil, &Error{Code: Unavailable}
		}
		return catalog, raw, nil
	}
	if (err != nil && !errors.Is(err, sql.ErrNoRows)) || savedRevision > revision {
		return repository.Catalog{}, nil, &Error{Code: Unavailable, Cause: err}
	}
	catalog := repository.Catalog{Version: 1, InstallationID: installationID, Generation: uint64(revision), WrittenAt: s.now().UTC().Truncate(time.Millisecond)}
	rows, err := tx.QueryContext(ctx, `SELECT id,name,architecture FROM nodes WHERE architecture IN ('amd64','arm64') ORDER BY id`)
	if err != nil {
		return catalog, nil, storeError(err)
	}
	defer rows.Close()
	for rows.Next() {
		var device repository.Device
		if err := rows.Scan(&device.ID, &device.Name, &device.Architecture); err != nil {
			return catalog, nil, storeError(err)
		}
		device.Name = cmp.Or(device.Name, device.ID.String())
		catalog.Devices = append(catalog.Devices, device)
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return catalog, nil, storeError(err)
	}
	rows, err = tx.QueryContext(ctx, `SELECT a.id,a.node_id,n.name,a.observed_at,a.present,a.definition FROM apps a JOIN nodes n ON n.id=a.node_id WHERE n.architecture IN ('amd64','arm64') ORDER BY a.id`)
	if err != nil {
		return catalog, nil, storeError(err)
	}
	defer rows.Close()
	for rows.Next() {
		var app inventory.App
		var raw []byte
		if err := rows.Scan(&app.ID, &app.NodeID, &app.NodeName, millis{&app.ObservedAt}, &app.Present, &raw); err != nil {
			return catalog, nil, storeError(err)
		}
		app.NodeName = cmp.Or(app.NodeName, app.NodeID.String())
		app.Definition = new(agentv1.DiscoveredApp)
		if proto.Unmarshal(raw, app.Definition) != nil {
			return catalog, nil, &Error{Code: Unavailable}
		}
		catalog.Apps = append(catalog.Apps, app)
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return catalog, nil, storeError(err)
	}
	rows, err = tx.QueryContext(ctx, `SELECT id,definition,credentials,initialized_at IS NOT NULL FROM destinations WHERE removed_at IS NULL ORDER BY id`)
	if err != nil {
		return catalog, nil, storeError(err)
	}
	defer rows.Close()
	for rows.Next() {
		var id uuid.UUID
		var public, raw []byte
		var initialized bool
		if err := rows.Scan(&id, &public, &raw, &initialized); err != nil {
			return catalog, nil, storeError(err)
		}
		var envelope secret.Envelope
		if json.Unmarshal(raw, &envelope) != nil {
			return catalog, nil, &Error{Code: Unavailable}
		}
		plain, err := ring.Open(secret.Binding{InstallationID: installationID, OwnerType: "destination", OwnerID: id, CredentialID: id, Version: 1}, envelope)
		if err != nil {
			return catalog, nil, storeError(err)
		}
		var entry destination.Entry
		err = json.Unmarshal(plain, &entry)
		clear(plain)
		var definition destination.Destination
		if err != nil || entry.Validate() != nil || json.Unmarshal(public, &definition) != nil {
			return catalog, nil, &Error{Code: Unavailable}
		}
		entry.Destination.CreatedAt = definition.CreatedAt
		expected, _ := json.Marshal(entry.Destination)
		if !bytes.Equal(public, expected) {
			return catalog, nil, &Error{Code: Unavailable}
		}
		d := entry.Destination
		catalog.Destinations = append(catalog.Destinations, repository.CatalogDestination{ID: d.ID, Name: d.Name, NodeID: d.NodeID, Location: d.Location, Credentials: entry.Credentials, CreatedAt: d.CreatedAt, Initialized: initialized})
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return catalog, nil, storeError(err)
	}
	var defaults []byte
	catalog.Defaults = new(backup.Settings)
	if err := tx.QueryRowContext(ctx, `SELECT auto_protect,settings FROM protection_defaults`).Scan(&catalog.AutoProtect, &defaults); err != nil {
		return catalog, nil, storeError(err)
	}
	if json.Unmarshal(defaults, catalog.Defaults) != nil {
		return catalog, nil, &Error{Code: Unavailable}
	}
	rows, err = tx.QueryContext(ctx, `SELECT p.app_id,p.enabled,p.settings FROM app_protection p JOIN apps a ON a.id=p.app_id JOIN nodes n ON n.id=a.node_id WHERE n.architecture IN ('amd64','arm64') ORDER BY p.app_id`)
	if err != nil {
		return catalog, nil, storeError(err)
	}
	defer rows.Close()
	for rows.Next() {
		var selection backup.Selection
		var raw []byte
		if err := rows.Scan(&selection.AppID, &selection.Enabled, &raw); err != nil {
			return catalog, nil, storeError(err)
		}
		if json.Unmarshal(raw, &selection.Settings) != nil {
			return catalog, nil, &Error{Code: Unavailable}
		}
		catalog.Protection = append(catalog.Protection, selection)
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return catalog, nil, storeError(err)
	}
	raw, err := repository.EncodeCatalog(catalog)
	if err != nil {
		return catalog, nil, storeError(err)
	}
	envelope, err := ring.Seal(binding, raw)
	if err != nil {
		clear(raw)
		return catalog, nil, storeError(err)
	}
	sealed, err = json.Marshal(envelope)
	if err != nil {
		clear(raw)
		return catalog, nil, storeError(err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO recovery_catalog (singleton,revision,envelope) VALUES (1,?,?) ON CONFLICT(singleton) DO UPDATE SET revision=excluded.revision,envelope=excluded.envelope`, revision, sealed); err != nil {
		clear(raw)
		return catalog, nil, storeError(err)
	}
	return catalog, raw, nil
}
