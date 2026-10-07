package repository

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/kopia/kopia/repo"
	"github.com/sirrobot01/synod/internal/backup"
	"github.com/sirrobot01/synod/internal/inventory"
	"github.com/sirrobot01/synod/internal/notification"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Catalog is the recovery state stored inside the encrypted repository.
// Increase Generation when the controller changes this state.
type Catalog struct {
	Notifications  *notification.Settings `json:"notifications,omitzero"`
	Version        int                    `json:"version"`
	InstallationID uuid.UUID              `json:"installation_id"`
	Generation     uint64                 `json:"generation"`
	WrittenAt      time.Time              `json:"written_at"`
	Devices        []Device               `json:"devices"`
	Apps           []inventory.App        `json:"apps"`
	Destinations   []CatalogDestination   `json:"destinations,omitempty"`
	AutoProtect    bool                   `json:"auto_protect"`
	Defaults       *backup.Settings       `json:"defaults,omitzero"`
	Protection     []backup.Selection     `json:"protection,omitempty"`
}

func (Catalog) String() string   { return "[private recovery catalog]" }
func (Catalog) GoString() string { return "[private recovery catalog]" }

// CatalogDestination keeps the settings needed to reopen other copies after controller loss.
type CatalogDestination struct {
	ID          uuid.UUID   `json:"id"`
	Name        string      `json:"name"`
	NodeID      uuid.UUID   `json:"node_id,omitzero"`
	Location    Location    `json:"location"`
	Credentials Credentials `json:"credentials"`
	Initialized bool        `json:"initialized"`
	CreatedAt   time.Time   `json:"created_at"`
}

func (CatalogDestination) String() string   { return "[private recovery destination]" }
func (CatalogDestination) GoString() string { return "[private recovery destination]" }

const MaxCatalogBytes = 8 << 20
const MaxCatalogTransferBytes = 512 << 10

// EncodeCatalog bounds and compresses recovery settings for encrypted agent delivery.
func EncodeCatalog(catalog Catalog) ([]byte, error) {
	if validateCatalog(catalog) != nil {
		return nil, ErrCatalogInvalid
	}
	raw, err := json.Marshal(catalog)
	defer clear(raw)
	if err != nil || len(raw) > MaxCatalogBytes {
		return nil, ErrCatalogInvalid
	}
	var result bytes.Buffer
	w := gzip.NewWriter(&result)
	if _, err := w.Write(raw); err != nil {
		w.Close()
		return nil, ErrCatalogInvalid
	}
	if err := w.Close(); err != nil || result.Len() > MaxCatalogTransferBytes {
		return nil, ErrCatalogInvalid
	}
	return result.Bytes(), nil
}

// DecodeCatalog bounds decompression before it reads private recovery settings.
func DecodeCatalog(data []byte) (Catalog, error) {
	if len(data) == 0 || len(data) > MaxCatalogTransferBytes {
		return Catalog{}, ErrCatalogInvalid
	}
	r, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return Catalog{}, ErrCatalogInvalid
	}
	defer r.Close()
	raw, err := io.ReadAll(io.LimitReader(r, MaxCatalogBytes+1))
	defer clear(raw)
	var result Catalog
	if err != nil || len(raw) > MaxCatalogBytes || json.Unmarshal(raw, &result) != nil || validateCatalog(result) != nil {
		return Catalog{}, ErrCatalogInvalid
	}
	return result, nil
}

type Device struct {
	ID           uuid.UUID `json:"id"`
	Name         string    `json:"name"`
	Architecture string    `json:"architecture"`
}

var ErrCatalogMissing = errors.New("repository has no Synod catalog")
var ErrCatalogConflict = errors.New("repository catalog conflicts with this installation or generation")
var ErrCatalogInvalid = errors.New("repository catalog is invalid or uses an unsupported version")

// WriteCatalog appends an immutable catalog and flushes it before success.
// The highest generation wins. Conflicting content at one generation is refused on read.
func (r *Repository) WriteCatalog(ctx context.Context, catalog Catalog) error {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if err := validateCatalog(catalog); err != nil {
		return err
	}
	data, err := json.Marshal(catalog)
	if err != nil || len(data) > MaxCatalogBytes {
		return ErrCatalogInvalid
	}
	r.write.Lock()
	defer r.write.Unlock()
	if err := r.CheckInstallation(ctx, catalog.InstallationID); err != nil {
		return err
	}
	previous, err := r.ReadCatalog(ctx)
	if err != nil && !errors.Is(err, ErrCatalogMissing) {
		return err
	}
	if err == nil {
		if previous.InstallationID != catalog.InstallationID || previous.Generation > catalog.Generation {
			return ErrCatalogConflict
		}
		if previous.Generation == catalog.Generation {
			prior, _ := json.Marshal(previous)
			if !bytes.Equal(prior, data) {
				return ErrCatalogConflict
			}
			return nil
		}
	}
	err = repo.WriteSession(ctx, r.repo, repo.WriteSessionOptions{Purpose: "write Synod recovery catalog"}, func(ctx context.Context, w repo.RepositoryWriter) error {
		var err error
		_, err = w.PutManifest(ctx, map[string]string{"type": "synod.catalog", "installation": catalog.InstallationID.String(), "generation": strconv.FormatUint(catalog.Generation, 10)}, catalog)
		return err
	})
	if fatal := r.fatal.Load(); fatal != nil {
		err = errors.Join(err, *fatal)
	}
	if err != nil {
		return &Error{Action: "write catalog", Cause: errors.Join(err, ctx.Err())}
	}
	return nil
}

// ReadCatalog refreshes the repository and reads its newest complete catalog.
func (r *Repository) ReadCatalog(ctx context.Context) (Catalog, error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if fatal := r.fatal.Load(); fatal != nil {
		return Catalog{}, &Error{Action: "read catalog", Cause: *fatal}
	}
	if err := r.repo.Refresh(ctx); err != nil {
		return Catalog{}, &Error{Action: "refresh", Cause: errors.Join(err, ctx.Err())}
	}
	entries, err := r.repo.FindManifests(ctx, map[string]string{"type": "synod.catalog"})
	if err != nil {
		return Catalog{}, &Error{Action: "find catalog", Cause: errors.Join(err, ctx.Err())}
	}
	if len(entries) == 0 {
		return Catalog{}, ErrCatalogMissing
	}
	var generation uint64
	var installation string
	for _, entry := range entries {
		g, err := strconv.ParseUint(entry.Labels["generation"], 10, 64)
		if err != nil || g == 0 {
			return Catalog{}, ErrCatalogInvalid
		}
		if installation != "" && installation != entry.Labels["installation"] {
			return Catalog{}, ErrCatalogConflict
		}
		installation = entry.Labels["installation"]
		generation = max(generation, g)
	}
	var selected Catalog
	var selectedJSON []byte
	for _, entry := range entries {
		if entry.Labels["generation"] != strconv.FormatUint(generation, 10) {
			continue
		}
		var catalog Catalog
		if _, err := r.repo.GetManifest(ctx, entry.ID, &catalog); err != nil {
			return Catalog{}, &Error{Action: "read catalog", Cause: errors.Join(err, ctx.Err())}
		}
		if catalog.Version != 1 || catalog.Generation != generation || catalog.InstallationID == uuid.Nil || catalog.InstallationID.String() != installation || catalog.WrittenAt.IsZero() {
			return Catalog{}, ErrCatalogInvalid
		}
		data, err := json.Marshal(catalog)
		if err != nil || len(data) > MaxCatalogBytes {
			return Catalog{}, ErrCatalogInvalid
		}
		if selectedJSON != nil && !bytes.Equal(selectedJSON, data) {
			return Catalog{}, ErrCatalogConflict
		}
		selected, selectedJSON = catalog, data
	}
	if selectedJSON == nil {
		return Catalog{}, fmt.Errorf("%w: no complete generation", ErrCatalogInvalid)
	}
	if err := validateCatalog(selected); err != nil {
		return Catalog{}, err
	}
	return selected, nil
}

func validateCatalog(catalog Catalog) error {
	if catalog.Notifications != nil && catalog.Notifications.Validate() != nil {
		return ErrCatalogInvalid
	}
	if len(catalog.Devices) > 256 || len(catalog.Apps) > 8192 || len(catalog.Destinations) > 64 || len(catalog.Protection) > 8192 {
		return ErrCatalogInvalid
	}
	if catalog.Version != 1 || catalog.InstallationID == uuid.Nil || catalog.Generation == 0 || catalog.WrittenAt.IsZero() {
		return ErrCatalogInvalid
	}
	devices := make(map[uuid.UUID]bool)
	for _, device := range catalog.Devices {
		if device.ID == uuid.Nil || devices[device.ID] || device.Name == "" || !slices.Contains([]string{"amd64", "arm64"}, device.Architecture) {
			return ErrCatalogInvalid
		}
		devices[device.ID] = true
	}
	apps := make(map[uuid.UUID]bool)
	for _, app := range catalog.Apps {
		if app.ID == uuid.Nil || apps[app.ID] || !devices[app.NodeID] || app.Definition == nil || inventory.Validate(&agentv1.AppInventory{ObservedAt: timestamppb.New(app.ObservedAt), Apps: []*agentv1.DiscoveredApp{app.Definition}}) != nil {
			return ErrCatalogInvalid
		}
		if app.ID != uuid.NewSHA1(app.NodeID, []byte(app.Definition.Key)) {
			return ErrCatalogInvalid
		}
		apps[app.ID] = true
	}
	targets := make(map[uuid.UUID]bool)
	for _, target := range catalog.Destinations {
		if target.ID == uuid.Nil || targets[target.ID] || target.Name == "" || len(target.Name) > 128 || Validate(target.Location, target.Credentials) != nil || ((target.Location.Kind == "local" || target.Location.Kind == "device") != (target.NodeID != uuid.Nil)) || (target.NodeID != uuid.Nil && !devices[target.NodeID]) || target.Credentials.DeviceIdentity != nil {
			return ErrCatalogInvalid
		}
		if target.Location.Kind == "device" && (target.Location.Device.NodeID != target.NodeID || target.Location.Device.DestinationID != target.ID) {
			return ErrCatalogInvalid
		}
		targets[target.ID] = true
	}
	if catalog.AutoProtect {
		if catalog.Defaults == nil || catalog.Defaults.Validate() != nil {
			return ErrCatalogInvalid
		}
		for _, id := range catalog.Defaults.Destinations {
			if !targets[id] {
				return ErrCatalogInvalid
			}
		}
	}
	selected := make(map[uuid.UUID]bool)
	for _, selection := range catalog.Protection {
		if !apps[selection.AppID] || selected[selection.AppID] {
			return ErrCatalogInvalid
		}
		selected[selection.AppID] = true
		if !selection.Enabled {
			continue
		}
		if selection.Settings.Validate() != nil {
			return ErrCatalogInvalid
		}
		for _, id := range selection.Settings.Destinations {
			if !targets[id] {
				return ErrCatalogInvalid
			}
		}
	}
	return nil
}
