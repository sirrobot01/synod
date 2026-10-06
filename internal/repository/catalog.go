package repository

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/kopia/kopia/repo"
	"github.com/sirrobot01/synod/internal/inventory"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Catalog is the recovery state stored inside the encrypted repository.
// Increase Generation when the controller changes this state.
type Catalog struct {
	Version        int             `json:"version"`
	InstallationID uuid.UUID       `json:"installation_id"`
	Generation     uint64          `json:"generation"`
	WrittenAt      time.Time       `json:"written_at"`
	Devices        []Device        `json:"devices"`
	Apps           []inventory.App `json:"apps"`
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
	if err != nil || len(data) > 8<<20 {
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
		if err != nil || len(data) > 8<<20 {
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
		apps[app.ID] = true
	}
	return nil
}
