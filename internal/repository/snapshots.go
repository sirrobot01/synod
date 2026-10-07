package repository

import (
	"cmp"
	"context"
	"slices"
	"time"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/backup"
)

// Snapshots lists one app's copies, newest first. Destination aliases share this list.
func (r *Repository) Snapshots(ctx context.Context, installationID, appID uuid.UUID) ([]backup.Snapshot, error) {
	if installationID == uuid.Nil || appID == uuid.Nil {
		return nil, backup.ErrInvalid
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if err := r.CheckInstallation(ctx, installationID); err != nil {
		return nil, err
	}
	if err := r.repo.Refresh(ctx); err != nil {
		return nil, &Error{Action: "refresh snapshots", Cause: err}
	}
	entries, err := r.repo.FindManifests(ctx, map[string]string{"type": "synod.backup", "installation": installationID.String(), "app": appID.String()})
	if err != nil {
		return nil, &Error{Action: "list app snapshots", Cause: err}
	}
	if len(entries) > 4096 {
		return nil, backup.ErrInvalid
	}
	seen := make(map[uuid.UUID]bool)
	result := make([]backup.Snapshot, 0, len(entries))
	for _, entry := range entries {
		var saved backup.Snapshot
		if _, err := r.repo.GetManifest(ctx, entry.ID, &saved); err != nil {
			return nil, &Error{Action: "read app snapshot", Cause: err}
		}
		if saved.ID == uuid.Nil || seen[saved.ID] || saved.ID.String() != entry.Labels["snapshot"] || saved.InstallationID != installationID || saved.App.ID != appID || saved.FilesID == "" || saved.RootID == "" || saved.CompletedAt.IsZero() {
			return nil, backup.ErrInvalid
		}
		seen[saved.ID] = true
		result = append(result, saved)
	}
	slices.SortFunc(result, func(a, b backup.Snapshot) int {
		if order := b.CompletedAt.Compare(a.CompletedAt); order != 0 {
			return order
		}
		return cmp.Compare(a.ID.String(), b.ID.String())
	})
	return result, nil
}
