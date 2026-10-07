package repository

import (
	"cmp"
	"context"
	"slices"
	"time"

	"github.com/google/uuid"
	"github.com/kopia/kopia/repo/manifest"
	"github.com/kopia/kopia/snapshot"
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

// LatestFiles returns the files of the newest snapshot of one app at one destination, or nil.
// A backup passes it to UploadFiles so unchanged files are not read again.
func (r *Repository) LatestFiles(ctx context.Context, installationID, appID, destinationID uuid.UUID) (*snapshot.Manifest, error) {
	saved, err := r.Snapshots(ctx, installationID, appID)
	if err != nil {
		return nil, err
	}
	for _, s := range saved {
		if s.DestinationID != destinationID {
			continue
		}
		files, err := snapshot.LoadSnapshot(ctx, r.repo, manifest.ID(s.FilesID))
		if err != nil {
			return nil, &Error{Action: "read previous snapshot files", Cause: err}
		}
		return files, nil
	}
	return nil, nil
}
