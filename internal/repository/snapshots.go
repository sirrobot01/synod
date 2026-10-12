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
		if s.DestinationID == destinationID {
			return r.Files(ctx, s)
		}
	}
	return nil, nil
}

// PriorSnapshot returns the newest snapshot of one app at one destination and its files,
// or nil. It reads the state that the backup's CheckInstallation refreshed, so it does not
// ask the destination again. A snapshot that another device wrote since then is missed;
// the backup then reads more files, and the result stays correct.
func (r *Repository) PriorSnapshot(ctx context.Context, installationID, appID, destinationID uuid.UUID) (*backup.Snapshot, *snapshot.Manifest, error) {
	entries, err := r.repo.FindManifests(ctx, map[string]string{"type": "synod.backup", "installation": installationID.String(), "app": appID.String(), "destination": destinationID.String()})
	if err != nil {
		return nil, nil, &Error{Action: "list app snapshots", Cause: err}
	}
	var newest *backup.Snapshot
	for _, entry := range entries {
		var saved backup.Snapshot
		if _, err := r.repo.GetManifest(ctx, entry.ID, &saved); err != nil {
			return nil, nil, &Error{Action: "read app snapshot", Cause: err}
		}
		if saved.ID.String() != entry.Labels["snapshot"] || saved.InstallationID != installationID || saved.App.ID != appID || saved.DestinationID != destinationID || saved.FilesID == "" || saved.RootID == "" || saved.CompletedAt.IsZero() {
			return nil, nil, backup.ErrInvalid
		}
		if newest == nil || saved.CompletedAt.After(newest.CompletedAt) || (saved.CompletedAt.Equal(newest.CompletedAt) && saved.ID.String() < newest.ID.String()) {
			newest = &saved
		}
	}
	if newest == nil {
		return nil, nil, nil
	}
	files, err := r.Files(ctx, *newest)
	if err != nil {
		return nil, nil, err
	}
	return newest, files, nil
}

// Files loads the Kopia files manifest of a listed snapshot. It refuses an incomplete root.
func (r *Repository) Files(ctx context.Context, saved backup.Snapshot) (*snapshot.Manifest, error) {
	files, err := snapshot.LoadSnapshot(ctx, r.repo, manifest.ID(saved.FilesID))
	if err != nil {
		return nil, &Error{Action: "read previous snapshot files", Cause: err}
	}
	if files.RootEntry == nil || files.RootObjectID().String() != saved.RootID || files.IncompleteReason != "" {
		return nil, ErrSnapshotIncomplete
	}
	return files, nil
}
