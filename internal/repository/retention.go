package repository

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	kfs "github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/manifest"
	"github.com/kopia/kopia/snapshot"
	"github.com/kopia/kopia/snapshot/policy"
	"github.com/sirrobot01/synod/internal/backup"
)

// ApplyRetention expires snapshots for one app and destination. It always keeps the newest copy.
// Kopia pins also keep a snapshot. Content reclamation is a separate maintenance step.
func (r *Repository) ApplyRetention(ctx context.Context, installationID, appID, destinationID uuid.UUID, retention backup.Retention) ([]uuid.UUID, error) {
	if installationID == uuid.Nil || appID == uuid.Nil || destinationID == uuid.Nil || retention.Validate() != nil {
		return nil, backup.ErrInvalid
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	r.write.Lock()
	defer r.write.Unlock()
	if err := r.CheckInstallation(ctx, installationID); err != nil {
		return nil, err
	}
	entries, err := r.repo.FindManifests(ctx, map[string]string{"type": "synod.backup", "installation": installationID.String(), "app": appID.String(), "destination": destinationID.String()})
	if err != nil {
		return nil, &Error{Action: "list retention snapshots", Cause: err}
	}
	var files []*snapshot.Manifest
	logical := make(map[manifest.ID]uuid.UUID)
	metadata := make(map[manifest.ID]manifest.ID)
	seen := make(map[uuid.UUID]bool)
	for _, entry := range entries {
		var saved backup.Snapshot
		if _, err := r.repo.GetManifest(ctx, entry.ID, &saved); err != nil {
			return nil, &Error{Action: "read retention snapshot", Cause: err}
		}
		if saved.ID == uuid.Nil || seen[saved.ID] || saved.ID.String() != entry.Labels["snapshot"] || saved.InstallationID != installationID || saved.App.ID != appID || saved.DestinationID != destinationID || saved.FilesID == "" || saved.RootID == "" || saved.CompletedAt.IsZero() {
			return nil, backup.ErrInvalid
		}
		seen[saved.ID] = true
		file, err := snapshot.LoadSnapshot(ctx, r.repo, manifest.ID(saved.FilesID))
		if err != nil {
			return nil, &Error{Action: "read retention files", Cause: err}
		}
		if file.RootEntry == nil || file.RootObjectID().String() != saved.RootID || file.IncompleteReason != "" || file.Tags["synod.snapshot"] != saved.ID.String() || file.Tags["synod.installation"] != installationID.String() || file.Tags["synod.app"] != appID.String() || file.Tags["synod.destination"] != destinationID.String() || logical[file.ID] != uuid.Nil {
			return nil, ErrSnapshotIncomplete
		}
		// Use the saved completion time for calendar periods. Do not rewrite the native manifest.
		file.StartTime = kfs.UTCTimestampFromTime(saved.CompletedAt)
		files = append(files, file)
		logical[file.ID] = saved.ID
		metadata[file.ID] = entry.ID
	}
	retentionPolicy := policy.RetentionPolicy{KeepLatest: new(policy.OptionalInt(1)), KeepDaily: new(policy.OptionalInt(retention.Daily)), KeepWeekly: new(policy.OptionalInt(retention.Weekly)), KeepMonthly: new(policy.OptionalInt(retention.Monthly))}
	retentionPolicy.ComputeRetentionReasons(files)
	var expired []*snapshot.Manifest
	var ids []uuid.UUID
	for _, file := range files {
		if len(file.RetentionReasons) == 0 && len(file.Pins) == 0 {
			expired = append(expired, file)
			ids = append(ids, logical[file.ID])
		}
	}
	if len(expired) == 0 {
		return ids, nil
	}
	err = repo.WriteSession(ctx, r.repo, repo.WriteSessionOptions{Purpose: "expire Synod snapshots"}, func(ctx context.Context, w repo.RepositoryWriter) error {
		for _, file := range expired {
			if err := w.DeleteManifest(ctx, metadata[file.ID]); err != nil {
				return err
			}
			if err := w.DeleteManifest(ctx, file.ID); err != nil {
				return err
			}
		}
		return nil
	})
	if fatal := r.fatal.Load(); fatal != nil {
		err = errors.Join(err, *fatal)
	}
	if err != nil {
		return nil, &Error{Action: "expire snapshots", Cause: errors.Join(err, ctx.Err())}
	}
	return ids, nil
}
