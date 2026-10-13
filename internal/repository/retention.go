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

// ApplyRetention expires snapshots for one app and destination. It keeps the newest and current copies.
// Kopia pins also keep a snapshot. Content reclamation is a separate maintenance step.
// The caller checks the installation on the same connection first, as a backup does when it
// opens the repository. ApplyRetention then reads that state and does not refresh it again.
func (r *Repository) ApplyRetention(ctx context.Context, installationID, appID, destinationID uuid.UUID, retention backup.Retention, current uuid.UUID) ([]uuid.UUID, error) {
	if installationID == uuid.Nil || appID == uuid.Nil || destinationID == uuid.Nil || retention.Validate() != nil {
		return nil, backup.ErrInvalid
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	r.write.Lock()
	defer r.write.Unlock()
	// A failed deletion session can leave its deletions in this connection's memory.
	// Do not start one that cannot finish.
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	expired, err := planExpiry(ctx, r.repo, installationID, appID, destinationID, retention, current)
	if err != nil || len(expired) == 0 {
		return nil, err
	}
	err = repo.WriteSession(ctx, r.repo, repo.WriteSessionOptions{Purpose: "expire Synod snapshots"}, func(ctx context.Context, w repo.RepositoryWriter) error {
		return expire(ctx, w, expired)
	})
	if fatal := r.fatal.Load(); fatal != nil {
		err = errors.Join(err, *fatal)
	}
	if err != nil {
		return nil, &Error{Action: "expire snapshots", Cause: errors.Join(err, ctx.Err())}
	}
	ids := make([]uuid.UUID, 0, len(expired))
	for _, e := range expired {
		ids = append(ids, e.id)
	}
	return ids, nil
}

// expiry names one snapshot that retention removes: its Synod metadata and its Kopia files.
type expiry struct {
	id              uuid.UUID
	metadata, files manifest.ID
}

func expire(ctx context.Context, w repo.RepositoryWriter, expired []expiry) error {
	for _, e := range expired {
		if err := w.DeleteManifest(ctx, e.metadata); err != nil {
			return err
		}
		if err := w.DeleteManifest(ctx, e.files); err != nil {
			return err
		}
	}
	return nil
}

// planExpiry decides from the completion times in the labels which snapshots retention
// removes. It reads a snapshot's manifests only before it expires that snapshot, to check
// its pins and its identity. A snapshot published without these labels is read in full.
func planExpiry(ctx context.Context, r repo.Repository, installationID, appID, destinationID uuid.UUID, retention backup.Retention, current uuid.UUID) ([]expiry, error) {
	entries, err := r.FindManifests(ctx, map[string]string{"type": "synod.backup", "installation": installationID.String(), "app": appID.String(), "destination": destinationID.String()})
	if err != nil {
		return nil, &Error{Action: "list retention snapshots", Cause: err}
	}
	var files []*snapshot.Manifest
	logical := make(map[manifest.ID]uuid.UUID)
	metadata := make(map[manifest.ID]manifest.ID)
	seen := make(map[uuid.UUID]bool)
	for _, entry := range entries {
		id, err := uuid.Parse(entry.Labels["snapshot"])
		if err != nil || id == uuid.Nil || seen[id] {
			return nil, backup.ErrInvalid
		}
		completed, timeErr := time.Parse(time.RFC3339Nano, entry.Labels["completed"])
		filesID := entry.Labels["files"]
		if timeErr != nil || filesID == "" {
			var saved backup.Snapshot
			if _, err := r.GetManifest(ctx, entry.ID, &saved); err != nil {
				return nil, &Error{Action: "read retention snapshot", Cause: err}
			}
			if saved.ID != id || saved.InstallationID != installationID || saved.App.ID != appID || saved.DestinationID != destinationID || saved.FilesID == "" || saved.RootID == "" || saved.CompletedAt.IsZero() {
				return nil, backup.ErrInvalid
			}
			completed, filesID = saved.CompletedAt, saved.FilesID
		}
		if logical[manifest.ID(filesID)] != uuid.Nil {
			return nil, ErrSnapshotIncomplete
		}
		seen[id] = true
		// Use the saved completion time for calendar periods. Do not rewrite the native manifest.
		files = append(files, &snapshot.Manifest{ID: manifest.ID(filesID), StartTime: kfs.UTCTimestampFromTime(completed)})
		logical[manifest.ID(filesID)] = id
		metadata[manifest.ID(filesID)] = entry.ID
	}
	if current != uuid.Nil && !seen[current] {
		return nil, backup.ErrInvalid
	}
	retentionPolicy := policy.RetentionPolicy{KeepLatest: new(policy.OptionalInt(1)), KeepDaily: new(policy.OptionalInt(retention.Daily)), KeepWeekly: new(policy.OptionalInt(retention.Weekly)), KeepMonthly: new(policy.OptionalInt(retention.Monthly))}
	retentionPolicy.ComputeRetentionReasons(files)
	var expired []expiry
	for _, candidate := range files {
		if len(candidate.RetentionReasons) != 0 || logical[candidate.ID] == current {
			continue
		}
		file, err := snapshot.LoadSnapshot(ctx, r, candidate.ID)
		if err != nil {
			return nil, &Error{Action: "read retention files", Cause: err}
		}
		id := logical[candidate.ID]
		if file.RootEntry == nil || file.IncompleteReason != "" || file.Tags["synod.snapshot"] != id.String() || file.Tags["synod.installation"] != installationID.String() || file.Tags["synod.app"] != appID.String() || file.Tags["synod.destination"] != destinationID.String() {
			return nil, ErrSnapshotIncomplete
		}
		// Kopia pins also keep a snapshot.
		if len(file.Pins) == 0 {
			expired = append(expired, expiry{id: id, metadata: metadata[candidate.ID], files: candidate.ID})
		}
	}
	return expired, nil
}
