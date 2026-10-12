package repository

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"syscall"
	"time"

	"github.com/google/uuid"
	kfs "github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/manifest"
	"github.com/kopia/kopia/snapshot"
	"github.com/kopia/kopia/snapshot/policy"
	"github.com/kopia/kopia/snapshot/snapshotfs"
	"github.com/kopia/kopia/snapshot/upload"
	"github.com/sirrobot01/synod/internal/backup"
)

var ErrSnapshotMissing = errors.New("backup snapshot not found")
var ErrSnapshotIncomplete = errors.New("backup snapshot is incomplete")

// UploadFiles writes deduplicated content. It does not publish a restorable snapshot.
// A live first pass may be incomplete. The final pass must be complete.
// Files whose size, time, mode, and owner match an entry in previous are not read again.
func (r *Repository) UploadFiles(ctx context.Context, root kfs.Entry, source snapshot.SourceInfo, preCopy bool, previous ...*snapshot.Manifest) (*snapshot.Manifest, error) {
	r.write.Lock()
	defer r.write.Unlock()
	var prepared *snapshot.Manifest
	err := repo.WriteSession(ctx, r.repo, repo.WriteSessionOptions{Purpose: "copy app files"}, func(ctx context.Context, w repo.RepositoryWriter) error {
		var err error
		prepared, err = copyFiles(ctx, w, root, source, preCopy, previous...)
		return err
	})
	if fatal := r.fatal.Load(); fatal != nil {
		err = errors.Join(err, *fatal)
	}
	if err != nil {
		return nil, &Error{Action: "copy app files", Cause: errors.Join(err, ctx.Err())}
	}
	return prepared, nil
}

// copyFiles copies root in an open write session.
func copyFiles(ctx context.Context, w repo.RepositoryWriter, root kfs.Entry, source snapshot.SourceInfo, preCopy bool, previous ...*snapshot.Manifest) (*snapshot.Manifest, error) {
	uploader := upload.NewUploader(w)
	uploader.EnableActions = false
	uploader.CheckpointInterval = 0
	uploader.DisableIgnoreRules = true
	uploader.FailFast = !preCopy
	// Writers wait for their pack uploads, so twice the CPU count keeps every core busy.
	uploader.ParallelUploads = min(2*runtime.NumCPU(), 16)
	// Large files are hashed in parallel parts at fixed offsets, so an append keeps earlier parts.
	policyTree := policy.BuildTree(nil, &policy.Policy{ErrorHandlingPolicy: policy.ErrorHandlingPolicy{IgnoreFileErrors: new(policy.OptionalBool(false)), IgnoreDirectoryErrors: new(policy.OptionalBool(false)), IgnoreUnknownTypes: new(policy.OptionalBool(false))}, UploadPolicy: policy.UploadPolicy{ParallelUploadAboveSize: new(policy.OptionalInt64(128 << 20))}})
	prepared, err := uploader.Upload(ctx, root, policyTree, source, slices.DeleteFunc(previous, func(m *snapshot.Manifest) bool { return m == nil || m.RootEntry == nil })...)
	if err != nil {
		return nil, err
	}
	if prepared.RootEntry == nil || (!preCopy && (prepared.IncompleteReason != "" || prepared.RootEntry.DirSummary == nil || prepared.RootEntry.DirSummary.FatalErrorCount != 0 || prepared.RootEntry.DirSummary.IgnoredErrorCount != 0)) {
		return nil, ErrSnapshotIncomplete
	}
	return prepared, ctx.Err()
}

// PublishSnapshot saves the files and metadata after the app has resumed.
func (r *Repository) PublishSnapshot(ctx context.Context, prepared *snapshot.Manifest, metadata backup.Snapshot) (backup.Snapshot, error) {
	metadata, err := checkPublication(prepared, metadata)
	if err != nil {
		return backup.Snapshot{}, err
	}
	r.write.Lock()
	defer r.write.Unlock()
	if err := r.CheckInstallation(ctx, metadata.InstallationID); err != nil {
		return backup.Snapshot{}, err
	}
	previous, err := r.Snapshot(ctx, metadata.ID)
	if err == nil {
		prior := previous
		prior.FilesID = ""
		a, _ := json.Marshal(prior)
		b, _ := json.Marshal(metadata)
		if !bytes.Equal(a, b) {
			return backup.Snapshot{}, ErrCatalogConflict
		}
		return previous, nil
	}
	if !errors.Is(err, ErrSnapshotMissing) {
		return backup.Snapshot{}, err
	}
	// Maintenance removes only content that no snapshot uses and that is older than 24 hours.
	// Reused content belongs to the newest snapshot, which retention keeps. A backup that runs
	// for more than an hour, for example after a restart, checks every referenced object.
	if _, err := r.repo.VerifyObject(ctx, prepared.RootObjectID()); err != nil {
		return backup.Snapshot{}, &Error{Action: "check snapshot root", Cause: errors.Join(ErrSnapshotIncomplete, err)}
	}
	if time.Since(metadata.StartedAt) > time.Hour {
		root := snapshotfs.EntryFromDirEntry(r.repo, prepared.RootEntry)
		defer root.Close()
		verifier := snapshotfs.NewVerifier(ctx, r.repo, snapshotfs.VerifierOptions{Parallelism: 2, FileQueueLength: 64, MaxErrors: 1})
		checked, verifyErr := verifier.InParallel(ctx, func(walker *snapshotfs.TreeWalker) error { return walker.Process(ctx, root, "app") })
		if verifyErr != nil || checked.ErrorCount != 0 {
			return backup.Snapshot{}, &Error{Action: "check snapshot objects", Cause: errors.Join(ErrSnapshotIncomplete, verifyErr, errors.Join(checked.Errors...))}
		}
	}
	err = repo.WriteSession(ctx, r.repo, repo.WriteSessionOptions{Purpose: "publish app snapshot"}, func(ctx context.Context, w repo.RepositoryWriter) error {
		metadata, err = saveSnapshot(ctx, w, prepared, metadata)
		return err
	})
	if fatal := r.fatal.Load(); fatal != nil {
		err = errors.Join(err, *fatal)
	}
	if err != nil {
		return backup.Snapshot{}, &Error{Action: "publish app snapshot", Cause: errors.Join(err, ctx.Err())}
	}
	return metadata, nil
}

// UploadSnapshot copies root and publishes the copy in the same write session when finish
// returns true. The repository then writes its index once for the whole backup. When finish
// returns false, the copy stays unpublished, as from UploadFiles. Content written in this
// session needs no age check, because maintenance cannot remove it during the session.
func (r *Repository) UploadSnapshot(ctx context.Context, root kfs.Entry, source snapshot.SourceInfo, previous *snapshot.Manifest, finish func(*snapshot.Manifest) (backup.Snapshot, bool, error)) (*snapshot.Manifest, *backup.Snapshot, error) {
	r.write.Lock()
	defer r.write.Unlock()
	var prepared *snapshot.Manifest
	var published *backup.Snapshot
	err := repo.WriteSession(ctx, r.repo, repo.WriteSessionOptions{Purpose: "copy and publish app snapshot"}, func(ctx context.Context, w repo.RepositoryWriter) error {
		var err error
		if prepared, err = copyFiles(ctx, w, root, source, false, previous); err != nil {
			return err
		}
		metadata, publish, err := finish(prepared)
		if err != nil || !publish {
			return err
		}
		if metadata, err = checkPublication(prepared, metadata); err != nil {
			return err
		}
		saved, err := saveSnapshot(ctx, w, prepared, metadata)
		published = &saved
		return err
	})
	if fatal := r.fatal.Load(); fatal != nil {
		err = errors.Join(err, *fatal)
	}
	if err != nil {
		return nil, nil, &Error{Action: "copy app files", Cause: errors.Join(err, ctx.Err())}
	}
	return prepared, published, nil
}

// saveSnapshot writes the Kopia snapshot and the Synod metadata that names it.
func saveSnapshot(ctx context.Context, w repo.RepositoryWriter, prepared *snapshot.Manifest, metadata backup.Snapshot) (backup.Snapshot, error) {
	prepared = prepared.Clone()
	prepared.Tags = map[string]string{"synod.snapshot": metadata.ID.String(), "synod.installation": metadata.InstallationID.String(), "synod.app": metadata.App.ID.String(), "synod.destination": metadata.DestinationID.String()}
	id, err := snapshot.SaveSnapshot(ctx, w, prepared)
	if err != nil {
		return backup.Snapshot{}, err
	}
	metadata.FilesID = string(id)
	_, err = w.PutManifest(ctx, map[string]string{"type": "synod.backup", "snapshot": metadata.ID.String(), "installation": metadata.InstallationID.String(), "app": metadata.App.ID.String(), "destination": metadata.DestinationID.String()}, metadata)
	return metadata, err
}

// checkPublication refuses incomplete copies and invalid metadata. It sets the root fields.
func checkPublication(prepared *snapshot.Manifest, metadata backup.Snapshot) (backup.Snapshot, error) {
	for _, image := range metadata.Images {
		// Validate requires a saved variant for the running image's platform.
		if image.Validate() != nil || image.SourceArchitecture != metadata.Architecture {
			return backup.Snapshot{}, backup.ErrInvalid
		}
	}
	if metadata.Consistency.Validate() != nil {
		return backup.Snapshot{}, backup.ErrInvalid
	}
	if prepared == nil || prepared.RootEntry == nil || prepared.RootEntry.DirSummary == nil || prepared.RootEntry.DirSummary.FatalErrorCount != 0 || prepared.RootEntry.DirSummary.IgnoredErrorCount != 0 || prepared.IncompleteReason != "" || metadata.ID == uuid.Nil || metadata.InstallationID == uuid.Nil || metadata.DestinationID == uuid.Nil || metadata.App.ID == uuid.Nil || metadata.App.NodeID == uuid.Nil || metadata.App.Definition == nil || metadata.StartedAt.IsZero() || metadata.CompletedAt.IsZero() || metadata.DowntimeMillis < 0 || metadata.Architecture == "" || metadata.CompletedAt.Before(metadata.StartedAt) || (metadata.Method != backup.Stop && metadata.Method != backup.Live && metadata.Method != backup.Dump) {
		return backup.Snapshot{}, backup.ErrInvalid
	}
	metadata.FilesID = ""
	metadata.RootID = prepared.RootObjectID().String()
	metadata.FileBytes = prepared.RootEntry.DirSummary.TotalFileSize
	encoded, err := json.Marshal(metadata)
	if err != nil || len(encoded) > 3<<20 {
		return backup.Snapshot{}, backup.ErrInvalid
	}
	return metadata, nil
}

// Snapshot reads one logical snapshot after refreshing the repository.
func (r *Repository) Snapshot(ctx context.Context, id uuid.UUID) (backup.Snapshot, error) {
	if fatal := r.fatal.Load(); fatal != nil {
		return backup.Snapshot{}, &Error{Action: "read snapshot", Cause: *fatal}
	}
	if id == uuid.Nil {
		return backup.Snapshot{}, backup.ErrInvalid
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if err := r.repo.Refresh(ctx); err != nil {
		return backup.Snapshot{}, &Error{Action: "refresh snapshots", Cause: err}
	}
	entries, err := r.repo.FindManifests(ctx, map[string]string{"type": "synod.backup", "snapshot": id.String()})
	if err != nil {
		return backup.Snapshot{}, &Error{Action: "find snapshot", Cause: err}
	}
	if len(entries) == 0 {
		return backup.Snapshot{}, ErrSnapshotMissing
	}
	if len(entries) != 1 {
		return backup.Snapshot{}, ErrCatalogConflict
	}
	var result backup.Snapshot
	if _, err := r.repo.GetManifest(ctx, entries[0].ID, &result); err != nil {
		return backup.Snapshot{}, &Error{Action: "read snapshot", Cause: err}
	}
	if result.ID != id || result.FilesID == "" || result.RootID == "" {
		return backup.Snapshot{}, backup.ErrInvalid
	}
	return result, nil
}

// RestoreFiles writes one snapshot into a new directory. It never replaces an existing path.
// This file restore preserves modes and times. App restore will apply recorded owners.
func (r *Repository) RestoreFiles(ctx context.Context, id uuid.UUID, target string) error {
	saved, err := r.Snapshot(ctx, id)
	if err != nil {
		return err
	}
	files, err := snapshot.LoadSnapshot(ctx, r.repo, manifest.ID(saved.FilesID))
	if err != nil {
		return &Error{Action: "load snapshot files", Cause: err}
	}
	if files.RootEntry == nil || files.RootObjectID().String() != saved.RootID || files.IncompleteReason != "" {
		return ErrSnapshotIncomplete
	}
	if !filepath.IsAbs(target) || filepath.Clean(target) != target {
		return backup.ErrInvalid
	}
	if err := os.Mkdir(target, 0700); err != nil {
		return &Error{Action: "create restore directory", Cause: err}
	}
	root, err := os.OpenRoot(target)
	if err != nil {
		return &Error{Action: "open restore directory", Cause: err}
	}
	defer root.Close()
	entry := snapshotfs.EntryFromDirEntry(r.repo, files.RootEntry)
	defer entry.Close()
	dir, ok := entry.(kfs.Directory)
	if !ok {
		return backup.ErrInvalid
	}
	var directories []struct {
		path string
		mode os.FileMode
		time time.Time
	}
	var copyEntry func(kfs.Entry, string) error
	copyEntry = func(entry kfs.Entry, name string) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		switch item := entry.(type) {
		case kfs.Directory:
			if name != "." {
				if err := root.Mkdir(name, 0700); err != nil {
					return err
				}
				directories = append(directories, struct {
					path string
					mode os.FileMode
					time time.Time
				}{name, entry.Mode(), entry.ModTime()})
			}
			return kfs.IterateEntries(ctx, item, func(_ context.Context, child kfs.Entry) error {
				defer child.Close()
				if child.Name() == "" || child.Name() == "." || child.Name() == ".." || strings.ContainsAny(child.Name(), "/\\\\\x00") {
					return backup.ErrInvalid
				}
				return copyEntry(child, filepath.Join(name, child.Name()))
			})
		case kfs.File:
			input, err := item.Open(ctx)
			if err != nil {
				return err
			}
			defer input.Close()
			file, err := root.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL|syscall.O_NOFOLLOW, 0600)
			if err != nil {
				return err
			}
			_, copyErr := io.Copy(file, input)
			if copyErr == nil {
				copyErr = file.Chmod(entry.Mode().Perm())
			}
			if copyErr == nil {
				timestamp := syscall.NsecToTimeval(int64(entry.ModTime().Nanosecond()))
				timestamp.Sec = entry.ModTime().Unix()
				copyErr = syscall.Futimes(int(file.Fd()), []syscall.Timeval{timestamp, timestamp})
			}
			return errors.Join(copyErr, file.Sync(), file.Close())
		case kfs.Symlink:
			link, err := item.Readlink(ctx)
			if err != nil {
				return err
			}
			return root.Symlink(link, name)
		default:
			return backup.ErrInvalid
		}
	}
	if err := copyEntry(dir, "."); err != nil {
		return &Error{Action: "restore snapshot files", Cause: errors.Join(err, ctx.Err())}
	}
	slices.Reverse(directories)
	for _, directory := range directories {
		file, err := root.Open(directory.path)
		if err != nil {
			return &Error{Action: "restore directory metadata", Cause: err}
		}
		modeErr := file.Chmod(directory.mode.Perm())
		timestamp := syscall.NsecToTimeval(int64(directory.time.Nanosecond()))
		timestamp.Sec = directory.time.Unix()
		if err := errors.Join(modeErr, syscall.Futimes(int(file.Fd()), []syscall.Timeval{timestamp, timestamp}), file.Sync(), file.Close()); err != nil {
			return &Error{Action: "restore directory metadata", Cause: err}
		}
	}
	return nil
}
