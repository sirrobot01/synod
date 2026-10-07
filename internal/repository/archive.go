package repository

import (
	"archive/tar"
	"context"
	"errors"
	"io"
	"io/fs"
	"path"
	"strings"

	"github.com/google/uuid"
	kfs "github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/fs/virtualfs"
	"github.com/kopia/kopia/repo/manifest"
	"github.com/kopia/kopia/snapshot"
	"github.com/kopia/kopia/snapshot/snapshotfs"
	"github.com/sirrobot01/synod/internal/backup"
)

// Archive streams one saved path as tar. Owners, modes, times, and links are preserved.
// targetName sets the tar root. With an empty name, a directory uses "." and a file uses its saved base name.
// The caller must extract into a new, isolated directory or volume.
func (r *Repository) Archive(ctx context.Context, id uuid.UUID, source, targetName string, output io.Writer) error {
	if targetName != "" && (!fs.ValidPath(targetName) || strings.ContainsAny(targetName, "\\\x00")) {
		return backup.ErrInvalid
	}
	entry, err := r.savedEntry(ctx, id, source)
	if err != nil {
		return err
	}
	defer entry.Close()
	return r.writeArchive(ctx, entry, source, targetName, output, nil)
}

// ArchivePath merges separately saved paths into a directory download.
// Virtual parent directories and database dumps use the same names as List.
func (r *Repository) ArchivePath(ctx context.Context, saved backup.Snapshot, source, targetName string, output io.Writer) error {
	if source == "" {
		source = "/"
	}
	archive, _, err := Locate(saved, source)
	if err != nil {
		return err
	}
	var entry kfs.Entry = virtualfs.NewStaticDirectory(path.Base(source), nil)
	if archive != "" {
		entry, err = r.savedEntry(ctx, saved.ID, archive)
		if err != nil {
			return err
		}
	}
	defer entry.Close()
	if targetName != "" && (!fs.ValidPath(targetName) || strings.ContainsAny(targetName, "\\\x00")) {
		return backup.ErrInvalid
	}
	return r.writeArchive(ctx, entry, source, targetName, output, &saved)
}

func (r *Repository) writeArchive(ctx context.Context, entry kfs.Entry, source, targetName string, output io.Writer, saved *backup.Snapshot) error {
	archive := tar.NewWriter(output)
	var writeEntry func(kfs.Entry, string, string) error
	writeEntry = func(entry kfs.Entry, name, original string) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		link := ""
		if item, ok := entry.(kfs.Symlink); ok {
			var err error
			link, err = item.Readlink(ctx)
			if err != nil {
				return err
			}
		}
		header, err := tar.FileInfoHeader(entry, link)
		if err != nil {
			return err
		}
		header.Name, header.Uname, header.Gname = name, "", ""
		header.Uid, header.Gid = int(entry.Owner().UserID), int(entry.Owner().GroupID)
		header.Format = tar.FormatPAX
		if err := archive.WriteHeader(header); err != nil {
			return err
		}
		switch item := entry.(type) {
		case kfs.Directory:
			// Only path boundaries need extra state. Ordinary directories stream each child.
			overrides := make(map[string]bool)
			if saved != nil {
				_, children, err := Locate(*saved, original)
				if err != nil {
					return err
				}
				for _, child := range children {
					overrides[child] = true
				}
			}
			err := kfs.IterateEntries(ctx, item, func(_ context.Context, child kfs.Entry) error {
				defer child.Close()
				if child.Name() == "" || child.Name() == "." || child.Name() == ".." || strings.ContainsAny(child.Name(), "/\\\x00") {
					return backup.ErrInvalid
				}
				if overrides[child.Name()] {
					return nil
				}
				return writeEntry(child, path.Join(name, child.Name()), path.Join(original, child.Name()))
			})
			if err != nil {
				return err
			}
			for child := range overrides {
				originalChild := path.Join(original, child)
				if original == "/" && child == DatabasesPath {
					originalChild = DatabasesPath
				}
				archived, _, err := Locate(*saved, originalChild)
				if err != nil {
					return err
				}
				var nested kfs.Entry = virtualfs.NewStaticDirectory(child, nil)
				if archived != "" {
					nested, err = r.savedEntry(ctx, saved.ID, archived)
					if err != nil {
						return err
					}
				}
				err = writeEntry(nested, path.Join(name, child), originalChild)
				nested.Close()
				if err != nil {
					return err
				}
			}
			return nil
		case kfs.File:
			reader, err := item.Open(ctx)
			if err != nil {
				return err
			}
			_, err = io.CopyN(archive, reader, header.Size)
			return errors.Join(err, reader.Close())
		case kfs.Symlink:
			return nil
		default:
			return backup.ErrInvalid
		}
	}
	name := path.Base(source)
	if _, ok := entry.(kfs.Directory); ok {
		name = "."
	}
	if targetName != "" {
		name = targetName
	}
	if name == "." && !entry.IsDir() {
		return backup.ErrInvalid
	}
	if err := writeEntry(entry, name, source); err != nil {
		return &Error{Action: "stream saved path", Cause: err}
	}
	return archive.Close()
}

// savedEntry validates the saved root and never traverses a symlink.
func (r *Repository) savedEntry(ctx context.Context, id uuid.UUID, source string) (kfs.Entry, error) {
	if !fs.ValidPath(source) || strings.ContainsAny(source, "\\\x00") {
		return nil, backup.ErrInvalid
	}
	saved, err := r.Snapshot(ctx, id)
	if err != nil {
		return nil, err
	}
	files, err := snapshot.LoadSnapshot(ctx, r.repo, manifest.ID(saved.FilesID))
	if err != nil {
		return nil, &Error{Action: "load snapshot files", Cause: err}
	}
	if files.RootEntry == nil || files.RootObjectID().String() != saved.RootID || files.IncompleteReason != "" {
		return nil, ErrSnapshotIncomplete
	}
	entry := snapshotfs.EntryFromDirEntry(r.repo, files.RootEntry)
	ready := false
	defer func() {
		if !ready {
			entry.Close()
		}
	}()
	if source != "." {
		for part := range strings.SplitSeq(source, "/") {
			directory, ok := entry.(kfs.Directory)
			if !ok {
				return nil, backup.ErrInvalid
			}
			next, err := directory.Child(ctx, part)
			if errors.Is(err, kfs.ErrEntryNotFound) {
				return nil, ErrPathMissing
			}
			if err != nil {
				return nil, &Error{Action: "find saved path", Cause: err}
			}
			entry.Close()
			entry = next
		}
	}
	ready = true
	return entry, nil
}

// ReadFile reads a bounded regular file from a snapshot.
func (r *Repository) ReadFile(ctx context.Context, id uuid.UUID, source string, limit int64) ([]byte, error) {
	if limit < 0 || limit > 16<<20 {
		return nil, backup.ErrInvalid
	}
	entry, err := r.savedEntry(ctx, id, source)
	if err != nil {
		return nil, err
	}
	defer entry.Close()
	file, ok := entry.(kfs.File)
	if !ok || entry.Size() > limit {
		return nil, backup.ErrInvalid
	}
	reader, err := file.Open(ctx)
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	data, err := io.ReadAll(io.LimitReader(reader, limit+1))
	if int64(len(data)) > limit {
		return nil, backup.ErrInvalid
	}
	return data, err
}

// Stat reads path metadata without resolving a saved symlink.
func (r *Repository) Stat(ctx context.Context, id uuid.UUID, source string) (fs.FileInfo, error) {
	entry, err := r.savedEntry(ctx, id, source)
	if err != nil {
		return nil, err
	}
	defer entry.Close()
	header, err := tar.FileInfoHeader(entry, "")
	if err != nil {
		return nil, err
	}
	return header.FileInfo(), nil
}
