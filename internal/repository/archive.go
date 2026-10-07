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
	"github.com/kopia/kopia/repo/manifest"
	"github.com/kopia/kopia/snapshot"
	"github.com/kopia/kopia/snapshot/snapshotfs"
	"github.com/sirrobot01/synod/internal/backup"
)

// Archive streams one saved path as tar. Owners, modes, times, and links are preserved.
// A directory has the tar root ".". A file has its saved base name.
// The caller must extract into a new, isolated directory or volume.
func (r *Repository) Archive(ctx context.Context, id uuid.UUID, source string, output io.Writer) error {
	if !fs.ValidPath(source) || strings.ContainsAny(source, "\\\x00") {
		return backup.ErrInvalid
	}
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
	entry := snapshotfs.EntryFromDirEntry(r.repo, files.RootEntry)
	defer func() { entry.Close() }()
	if source != "." {
		for part := range strings.SplitSeq(source, "/") {
			directory, ok := entry.(kfs.Directory)
			if !ok {
				return backup.ErrInvalid
			}
			next, err := directory.Child(ctx, part)
			if err != nil {
				return &Error{Action: "find saved path", Cause: err}
			}
			entry.Close()
			entry = next
		}
	}
	archive := tar.NewWriter(output)
	var writeEntry func(kfs.Entry, string) error
	writeEntry = func(entry kfs.Entry, name string) error {
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
			return kfs.IterateEntries(ctx, item, func(_ context.Context, child kfs.Entry) error {
				defer child.Close()
				if child.Name() == "" || child.Name() == "." || child.Name() == ".." || strings.ContainsAny(child.Name(), "/\\\x00") {
					return backup.ErrInvalid
				}
				return writeEntry(child, path.Join(name, child.Name()))
			})
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
	if err := writeEntry(entry, name); err != nil {
		return &Error{Action: "stream saved path", Cause: err}
	}
	return archive.Close()
}
