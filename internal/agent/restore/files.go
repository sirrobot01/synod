package restore

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"path"
	"slices"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/backup"
	"github.com/sirrobot01/synod/internal/repository"
	"github.com/sirrobot01/synod/internal/restore"
)

// chunkWriter sends content in pieces that fit one transport message.
type chunkWriter func(data []byte) error

func (send chunkWriter) Write(p []byte) (int, error) {
	written := 0
	for len(p) > 0 {
		n := min(len(p), 256<<10)
		if err := send(bytes.Clone(p[:n])); err != nil {
			return written, err
		}
		p, written = p[n:], written+n
	}
	return written, nil
}

// ServeFile answers one file request. It sends exactly one header, then content for a successful read.
// A content error after the header ends the stream, so the reader sees an incomplete transfer.
func (w *Worker) ServeFile(ctx context.Context, request restore.FileRequest, send func(header, data []byte) error) error {
	b := w.Backup
	if request.Validate() != nil || request.NodeID != b.NodeID || request.InstallationID != b.InstallationID {
		return restore.ErrInvalid
	}
	var header restore.FileHeader
	answer := func() error {
		raw, err := json.Marshal(header)
		if err != nil {
			return err
		}
		// Keep metadata below the transport message limit. Content has its own stream.
		for len(raw) > 900<<10 && (len(header.Snapshots) > 0 || len(header.Entries) > 0) {
			header.Truncated = true
			if len(header.Snapshots) > 0 {
				header.Snapshots = header.Snapshots[:len(header.Snapshots)-1]
			} else {
				header.Entries = header.Entries[:len(header.Entries)-1]
			}
			raw, err = json.Marshal(header)
			if err != nil {
				return err
			}
		}
		return send(raw, nil)
	}
	fail := func(err error) error {
		header = restore.FileHeader{FailureCode: "file_read_failed", SnapshotID: header.SnapshotID, DestinationID: header.DestinationID}
		switch failed, ok := errors.AsType[*failure](err); {
		case ok:
			header.FailureCode = failed.Code
		case errors.Is(err, repository.ErrPathMissing):
			header.FailureCode = "path_not_found"
		case errors.Is(err, backup.ErrInvalid):
			header.FailureCode = "path_invalid"
		}
		return answer()
	}
	if request.Action == restore.FileSnapshots {
		summaries, unavailable, err := w.snapshots(ctx, request.AppID, request.DestinationID)
		if err != nil {
			return fail(err)
		}
		header.Snapshots = summaries
		if len(header.Snapshots) > 1000 {
			header.Snapshots, header.Truncated = header.Snapshots[:1000], true
		}
		if unavailable {
			header.Warnings = []string{"destination_unavailable"}
		}
		return answer()
	}
	r, saved, destinationID, unavailable, err := w.openSnapshot(ctx, request.AppID, request.SnapshotID, request.DestinationID)
	if err != nil {
		return fail(err)
	}
	defer r.Close(context.Background())
	header.SnapshotID, header.DestinationID = saved.ID, destinationID
	if unavailable {
		header.Warnings = []string{"destination_unavailable"}
	}
	archive, _, err := repository.Locate(saved, request.Path)
	if err != nil {
		return fail(err)
	}
	var info fs.FileInfo
	if archive != "" {
		info, err = r.Stat(ctx, saved.ID, archive)
		if err != nil {
			return fail(err)
		}
	}
	if request.Action == restore.FileList {
		header.PathType = "directory"
		if info != nil && !info.IsDir() {
			header.PathType = "other"
			if info.Mode().IsRegular() {
				header.PathType = "file"
			}
			if info.Mode()&fs.ModeSymlink != 0 {
				header.PathType = "symlink"
			}
		}
		header.Entries, header.Truncated, err = r.List(ctx, saved, request.Path, 4000)
		if err != nil {
			return fail(err)
		}
		return answer()
	}
	header.Name = path.Base(request.Path)
	if header.Name == "/" || header.Name == "." {
		header.Name = "snapshot"
	}
	content := chunkWriter(func(data []byte) error { return send(nil, data) })
	switch {
	case info == nil || info.IsDir():
		header.Format = "tar"
		if err := answer(); err != nil {
			return err
		}
		return r.ArchivePath(ctx, saved, request.Path, header.Name, content)
	case info.Mode().IsRegular():
		reader, size, err := r.OpenFile(ctx, saved.ID, archive)
		if err != nil {
			return fail(err)
		}
		defer reader.Close()
		header.Format, header.Size = "file", size
		if err := answer(); err != nil {
			return err
		}
		copied, err := io.Copy(content, reader)
		if err == nil && copied != size {
			err = io.ErrUnexpectedEOF
		}
		return err
	default:
		return fail(&failure{Code: "path_unsupported"})
	}
}

// snapshots lists the copies of one app in each destination that this agent can open, newest first.
func (w *Worker) snapshots(ctx context.Context, appID, destinationID uuid.UUID) ([]restore.SnapshotSummary, bool, error) {
	b := w.Backup
	plan, err := b.Journal.DestinationPlan(ctx, b.Keyring)
	if err != nil {
		return nil, false, &failure{Code: "destination_unavailable", Cause: err}
	}
	byID := make(map[uuid.UUID]*restore.SnapshotSummary)
	unavailable, opened := false, false
	for _, entry := range plan.Destinations {
		id := entry.Destination.ID
		if destinationID != uuid.Nil && id != destinationID {
			continue
		}
		location, credentials, err := plan.Connection(id, b.Identity)
		if err != nil {
			unavailable = true
			continue
		}
		r, err := repository.Open(ctx, location, credentials)
		if err != nil {
			unavailable = true
			continue
		}
		list, err := r.Snapshots(ctx, b.InstallationID, appID)
		r.Close(context.Background())
		if err != nil {
			unavailable = true
			continue
		}
		opened = true
		for _, saved := range list {
			if summary := byID[saved.ID]; summary != nil {
				summary.DestinationIDs = append(summary.DestinationIDs, id)
				continue
			}
			byID[saved.ID] = new(restore.Summarize(saved, id))
		}
	}
	if !opened {
		return nil, unavailable, &failure{Code: "destination_unavailable"}
	}
	result := make([]restore.SnapshotSummary, 0, len(byID))
	for _, summary := range byID {
		result = append(result, *summary)
	}
	slices.SortFunc(result, func(a, b restore.SnapshotSummary) int {
		return cmp.Or(b.CompletedAt.Compare(a.CompletedAt), cmp.Compare(a.ID.String(), b.ID.String()))
	})
	return result, unavailable, nil
}
