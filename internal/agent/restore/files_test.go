package restore

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/restore"
)

func TestFileRestoreReadsRealBackupAndRefusesOtherApps(t *testing.T) {
	f := newFixture(t, false)
	w := Worker{Backup: f.worker}
	request := restore.FileRequest{ID: uuid.New(), InstallationID: f.worker.InstallationID, NodeID: f.worker.NodeID, AppID: f.app.ID, Action: restore.FileSnapshots}
	read := func(request restore.FileRequest) (restore.FileHeader, []byte) {
		t.Helper()
		var header restore.FileHeader
		var output bytes.Buffer
		headers := 0
		err := w.ServeFile(t.Context(), request, func(raw, data []byte) error {
			if len(raw) != 0 {
				headers++
				if err := json.Unmarshal(raw, &header); err != nil {
					return err
				}
			}
			if len(data) != 0 && headers != 1 {
				t.Error("content arrived without a header")
			}
			output.Write(data)
			return nil
		})
		if err != nil || headers != 1 {
			t.Fatal("file response failed", err, headers)
		}
		return header, output.Bytes()
	}
	header, _ := read(request)
	if len(header.Snapshots) != 1 || header.Snapshots[0].ID != f.saved.ID {
		t.Fatal("snapshot list changed", header.FailureCode)
	}
	request.Action, request.Path = restore.FileList, f.project
	header, _ = read(request)
	if header.FailureCode != "" || len(header.Entries) != 3 || header.SnapshotID != f.saved.ID {
		t.Fatal("saved directory differs", header)
	}
	request.Action, request.Path = restore.FileRead, filepath.Join(f.project, "state")
	header, data := read(request)
	if header.Format != "file" || string(data) != "private-restored-value" {
		t.Fatal("saved file differs", header.FailureCode)
	}
	request.Path = f.project
	header, data = read(request)
	if header.Format != "tar" {
		t.Fatal("directory is not an archive", header.FailureCode)
	}
	reader := tar.NewReader(bytes.NewReader(data))
	found := false
	for {
		entry, err := reader.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if filepath.Base(entry.Name) == ".env" {
			content, err := io.ReadAll(reader)
			if err != nil || string(content) != "SECRET=private-restored-env" {
				t.Fatal("environment file changed", err)
			}
			found = true
		}
	}
	if !found {
		t.Fatal("directory omitted environment file")
	}
	request.Path = filepath.Join(f.project, "not-saved")
	if header, _ := read(request); header.FailureCode != "path_not_found" {
		t.Fatal("missing path accepted", header.FailureCode)
	}
	request.Path, request.AppID, request.SnapshotID = f.project, uuid.New(), f.saved.ID
	if header, _ := read(request); header.FailureCode != "snapshot_invalid" {
		t.Fatal("another app's snapshot was read", header.FailureCode)
	}
	request.AppID, request.InstallationID = f.app.ID, uuid.New()
	if err := w.ServeFile(t.Context(), request, func([]byte, []byte) error { t.Error("wrong installation received content"); return nil }); !errors.Is(err, restore.ErrInvalid) {
		t.Fatal("installation mismatch accepted", err)
	}
	request.InstallationID = f.worker.InstallationID
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	sends := 0
	err := w.ServeFile(ctx, request, func(raw, data []byte) error {
		sends++
		if len(data) != 0 {
			cancel()
			return context.Canceled
		}
		return nil
	})
	if err == nil || sends < 2 {
		t.Fatal("cancelled archive transfer succeeded")
	}
}
