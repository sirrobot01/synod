package api

import (
	"context"
	"errors"
	"io"
	"mime"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/controlstream"
	"github.com/sirrobot01/synod/internal/restore"
)

func (s *Server) listSnapshots(w http.ResponseWriter, r *http.Request) {
	s.fileResponse(w, r, restore.FileSnapshots)
}
func (s *Server) listFiles(w http.ResponseWriter, r *http.Request) {
	s.fileResponse(w, r, restore.FileList)
}
func (s *Server) downloadFile(w http.ResponseWriter, r *http.Request) {
	s.fileResponse(w, r, restore.FileRead)
}

func (s *Server) fileResponse(w http.ResponseWriter, r *http.Request, action string) {
	input := restore.FileRequest{ID: uuid.New(), InstallationID: s.installationID, Action: action, Path: r.URL.Query().Get("path")}
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil || id == uuid.Nil {
		s.writeError(w, r, 400, "invalid_request", "Use an app ID.")
		return
	}
	input.AppID = id
	for key, target := range map[string]*uuid.UUID{"snapshot": &input.SnapshotID, "destination": &input.DestinationID, "via": &input.NodeID} {
		if value := r.URL.Query().Get(key); value != "" {
			id, err := uuid.Parse(value)
			if err != nil || id == uuid.Nil {
				s.writeError(w, r, 400, "invalid_request", "Use IDs for snapshot, destination, and via.")
				return
			}
			*target = id
		}
	}
	// Selection fills the node ID. Validate the remaining fields before dispatch.
	checked := input
	checked.NodeID = input.ID
	if checked.Validate() != nil {
		s.writeError(w, r, 400, "invalid_request", "Use a saved path and valid snapshot choices.")
		return
	}
	if s.ReadFile == nil {
		s.writeError(w, r, 503, "dependency_unavailable", "Start an approved agent with access to the destination.")
		return
	}
	if action == restore.FileRead {
		if err := http.NewResponseController(w).SetWriteDeadline(time.Now().Add(12 * time.Hour)); err != nil && !errors.Is(err, http.ErrNotSupported) {
			s.writeError(w, r, 503, "dependency_unavailable", "The download could not start. Retry the request.")
			return
		}
	}
	header, body, err := s.ReadFile(r.Context(), input)
	if err != nil {
		if errors.Is(err, controlstream.ErrDisconnected) || errors.Is(err, controlstream.ErrBusy) || r.Context().Err() != nil || errors.Is(err, context.DeadlineExceeded) {
			s.writeError(w, r, 503, "dependency_unavailable", "Start an approved agent with current destination settings, then retry.")
			return
		}
		s.storeError(w, r, err)
		return
	}
	defer body.Close()
	if header.FailureCode != "" || action != restore.FileRead {
		if _, err := io.Copy(io.Discard, body); err != nil {
			s.writeError(w, r, 502, "file_transfer_incomplete", "The agent response was interrupted. Retry the request.")
			return
		}
		if header.FailureCode != "" {
			code, message := http.StatusBadGateway, "The saved content could not be read. Check the destination and retry."
			switch header.FailureCode {
			case "path_not_found", "path_not_saved", "snapshot_not_found":
				code, message = 404, "The path or snapshot was not found. List the saved paths and retry."
			case "path_invalid", "path_unsupported", "snapshot_invalid":
				code, message = 400, "The saved path or snapshot cannot be read. Check the selection and retry."
			}
			s.writeError(w, r, code, "file_read_failed", message)
			return
		}
		s.writeJSON(w, r, header)
		return
	}
	w.Header().Set("X-Synod-Snapshot-ID", header.SnapshotID.String())
	w.Header().Set("X-Synod-Destination-ID", header.DestinationID.String())
	w.Header().Set("X-Synod-Format", header.Format)
	for _, warning := range header.Warnings {
		w.Header().Add("X-Synod-Warning", warning)
	}
	name := header.Name
	w.Header().Set("Content-Type", "application/octet-stream")
	if header.Format == "tar" {
		name += ".tar"
		w.Header().Set("Content-Type", "application/x-tar")
	}
	w.Header().Set("Content-Disposition", mime.FormatMediaType("attachment", map[string]string{"filename": name}))
	// This trailer proves that the agent reached a clean end, even for an empty file.
	w.Header().Set("Trailer", "X-Synod-Transfer")
	w.WriteHeader(http.StatusOK)
	if _, err := io.Copy(w, body); err != nil {
		panic(http.ErrAbortHandler)
	}
	w.Header().Set("X-Synod-Transfer", "complete")
}
