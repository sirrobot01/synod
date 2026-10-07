package restore

import (
	"slices"
	"strings"
	"time"
	"unicode"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/backup"
	"github.com/sirrobot01/synod/internal/repository"
)

// File request actions. Snapshots lists the copies of an app. List reads one directory. Read streams content.
const (
	FileSnapshots = "snapshots"
	FileList      = "list"
	FileRead      = "read"
)

// FileRequest asks one online agent to read a repository. It holds IDs and a path only.
// An absent DestinationID lets the agent check every destination that it can open.
// An absent SnapshotID selects the newest snapshot. The header names the snapshot that was read.
type FileRequest struct {
	ID             uuid.UUID `json:"id"`
	InstallationID uuid.UUID `json:"installation_id"`
	NodeID         uuid.UUID `json:"node_id"`
	Action         string    `json:"action"`
	AppID          uuid.UUID `json:"app_id"`
	SnapshotID     uuid.UUID `json:"snapshot_id,omitzero"`
	DestinationID  uuid.UUID `json:"destination_id,omitzero"`
	Path           string    `json:"path,omitempty"`
}

func (r FileRequest) Validate() error {
	if r.ID == uuid.Nil || r.InstallationID == uuid.Nil || r.NodeID == uuid.Nil || r.AppID == uuid.Nil || len(r.Path) > 4096 || strings.ContainsFunc(r.Path, unicode.IsControl) {
		return ErrInvalid
	}
	switch r.Action {
	case FileSnapshots:
		if r.SnapshotID != uuid.Nil || r.Path != "" {
			return ErrInvalid
		}
	case FileList, FileRead:
	default:
		return ErrInvalid
	}
	return nil
}

// SnapshotSummary describes one app snapshot without container definitions or credentials.
type SnapshotSummary struct {
	ID             uuid.UUID          `json:"id"`
	DestinationIDs []uuid.UUID        `json:"destination_ids"`
	SourceNodeID   uuid.UUID          `json:"source_node_id"`
	SourceNodeName string             `json:"source_node_name"`
	Architecture   string             `json:"architecture"`
	Method         backup.Method      `json:"method"`
	StartedAt      time.Time          `json:"started_at"`
	CompletedAt    time.Time          `json:"completed_at"`
	FileBytes      int64              `json:"file_bytes"`
	Paths          []string           `json:"paths"`
	Databases      []string           `json:"databases,omitempty"`
	Exclusions     []backup.Exclusion `json:"exclusions,omitempty"`
}

// Summarize keeps the fields that a user needs to choose a snapshot.
func Summarize(saved backup.Snapshot, destinationID uuid.UUID) SnapshotSummary {
	summary := SnapshotSummary{ID: saved.ID, DestinationIDs: []uuid.UUID{destinationID}, SourceNodeID: saved.App.NodeID, SourceNodeName: saved.App.NodeName, Architecture: saved.Architecture, Method: saved.Method, StartedAt: saved.StartedAt, CompletedAt: saved.CompletedAt, FileBytes: saved.FileBytes, Exclusions: saved.Exclusions}
	for _, item := range saved.Paths {
		if !slices.Contains(summary.Paths, item.Source) {
			summary.Paths = append(summary.Paths, item.Source)
		}
	}
	if len(saved.Dumps) > 0 {
		_, summary.Databases, _ = repository.Locate(saved, repository.DatabasesPath)
	}
	return summary
}

// FileHeader is the first answer to a FileRequest. Content chunks follow only for a successful read.
// A read of a directory streams a tar archive. A read of a file streams the file.
type FileHeader struct {
	FailureCode   string             `json:"failure_code,omitempty"`
	SnapshotID    uuid.UUID          `json:"snapshot_id,omitzero"`
	DestinationID uuid.UUID          `json:"destination_id,omitzero"`
	Snapshots     []SnapshotSummary  `json:"snapshots,omitempty"`
	Entries       []repository.Entry `json:"entries,omitempty"`
	Truncated     bool               `json:"truncated,omitzero"`
	Format        string             `json:"format,omitempty"`
	Name          string             `json:"name,omitempty"`
	Size          int64              `json:"size,omitzero"`
	Warnings      []string           `json:"warning_codes,omitempty"`
}
