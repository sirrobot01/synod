// Package backup defines immutable backup work and saved snapshot metadata.
package backup

import (
	"errors"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/inventory"
)

type Method string

const (
	Stop Method = "stop"
	Live Method = "live"
)

var ErrInvalid = errors.New("invalid backup request or snapshot")

// Request contains no credentials or file contents. The agent resolves its saved destination.
type Request struct {
	ID             uuid.UUID `json:"id"`
	InstallationID uuid.UUID `json:"installation_id"`
	AppID          uuid.UUID `json:"app_id"`
	NodeID         uuid.UUID `json:"node_id"`
	SourceKey      string    `json:"source_key"`
	DestinationID  uuid.UUID `json:"destination_id"`
	Method         Method    `json:"method"`
	Initialize     bool      `json:"initialize,omitzero"`
	RequestedAt    time.Time `json:"requested_at"`
}

func (r Request) Validate() error {
	if r.ID == uuid.Nil || r.InstallationID == uuid.Nil || r.AppID == uuid.Nil || r.NodeID == uuid.Nil || r.DestinationID == uuid.Nil || r.RequestedAt.IsZero() || (r.Method != Stop && r.Method != Live) || len(r.SourceKey) > 512 || !(strings.HasPrefix(r.SourceKey, "compose:") || strings.HasPrefix(r.SourceKey, "container:")) || uuid.NewSHA1(r.NodeID, []byte(r.SourceKey)) != r.AppID {
		return ErrInvalid
	}
	return nil
}

// Path maps an original path to its root in the snapshot.
type Path struct {
	Source  string `json:"source"`
	Archive string `json:"archive"`
	Kind    string `json:"kind"`
}

type Exclusion struct {
	Source string `json:"source"`
	Reason string `json:"reason"`
}

// Snapshot is stored inside the encrypted repository beside its native Kopia snapshot.
type Snapshot struct {
	ID             uuid.UUID     `json:"id"`
	InstallationID uuid.UUID     `json:"installation_id"`
	DestinationID  uuid.UUID     `json:"destination_id"`
	App            inventory.App `json:"app"`
	Method         Method        `json:"method"`
	Architecture   string        `json:"architecture"`
	StartedAt      time.Time     `json:"started_at"`
	CompletedAt    time.Time     `json:"completed_at"`
	DowntimeMillis int64         `json:"downtime_ms"`
	Paths          []Path        `json:"paths"`
	Exclusions     []Exclusion   `json:"exclusions"`
	FilesID        string        `json:"files_id"`
	RootID         string        `json:"root_id"`
	FileBytes      int64         `json:"file_bytes"`
}

// Result is safe for controller metadata. Definitions and file contents stay in the repository.
type Result struct {
	SnapshotID       uuid.UUID `json:"snapshot_id,omitzero"`
	DestinationID    uuid.UUID `json:"destination_id"`
	StartedAt        time.Time `json:"started_at"`
	CompletedAt      time.Time `json:"completed_at"`
	DowntimeMillis   int64     `json:"downtime_ms"`
	FileBytes        int64     `json:"file_bytes"`
	FailureCode      string    `json:"failure_code,omitempty"`
	RepositoryOpened bool      `json:"repository_opened,omitzero"`
	RestartPending   []string  `json:"restart_pending,omitempty"`
}

// Submission holds the choices made by an administrator. The controller resolves the device.
type Submission struct {
	ID            uuid.UUID `json:"id"`
	AppID         uuid.UUID `json:"app_id"`
	DestinationID uuid.UUID `json:"destination_id"`
	Method        Method    `json:"method"`
}

// Event is resent until a terminal result is durably acknowledged.
type Event struct {
	ID     uuid.UUID `json:"id"`
	Status string    `json:"status"`
	Result Result    `json:"result"`
}

func (e Event) Validate(request Request) error {
	result := e.Result
	if e.ID != request.ID || result.DestinationID != request.DestinationID || result.DowntimeMillis < 0 || result.FileBytes < 0 || len(result.RestartPending) > 256 {
		return ErrInvalid
	}
	for _, id := range result.RestartPending {
		if len(id) != 64 || strings.ContainsFunc(id, func(r rune) bool { return !strings.ContainsRune("0123456789abcdef", r) }) {
			return ErrInvalid
		}
	}
	switch e.Status {
	case "queued", "running", "waiting":
		if e.Status == "queued" && (result.RepositoryOpened || !result.StartedAt.IsZero() || len(result.RestartPending) > 0) {
			return ErrInvalid
		}
		if result.SnapshotID != uuid.Nil || !result.CompletedAt.IsZero() || result.FailureCode != "" {
			return ErrInvalid
		}
	case "succeeded", "failed":
		if result.StartedAt.IsZero() || result.CompletedAt.IsZero() || result.CompletedAt.Before(result.StartedAt) || len(result.RestartPending) != 0 {
			return ErrInvalid
		}
		if e.Status == "succeeded" {
			if result.SnapshotID != request.ID || !result.RepositoryOpened || result.FailureCode != "" {
				return ErrInvalid
			}
		} else if result.SnapshotID != uuid.Nil || result.FileBytes != 0 || !slices.Contains([]string{"destination_unavailable", "repository_open_failed", "repository_installation_conflict", "app_scan_failed", "database_dump_unavailable", "app_definition_incomplete", "app_state_unsafe", "app_definition_limit", "standalone_definition_unsupported", "source_unavailable", "app_definition_excluded", "first_copy_failed", "journal_save_failed", "app_stop_failed", "app_changed_during_backup", "final_copy_failed", "agent_interrupted", "snapshot_publish_failed"}, result.FailureCode) {
			return ErrInvalid
		}
	default:
		return ErrInvalid
	}
	return nil
}
