// Package backup defines immutable backup work and saved snapshot metadata.
package backup

import (
	"bytes"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/inventory"
)

type Method string

const (
	Auto Method = "auto"
	Stop Method = "stop"
	Live Method = "live"
	Dump Method = "dump"
)

var ErrInvalid = errors.New("invalid backup request or snapshot")

// Request contains no credentials or file contents. The agent resolves its saved destination.
type Request struct {
	ID             uuid.UUID  `json:"id"`
	InstallationID uuid.UUID  `json:"installation_id"`
	AppID          uuid.UUID  `json:"app_id"`
	NodeID         uuid.UUID  `json:"node_id"`
	SourceKey      string     `json:"source_key"`
	DestinationID  uuid.UUID  `json:"destination_id"`
	Method         Method     `json:"method"`
	Initialize     bool       `json:"initialize,omitzero"`
	RequestedAt    time.Time  `json:"requested_at"`
	ScheduleID     uuid.UUID  `json:"schedule_id,omitzero"`
	ScheduledAt    time.Time  `json:"scheduled_at,omitzero"`
	Retention      *Retention `json:"retention,omitzero"`
	// RestoreID marks a safety snapshot that a replace takes. The restore result reports it.
	RestoreID uuid.UUID `json:"restore_id,omitzero"`
	// IncludeLarge comes from the app's saved settings. The agent copies these skipped paths.
	IncludeLarge []string `json:"include_large,omitempty"`
}

func (r Request) Validate() error {
	if r.ID == uuid.Nil || r.InstallationID == uuid.Nil || r.AppID == uuid.Nil || r.NodeID == uuid.Nil || r.DestinationID == uuid.Nil || r.RequestedAt.IsZero() || (r.Method != Auto && r.Method != Stop && r.Method != Live && r.Method != Dump) || len(r.SourceKey) > 512 || !(strings.HasPrefix(r.SourceKey, "compose:") || strings.HasPrefix(r.SourceKey, "container:")) || uuid.NewSHA1(r.NodeID, []byte(r.SourceKey)) != r.AppID {
		return ErrInvalid
	}
	if r.RestoreID != uuid.Nil && (r.ScheduleID != uuid.Nil || r.Initialize || r.Retention != nil || r.ID != SafetyID(r.RestoreID)) {
		return ErrInvalid
	}
	if (r.ScheduleID == uuid.Nil) != r.ScheduledAt.IsZero() || (r.ScheduleID != uuid.Nil && (r.Initialize || !r.RequestedAt.Equal(r.ScheduledAt) || r.Retention == nil)) || (r.Retention != nil && r.Retention.Validate() != nil) {
		return ErrInvalid
	}
	return ValidateIncluded(r.IncludeLarge)
}

// SafetyID derives the one safety snapshot ID that a restore can take.
func SafetyID(restoreID uuid.UUID) uuid.UUID { return uuid.NewSHA1(restoreID, []byte("safety")) }

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

// DatabaseDump maps a native dump to the container that must load it at restore.
type DatabaseDump struct {
	ContainerID string `json:"container_id"`
	Database    string `json:"database"`
	Archive     string `json:"archive"`
	Format      string `json:"format"`
}

// Image records immutable platform evidence for a saved container image.
type Image struct {
	ContainerID   string   `json:"container_id"`
	Reference     string   `json:"reference"`
	Architectures []string `json:"architectures"`
	LocalOnly     bool     `json:"local_only,omitzero"`
}

// MountSource records the type and owner of a bind source, including skipped sources.
type MountSource struct {
	Source  string `json:"source"`
	Mode    uint32 `json:"mode"`
	UserID  uint32 `json:"uid"`
	GroupID uint32 `json:"gid"`
}

// Snapshot is stored inside the encrypted repository beside its native Kopia snapshot.
type Snapshot struct {
	ID             uuid.UUID     `json:"id"`
	InstallationID uuid.UUID     `json:"installation_id"`
	DestinationID  uuid.UUID     `json:"destination_id"`
	App            inventory.App `json:"app"`
	Method         Method        `json:"method"`
	Architecture   string        `json:"architecture"`
	Images         []Image       `json:"images"`
	MountSources   []MountSource `json:"mount_sources,omitempty"`
	StartedAt      time.Time     `json:"started_at"`
	CompletedAt    time.Time     `json:"completed_at"`
	DowntimeMillis int64         `json:"downtime_ms"`
	SQLite         []string      `json:"sqlite,omitempty"`
	// Changing lists files that kept changing while a live copy read them. They are saved as read.
	Changing   []string       `json:"changing,omitempty"`
	Dumps      []DatabaseDump `json:"dumps,omitempty"`
	Paths      []Path         `json:"paths"`
	Exclusions []Exclusion    `json:"exclusions"`
	FilesID    string         `json:"files_id"`
	RootID     string         `json:"root_id"`
	FileBytes  int64          `json:"file_bytes"`
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
	WarningCodes     []string  `json:"warning_codes,omitempty"`
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
	ID      uuid.UUID `json:"id"`
	Status  string    `json:"status"`
	Result  Result    `json:"result"`
	Request *Request  `json:"request,omitzero"`
}

func (e Event) Validate(request Request) error {
	if e.Request != nil {
		expected, _ := json.Marshal(request)
		provided, _ := json.Marshal(e.Request)
		if !bytes.Equal(expected, provided) {
			return ErrInvalid
		}
	}
	if request.ScheduleID != uuid.Nil && e.Request == nil {
		return ErrInvalid
	}
	result := e.Result
	if e.ID != request.ID || result.DestinationID != request.DestinationID || result.DowntimeMillis < 0 || result.FileBytes < 0 || len(result.RestartPending) > 256 {
		return ErrInvalid
	}
	if len(result.WarningCodes) > 3 || (len(result.WarningCodes) != 0 && !result.RepositoryOpened) {
		return ErrInvalid
	}
	for i, warning := range result.WarningCodes {
		if !slices.Contains([]string{"files_changed_during_copy", "catalog_write_failed", "retention_failed"}, warning) || slices.Contains(result.WarningCodes[:i], warning) {
			return ErrInvalid
		}
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
		if result.SnapshotID != uuid.Nil || !result.CompletedAt.IsZero() || result.FailureCode != "" || len(result.WarningCodes) != 0 {
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
		} else if result.SnapshotID != uuid.Nil || result.FileBytes != 0 || !slices.Contains([]string{"destination_unavailable", "repository_open_failed", "repository_installation_conflict", "app_scan_failed", "image_inspection_failed", "database_dump_failed", "database_unavailable", "database_not_detected", "app_definition_incomplete", "app_state_unsafe", "app_definition_limit", "standalone_definition_unsupported", "source_unavailable", "app_definition_excluded", "first_copy_failed", "journal_save_failed", "app_stop_failed", "app_changed_during_backup", "final_copy_failed", "sqlite_copy_failed", "sqlite_copy_unavailable", "agent_interrupted", "snapshot_publish_failed"}, result.FailureCode) {
			return ErrInvalid
		}
	default:
		return ErrInvalid
	}
	return nil
}
