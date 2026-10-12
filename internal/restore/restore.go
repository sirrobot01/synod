// Package restore defines durable app restore requests and public results.
package restore

import (
	"errors"
	"path/filepath"
	"slices"
	"strings"
	"time"
	"unicode"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/proof"
)

var ErrInvalid = errors.New("invalid restore request or result")

type Submission struct {
	ID            uuid.UUID `json:"id"`
	AppID         uuid.UUID `json:"app_id"`
	NodeID        uuid.UUID `json:"node_id"`
	DestinationID uuid.UUID `json:"destination_id,omitzero"`
	SnapshotID    uuid.UUID `json:"snapshot_id,omitzero"`
	Replace       bool      `json:"replace,omitzero"`
}

// Request selects the latest available snapshot when SnapshotID is absent.
// An absent DestinationID permits every destination available to the target.
type Request struct {
	Submission
	Kind           string    `json:"kind"`
	InstallationID uuid.UUID `json:"installation_id"`
	RequestedAt    time.Time `json:"requested_at"`
}

func (r Request) Validate() error {
	if r.Kind != "restore" || r.ID == uuid.Nil || r.AppID == uuid.Nil || r.NodeID == uuid.Nil || r.InstallationID == uuid.Nil || r.RequestedAt.IsZero() {
		return ErrInvalid
	}
	return nil
}

// Result contains no container definitions, credentials, or database output.
type Result struct {
	SnapshotID       uuid.UUID `json:"snapshot_id,omitzero"`
	DestinationID    uuid.UUID `json:"destination_id,omitzero"`
	SafetySnapshotID uuid.UUID `json:"safety_snapshot_id,omitzero"`
	TargetAppID      uuid.UUID `json:"target_app_id,omitzero"`
	ProjectDirectory string    `json:"project_directory,omitempty"`
	StartedAt        time.Time `json:"started_at,omitzero"`
	CompletedAt      time.Time `json:"completed_at,omitzero"`
	FailureCode      string    `json:"failure_code,omitempty"`
	Resource         string    `json:"resource,omitempty"`
	RecoveryPending  []string  `json:"recovery_pending,omitempty"`
	WarningCodes     []string  `json:"warning_codes,omitempty"`
	// Checks verify the restored data and the started app. A failed check is a warning.
	Checks []proof.Check `json:"checks,omitempty"`
}

type Event struct {
	ID     uuid.UUID `json:"id"`
	Status string    `json:"status"`
	Result Result    `json:"result"`
}

func (e Event) Validate(request Request) error {
	r := e.Result
	if request.Validate() != nil || e.ID != request.ID || (r.SnapshotID != uuid.Nil && request.SnapshotID != uuid.Nil && r.SnapshotID != request.SnapshotID) || (r.DestinationID != uuid.Nil && request.DestinationID != uuid.Nil && r.DestinationID != request.DestinationID) || (r.SafetySnapshotID != uuid.Nil && (!request.Replace || r.DestinationID == uuid.Nil)) || len(r.Resource) > 2048 || strings.ContainsFunc(r.Resource, unicode.IsControl) || len(r.ProjectDirectory) > 4096 || (r.ProjectDirectory != "" && (!filepath.IsAbs(r.ProjectDirectory) || strings.ContainsFunc(r.ProjectDirectory, unicode.IsControl))) || len(r.RecoveryPending) > 256 || len(r.WarningCodes) > 4 || len(r.Checks) > 1024 {
		return ErrInvalid
	}
	for i, warning := range r.WarningCodes {
		if !slices.Contains([]string{"excluded_paths", "destination_unavailable", "cleanup_pending", "restore_check_failed"}, warning) || slices.Contains(r.WarningCodes[:i], warning) {
			return ErrInvalid
		}
	}
	for _, check := range r.Checks {
		if check.Validate() != nil {
			return ErrInvalid
		}
	}
	for _, id := range r.RecoveryPending {
		if len(id) != 64 || strings.ContainsFunc(id, func(c rune) bool { return !strings.ContainsRune("0123456789abcdef", c) }) {
			return ErrInvalid
		}
	}
	switch e.Status {
	case "queued", "running", "waiting":
		if !r.CompletedAt.IsZero() || r.FailureCode != "" || r.TargetAppID != uuid.Nil {
			return ErrInvalid
		}
		if e.Status == "queued" && (!r.StartedAt.IsZero() || r.SnapshotID != uuid.Nil || r.SafetySnapshotID != uuid.Nil || len(r.RecoveryPending) != 0) {
			return ErrInvalid
		}
	case "succeeded", "failed":
		if r.StartedAt.IsZero() || r.CompletedAt.IsZero() || r.CompletedAt.Before(r.StartedAt) || len(r.RecoveryPending) != 0 {
			return ErrInvalid
		}
		if e.Status == "succeeded" {
			if r.SnapshotID == uuid.Nil || r.DestinationID == uuid.Nil || r.TargetAppID == uuid.Nil || r.ProjectDirectory == "" || r.FailureCode != "" {
				return ErrInvalid
			}
		} else if r.TargetAppID != uuid.Nil || !slices.Contains([]string{"destination_unavailable", "repository_open_failed", "snapshot_not_found", "snapshot_invalid", "image_platform_unsupported", "image_archive_missing", "image_archive_invalid", "image_load_failed", "compose_unavailable", "definition_unsupported", "app_conflict", "port_conflict", "host_port_unavailable", "host_requirements_unverified", "device_unavailable", "mount_unavailable", "network_conflict", "source_path_unavailable", "file_restore_failed", "volume_restore_failed", "database_restore_failed", "safety_backup_failed", "app_changed_during_restore", "app_stop_failed", "compose_start_failed", "agent_interrupted"}, r.FailureCode) {
			return ErrInvalid
		}
	default:
		return ErrInvalid
	}
	return nil
}
