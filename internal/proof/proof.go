// Package proof defines repository checks, isolated restore drills, and repository maintenance.
package proof

import (
	"errors"
	"slices"
	"strings"
	"time"
	"unicode"

	"github.com/google/uuid"
)

type Kind string

const (
	Integrity Kind = "integrity_check"
	Drill     Kind = "restore_drill"
	// Maintenance reclaims expired repository content. Only the repository's maintenance owner runs it.
	Maintenance Kind = "repository_maintenance"
)

func (k Kind) Valid() bool { return k == Integrity || k == Drill || k == Maintenance }

var ErrInvalid = errors.New("invalid proof request or result")

type Submission struct {
	ID            uuid.UUID `json:"id"`
	Kind          Kind      `json:"kind"`
	NodeID        uuid.UUID `json:"node_id"`
	AppID         uuid.UUID `json:"app_id,omitzero"`
	DestinationID uuid.UUID `json:"destination_id,omitzero"`
	SnapshotID    uuid.UUID `json:"snapshot_id,omitzero"`
}

type Request struct {
	Submission
	InstallationID uuid.UUID `json:"installation_id"`
	RequestedAt    time.Time `json:"requested_at"`
	ScheduleID     uuid.UUID `json:"schedule_id,omitzero"`
	ScheduledAt    time.Time `json:"scheduled_at,omitzero"`
}

func (r Request) Validate() error {
	if r.ID == uuid.Nil || r.NodeID == uuid.Nil || r.InstallationID == uuid.Nil || r.RequestedAt.IsZero() {
		return ErrInvalid
	}
	switch r.Kind {
	case Integrity:
		if r.DestinationID == uuid.Nil || r.AppID != uuid.Nil || r.SnapshotID != uuid.Nil {
			return ErrInvalid
		}
	case Maintenance:
		// The controller cannot request maintenance. Only the owner's schedule admits it.
		if r.DestinationID == uuid.Nil || r.AppID != uuid.Nil || r.SnapshotID != uuid.Nil || r.ScheduleID == uuid.Nil {
			return ErrInvalid
		}
	case Drill:
		if r.AppID == uuid.Nil {
			return ErrInvalid
		}
	default:
		return ErrInvalid
	}
	if (r.ScheduleID == uuid.Nil) != r.ScheduledAt.IsZero() || (r.ScheduleID != uuid.Nil && (!r.RequestedAt.Equal(r.ScheduledAt) || r.SnapshotID != uuid.Nil)) {
		return ErrInvalid
	}
	return nil
}

// Schedule keeps its original definition so an offline agent can report old work.
type Schedule struct {
	ID             uuid.UUID `json:"id"`
	Kind           Kind      `json:"kind"`
	InstallationID uuid.UUID `json:"installation_id"`
	NodeID         uuid.UUID `json:"node_id"`
	AppID          uuid.UUID `json:"app_id,omitzero"`
	DestinationID  uuid.UUID `json:"destination_id,omitzero"`
	CreatedAt      time.Time `json:"created_at"`
}

func (s Schedule) Validate() error {
	r := Request{Submission: Submission{ID: s.ID, Kind: s.Kind, NodeID: s.NodeID, AppID: s.AppID, DestinationID: s.DestinationID}, InstallationID: s.InstallationID, RequestedAt: s.CreatedAt, ScheduleID: s.ID, ScheduledAt: s.CreatedAt}
	return r.Validate()
}

// Due returns one current slot. The first slot is due when the schedule starts.
// Later checks run each seven days, and maintenance runs each day. Later drills run at the start of each UTC month.
func (s Schedule) Due(now time.Time) time.Time {
	if s.Validate() != nil || now.Before(s.CreatedAt) {
		return time.Time{}
	}
	switch s.Kind {
	case Integrity:
		return s.CreatedAt.Add(now.Sub(s.CreatedAt) / (7 * 24 * time.Hour) * (7 * 24 * time.Hour))
	case Maintenance:
		return s.CreatedAt.Add(now.Sub(s.CreatedAt) / (24 * time.Hour) * (24 * time.Hour))
	}
	now = now.UTC()
	month := time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, time.UTC)
	if month.Before(s.CreatedAt) {
		return s.CreatedAt
	}
	return month
}

func (s Schedule) Request(slot time.Time) (Request, error) {
	if slot.IsZero() || !s.Due(slot).Equal(slot) {
		return Request{}, ErrInvalid
	}
	r := Request{Submission: Submission{ID: uuid.NewSHA1(s.ID, []byte(slot.UTC().Format(time.RFC3339Nano))), Kind: s.Kind, NodeID: s.NodeID, AppID: s.AppID, DestinationID: s.DestinationID}, InstallationID: s.InstallationID, RequestedAt: slot, ScheduleID: s.ID, ScheduledAt: slot}
	return r, r.Validate()
}

func (s Schedule) Matches(r Request) bool {
	expected, err := s.Request(r.ScheduledAt)
	return err == nil && r == expected
}

// Result contains no app definitions, file contents, or database output.
type Result struct {
	DestinationID uuid.UUID `json:"destination_id,omitzero"`
	SnapshotID    uuid.UUID `json:"snapshot_id,omitzero"`
	StartedAt     time.Time `json:"started_at,omitzero"`
	CompletedAt   time.Time `json:"completed_at,omitzero"`
	Contents      int64     `json:"contents,omitzero"`
	Read          int64     `json:"read,omitzero"`
	BytesRead     int64     `json:"bytes_read,omitzero"`
	Containers    int       `json:"containers,omitzero"`
	// Maintenance is completed, claimed, or handing_over. Space is reclaimed only after completed runs.
	Maintenance string `json:"maintenance,omitempty"`
	FailureCode string `json:"failure_code,omitempty"`
	Resource    string `json:"resource,omitempty"`
}

type Event struct {
	ID      uuid.UUID `json:"id"`
	Status  string    `json:"status"`
	Result  Result    `json:"result"`
	Request *Request  `json:"request,omitzero"`
}

func (e Event) Validate(request Request) error {
	r := e.Result
	if request.Validate() != nil || e.ID != request.ID || (e.Request != nil && *e.Request != request) || (request.ScheduleID != uuid.Nil && e.Request == nil) || r.Contents < 0 || r.Read < 0 || r.Read > r.Contents || r.BytesRead < 0 || r.Containers < 0 || r.Containers > 256 || len(r.Resource) > 2048 || strings.ContainsFunc(r.Resource, unicode.IsControl) {
		return ErrInvalid
	}
	if (r.DestinationID != uuid.Nil && request.DestinationID != uuid.Nil && r.DestinationID != request.DestinationID) || (r.SnapshotID != uuid.Nil && request.SnapshotID != uuid.Nil && r.SnapshotID != request.SnapshotID) || (request.Kind == Integrity && (r.SnapshotID != uuid.Nil || r.Containers != 0)) || (request.Kind == Drill && (r.Contents != 0 || r.Read != 0 || r.BytesRead != 0)) || (request.Kind == Maintenance && (r.SnapshotID != uuid.Nil || r.Containers != 0 || r.Contents != 0 || r.Read != 0 || r.BytesRead != 0)) || (r.Maintenance != "" && (request.Kind != Maintenance || !slices.Contains([]string{"completed", "claimed", "handing_over"}, r.Maintenance))) {
		return ErrInvalid
	}
	switch e.Status {
	case "queued":
		if r != (Result{}) {
			return ErrInvalid
		}
	case "running", "waiting":
		if !r.CompletedAt.IsZero() || r.FailureCode != "" || r.Resource != "" || r.Maintenance != "" {
			return ErrInvalid
		}
	case "succeeded", "failed":
		if r.StartedAt.IsZero() || r.CompletedAt.IsZero() || r.CompletedAt.Before(r.StartedAt) {
			return ErrInvalid
		}
		if e.Status == "succeeded" {
			if r.FailureCode != "" || r.Resource != "" || r.DestinationID == uuid.Nil || (request.Kind == Integrity && r.Read == 0) || (request.Kind == Drill && (r.SnapshotID == uuid.Nil || r.Containers == 0)) || (request.Kind == Maintenance && r.Maintenance == "") {
				return ErrInvalid
			}
		} else if !slices.Contains([]string{"destination_unavailable", "repository_open_failed", "integrity_check_failed", "snapshot_not_found", "snapshot_invalid", "image_platform_unsupported", "image_unavailable", "compose_unavailable", "definition_unsupported", "app_conflict", "source_path_unavailable", "file_restore_failed", "volume_restore_failed", "database_restore_failed", "compose_start_failed", "drill_container_stopped", "drill_isolation_failed", "maintenance_not_owner", "maintenance_failed", "agent_interrupted"}, r.FailureCode) || r.Maintenance != "" {
			return ErrInvalid
		}
	default:
		return ErrInvalid
	}
	return nil
}

type Status struct {
	ID            uuid.UUID `json:"id"`
	NodeID        uuid.UUID `json:"node_id"`
	DestinationID uuid.UUID `json:"destination_id,omitzero"`
	State         string    `json:"state"`
	Result        Result    `json:"result"`
}
