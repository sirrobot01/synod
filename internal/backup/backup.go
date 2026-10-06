// Package backup defines immutable backup work and saved snapshot metadata.
package backup

import (
	"errors"
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
