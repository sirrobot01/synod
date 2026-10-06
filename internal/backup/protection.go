package backup

import (
	"time"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/inventory"
)

// ProtectionUpdate confirms the plan at ExpectedRevision. All also sets the default for new apps.
type ProtectionUpdate struct {
	ID               uuid.UUID   `json:"id"`
	ExpectedRevision int64       `json:"expected_revision"`
	All              bool        `json:"all"`
	AppIDs           []uuid.UUID `json:"app_ids,omitempty"`
	Enabled          bool        `json:"enabled"`
	Settings         Settings    `json:"settings"`
}

type ProtectionPlan struct {
	Revision    int64     `json:"revision"`
	AutoProtect bool      `json:"auto_protect"`
	Defaults    Settings  `json:"defaults"`
	Apps        []AppPlan `json:"apps"`
}

type AppPlan struct {
	App                   inventory.App  `json:"app"`
	Enabled               bool           `json:"enabled"`
	Settings              Settings       `json:"settings"`
	ScheduleID            uuid.UUID      `json:"schedule_id,omitzero"`
	ChangedAt             time.Time      `json:"changed_at,omitzero"`
	BlockedCode           string         `json:"blocked_code,omitempty"`
	OnlyOnSource          bool           `json:"only_on_source"`
	State                 string         `json:"state"`
	DataLossWindowSeconds *int64         `json:"data_loss_window_seconds,omitzero"`
	Targets               []TargetStatus `json:"targets"`
}

type TargetStatus struct {
	DestinationID         uuid.UUID `json:"destination_id"`
	Selected              bool      `json:"selected"`
	State                 string    `json:"state"`
	DueAt                 time.Time `json:"due_at,omitzero"`
	LastOperationID       uuid.UUID `json:"last_operation_id,omitzero"`
	FailureCode           string    `json:"failure_code,omitempty"`
	LastGood              *Result   `json:"last_good,omitzero"`
	DataLossWindowSeconds *int64    `json:"data_loss_window_seconds,omitzero"`
}

// BlockedCode names a discovery condition that prevents a complete app backup.
func BlockedCode(app inventory.App) string {
	if !app.Present {
		return "app_missing"
	}
	if app.Definition == nil || len(app.Definition.Issues) > 0 {
		return "app_definition_incomplete"
	}
	for _, c := range app.Definition.Containers {
		if c.Database != "" {
			return "database_dump_unavailable"
		}
	}
	return ""
}
