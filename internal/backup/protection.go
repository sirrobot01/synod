package backup

import (
	"time"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/inventory"
	"github.com/sirrobot01/synod/internal/proof"
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

type ProtectionConfirmation struct {
	ID       uuid.UUID `json:"id"`
	Revision int64     `json:"revision"`
}

// Selection is saved in the recovery catalog, including explicit opt-outs.
type Selection struct {
	AppID    uuid.UUID `json:"app_id"`
	Enabled  bool      `json:"enabled"`
	Settings Settings  `json:"settings"`
}

type ProtectionState string

const (
	Unprotected ProtectionState = "unprotected"
	Pending     ProtectionState = "pending"
	Protected   ProtectionState = "protected"
	Retained    ProtectionState = "retained"
	Overdue     ProtectionState = "overdue"
	Partial     ProtectionState = "partial"
	Blocked     ProtectionState = "blocked"
	Warning     ProtectionState = "warning"
	Running     ProtectionState = "running"
	Waiting     ProtectionState = "waiting"
	Failed      ProtectionState = "failed"
)

type AppPlan struct {
	LastDrill             *proof.Status   `json:"last_drill,omitzero"`
	RestoreTestedAt       *time.Time      `json:"restore_tested_at,omitzero"`
	App                   inventory.App   `json:"app"`
	Enabled               bool            `json:"enabled"`
	SettingsCurrent       bool            `json:"settings_current"`
	Settings              Settings        `json:"settings"`
	ScheduleID            uuid.UUID       `json:"schedule_id,omitzero"`
	ChangedAt             time.Time       `json:"changed_at,omitzero"`
	BlockedCode           string          `json:"blocked_code,omitempty"`
	OnlyOnSource          bool            `json:"only_on_source"`
	State                 ProtectionState `json:"state"`
	DataLossWindowSeconds *int64          `json:"data_loss_window_seconds,omitzero"`
	Targets               []TargetStatus  `json:"targets"`
}

type TargetStatus struct {
	DestinationID         uuid.UUID       `json:"destination_id"`
	Selected              bool            `json:"selected"`
	State                 ProtectionState `json:"state"`
	DueAt                 time.Time       `json:"due_at,omitzero"`
	LastOperationID       uuid.UUID       `json:"last_operation_id,omitzero"`
	FailureCode           string          `json:"failure_code,omitempty"`
	LastGood              *Result         `json:"last_good,omitzero"`
	DataLossWindowSeconds *int64          `json:"data_loss_window_seconds,omitzero"`
}

// BlockedCode names a discovery condition that prevents a complete app backup.
func BlockedCode(app inventory.App) string {
	if !app.Present {
		return "app_missing"
	}
	if app.Definition == nil || len(app.Definition.Issues) > 0 {
		return "app_definition_incomplete"
	}
	return ""
}
