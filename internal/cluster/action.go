package cluster

import (
	"time"

	"github.com/google/uuid"
)

// ActionPlan describes the exact cluster action an administrator can confirm.
type ActionPlan struct {
	PlanToken        uuid.UUID   `json:"plan_token"`
	ClusterID        uuid.UUID   `json:"cluster_id"`
	ClusterName      string      `json:"cluster_name"`
	Action           string      `json:"action"`
	ExpectedRevision int64       `json:"expected_revision"`
	DataDisposition  string      `json:"data_disposition"`
	RetainedVolumes  []uuid.UUID `json:"retained_volumes"`
	// MemberID names the target of replica removal or setup recovery.
	MemberID   uuid.UUID `json:"member_id,omitzero"`
	ResourceID uuid.UUID `json:"resource_id,omitzero"`
	Downtime   string    `json:"downtime"`
	ExpiresAt  time.Time `json:"expires_at"`
}

type ActionConfirmation struct {
	PlanToken       uuid.UUID `json:"plan_token"`
	Confirm         bool      `json:"confirm"`
	DataDisposition string    `json:"data_disposition"`
}
