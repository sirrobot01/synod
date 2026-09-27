package operation

import (
	"time"

	"github.com/google/uuid"
)

// Status is the durable state of one operation.
type Status string

const (
	Pending    Status = "pending"
	Running    Status = "running"
	Waiting    Status = "waiting"
	Cancelling Status = "cancelling"
	Succeeded  Status = "succeeded"
	Failed     Status = "failed"
	Cancelled  Status = "cancelled"
)

// Operation describes durable work. PlanVersion zero forbids command dispatch.
type Operation struct {
	ID                      uuid.UUID  `json:"id"`
	InstallationID          uuid.UUID  `json:"installation_id"`
	ClusterID               uuid.UUID  `json:"cluster_id"`
	ActorID                 string     `json:"actor_id"`
	APIOperation            string     `json:"api_operation"`
	RequestHash             [32]byte   `json:"-"`
	IdempotencyKey          string     `json:"-"`
	DesiredRevision         int64      `json:"desired_revision"`
	Kind                    string     `json:"kind"`
	RetryOf                 *uuid.UUID `json:"retry_of,omitempty"`
	Status                  Status     `json:"status"`
	PlanVersion             int        `json:"plan_version"`
	OwnerID                 *string    `json:"owner_id"`
	ExecutionEpoch          int64      `json:"execution_epoch"`
	ClaimExpiresAt          *time.Time `json:"claim_expires_at"`
	Attempts                int        `json:"attempts"`
	NextAttemptAt           *time.Time `json:"next_attempt_at"`
	CancellationRequestedAt *time.Time `json:"cancellation_requested_at"`
	CreatedAt               time.Time  `json:"created_at"`
	UpdatedAt               time.Time  `json:"updated_at"`
	FinishedAt              *time.Time `json:"finished_at"`
	FailureCode             *string    `json:"failure_code"`
}
