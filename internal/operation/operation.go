// Package operation defines durable work and its state transitions.
package operation

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

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

type Operation struct {
	Input       []byte          `json:"-"`
	Result      json.RawMessage `json:"result,omitempty"`
	ID          uuid.UUID       `json:"id"`
	AppID       uuid.UUID       `json:"app_id"`
	NodeID      uuid.UUID       `json:"node_id"`
	ActorID     string          `json:"actor_id"`
	Kind        string          `json:"kind"`
	Status      Status          `json:"status"`
	FailureCode string          `json:"failure_code"`
	CreatedAt   time.Time       `json:"created_at"`
	UpdatedAt   time.Time       `json:"updated_at"`
	FinishedAt  *time.Time      `json:"finished_at"`
}

func (s Status) Terminal() bool {
	return s == Succeeded || s == Failed || s == Cancelled
}

// CanTransition rejects changes to a terminal result. Waiting work needs inspection before it resumes.
func (s Status) CanTransition(next Status) bool {
	switch s {
	case Pending:
		return next == Running || next == Cancelled
	case Running:
		return next == Waiting || next == Cancelling || next == Succeeded || next == Failed
	case Waiting:
		return next == Running || next == Cancelling || next == Failed
	case Cancelling:
		return next == Cancelled || next == Failed
	default:
		return false
	}
}
