package app

import (
	"github.com/google/uuid"
	"time"
)

// Admission records stored desired state. It does not report running copies.
type Admission struct {
	OperationID     uuid.UUID `json:"operation_id"`
	AppID           uuid.UUID `json:"app_id"`
	DesiredRevision int64     `json:"desired_revision"`
	AcceptedAt      time.Time `json:"accepted_at"`
	Replayed        bool      `json:"replayed"`
}
