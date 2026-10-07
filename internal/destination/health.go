package destination

import (
	"github.com/google/uuid"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"time"
)

func ValidateHealth(report *agentv1.DestinationHealth) error {
	if report == nil || report.ObservedAt.CheckValid() != nil || len(report.Destinations) > 64 {
		return ErrInvalid
	}
	seen := make(map[uuid.UUID]bool, len(report.Destinations))
	for _, space := range report.Destinations {
		if space == nil {
			return ErrInvalid
		}
		id, err := uuid.Parse(space.DestinationId)
		if err != nil || id == uuid.Nil || seen[id] || space.TotalBytes <= 0 || space.AvailableBytes < 0 || space.AvailableBytes > space.TotalBytes {
			return ErrInvalid
		}
		seen[id] = true
	}
	return nil
}

// Space is known only while a recent filesystem report is available.
type Space struct {
	DestinationID  uuid.UUID `json:"destination_id"`
	Known          bool      `json:"known"`
	ObservedAt     time.Time `json:"observed_at,omitzero"`
	TotalBytes     int64     `json:"total_bytes"`
	AvailableBytes int64     `json:"available_bytes"`
}
