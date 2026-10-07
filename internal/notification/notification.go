// Package notification defines alert policy and private notification settings.
package notification

import (
	"errors"
	"net/url"
	"slices"
	"strings"
	"time"
	"unicode"

	"github.com/google/uuid"
)

var ErrInvalid = errors.New("invalid notification settings; check the URL and alert thresholds")
var ErrRejected = errors.New("notification rejected; check the notification service and credentials")
var ErrDelivery = errors.New("notification delivery failed; check the notification service and credentials")

type Settings struct {
	URL               string `json:"url,omitempty"`
	OverdueGraceHours int    `json:"overdue_grace_hours"`
	MissingAfterHours int    `json:"missing_after_hours"`
	FullPercent       int    `json:"full_percent"`
}

func (Settings) String() string   { return "[private notification settings]" }
func (Settings) GoString() string { return "[private notification settings]" }

func DefaultSettings() Settings {
	return Settings{OverdueGraceHours: 24, MissingAfterHours: 168, FullPercent: 90}
}

func (s Settings) Validate() error {
	if s.OverdueGraceHours < 1 || s.OverdueGraceHours > 2160 || s.MissingAfterHours < 1 || s.MissingAfterHours > 2160 || s.FullPercent < 1 || s.FullPercent > 100 || len(s.URL) > 8192 || strings.ContainsFunc(s.URL, unicode.IsControl) {
		return ErrInvalid
	}
	if s.URL == "" {
		return nil
	}
	parsed, err := url.Parse(s.URL)
	if err != nil || parsed.Host == "" || !slices.Contains([]string{"ntfy", "gotify", "discord", "slack", "smtp", "generic", "generic+http", "generic+https"}, parsed.Scheme) {
		return ErrInvalid
	}
	return nil
}

type PublicSettings struct {
	Configured        bool   `json:"configured"`
	Service           string `json:"service,omitempty"`
	OverdueGraceHours int    `json:"overdue_grace_hours"`
	MissingAfterHours int    `json:"missing_after_hours"`
	FullPercent       int    `json:"full_percent"`
}

func (s Settings) Public() PublicSettings {
	result := PublicSettings{Configured: s.URL != "", OverdueGraceHours: s.OverdueGraceHours, MissingAfterHours: s.MissingAfterHours, FullPercent: s.FullPercent}
	if parsed, err := url.Parse(s.URL); err == nil {
		result.Service = parsed.Scheme
	}
	return result
}

type Update struct {
	ID               uuid.UUID `json:"id"`
	ExpectedRevision int64     `json:"expected_revision"`
	Settings         Settings  `json:"settings"`
}

func (Update) String() string   { return "[private notification update]" }
func (Update) GoString() string { return "[private notification update]" }

type State string

const (
	Pending   State = "pending"
	Sending   State = "sending"
	Sent      State = "sent"
	Failed    State = "failed"
	Cancelled State = "cancelled"
)

type Alert struct {
	ID            uuid.UUID  `json:"id"`
	Kind          string     `json:"kind"`
	NodeID        uuid.UUID  `json:"node_id,omitzero"`
	AppID         uuid.UUID  `json:"app_id,omitzero"`
	DestinationID uuid.UUID  `json:"destination_id,omitzero"`
	OperationID   uuid.UUID  `json:"operation_id,omitzero"`
	Message       string     `json:"message"`
	CreatedAt     time.Time  `json:"created_at"`
	State         State      `json:"state"`
	Attempts      int        `json:"attempts"`
	NextAttemptAt time.Time  `json:"next_attempt_at,omitzero"`
	DeliveredAt   *time.Time `json:"delivered_at,omitzero"`
	FailureCode   string     `json:"failure_code,omitempty"`
}

type Status struct {
	Revision int64          `json:"revision"`
	Settings PublicSettings `json:"settings"`
	Alerts   []Alert        `json:"alerts"`
}

// TestRequest admits one durable notification without disclosing its destination.
type TestRequest struct {
	ID               uuid.UUID `json:"id"`
	ExpectedRevision int64     `json:"expected_revision"`
}
type Confirmation struct {
	ID       uuid.UUID `json:"id"`
	Revision int64     `json:"revision"`
}
