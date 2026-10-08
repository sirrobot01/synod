package backup

import (
	"bytes"
	"encoding/json"
	"path/filepath"
	"slices"
	"time"
	_ "time/tzdata"

	"github.com/google/uuid"
)

// Retention keeps the newest snapshot in each selected calendar period.
// The newest snapshot is always kept, even when every count is zero.
type Retention struct {
	Daily   int `json:"daily"`
	Weekly  int `json:"weekly"`
	Monthly int `json:"monthly"`
}

func (r Retention) Validate() error {
	if r.Daily < 0 || r.Daily > 366 || r.Weekly < 0 || r.Weekly > 104 || r.Monthly < 0 || r.Monthly > 120 {
		return ErrInvalid
	}
	return nil
}

// Settings contains the choices that apply to one app and its future snapshots.
type Settings struct {
	Destinations []uuid.UUID `json:"destinations"`
	Method       Method      `json:"method"`
	At           string      `json:"at"`
	TimeZone     string      `json:"time_zone"`
	Retention    Retention   `json:"retention"`
	// IncludeLarge lists paths that the user includes although Synod skips them by default.
	IncludeLarge []string `json:"include_large,omitempty"`
}

func DefaultSettings() Settings {
	return Settings{Method: Auto, At: "03:00", TimeZone: "UTC", Retention: Retention{Daily: 7, Weekly: 4, Monthly: 6}}
}

func (s Settings) Validate() error {
	if len(s.Destinations) == 0 || len(s.Destinations) > 64 || (s.Method != Auto && s.Method != Stop && s.Method != Live && s.Method != Dump) || s.Retention.Validate() != nil || len(s.At) != 5 || len(s.TimeZone) == 0 || len(s.TimeZone) > 128 || s.TimeZone == "Local" {
		return ErrInvalid
	}
	if _, err := time.Parse("15:04", s.At); err != nil {
		return ErrInvalid
	}
	if _, err := time.LoadLocation(s.TimeZone); err != nil {
		return ErrInvalid
	}
	for i, id := range s.Destinations {
		if id == uuid.Nil || slices.Contains(s.Destinations[:i], id) {
			return ErrInvalid
		}
	}
	return ValidateIncluded(s.IncludeLarge)
}

// ValidateIncluded accepts at most 64 distinct, clean, absolute paths below the root.
func ValidateIncluded(paths []string) error {
	if len(paths) > 64 {
		return ErrInvalid
	}
	for i, p := range paths {
		if len(p) > 4096 || !filepath.IsAbs(p) || filepath.Clean(p) != p || p == "/" || slices.Contains(paths[:i], p) {
			return ErrInvalid
		}
	}
	return nil
}

// Schedule is immutable. A settings change gets a new schedule ID.
type Schedule struct {
	ID             uuid.UUID `json:"id"`
	InstallationID uuid.UUID `json:"installation_id"`
	AppID          uuid.UUID `json:"app_id"`
	NodeID         uuid.UUID `json:"node_id"`
	SourceKey      string    `json:"source_key"`
	Settings       Settings  `json:"settings"`
	CreatedAt      time.Time `json:"created_at"`
}

func (s Schedule) Validate() error {
	if s.ID == uuid.Nil || s.InstallationID == uuid.Nil || s.NodeID == uuid.Nil || s.AppID != uuid.NewSHA1(s.NodeID, []byte(s.SourceKey)) || s.CreatedAt.IsZero() || s.Settings.Validate() != nil {
		return ErrInvalid
	}
	request := Request{ID: s.ID, InstallationID: s.InstallationID, AppID: s.AppID, NodeID: s.NodeID, SourceKey: s.SourceKey, DestinationID: s.Settings.Destinations[0], Method: s.Settings.Method, RequestedAt: s.CreatedAt}
	return request.Validate()
}

// Due returns one current slot. An overdue device does not replay missed days.
// A new plan starts at once, then uses its daily time and IANA time zone.
func (s Schedule) Due(now time.Time) time.Time {
	if now.Before(s.CreatedAt) || s.Validate() != nil {
		return time.Time{}
	}
	zone, _ := time.LoadLocation(s.Settings.TimeZone)
	clock, _ := time.Parse("15:04", s.Settings.At)
	local := now.In(zone)
	due := dailyTime(local, clock)
	if due.After(now) {
		due = dailyTime(local.AddDate(0, 0, -1), clock)
	}
	if due.Before(s.CreatedAt) {
		return s.CreatedAt.UTC()
	}
	return due.UTC()
}

// dailyTime moves a skipped local time forward by the clock gap. A repeated time has one slot.
func dailyTime(day, clock time.Time) time.Time {
	due := time.Date(day.Year(), day.Month(), day.Day(), clock.Hour(), clock.Minute(), 0, 0, day.Location())
	wanted := time.Date(day.Year(), day.Month(), day.Day(), clock.Hour(), clock.Minute(), 0, 0, time.UTC)
	wall := time.Date(due.Year(), due.Month(), due.Day(), due.Hour(), due.Minute(), 0, 0, time.UTC)
	if wall.Before(wanted) {
		due = due.Add(wanted.Sub(wall))
	}
	return due
}

// Request derives the same job bytes on the controller and the agent.
func (s Schedule) Request(destinationID uuid.UUID, slot time.Time) (Request, error) {
	if s.Validate() != nil || !slices.Contains(s.Settings.Destinations, destinationID) || slot.IsZero() || !s.Due(slot).Equal(slot) {
		return Request{}, ErrInvalid
	}
	slot = slot.UTC()
	request := Request{
		ID:             uuid.NewSHA1(s.ID, []byte(destinationID.String()+"/"+slot.Format(time.RFC3339Nano))),
		InstallationID: s.InstallationID, AppID: s.AppID, NodeID: s.NodeID, SourceKey: s.SourceKey,
		DestinationID: destinationID, Method: s.Settings.Method, RequestedAt: slot,
		ScheduleID: s.ID, ScheduledAt: slot, Retention: new(s.Settings.Retention),
		IncludeLarge: s.Settings.IncludeLarge,
	}
	return request, request.Validate()
}

// Matches rejects a job that changes the saved schedule's immutable choices.
func (s Schedule) Matches(request Request) bool {
	expected, err := s.Request(request.DestinationID, request.ScheduledAt)
	if err != nil {
		return false
	}
	a, _ := json.Marshal(expected)
	b, _ := json.Marshal(request)
	return bytes.Equal(a, b)
}
