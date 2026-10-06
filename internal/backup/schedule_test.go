package backup

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestScheduleCatchesUpOnceAndKeepsImmutableRequests(t *testing.T) {
	node, destination := uuid.New(), uuid.New()
	settings := DefaultSettings()
	settings.Destinations = []uuid.UUID{destination}
	created := time.Date(2026, 9, 1, 12, 30, 0, 0, time.UTC)
	schedule := Schedule{ID: uuid.New(), InstallationID: uuid.New(), AppID: uuid.NewSHA1(node, []byte("compose:notes")), NodeID: node, SourceKey: "compose:notes", CreatedAt: created, Settings: settings}
	for _, test := range []struct{ now, due time.Time }{
		{created.Add(-time.Second), time.Time{}},
		{created, created},
		{created.Add(time.Hour), created},
		{time.Date(2026, 10, 6, 2, 59, 0, 0, time.UTC), time.Date(2026, 10, 5, 3, 0, 0, 0, time.UTC)},
		{time.Date(2026, 10, 6, 3, 0, 0, 0, time.UTC), time.Date(2026, 10, 6, 3, 0, 0, 0, time.UTC)},
	} {
		if due := schedule.Due(test.now); !due.Equal(test.due) {
			t.Fatalf("due at %s: %s, want %s", test.now, due, test.due)
		}
	}
	slot := schedule.Due(created.AddDate(0, 1, 0))
	request, err := schedule.Request(destination, slot)
	if err != nil || !schedule.Matches(request) || request.Initialize {
		t.Fatalf("scheduled request: %v", err)
	}
	raw, _ := json.Marshal(schedule)
	var reopened Schedule
	if err := json.Unmarshal(raw, &reopened); err != nil {
		t.Fatal(err)
	}
	retry, err := reopened.Request(destination, slot)
	if err != nil || !schedule.Matches(retry) || retry.ID != request.ID {
		t.Fatalf("schedule replay differs: %v", err)
	}
	if _, err := schedule.Request(uuid.New(), slot); err == nil {
		t.Fatal("unselected destination accepted")
	}
	if _, err := schedule.Request(destination, slot.Add(time.Minute)); err == nil {
		t.Fatal("invented daily slot accepted")
	}
	request.Initialize = true
	if request.Validate() == nil || schedule.Matches(request) {
		t.Fatal("scheduled repository creation accepted")
	}
	request.Initialize = false
	request.Retention.Daily++
	if schedule.Matches(request) {
		t.Fatal("changed retention accepted")
	}
}

func TestDailyScheduleUsesTimeZoneAcrossDST(t *testing.T) {
	node := uuid.New()
	settings := DefaultSettings()
	settings.Destinations = []uuid.UUID{uuid.New()}
	settings.TimeZone = "America/New_York"
	schedule := Schedule{ID: uuid.New(), InstallationID: uuid.New(), AppID: uuid.NewSHA1(node, []byte("compose:notes")), NodeID: node, SourceKey: "compose:notes", CreatedAt: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), Settings: settings}
	for _, test := range []struct{ now, due string }{
		{"2026-03-07T12:00:00Z", "2026-03-07T08:00:00Z"},
		{"2026-03-08T12:00:00Z", "2026-03-08T07:00:00Z"},
		{"2026-11-01T07:59:59Z", "2026-10-31T07:00:00Z"},
		{"2026-11-01T08:00:00Z", "2026-11-01T08:00:00Z"},
	} {
		now, _ := time.Parse(time.RFC3339, test.now)
		if got := schedule.Due(now).Format(time.RFC3339); got != test.due {
			t.Fatalf("due at %s: %s, want %s", test.now, got, test.due)
		}
	}
	schedule.Settings.At = "02:30"
	beforeGapRun := time.Date(2026, 3, 8, 7, 29, 0, 0, time.UTC)
	atGapRun := beforeGapRun.Add(time.Minute)
	if schedule.Due(beforeGapRun).Day() != 7 || !schedule.Due(atGapRun).Equal(atGapRun) {
		t.Fatal("a skipped time did not move forward by the clock gap")
	}
	schedule.Settings.At = "01:30"
	first := time.Date(2026, 11, 1, 5, 30, 0, 0, time.UTC)
	if !schedule.Due(first).Equal(schedule.Due(first.Add(time.Hour))) {
		t.Fatal("a repeated clock time created two daily slots")
	}
	schedule.Settings.TimeZone = "Local"
	if schedule.Validate() == nil {
		t.Fatal("device-dependent time zone accepted")
	}
}
