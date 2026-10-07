package proof

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestSchedulesCatchUpOnceAcrossRestartAndMonthEnds(t *testing.T) {
	created := time.Date(2026, 1, 31, 23, 0, 0, 0, time.UTC)
	for _, kind := range []Kind{Integrity, Drill} {
		s := Schedule{ID: uuid.New(), Kind: kind, InstallationID: uuid.New(), NodeID: uuid.New(), CreatedAt: created}
		if kind == Integrity {
			s.DestinationID = uuid.New()
		} else {
			s.AppID = uuid.New()
		}
		if !s.Due(created.Add(-time.Second)).IsZero() || !s.Due(created).Equal(created) {
			t.Fatal("schedule started at the wrong time")
		}
		later := created.AddDate(0, 3, 0)
		slot := s.Due(later)
		if kind == Drill && !slot.Equal(time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)) {
			t.Fatalf("monthly catch-up: %v", slot)
		}
		if kind == Integrity && later.Sub(slot) >= 7*24*time.Hour {
			t.Fatal("weekly catch-up is not current")
		}
		request, err := s.Request(slot)
		if err != nil || !s.Matches(request) {
			t.Fatalf("schedule request: %v", err)
		}
		raw, _ := json.Marshal(s)
		var restored Schedule
		if json.Unmarshal(raw, &restored) != nil || !restored.Matches(request) {
			t.Fatal("restart changed the request")
		}
		request.ID = uuid.New()
		if restored.Matches(request) {
			t.Fatal("forged scheduled job accepted")
		}
	}
}

func TestEventsRequireProofAndKeepFailureDetailsPrivate(t *testing.T) {
	r := Request{Submission: Submission{ID: uuid.New(), Kind: Drill, AppID: uuid.New(), NodeID: uuid.New()}, InstallationID: uuid.New(), RequestedAt: time.Now().UTC()}
	e := Event{ID: r.ID, Status: "succeeded", Result: Result{StartedAt: r.RequestedAt, CompletedAt: r.RequestedAt.Add(time.Minute), DestinationID: uuid.New(), SnapshotID: uuid.New(), Containers: 2}}
	if e.Validate(r) != nil {
		t.Fatal("valid drill refused")
	}
	e.Result.Containers = 0
	if e.Validate(r) == nil {
		t.Fatal("empty drill accepted")
	}
	e.Status, e.Result.FailureCode = "failed", "tool printed a private password"
	if e.Validate(r) == nil {
		t.Fatal("raw tool error accepted")
	}
	e.Result.FailureCode = "drill_container_stopped"
	if e.Validate(r) != nil {
		t.Fatal("safe failure refused")
	}
	r.Kind, r.AppID, r.DestinationID = Integrity, uuid.Nil, e.Result.DestinationID
	e.Status, e.Result.FailureCode, e.Result.SnapshotID = "succeeded", "", uuid.Nil
	e.Result.Contents = 100
	if e.Validate(r) == nil {
		t.Fatal("a check without a content read succeeded")
	}
	e.Result.Read = 1
	if e.Validate(r) != nil {
		t.Fatal("valid sample check refused")
	}
}
