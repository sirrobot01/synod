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

func TestDrillProofIsCompleteOnlyWhenEveryCheckPassed(t *testing.T) {
	passed := Check{Kind: CheckDataFiles, Resource: "/srv/app", Status: CheckPassed}
	for _, test := range []struct {
		name     string
		result   Result
		complete bool
	}{
		{name: "no checks", result: Result{}},
		{name: "all passed", result: Result{Checks: []Check{passed, {Kind: CheckHTTP, Resource: "app:80", Status: CheckPassed}}}, complete: true},
		{name: "unchecked function", result: Result{Checks: []Check{passed, {Kind: CheckAppFunction, Resource: "worker", Status: CheckUnchecked}}}},
		{name: "failed check", result: Result{Checks: []Check{passed, {Kind: CheckHTTP, Resource: "app:80", Status: CheckFailed}}}},
		{name: "failure code", result: Result{FailureCode: "drill_container_stopped", Checks: []Check{passed}}},
	} {
		if got := test.result.Complete(); got != test.complete {
			t.Errorf("%s: Complete() = %t", test.name, got)
		}
	}
}

func TestEventsValidateDrillChecks(t *testing.T) {
	now := time.Now().UTC()
	drill := Request{Submission: Submission{ID: uuid.New(), Kind: Drill, AppID: uuid.New(), NodeID: uuid.New()}, InstallationID: uuid.New(), RequestedAt: now}
	integrity := Request{Submission: Submission{ID: uuid.New(), Kind: Integrity, DestinationID: uuid.New(), NodeID: uuid.New()}, InstallationID: uuid.New(), RequestedAt: now}
	result := Result{DestinationID: uuid.New(), SnapshotID: uuid.New(), StartedAt: now, CompletedAt: now, Containers: 1, Checks: []Check{{Kind: CheckPort, Resource: "app:80", Status: CheckPassed, Detail: "connected"}}}
	if err := (Event{ID: drill.ID, Status: "succeeded", Result: result}).Validate(drill); err != nil {
		t.Fatal(err)
	}
	failed := result
	failed.FailureCode, failed.Resource = "drill_check_failed", "http_response app:80"
	if err := (Event{ID: drill.ID, Status: "failed", Result: failed}).Validate(drill); err != nil {
		t.Fatal(err)
	}
	for name, check := range map[string]Check{
		"kind":    {Kind: "process_alive", Status: CheckPassed},
		"status":  {Kind: CheckPort, Status: "skipped"},
		"control": {Kind: CheckPort, Status: CheckPassed, Detail: "line\nbreak"},
	} {
		bad := result
		bad.Checks = []Check{check}
		if (Event{ID: drill.ID, Status: "succeeded", Result: bad}).Validate(drill) == nil {
			t.Errorf("accepted a check with a bad %s", name)
		}
	}
	checked := Result{DestinationID: integrity.DestinationID, StartedAt: now, CompletedAt: now, Contents: 2, Read: 1, Checks: result.Checks}
	if (Event{ID: integrity.ID, Status: "succeeded", Result: checked}).Validate(integrity) == nil {
		t.Error("an integrity check reported drill checks")
	}
	if err := (Event{ID: drill.ID, Status: "queued"}).Validate(drill); err != nil {
		t.Errorf("queued event: %v", err)
	}
}
