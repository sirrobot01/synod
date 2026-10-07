package restore

import (
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestRestoreResultCannotChangeSelectionOrSkipRecovery(t *testing.T) {
	r := Request{Submission: Submission{ID: uuid.New(), AppID: uuid.New(), NodeID: uuid.New(), DestinationID: uuid.New(), SnapshotID: uuid.New()}, Kind: "restore", InstallationID: uuid.New(), RequestedAt: time.Now().UTC()}
	e := Event{ID: r.ID, Status: "succeeded", Result: Result{SnapshotID: r.SnapshotID, DestinationID: r.DestinationID, TargetAppID: uuid.New(), ProjectDirectory: "/srv/restored", StartedAt: r.RequestedAt, CompletedAt: time.Now().UTC()}}
	if e.Validate(r) != nil {
		t.Fatal("valid result refused")
	}
	for _, change := range []func(*Event){
		func(e *Event) { e.Result.SnapshotID = uuid.New() },
		func(e *Event) { e.Result.DestinationID = uuid.New() },
		func(e *Event) { e.Result.SafetySnapshotID = uuid.New() },
		func(e *Event) { e.Result.RecoveryPending = []string{strings.Repeat("a", 64)} },
		func(e *Event) { e.Result.Resource = "image\nsecret" },
		func(e *Event) { e.Result.ProjectDirectory = "relative" },
		func(e *Event) { e.Result.FailureCode = "unknown" },
	} {
		changed := e
		change(&changed)
		if changed.Validate(r) == nil {
			t.Fatal("invalid terminal result accepted")
		}
	}
}
