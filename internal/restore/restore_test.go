package restore

import (
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/backup"
	"github.com/sirrobot01/synod/internal/proof"
)

func TestRestoreResultCannotChangeSelectionOrSkipRecovery(t *testing.T) {
	r := Request{Submission: Submission{ID: uuid.New(), AppID: uuid.New(), NodeID: uuid.New(), DestinationID: uuid.New(), SnapshotID: uuid.New()}, Kind: "restore", InstallationID: uuid.New(), RequestedAt: time.Now().UTC()}
	e := Event{ID: r.ID, Status: "succeeded", Result: Result{SnapshotID: r.SnapshotID, DestinationID: r.DestinationID, TargetAppID: uuid.New(), ProjectDirectory: "/srv/restored", StartedAt: r.RequestedAt, CompletedAt: time.Now().UTC()}}
	if e.Validate(r) != nil {
		t.Fatal("valid result refused")
	}
	checked := e
	checked.Result.WarningCodes = []string{"restore_check_failed"}
	checked.Result.Checks = []proof.Check{{Kind: proof.CheckHTTP, Resource: "app:80", Status: proof.CheckFailed, Detail: "status=500"}}
	if err := checked.Validate(r); err != nil {
		t.Fatalf("a restore with a failed check was refused: %v", err)
	}
	for _, change := range []func(*Event){
		func(e *Event) { e.Result.SnapshotID = uuid.New() },
		func(e *Event) { e.Result.DestinationID = uuid.New() },
		func(e *Event) { e.Result.SafetySnapshotID = uuid.New() },
		func(e *Event) { e.Result.RecoveryPending = []string{strings.Repeat("a", 64)} },
		func(e *Event) { e.Result.Resource = "image\nsecret" },
		func(e *Event) { e.Result.ProjectDirectory = "relative" },
		func(e *Event) { e.Result.FailureCode = "unknown" },
		func(e *Event) { e.Result.Checks = []proof.Check{{Kind: "process_alive", Status: proof.CheckPassed}} },
	} {
		changed := e
		change(&changed)
		if changed.Validate(r) == nil {
			t.Fatal("invalid terminal result accepted")
		}
	}
}

func TestSnapshotSummaryShowsLimitsAndRestorablePlatforms(t *testing.T) {
	saved := backup.Snapshot{ID: uuid.New(), Consistency: backup.ConsistencyFilesystem, Limits: []string{backup.DataExcluded}, Images: []backup.Image{
		{ContainerID: "web", Architectures: []string{"amd64", "arm64"}},
		{ContainerID: "worker", LocalOnly: true, Architectures: []string{"arm64"}},
	}}
	summary := Summarize(saved, uuid.New())
	if summary.Consistency != backup.ConsistencyFilesystem || !slices.Equal(summary.Limits, []string{backup.DataExcluded}) || !slices.Equal(summary.Architectures, []string{"arm64"}) {
		t.Fatalf("summary: %+v", summary)
	}
	saved.Images = nil
	if summary := Summarize(saved, uuid.New()); len(summary.Architectures) != 0 {
		t.Fatalf("a snapshot without images claimed platforms: %v", summary.Architectures)
	}
}
