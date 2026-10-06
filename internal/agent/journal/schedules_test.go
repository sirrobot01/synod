package journal

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/backup"
	"github.com/sirrobot01/synod/internal/destination"
	"github.com/sirrobot01/synod/internal/repository"
)

func TestCachedScheduleSurvivesOutageRestartAndCatchesUpOnce(t *testing.T) {
	installation, node, target := uuid.New(), uuid.New(), uuid.New()
	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	ring, err := DestinationKeyring(installation, node, privateKey)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "agent.db")
	j, err := Open(t.Context(), path, installation, node)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { j.Close() })
	now := time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC)
	settings := backup.DefaultSettings()
	settings.Destinations = []uuid.UUID{target}
	schedule := backup.Schedule{ID: uuid.New(), InstallationID: installation, NodeID: node, AppID: uuid.NewSHA1(node, []byte("compose:notes")), SourceKey: "compose:notes", CreatedAt: now, Settings: settings}
	plan := destination.Plan{InstallationID: installation, Revision: 1, Schedules: []backup.Schedule{schedule}, Destinations: []destination.Entry{{Destination: destination.Destination{ID: target, Name: "local", NodeID: node, Location: repository.Location{Kind: "local", Path: "/backup"}}, Credentials: repository.Credentials{Password: "private-schedule-password"}}}}
	if err := j.SaveDestinationPlan(t.Context(), ring, plan); err != nil {
		t.Fatal(err)
	}
	if count, err := j.AdmitScheduled(t.Context(), ring, now); err != nil || count != 0 {
		t.Fatalf("schedule tried to create an uninitialized repository: %d %v", count, err)
	}
	plan.Revision++
	plan.Ready = []uuid.UUID{target}
	if err := j.SaveDestinationPlan(t.Context(), ring, plan); err != nil {
		t.Fatal(err)
	}
	if count, err := j.AdmitScheduled(t.Context(), ring, now); err != nil || count != 1 {
		t.Fatalf("first schedule: %d %v", count, err)
	}
	if count, err := j.AdmitScheduled(t.Context(), ring, now); err != nil || count != 0 {
		t.Fatalf("repeated schedule: %d %v", count, err)
	}
	pending, err := j.Pending(t.Context())
	if err != nil || len(pending) != 1 {
		t.Fatalf("saved daily work: %v", err)
	}
	var first backup.Request
	if json.Unmarshal(pending[0].Input, &first) != nil || !schedule.Matches(first) || first.Initialize {
		t.Fatal("daily work differs from the saved plan")
	}
	if err := j.Start(t.Context(), first.ID); err != nil {
		t.Fatal(err)
	}
	later := now.AddDate(0, 0, 10)
	if count, err := j.AdmitScheduled(t.Context(), ring, later); err != nil || count != 0 {
		t.Fatalf("new work bypassed pending recovery: %d %v", count, err)
	}
	failed, _ := json.Marshal(backup.Result{DestinationID: target, StartedAt: now, CompletedAt: now.Add(time.Minute), FailureCode: "agent_interrupted", RepositoryOpened: true})
	if err := j.Complete(t.Context(), first.ID, failed); err != nil {
		t.Fatal(err)
	}
	// Local connection proof permits the next slot even if the controller did not mark it ready.
	plan.Revision++
	plan.Ready = nil
	if err := j.SaveDestinationPlan(t.Context(), ring, plan); err != nil {
		t.Fatal(err)
	}
	if count, err := j.AdmitScheduled(t.Context(), ring, later); err != nil || count != 1 {
		t.Fatalf("overdue catch-up: %d %v", count, err)
	}
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	j, err = Open(t.Context(), path, installation, node)
	if err != nil {
		t.Fatal(err)
	}
	pending, err = j.Pending(t.Context())
	if err != nil || len(pending) != 1 {
		t.Fatalf("catch-up replayed missed days: %d %v", len(pending), err)
	}
	var current backup.Request
	if json.Unmarshal(pending[0].Input, &current) != nil || !current.ScheduledAt.Equal(schedule.Due(later)) {
		t.Fatal("catch-up is not the latest due slot")
	}
	if count, err := j.AdmitScheduled(t.Context(), ring, later); err != nil || count != 0 {
		t.Fatalf("restart replayed daily work: %d %v", count, err)
	}
	if err := j.Start(t.Context(), current.ID); err != nil {
		t.Fatal(err)
	}
	success, _ := json.Marshal(backup.Result{SnapshotID: current.ID, DestinationID: target, StartedAt: later, CompletedAt: later.Add(time.Minute), RepositoryOpened: true})
	if err := j.Complete(t.Context(), current.ID, success); err != nil {
		t.Fatal(err)
	}
	if err := j.Acknowledge(t.Context(), current.ID); err != nil {
		t.Fatal(err)
	}
	later = later.AddDate(0, 0, 1)
	manual := current
	manual.ID, manual.ScheduleID, manual.ScheduledAt = uuid.New(), uuid.Nil, time.Time{}
	manual.RequestedAt = later.Add(-time.Minute)
	raw, _ := json.Marshal(manual)
	if _, err := j.Admit(t.Context(), manual.ID, raw); err != nil {
		t.Fatal(err)
	}
	if err := j.Start(t.Context(), manual.ID); err != nil {
		t.Fatal(err)
	}
	success, _ = json.Marshal(backup.Result{SnapshotID: manual.ID, DestinationID: target, StartedAt: later.Add(-time.Minute), CompletedAt: later, RepositoryOpened: true})
	if err := j.Complete(t.Context(), manual.ID, success); err != nil {
		t.Fatal(err)
	}
	if err := j.Acknowledge(t.Context(), manual.ID); err != nil {
		t.Fatal(err)
	}
	if count, err := j.AdmitScheduled(t.Context(), ring, later); err != nil || count != 0 {
		t.Fatalf("a newer manual copy did not suppress catch-up: %d %v", count, err)
	}
	plan.Revision++
	plan.Schedules = nil
	if err := j.SaveDestinationPlan(t.Context(), ring, plan); err != nil {
		t.Fatal(err)
	}
	if count, err := j.AdmitScheduled(t.Context(), ring, later.AddDate(0, 0, 1)); err != nil || count != 0 {
		t.Fatalf("disabled schedule admitted work: %d %v", count, err)
	}
	records, err := j.Records(t.Context())
	if err != nil || len(records) != 1 || records[0].ID != first.ID || records[0].State != Terminal {
		t.Fatalf("plan replacement lost an undelivered result: %v", err)
	}
}

func TestOutageResultsDoNotBlockThePendingQueue(t *testing.T) {
	j, err := Open(t.Context(), filepath.Join(t.TempDir(), "agent.db"), uuid.New(), uuid.New())
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	for range 520 {
		id := uuid.New()
		if _, err := j.Admit(t.Context(), id, []byte(`{"work":"fixture"}`)); err != nil {
			t.Fatal(err)
		}
		if err := j.Start(t.Context(), id); err != nil {
			t.Fatal(err)
		}
		if err := j.Complete(t.Context(), id, []byte(`{"result":"saved"}`)); err != nil {
			t.Fatal(err)
		}
	}
	var recovery uuid.UUID
	for range 64 {
		recovery = uuid.New()
		if _, err := j.Admit(t.Context(), recovery, []byte(`{"work":"pending"}`)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := j.Admit(t.Context(), uuid.New(), []byte(`{"work":"too many"}`)); !errors.Is(err, ErrBusy) {
		t.Fatalf("pending queue exceeded its bound: %v", err)
	}
	if err := j.Start(t.Context(), recovery); err != nil {
		t.Fatal(err)
	}
	pending, err := j.Pending(t.Context())
	if err != nil || len(pending) != 64 || pending[0].ID != recovery || pending[0].State != Running {
		t.Fatalf("result backlog hid recovery work: %d %v", len(pending), err)
	}
	batch, err := j.Records(t.Context())
	if err != nil || len(batch) != 512 {
		t.Fatalf("unbounded result batch: %d %v", len(batch), err)
	}
	for _, record := range batch {
		if record.State != Terminal {
			t.Fatal("progress went ahead of the terminal backlog")
		}
		if err := j.Acknowledge(t.Context(), record.ID); err != nil {
			t.Fatal(err)
		}
	}
	batch, err = j.Records(t.Context())
	if err != nil || len(batch) != 72 {
		t.Fatalf("result backlog lost records: %d %v", len(batch), err)
	}
}
