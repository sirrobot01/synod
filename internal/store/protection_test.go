package store

import (
	"bytes"
	"encoding/json"
	"errors"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/backup"
	"github.com/sirrobot01/synod/internal/destination"
	"github.com/sirrobot01/synod/internal/operation"
	"github.com/sirrobot01/synod/internal/repository"
	"github.com/sirrobot01/synod/internal/secret"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestProtectionPersistsAutoProtectOptOutAndOfflineResults(t *testing.T) {
	path := filepath.Join(t.TempDir(), "metadata.db")
	s, err := Open(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	ca := authority(t)
	node, _ := join(t, s, ca)
	session := uuid.New()
	if err := s.RecordAgentSession(t.Context(), node, session, &agentv1.Hello{NodeId: node.String(), Architecture: "arm64", Hostname: "device"}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ApproveNode(t.Context(), node, "admin", "approve"); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC)
	s.now = func() time.Time { return now }
	report := &agentv1.AppInventory{ObservedAt: timestamppb.New(now), Apps: []*agentv1.DiscoveredApp{
		{Key: "compose:notes", Name: "notes", Kind: "compose", Containers: []*agentv1.DiscoveredContainer{{Id: "notes", Name: "notes", Image: "alpine"}}},
		{Key: "compose:database", Name: "database", Kind: "compose", Containers: []*agentv1.DiscoveredContainer{{Id: "database", Name: "database", Image: "postgres", Database: "postgres"}}},
	}}
	if err := s.RecordInventory(t.Context(), node, session, report); err != nil {
		t.Fatal(err)
	}
	keyID := uuid.New()
	ring, err := secret.NewKeyring(keyID, map[uuid.UUID][]byte{keyID: bytes.Repeat([]byte{35}, 32)})
	if err != nil {
		t.Fatal(err)
	}
	entry := destination.Entry{Destination: destination.Destination{ID: uuid.New(), Name: "Local", NodeID: node, Location: repository.Location{Kind: "local", Path: "/backups"}}, Credentials: repository.Credentials{Password: "private-protection-password"}}
	if _, err := s.AddDestination(t.Context(), ca.InstallationID(), ring, entry, "admin", "destination"); err != nil {
		t.Fatal(err)
	}
	plan, err := s.ProtectionPlan(t.Context())
	if err != nil || len(plan.Apps) != 2 || plan.AutoProtect {
		t.Fatalf("initial plan: %v", err)
	}
	settings := backup.DefaultSettings()
	settings.Destinations = []uuid.UUID{entry.Destination.ID}
	update := backup.ProtectionUpdate{ID: uuid.New(), ExpectedRevision: plan.Revision, All: true, Enabled: true, Settings: settings}
	revision, err := s.UpdateProtection(t.Context(), ca.InstallationID(), update, "admin", "protect")
	if err != nil || revision <= plan.Revision {
		t.Fatalf("protect all: %v %v", err, errors.Unwrap(err))
	}
	if again, err := s.UpdateProtection(t.Context(), ca.InstallationID(), update, "admin", "retry"); err != nil || again != revision {
		t.Fatalf("idempotent plan: %v", err)
	}
	changed := update
	changed.Settings.At = "04:00"
	if _, err := s.UpdateProtection(t.Context(), ca.InstallationID(), changed, "admin", "changed"); code(err) != IdempotencyConflict {
		t.Fatalf("changed retry accepted: %v", err)
	}
	changed.ID = uuid.New()
	if _, err := s.UpdateProtection(t.Context(), ca.InstallationID(), changed, "admin", "stale"); code(err) != PlanConflict {
		t.Fatalf("stale preview accepted: %v", err)
	}
	plan, err = s.ProtectionPlan(t.Context())
	if err != nil || !plan.AutoProtect {
		t.Fatalf("saved plan: %v", err)
	}
	appID := uuid.NewSHA1(node, []byte("compose:notes"))
	for _, app := range plan.Apps {
		if !app.Enabled || !app.OnlyOnSource || app.ScheduleID == uuid.Nil {
			t.Fatalf("unsaved app choices: %s", app.App.ID)
		}
		if app.App.Definition.Name == "database" && (app.State != "blocked" || app.BlockedCode != "database_dump_unavailable") {
			t.Fatal("database claims protection before dump support")
		}
	}
	initializers, err := s.PendingInitializations(t.Context())
	if err != nil || len(initializers) != 1 || initializers[0].Schedule.AppID != appID {
		t.Fatalf("initializers: %d %v", len(initializers), err)
	}
	schedule := initializers[0].Schedule
	private, err := s.DestinationPlan(t.Context(), ca.InstallationID(), node, session, ring)
	if err != nil || len(private.Schedules) != 1 || len(private.Ready) != 0 {
		t.Fatalf("scheduled agent plan: %v %v", err, errors.Unwrap(err))
	}
	recovery, err := repository.DecodeCatalog(private.Catalog)
	if err != nil || !recovery.AutoProtect || len(recovery.Protection) != 2 || len(recovery.Destinations) != 1 || recovery.Destinations[0].Credentials.Password != entry.Credentials.Password {
		t.Fatalf("recovery choices missing: %v", err)
	}
	var wrapped []byte
	if err := s.db.QueryRowContext(t.Context(), `SELECT envelope FROM recovery_catalog`).Scan(&wrapped); err != nil {
		t.Fatal(err)
	}
	var envelope secret.Envelope
	if json.Unmarshal(wrapped, &envelope) != nil {
		t.Fatal("recovery cache is not wrapped")
	}
	plain, err := ring.Open(secret.Binding{InstallationID: ca.InstallationID(), OwnerType: "recovery_catalog", OwnerID: ca.InstallationID(), CredentialID: ca.InstallationID(), Version: private.Revision}, envelope)
	if err != nil || !bytes.Equal(plain, private.Catalog) {
		t.Fatalf("recovery cache cannot be authenticated: %v", err)
	}
	clear(plain)
	now = now.Add(time.Second)
	repeated, err := s.DestinationPlan(t.Context(), ca.InstallationID(), node, session, ring)
	if err != nil || !bytes.Equal(private.Catalog, repeated.Catalog) {
		t.Fatalf("same revision changed the recovery catalog: %v", err)
	}
	now = now.Add(-time.Second)
	op, err := s.CreateInitializationBackup(t.Context(), schedule.ID, entry.Destination.ID, schedule.Due(now))
	if err != nil {
		t.Fatal(err, errors.Unwrap(err))
	}
	var request backup.Request
	if json.Unmarshal(op.Input, &request) != nil || !request.Initialize || request.Retention == nil || request.Retention.Daily != 7 {
		t.Fatal("initial backup lacks creation assignment or retention")
	}
	if err := s.RemoveDestination(t.Context(), entry.Destination.ID, "admin", "remove"); code(err) != OperationConflict {
		t.Fatalf("removed a selected destination: %v", err)
	}
	result := backup.Result{SnapshotID: request.ID, DestinationID: request.DestinationID, StartedAt: now, CompletedAt: now.Add(time.Minute), FileBytes: 128, RepositoryOpened: true, WarningCodes: []string{"catalog_write_failed"}}
	if err := s.RecordBackupEvent(t.Context(), node, session, backup.Event{ID: request.ID, Status: "succeeded", Result: result}); err != nil {
		t.Fatal(err, errors.Unwrap(err))
	}
	if pending, err := s.PendingInitializations(t.Context()); err != nil || len(pending) != 0 {
		t.Fatalf("opened repository still needs initialization: %v", err)
	}
	now = now.Add(time.Hour)
	plan, err = s.ProtectionPlan(t.Context())
	if err != nil {
		t.Fatal(err, errors.Unwrap(err))
	}
	index := slices.IndexFunc(plan.Apps, func(app backup.AppPlan) bool { return app.App.ID == appID })
	app := plan.Apps[index]
	if app.State != backup.Warning || app.DataLossWindowSeconds == nil || *app.DataLossWindowSeconds != 3600 || len(app.Targets) != 1 || app.Targets[0].LastGood.SnapshotID != request.ID {
		t.Fatal("status does not show the saved snapshot and data-loss window")
	}
	now = now.AddDate(0, 0, 1)
	overdue, err := s.ProtectionPlan(t.Context())
	if err != nil || overdue.Apps[index].State != backup.Overdue {
		t.Fatal("a maintenance warning hid an overdue backup")
	}
	now = now.AddDate(0, 0, -1)
	update = backup.ProtectionUpdate{ID: uuid.New(), ExpectedRevision: plan.Revision, AppIDs: []uuid.UUID{appID}, Enabled: false}
	if _, err := s.UpdateProtection(t.Context(), ca.InstallationID(), update, "admin", "off"); err != nil {
		t.Fatal(err)
	}
	report.ObservedAt = timestamppb.New(now)
	report.Apps = append(report.Apps, &agentv1.DiscoveredApp{Key: "compose:new", Name: "new", Kind: "compose", Containers: []*agentv1.DiscoveredContainer{{Id: "new", Name: "new", Image: "alpine"}}})
	if err := s.RecordInventory(t.Context(), node, session, report); err != nil {
		t.Fatal(err)
	}
	plan, err = s.ProtectionPlan(t.Context())
	if err != nil || len(plan.Apps) != 3 {
		t.Fatalf("new app plan: %v", err)
	}
	for _, app := range plan.Apps {
		if app.App.ID == appID && app.Enabled {
			t.Fatal("auto-protect undid an explicit opt-out")
		}
		if app.App.Definition.Name == "new" && (!app.Enabled || app.Settings.At != settings.At) {
			t.Fatal("new app did not inherit confirmed defaults")
		}
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	now = time.Date(2026, 10, 7, 4, 0, 0, 0, time.UTC)
	s.now = func() time.Time { return now }
	job, err := schedule.Request(entry.Destination.ID, schedule.Due(now))
	if err != nil {
		t.Fatal(err)
	}
	event := backup.Event{ID: job.ID, Request: &job, Status: "succeeded", Result: backup.Result{SnapshotID: job.ID, DestinationID: job.DestinationID, StartedAt: job.ScheduledAt, CompletedAt: job.ScheduledAt.Add(time.Minute), FileBytes: 256, RepositoryOpened: true}}
	if err := s.RecordBackupEvent(t.Context(), node, uuid.New(), event); !errors.Is(err, ErrStaleSession) {
		t.Fatalf("old session created a scheduled operation: %v", err)
	}
	for range 2 {
		if err := s.RecordBackupEvent(t.Context(), node, session, event); err != nil {
			t.Fatalf("offline result after opt-out and restart: %v %v", err, errors.Unwrap(err))
		}
	}
	saved, err := s.GetOperation(t.Context(), job.ID)
	if err != nil || saved.Status != operation.Succeeded || saved.ActorID != "schedule:"+schedule.ID.String() {
		t.Fatalf("scheduled operation missing: %v", err)
	}
	plan, err = s.ProtectionPlan(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	index = slices.IndexFunc(plan.Apps, func(app backup.AppPlan) bool { return app.App.ID == appID })
	if app := plan.Apps[index]; app.State != "unprotected" || app.DataLossWindowSeconds == nil || *app.DataLossWindowSeconds != 3600 || app.Targets[0].LastGood.SnapshotID != job.ID || app.Targets[0].Selected {
		t.Fatal("opt-out erased the last successful copy from status")
	}
	changedJob := job
	changedJob.Method = backup.Live
	event.Request = &changedJob
	if err := s.RecordBackupEvent(t.Context(), node, session, event); err == nil {
		t.Fatal("changed scheduled request accepted")
	}
	future, err := schedule.Request(entry.Destination.ID, schedule.Due(now.AddDate(0, 0, 2)))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.RecordBackupEvent(t.Context(), node, session, backup.Event{ID: future.ID, Request: &future, Status: "queued", Result: backup.Result{DestinationID: future.DestinationID}}); err == nil {
		t.Fatal("future daily job accepted")
	}
	now = now.AddDate(0, 0, 1)
	delayed, err := schedule.Request(entry.Destination.ID, schedule.CreatedAt)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.RecordBackupEvent(t.Context(), node, session, backup.Event{ID: delayed.ID, Request: &delayed, Status: "succeeded", Result: backup.Result{SnapshotID: delayed.ID, DestinationID: delayed.DestinationID, StartedAt: now.Add(-time.Minute), CompletedAt: now, RepositoryOpened: true}}); err != nil {
		t.Fatal(err)
	}
	plan, err = s.ProtectionPlan(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	index = slices.IndexFunc(plan.Apps, func(app backup.AppPlan) bool { return app.App.ID == appID })
	if app := plan.Apps[index]; app.DataLossWindowSeconds == nil || *app.DataLossWindowSeconds != 60 || app.Targets[0].LastGood.SnapshotID != delayed.ID {
		t.Fatal("status used job admission order instead of the actual backup time")
	}
}
