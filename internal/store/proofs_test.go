package store

import (
	"bytes"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/backup"
	"github.com/sirrobot01/synod/internal/destination"
	"github.com/sirrobot01/synod/internal/notification"
	"github.com/sirrobot01/synod/internal/operation"
	"github.com/sirrobot01/synod/internal/proof"
	"github.com/sirrobot01/synod/internal/repository"
	"github.com/sirrobot01/synod/internal/secret"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestProofSchedulesSurviveSettingsChangesAndReportHonestStatus(t *testing.T) {
	path := filepath.Join(t.TempDir(), "controller.db")
	s, err := Open(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	ca := authority(t)
	node, _ := join(t, s, ca)
	session := uuid.New()
	if err := s.RecordAgentSession(t.Context(), node, session, &agentv1.Hello{NodeId: node.String(), Architecture: "arm64"}); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Truncate(time.Millisecond)
	s.now = func() time.Time { return now }
	report := &agentv1.AppInventory{ObservedAt: timestamppb.New(now), Apps: []*agentv1.DiscoveredApp{{Key: "compose:proof", Name: "proof", Kind: "compose", Containers: []*agentv1.DiscoveredContainer{{Id: "app", Name: "app", Image: "alpine"}}}}}
	if err := s.RecordInventory(t.Context(), node, session, report); err != nil {
		t.Fatal(err)
	}
	keyID := uuid.New()
	ring, err := secret.NewKeyring(keyID, map[uuid.UUID][]byte{keyID: bytes.Repeat([]byte{24}, 32)})
	if err != nil {
		t.Fatal(err)
	}
	destID := uuid.New()
	entry := destination.Entry{Destination: destination.Destination{ID: destID, Name: "proof", Location: repository.Location{Kind: "s3", S3: &repository.S3Location{Endpoint: "https://storage.example", Bucket: "proof"}}}, Credentials: repository.Credentials{Password: "private-proof-password", AccessKeyID: "key", SecretAccessKey: "secret"}}
	if _, err := s.AddDestination(t.Context(), ca.InstallationID(), ring, entry, "admin", "destination"); err != nil {
		t.Fatal(err)
	}
	input := proof.Submission{ID: uuid.New(), Kind: proof.Integrity, NodeID: node, DestinationID: destID}
	if _, err := s.CreateProof(t.Context(), ca.InstallationID(), input, "admin", "check"); code(err) != NotFound {
		t.Fatalf("unapproved proof: %v", err)
	}
	if _, err := s.ApproveNode(t.Context(), node, "admin", "approve"); err != nil {
		t.Fatal(err)
	}
	op, err := s.CreateProof(t.Context(), ca.InstallationID(), input, "admin", "check")
	if err != nil {
		t.Fatal(err, errors.Unwrap(err))
	}
	if err := s.TransitionOperation(t.Context(), op.ID, operation.Pending, operation.Cancelled, ""); code(err) != OperationConflict {
		t.Fatalf("cancelled proof without agent cleanup: %v", err)
	}
	if err := s.RemoveDestination(t.Context(), destID, "admin", "remove"); code(err) != OperationConflict {
		t.Fatalf("removed active proof destination: %v", err)
	}
	changed := input
	changed.DestinationID = uuid.New()
	if _, err := s.CreateProof(t.Context(), ca.InstallationID(), changed, "admin", "changed"); code(err) != IdempotencyConflict {
		t.Fatalf("changed immutable proof: %v", err)
	}
	plan, err := s.ProtectionPlan(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	settings := backup.DefaultSettings()
	settings.Destinations = []uuid.UUID{destID}
	if _, err := s.UpdateProtection(t.Context(), ca.InstallationID(), backup.ProtectionUpdate{ID: uuid.New(), ExpectedRevision: plan.Revision, All: true, Enabled: true, Settings: settings}, "admin", "protect"); err != nil {
		t.Fatal(err)
	}
	private, err := s.DestinationPlan(t.Context(), ca.InstallationID(), node, session, ring)
	if err != nil || len(private.Proofs) != 3 {
		t.Fatalf("proof schedules: %v", err)
	}
	var drill proof.Request
	for _, schedule := range private.Proofs {
		if schedule.Kind == proof.Drill {
			drill, err = schedule.Request(schedule.CreatedAt)
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	// Delivery of an old schedule remains valid after opt-out and controller restart.
	if _, err := s.UpdateProtection(t.Context(), ca.InstallationID(), backup.ProtectionUpdate{ID: uuid.New(), ExpectedRevision: private.Revision, All: true, Enabled: false}, "admin", "off"); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	s.now = func() time.Time { return now }
	again, err := s.CreateProof(t.Context(), ca.InstallationID(), input, "admin", "retry")
	if err != nil || !bytes.Equal(op.Input, again.Input) {
		t.Fatal("restart changed the request", err)
	}
	checks, err := s.ProofRequests(t.Context())
	if err != nil || len(checks) != 1 {
		t.Fatal("restart lost the pending check", err)
	}
	event := proof.Event{ID: drill.ID, Status: "succeeded", Request: &drill, Result: proof.Result{DestinationID: destID, SnapshotID: uuid.New(), StartedAt: now, CompletedAt: now.Add(time.Minute), Containers: 2, Checks: []proof.Check{{Kind: proof.CheckDataFiles, Resource: "/srv/app", Status: proof.CheckPassed}}}}
	if err := s.RecordProofEvent(t.Context(), node, uuid.New(), event); !errors.Is(err, ErrStaleSession) {
		t.Fatalf("stale proof session: %v", err)
	}
	forged := event
	forged.Request = new(drill)
	forged.Request.ID, forged.ID = uuid.New(), uuid.New()
	if err := s.RecordProofEvent(t.Context(), node, session, forged); code(err) != InvalidRequest {
		t.Fatalf("forged schedule admitted: %v", err)
	}
	for range 2 {
		if err := s.RecordProofEvent(t.Context(), node, session, event); err != nil {
			t.Fatal(err, errors.Unwrap(err))
		}
	}
	now = now.Add(time.Hour)
	failed := proof.Submission{ID: uuid.New(), Kind: proof.Drill, AppID: drill.AppID, NodeID: node}
	if _, err := s.CreateProof(t.Context(), ca.InstallationID(), failed, "admin", "drill"); err != nil {
		t.Fatal(err)
	}
	if err := s.RecordProofEvent(t.Context(), node, session, proof.Event{ID: failed.ID, Status: "failed", Result: proof.Result{StartedAt: now, CompletedAt: now, FailureCode: "drill_container_stopped", Resource: "app"}}); err != nil {
		t.Fatal(err)
	}
	plan, err = s.ProtectionPlan(t.Context())
	if err != nil || len(plan.Apps) != 1 || plan.Apps[0].RestoreTestedAt == nil || !plan.Apps[0].RestoreTestedAt.Equal(event.Result.CompletedAt) || plan.Apps[0].LastDrill == nil || plan.Apps[0].LastDrill.ID != failed.ID || plan.Apps[0].LastDrill.State != "failed" {
		t.Fatal("later failure hid the last good drill", err)
	}
	check := proof.Event{ID: input.ID, Status: "succeeded", Result: proof.Result{DestinationID: destID, StartedAt: now, CompletedAt: now, Contents: 100, Read: 2, BytesRead: 4000}}
	if err := s.RecordProofEvent(t.Context(), node, session, check); err != nil {
		t.Fatal(err)
	}
	listing, err := s.ListDestinations(t.Context())
	if err != nil || len(listing.Checks) != 1 || listing.Checks[0].Result.Read != 2 || listing.Checks[0].DestinationID != destID {
		t.Fatal("missing integrity status", err)
	}
	event.Result.Containers++
	if err := s.RecordProofEvent(t.Context(), node, session, event); code(err) != OperationConflict {
		t.Fatalf("terminal result changed: %v", err)
	}
}

func TestRepositoryMaintenanceHasOneOwnerAndAlertsOnFailure(t *testing.T) {
	s, ca := open(t), authority(t)
	now := time.Now().UTC().Truncate(time.Millisecond)
	s.now = func() time.Time { return now }
	key := uuid.New()
	ring, err := secret.NewKeyring(key, map[uuid.UUID][]byte{key: bytes.Repeat([]byte{31}, 32)})
	if err != nil {
		t.Fatal(err)
	}
	connect := func(node uuid.UUID) uuid.UUID {
		t.Helper()
		session := uuid.New()
		if err := s.RecordAgentSession(t.Context(), node, session, &agentv1.Hello{NodeId: node.String(), Architecture: "arm64", DestinationAddress: "127.0.0.1:7443"}); err != nil {
			t.Fatal(err)
		}
		return session
	}
	first, _ := join(t, s, ca)
	firstSession := connect(first)
	if _, err := s.ApproveNode(t.Context(), first, "admin", "approve-first"); err != nil {
		t.Fatal(err)
	}
	now = now.Add(time.Minute)
	second, _ := join(t, s, ca)
	secondSession := connect(second)
	if _, err := s.ApproveNode(t.Context(), second, "admin", "approve-second"); err != nil {
		t.Fatal(err)
	}
	remote := destination.Entry{Destination: destination.Destination{ID: uuid.New(), Name: "remote", Location: repository.Location{Kind: "s3", S3: &repository.S3Location{Endpoint: "https://storage.example", Bucket: "synod"}}}, Credentials: repository.Credentials{Password: "private-remote", AccessKeyID: "key", SecretAccessKey: "secret"}}
	device := destination.Entry{Destination: destination.Destination{ID: uuid.New(), NodeID: second, Name: "device", Location: repository.Location{Kind: "device", Path: "/srv/synod", Device: &repository.DeviceLocation{NodeID: second, DestinationID: uuid.Nil}}}, Credentials: repository.Credentials{Password: "private-device"}}
	device.Destination.Location.Device.DestinationID = device.Destination.ID
	for _, entry := range []destination.Entry{remote, device} {
		if _, err := s.AddDestination(t.Context(), ca.InstallationID(), ring, entry, "admin", entry.Destination.Name); err != nil {
			t.Fatal(err, errors.Unwrap(err))
		}
	}
	owned := func(node, session uuid.UUID) map[uuid.UUID]proof.Schedule {
		t.Helper()
		plan, err := s.DestinationPlan(t.Context(), ca.InstallationID(), node, session, ring)
		if err != nil {
			t.Fatal(err, errors.Unwrap(err))
		}
		schedules := map[uuid.UUID]proof.Schedule{}
		for _, schedule := range plan.Proofs {
			if schedule.Kind == proof.Maintenance {
				schedules[schedule.DestinationID] = schedule
			}
		}
		return schedules
	}
	firstOwns, secondOwns := owned(first, firstSession), owned(second, secondSession)
	if len(firstOwns) != 1 || firstOwns[remote.Destination.ID].NodeID != first || len(secondOwns) != 1 || secondOwns[device.Destination.ID].NodeID != second {
		t.Fatalf("maintenance owners: first %v, second %v", firstOwns, secondOwns)
	}
	if _, err := s.CreateProof(t.Context(), ca.InstallationID(), proof.Submission{ID: uuid.New(), Kind: proof.Maintenance, NodeID: first, DestinationID: remote.Destination.ID}, "admin", "manual"); code(err) != InvalidRequest {
		t.Fatalf("manual maintenance request: %v", err)
	}

	config := notification.DefaultSettings()
	config.URL = "generic+http://127.0.0.1:1/secret"
	if _, err := s.ConfigureNotifications(t.Context(), ca.InstallationID(), ring, notification.Update{ID: uuid.New(), ExpectedRevision: 1, Settings: config}, "admin", "configure"); err != nil {
		t.Fatal(err)
	}
	request, err := firstOwns[remote.Destination.ID].Request(firstOwns[remote.Destination.ID].Due(now))
	if err != nil {
		t.Fatal(err)
	}
	failed := proof.Event{ID: request.ID, Status: "failed", Request: &request, Result: proof.Result{DestinationID: remote.Destination.ID, StartedAt: now, CompletedAt: now, FailureCode: "maintenance_failed"}}
	if err := s.RecordProofEvent(t.Context(), first, firstSession, failed); err != nil {
		t.Fatal(err, errors.Unwrap(err))
	}
	if err := s.RefreshNotifications(t.Context(), ca.InstallationID(), ring); err != nil {
		t.Fatal(err)
	}
	status, err := s.NotificationStatus(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	alerts := 0
	for _, alert := range status.Alerts {
		if alert.Kind == "maintenance_failed" && alert.OperationID == request.ID && alert.DestinationID == remote.Destination.ID {
			alerts++
		}
	}
	if alerts != 1 {
		t.Fatalf("maintenance failure alerts: %d", alerts)
	}

	// A short absence keeps the owner. A long absence moves remote maintenance to a device that is present.
	listing, err := s.ListDestinations(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	revision := listing.Revision
	now = now.Add(MaintenanceOwnerMissing - time.Hour)
	connect(second)
	if err := s.ReassignMaintenance(t.Context()); err != nil {
		t.Fatal(err)
	}
	if listing, err = s.ListDestinations(t.Context()); err != nil || listing.Revision != revision {
		t.Fatal("a short absence moved maintenance", err)
	}
	now = now.Add(2 * time.Hour)
	secondSession = connect(second)
	if err := s.ReassignMaintenance(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := s.ReassignMaintenance(t.Context()); err != nil {
		t.Fatal(err)
	}
	listing, err = s.ListDestinations(t.Context())
	if err != nil || listing.Revision != revision+1 {
		t.Fatal("missing owner kept maintenance or reassignment repeated", err)
	}
	for _, m := range listing.Maintenance {
		want := second
		if m.NodeID != want {
			t.Fatalf("destination %s maintenance owner %s, want %s", m.DestinationID, m.NodeID, want)
		}
		if m.DestinationID == remote.Destination.ID && (m.Last == nil || m.Last.ID != request.ID || m.Last.State != "failed" || m.Last.Result.FailureCode != "maintenance_failed") {
			t.Fatalf("remote maintenance status: %+v", m.Last)
		}
	}
	if secondOwns = owned(second, secondSession); len(secondOwns) != 2 {
		t.Fatalf("new owner schedules: %v", secondOwns)
	}
	if firstOwns = owned(first, connect(first)); len(firstOwns) != 0 {
		t.Fatalf("returned device kept maintenance: %v", firstOwns)
	}
}
