package store

import (
	"bytes"
	"slices"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/backup"
	"github.com/sirrobot01/synod/internal/destination"
	"github.com/sirrobot01/synod/internal/proof"
	"github.com/sirrobot01/synod/internal/repository"
	"github.com/sirrobot01/synod/internal/secret"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestProtectionRequiresOneAppCapture(t *testing.T) {
	s, ca := open(t), authority(t)
	node, _ := join(t, s, ca)
	session := uuid.New()
	if err := s.RecordAgentSession(t.Context(), node, session, &agentv1.Hello{NodeId: node.String(), Architecture: "amd64"}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ApproveNode(t.Context(), node, "test", "approve"); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 10, 8, 12, 0, 0, 0, time.UTC)
	s.now = func() time.Time { return now }
	if err := s.RecordInventory(t.Context(), node, session, &agentv1.AppInventory{ObservedAt: timestamppb.New(now), Apps: []*agentv1.DiscoveredApp{{Key: "compose:test", Name: "test", Kind: "compose", Containers: []*agentv1.DiscoveredContainer{{Id: "container", Name: "test", Image: "alpine"}}}}}); err != nil {
		t.Fatal(err)
	}
	key := uuid.New()
	ring, err := secret.NewKeyring(key, map[uuid.UUID][]byte{key: bytes.Repeat([]byte{1}, 32)})
	if err != nil {
		t.Fatal(err)
	}
	dest := destination.Entry{Destination: destination.Destination{ID: uuid.New(), Name: "disk", NodeID: node, Location: repository.Location{Kind: "local", Path: "/backup"}}, Credentials: repository.Credentials{Password: "test-password"}}
	if _, err := s.AddDestination(t.Context(), ca.InstallationID(), ring, dest, "test", "destination"); err != nil {
		t.Fatal(err)
	}
	plan, err := s.ProtectionPlan(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	settings := backup.DefaultSettings()
	settings.Destinations = []uuid.UUID{dest.Destination.ID}
	if _, err := s.UpdateProtection(t.Context(), ca.InstallationID(), backup.ProtectionUpdate{ID: uuid.New(), ExpectedRevision: plan.Revision, All: true, Enabled: true, Settings: settings}, "test", "protect"); err != nil {
		t.Fatal(err)
	}
	appID := uuid.NewSHA1(node, []byte("compose:test"))
	record := func(scope backup.Consistency, limits ...string) uuid.UUID {
		t.Helper()
		now = now.Add(time.Minute)
		id := uuid.New()
		if _, err := s.CreateBackup(t.Context(), ca.InstallationID(), backup.Submission{ID: id, AppID: appID, DestinationID: dest.Destination.ID, Method: backup.Auto}, "test", "backup"); err != nil {
			t.Fatal(err)
		}
		result := backup.Result{SnapshotID: id, DestinationID: dest.Destination.ID, StartedAt: now, CompletedAt: now, RepositoryOpened: true, Consistency: "application_consistent"}
		if err := s.RecordBackupEvent(t.Context(), node, session, backup.Event{ID: id, Status: "succeeded", Result: result}); code(err) != InvalidRequest {
			t.Fatalf("accepted an unsupported application consistency claim: %v", err)
		}
		result.Consistency, result.Limits = scope, []string{"unknown_limit"}
		if err := s.RecordBackupEvent(t.Context(), node, session, backup.Event{ID: id, Status: "succeeded", Result: result}); code(err) != InvalidRequest {
			t.Fatalf("accepted an unknown limit: %v", err)
		}
		result.Limits = limits
		if err := s.RecordBackupEvent(t.Context(), node, session, backup.Event{ID: id, Status: "succeeded", Result: result}); err != nil {
			t.Fatal(err)
		}
		return id
	}
	drill := func(checks ...proof.Check) {
		t.Helper()
		now = now.Add(time.Minute)
		op, err := s.CreateProof(t.Context(), ca.InstallationID(), proof.Submission{ID: uuid.New(), Kind: proof.Drill, NodeID: node, AppID: appID, DestinationID: dest.Destination.ID}, "test", uuid.NewString())
		if err != nil {
			t.Fatal(err)
		}
		result := proof.Result{DestinationID: dest.Destination.ID, SnapshotID: uuid.New(), StartedAt: now, CompletedAt: now.Add(time.Second), Containers: 1, Checks: checks}
		if err := s.RecordProofEvent(t.Context(), node, session, proof.Event{ID: op.ID, Status: "succeeded", Result: result}); err != nil {
			t.Fatal(err)
		}
	}
	expect := func(name string, app, target backup.ProtectionState, limits ...string) backup.AppPlan {
		t.Helper()
		plan, err := s.ProtectionPlan(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		entry := plan.Apps[0]
		slices.Sort(entry.LimitCodes)
		slices.Sort(limits)
		if entry.State != app || !slices.Equal(entry.LimitCodes, limits) || entry.DataLossWindowSeconds == nil || len(entry.Targets) != 1 || entry.Targets[0].State != target {
			t.Fatalf("%s: state %s, target %s, limits %v", name, entry.State, entry.Targets[0].State, entry.LimitCodes)
		}
		return entry
	}
	for _, scope := range []backup.Consistency{"", backup.ConsistencyUnverified} {
		record(scope)
		expect("unverified "+string(scope), backup.Partial, backup.Partial, backup.AppConsistencyUnverified, backup.RestoreUnproven)
	}
	// A new app cannot gain full protection from a coherent copy before a drill restores it.
	record(backup.ConsistencyFilesystem)
	expect("coherent copy without drill", backup.Partial, backup.Protected, backup.RestoreUnproven)
	passed := proof.Check{Kind: proof.CheckDataFiles, Resource: "/srv/app", Status: proof.CheckPassed}
	drill(passed, proof.Check{Kind: proof.CheckAppFunction, Resource: "worker", Status: proof.CheckUnchecked})
	if entry := expect("incomplete drill", backup.Partial, backup.Protected, backup.RestoreUnproven); entry.RestoreTestedAt != nil {
		t.Fatal("an incomplete drill set the restore test date")
	}
	drill(passed, proof.Check{Kind: proof.CheckHTTP, Resource: "app:80", Status: proof.CheckPassed})
	if entry := expect("complete drill", backup.Protected, backup.Protected); entry.RestoreTestedAt == nil {
		t.Fatal("a complete drill did not set the restore test date")
	}
	for _, limit := range backup.SnapshotLimits {
		record(backup.ConsistencyFilesystem, limit)
		expect(limit, backup.Partial, backup.Partial, limit)
	}
	record(backup.ConsistencyUnverified, backup.DataExcluded, backup.ContainerLayerData)
	expect("several limits", backup.Partial, backup.Partial, backup.AppConsistencyUnverified, backup.DataExcluded, backup.ContainerLayerData)
}
