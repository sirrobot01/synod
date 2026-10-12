package store

import (
	"bytes"
	"slices"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/backup"
	"github.com/sirrobot01/synod/internal/destination"
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
	for _, scope := range []backup.Consistency{"", backup.ConsistencyUnverified, backup.ConsistencyFilesystem} {
		now = now.Add(time.Minute)
		id := uuid.New()
		if _, err := s.CreateBackup(t.Context(), ca.InstallationID(), backup.Submission{ID: id, AppID: appID, DestinationID: dest.Destination.ID, Method: backup.Auto}, "test", "backup"); err != nil {
			t.Fatal(err)
		}
		result := backup.Result{SnapshotID: id, DestinationID: dest.Destination.ID, StartedAt: now, CompletedAt: now, RepositoryOpened: true, Consistency: "application_consistent"}
		if err := s.RecordBackupEvent(t.Context(), node, session, backup.Event{ID: id, Status: "succeeded", Result: result}); code(err) != InvalidRequest {
			t.Fatalf("accepted an unsupported application consistency claim: %v", err)
		}
		result.Consistency = scope
		if err := s.RecordBackupEvent(t.Context(), node, session, backup.Event{ID: id, Status: "succeeded", Result: result}); err != nil {
			t.Fatal(err)
		}
		plan, err = s.ProtectionPlan(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		app := plan.Apps[0]
		if scope == backup.ConsistencyFilesystem {
			// One filesystem snapshot of every root, with the database files, is one capture time.
			if app.State != backup.Protected || len(app.LimitCodes) != 0 || app.Targets[0].State != backup.Protected || len(app.Targets[0].LimitCodes) != 0 {
				t.Fatalf("a coherent snapshot did not give full protection: %+v", app)
			}
			continue
		}
		if app.State != backup.Partial || !slices.Contains(app.LimitCodes, backup.AppConsistencyUnverified) || app.DataLossWindowSeconds == nil {
			t.Fatalf("scope %q claimed full protection or lost its saved copy: %+v", scope, app)
		}
		if len(app.Targets) != 1 || app.Targets[0].State != backup.Partial || app.Targets[0].LastGood.SnapshotID != id || !slices.Contains(app.Targets[0].LimitCodes, backup.AppConsistencyUnverified) {
			t.Fatalf("target hides its consistency limit: %+v", app.Targets)
		}
	}
}
