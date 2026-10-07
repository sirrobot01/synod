package store

import (
	"bytes"
	json "encoding/json/v2"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/backup"
	"github.com/sirrobot01/synod/internal/destination"
	"github.com/sirrobot01/synod/internal/notification"
	"github.com/sirrobot01/synod/internal/proof"
	"github.com/sirrobot01/synod/internal/repository"
	"github.com/sirrobot01/synod/internal/secret"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestNotificationSettingsQueueRestartAndRecovery(t *testing.T) {
	path := filepath.Join(t.TempDir(), "metadata.db")
	s, err := Open(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	now := time.Now().UTC().Truncate(time.Millisecond)
	s.now = func() time.Time { return now }
	installation, key := uuid.New(), uuid.New()
	ring, err := secret.NewKeyring(key, map[uuid.UUID][]byte{key: bytes.Repeat([]byte{13}, 32)})
	if err != nil {
		t.Fatal(err)
	}
	settings := notification.DefaultSettings()
	settings.URL = "generic+http://127.0.0.1:1/private-notification-token"
	update := notification.Update{ID: uuid.New(), ExpectedRevision: 1, Settings: settings}
	for range 2 {
		if rev, err := s.ConfigureNotifications(t.Context(), installation, ring, update, "admin", "configure"); err != nil || rev != 2 {
			t.Fatal("configure", err)
		}
	}
	update.Settings.FullPercent = 95
	if _, err := s.ConfigureNotifications(t.Context(), installation, ring, update, "admin", "changed"); code(err) != IdempotencyConflict {
		t.Fatal("changed retry", err)
	}
	update.ID = uuid.New()
	if _, err := s.ConfigureNotifications(t.Context(), installation, ring, update, "admin", "stale"); code(err) != NotificationConflict {
		t.Fatal("stale revision", err)
	}
	var public, sealed []byte
	if err := s.db.QueryRow(`SELECT public,credentials FROM notification_settings`).Scan(&public, &sealed); err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(public, []byte("private-notification-token")) || bytes.Contains(sealed, []byte("private-notification-token")) {
		t.Fatal("plaintext URL saved")
	}
	request := notification.TestRequest{ID: uuid.New(), ExpectedRevision: 2}
	for range 2 {
		if err := s.TestNotification(t.Context(), request, "admin", "test"); err != nil {
			t.Fatal(err)
		}
	}
	delivery, err := s.ClaimNotification(t.Context(), installation, ring)
	if err != nil || delivery == nil || delivery.ID != request.ID || delivery.Attempt != 1 || delivery.Settings.URL != settings.URL {
		t.Fatal("claim lost settings", err)
	}
	if another, err := s.ClaimNotification(t.Context(), installation, ring); err != nil || another != nil {
		t.Fatal("duplicate lease", err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	s.now = func() time.Time { return now }
	now = now.Add(31 * time.Second)
	second, err := s.ClaimNotification(t.Context(), installation, ring)
	if err != nil || second == nil || second.ID != request.ID || second.Attempt != 2 {
		t.Fatal("lease did not survive restart", err)
	}
	if err := s.CompleteNotification(t.Context(), request.ID, 1, nil); err != nil {
		t.Fatal(err)
	}
	status, err := s.NotificationStatus(t.Context())
	if err != nil || len(status.Alerts) != 1 || status.Alerts[0].State != "sending" {
		t.Fatal("old attempt overwrote lease", err)
	}
	for attempt := 2; attempt <= 5; attempt++ {
		if attempt > 2 {
			now = now.Add(10 * time.Minute)
			delivery, err = s.ClaimNotification(t.Context(), installation, ring)
			if err != nil || delivery == nil || delivery.Attempt != attempt {
				t.Fatal("retry failed", attempt, err)
			}
		}
		if err := s.CompleteNotification(t.Context(), request.ID, attempt, errors.New("secret-provider-error")); err != nil {
			t.Fatal(err)
		}
	}
	status, err = s.NotificationStatus(t.Context())
	if err != nil || status.Alerts[0].State != "failed" || status.Alerts[0].Attempts != 5 || status.Alerts[0].FailureCode != "delivery_failed" {
		t.Fatal("retry bound missing", err)
	}
	raw, _ := json.Marshal(status)
	if bytes.Contains(raw, []byte("private-notification-token")) || bytes.Contains(raw, []byte("secret-provider-error")) {
		t.Fatal("secret in status")
	}
	tx, err := s.db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	var revision int64
	if err := tx.QueryRow(`SELECT revision FROM destination_revision`).Scan(&revision); err != nil {
		t.Fatal(err)
	}
	catalog, _, err := s.recoveryCatalog(t.Context(), tx, installation, revision, ring)
	tx.Rollback()
	if err != nil || catalog.Notifications == nil || catalog.Notifications.URL != settings.URL {
		t.Fatal("catalog lost settings", err)
	}
	replacement := open(t)
	newKey := uuid.New()
	newRing, err := secret.NewKeyring(newKey, map[uuid.UUID][]byte{newKey: bytes.Repeat([]byte{17}, 32)})
	if err != nil {
		t.Fatal(err)
	}
	if err := replacement.ImportCatalog(t.Context(), catalog, newRing); err != nil {
		t.Fatal("import", err)
	}
	if err := replacement.TestNotification(t.Context(), notification.TestRequest{ID: uuid.New(), ExpectedRevision: 1}, "admin", "recovered"); err != nil {
		t.Fatal(err)
	}
	recovered, err := replacement.ClaimNotification(t.Context(), installation, newRing)
	if err != nil || recovered == nil || recovered.Settings != settings {
		t.Fatal("recovered URL cannot decrypt", err)
	}
	request.ID = uuid.New()
	if err := s.TestNotification(t.Context(), request, "admin", "test2"); err != nil {
		t.Fatal(err)
	}
	update.ExpectedRevision = 2
	update.Settings.URL = ""
	if _, err := s.ConfigureNotifications(t.Context(), installation, ring, update, "admin", "disable"); err != nil {
		t.Fatal(err)
	}
	status, err = s.NotificationStatus(t.Context())
	if err != nil || status.Settings.Configured {
		t.Fatal(err)
	}
	for _, a := range status.Alerts {
		if a.State == "pending" || a.State == "sending" {
			t.Fatal("disabled queue remained live")
		}
	}
	if delivery, err := s.ClaimNotification(t.Context(), installation, ring); err != nil || delivery != nil {
		t.Fatal("disabled send", err)
	}
}

func TestNotificationRulesPartTimeDevicesAndOperationFailures(t *testing.T) {
	s, ca := open(t), authority(t)
	node, _ := join(t, s, ca)
	session := uuid.New()
	now := time.Now().UTC().Truncate(time.Millisecond)
	s.now = func() time.Time { return now }
	if err := s.RecordAgentSession(t.Context(), node, session, &agentv1.Hello{NodeId: node.String(), Architecture: "arm64"}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ApproveNode(t.Context(), node, "admin", "approve"); err != nil {
		t.Fatal(err)
	}
	appID := uuid.NewSHA1(node, []byte("compose:notes"))
	if err := s.RecordInventory(t.Context(), node, session, &agentv1.AppInventory{ObservedAt: timestamppb.New(now), Apps: []*agentv1.DiscoveredApp{{Key: "compose:notes", Name: "notes", Kind: "compose", Containers: []*agentv1.DiscoveredContainer{{Id: "test", Name: "notes", Image: "alpine"}}}}}); err != nil {
		t.Fatal(err)
	}
	key := uuid.New()
	ring, err := secret.NewKeyring(key, map[uuid.UUID][]byte{key: bytes.Repeat([]byte{15}, 32)})
	if err != nil {
		t.Fatal(err)
	}
	dest := destination.Entry{Destination: destination.Destination{ID: uuid.New(), NodeID: node, Name: "backup", Location: repository.Location{Kind: "local", Path: "/backups"}}, Credentials: repository.Credentials{Password: "private-repository"}}
	if _, err := s.AddDestination(t.Context(), ca.InstallationID(), ring, dest, "admin", "destination"); err != nil {
		t.Fatal(err)
	}
	config := notification.DefaultSettings()
	config.URL = "generic+http://127.0.0.1:1/secret"
	if _, err := s.ConfigureNotifications(t.Context(), ca.InstallationID(), ring, notification.Update{ID: uuid.New(), ExpectedRevision: 1, Settings: config}, "admin", "configure"); err != nil {
		t.Fatal(err)
	}
	plan, err := s.ProtectionPlan(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	settings := backup.DefaultSettings()
	settings.Destinations = []uuid.UUID{dest.Destination.ID}
	if _, err := s.UpdateProtection(t.Context(), ca.InstallationID(), backup.ProtectionUpdate{ID: uuid.New(), ExpectedRevision: plan.Revision, All: true, Enabled: true, Settings: settings}, "admin", "protect"); err != nil {
		t.Fatal(err)
	}
	refresh := func() notification.Status {
		t.Helper()
		if err := s.RefreshNotifications(t.Context(), ca.InstallationID(), ring); err != nil {
			t.Fatal("refresh", err, errors.Unwrap(err))
		}
		status, err := s.NotificationStatus(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		return status
	}
	if len(refresh().Alerts) != 0 {
		t.Fatal("new plan alerted")
	}
	now = now.Add(72 * time.Hour)
	if len(refresh().Alerts) != 0 {
		t.Fatal("part-time device alerted before missing threshold")
	}
	session = uuid.New()
	if err := s.RecordAgentSession(t.Context(), node, session, &agentv1.Hello{NodeId: node.String(), Architecture: "arm64"}); err != nil {
		t.Fatal(err)
	}
	if len(refresh().Alerts) != 0 {
		t.Fatal("reconnection did not allow catch-up")
	}
	now = now.Add(6 * time.Minute)
	if err := s.RecordAgentHeartbeat(t.Context(), node, session, &agentv1.Heartbeat{Sequence: 1, ObservedAt: timestamppb.New(now)}); err != nil {
		t.Fatal(err)
	}
	status := refresh()
	if len(status.Alerts) != 1 || status.Alerts[0].Kind != "backup_overdue" {
		t.Fatal("missing overdue alert", status.Alerts)
	}
	if len(refresh().Alerts) != 1 {
		t.Fatal("duplicate condition alert")
	}
	op, err := s.CreateBackup(t.Context(), ca.InstallationID(), backup.Submission{ID: uuid.New(), AppID: appID, DestinationID: dest.Destination.ID, Method: backup.Stop}, "admin", "backup")
	if err != nil {
		t.Fatal(err)
	}
	if status := refresh(); status.Alerts[0].State != "cancelled" {
		t.Fatal("catch-up left overdue send queued")
	}
	event := backup.Event{ID: op.ID, Status: "succeeded", Result: backup.Result{DestinationID: dest.Destination.ID, SnapshotID: op.ID, StartedAt: now, CompletedAt: now.Add(time.Second), RepositoryOpened: true}}
	if err := s.RecordBackupEvent(t.Context(), node, session, event); err != nil {
		t.Fatal(err)
	}
	if len(refresh().Alerts) != 1 {
		t.Fatal("good catch-up backup was called overdue")
	}
	op, err = s.CreateBackup(t.Context(), ca.InstallationID(), backup.Submission{ID: uuid.New(), AppID: appID, DestinationID: dest.Destination.ID, Method: backup.Stop}, "admin", "failed-backup")
	if err != nil {
		t.Fatal(err)
	}
	failed := backup.Event{ID: op.ID, Status: "failed", Result: backup.Result{DestinationID: dest.Destination.ID, StartedAt: now, CompletedAt: now.Add(time.Second), FailureCode: "source_unavailable"}}
	for range 2 {
		if err := s.RecordBackupEvent(t.Context(), node, session, failed); err != nil {
			t.Fatal(err)
		}
	}
	drill, err := s.CreateProof(t.Context(), ca.InstallationID(), proof.Submission{ID: uuid.New(), Kind: proof.Drill, NodeID: node, AppID: appID, DestinationID: dest.Destination.ID}, "admin", "drill")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.RecordProofEvent(t.Context(), node, session, proof.Event{ID: drill.ID, Status: "failed", Result: proof.Result{DestinationID: dest.Destination.ID, StartedAt: now, CompletedAt: now.Add(time.Second), FailureCode: "drill_container_stopped"}}); err != nil {
		t.Fatal(err)
	}
	status = refresh()
	if len(status.Alerts) != 3 {
		t.Fatalf("failed events generated %d alerts", len(status.Alerts))
	}
	if len(refresh().Alerts) != 3 {
		t.Fatal("failure replay duplicated alert")
	}
	now = now.Add(8 * 24 * time.Hour)
	status = refresh()
	if len(status.Alerts) != 4 {
		t.Fatal("missing device did not alert", status.Alerts)
	}
	session = uuid.New()
	if err := s.RecordAgentSession(t.Context(), node, session, &agentv1.Hello{NodeId: node.String(), Architecture: "arm64"}); err != nil {
		t.Fatal(err)
	}
	refresh()
	now = now.Add(8 * 24 * time.Hour)
	if len(refresh().Alerts) != 5 {
		t.Fatal("second missing episode did not alert")
	}
}

func TestNotificationCapacityBindsOwnerSessionAndFreshness(t *testing.T) {
	s, ca := open(t), authority(t)
	node, _ := join(t, s, ca)
	session := uuid.New()
	now := time.Now().UTC().Truncate(time.Millisecond)
	s.now = func() time.Time { return now }
	if err := s.RecordAgentSession(t.Context(), node, session, &agentv1.Hello{NodeId: node.String(), Architecture: "amd64"}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ApproveNode(t.Context(), node, "admin", "approve"); err != nil {
		t.Fatal(err)
	}
	key := uuid.New()
	ring, err := secret.NewKeyring(key, map[uuid.UUID][]byte{key: bytes.Repeat([]byte{11}, 32)})
	if err != nil {
		t.Fatal(err)
	}
	dest := destination.Entry{Destination: destination.Destination{ID: uuid.New(), NodeID: node, Name: "local", Location: repository.Location{Kind: "local", Path: "/backup"}}, Credentials: repository.Credentials{Password: "password"}}
	if _, err := s.AddDestination(t.Context(), ca.InstallationID(), ring, dest, "admin", "destination"); err != nil {
		t.Fatal(err)
	}
	settings := notification.DefaultSettings()
	settings.URL = "generic+http://127.0.0.1:1/secret"
	if _, err := s.ConfigureNotifications(t.Context(), ca.InstallationID(), ring, notification.Update{ID: uuid.New(), ExpectedRevision: 1, Settings: settings}, "admin", "configure"); err != nil {
		t.Fatal(err)
	}
	report := &agentv1.DestinationHealth{ObservedAt: timestamppb.New(now), Destinations: []*agentv1.DestinationSpace{{DestinationId: dest.Destination.ID.String(), TotalBytes: 1000, AvailableBytes: 10}}}
	if err := s.RecordDestinationHealth(t.Context(), node, uuid.New(), report); !errors.Is(err, ErrStaleSession) {
		t.Fatal("old session accepted", err)
	}
	if err := s.RecordDestinationHealth(t.Context(), node, session, report); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := s.RefreshNotifications(t.Context(), ca.InstallationID(), ring); err != nil {
			t.Fatal(err)
		}
	}
	status, err := s.NotificationStatus(t.Context())
	if err != nil || len(status.Alerts) != 1 || status.Alerts[0].Kind != "destination_full" {
		t.Fatal("capacity alert missing", err)
	}
	listing, err := s.ListDestinations(t.Context())
	if err != nil || len(listing.Space) != 1 || !listing.Space[0].Known {
		t.Fatal("capacity status missing", err)
	}
	now = now.Add(6 * time.Minute)
	if err := s.RefreshNotifications(t.Context(), ca.InstallationID(), ring); err != nil {
		t.Fatal(err)
	}
	status, err = s.NotificationStatus(t.Context())
	if err != nil || status.Alerts[0].State != "cancelled" {
		t.Fatal("stale capacity still active", err)
	}
	listing, err = s.ListDestinations(t.Context())
	if err != nil || listing.Space[0].Known {
		t.Fatal("stale measurement claimed current", err)
	}
	if err := s.RecordDestinationHealth(t.Context(), node, session, report); code(err) != InvalidRequest {
		t.Fatal("stale report accepted", err)
	}
	report.ObservedAt = timestamppb.New(now)
	report.Destinations[0].AvailableBytes = 1001
	if err := s.RecordDestinationHealth(t.Context(), node, session, report); code(err) != InvalidRequest {
		t.Fatal("invalid size accepted", err)
	}
}

func TestNotificationQueueBoundsAdmissionAndFinalCrash(t *testing.T) {
	s := open(t)
	installation, key := uuid.New(), uuid.New()
	ring, err := secret.NewKeyring(key, map[uuid.UUID][]byte{key: bytes.Repeat([]byte{11}, 32)})
	if err != nil {
		t.Fatal(err)
	}
	settings := notification.DefaultSettings()
	settings.URL = "generic+http://127.0.0.1:1/secret"
	if _, err := s.ConfigureNotifications(t.Context(), installation, ring, notification.Update{ID: uuid.New(), ExpectedRevision: 1, Settings: settings}, "admin", "configure"); err != nil {
		t.Fatal(err)
	}
	for range 512 {
		if err := s.TestNotification(t.Context(), notification.TestRequest{ID: uuid.New(), ExpectedRevision: 2}, "admin", "test"); err != nil {
			t.Fatal(err)
		}
	}
	refused := notification.TestRequest{ID: uuid.New(), ExpectedRevision: 2}
	if err := s.TestNotification(t.Context(), refused, "admin", "full"); code(err) != NotificationQueueFull {
		t.Fatal("unbounded notification queue", err)
	}
	first, err := s.ClaimNotification(t.Context(), installation, ring)
	if err != nil || first == nil {
		t.Fatal(err)
	}
	if err := s.CompleteNotification(t.Context(), first.ID, first.Attempt, nil); err != nil {
		t.Fatal(err)
	}
	if err := s.TestNotification(t.Context(), refused, "admin", "retry"); err != nil {
		t.Fatal("admission did not recover", err)
	}
	now := time.Now().UTC()
	s.now = func() time.Time { return now }
	if _, err := s.db.Exec(`UPDATE notification_alerts SET state='sending',attempts=5,next_attempt_at=? WHERE id=?`, now.Add(-time.Minute).UnixMilli(), refused.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ClaimNotification(t.Context(), installation, ring); err != nil {
		t.Fatal(err)
	}
	var state, failure string
	if err := s.db.QueryRow(`SELECT state,failure_code FROM notification_alerts WHERE id=?`, refused.ID).Scan(&state, &failure); err != nil {
		t.Fatal(err)
	}
	if state != "failed" || failure != "delivery_unconfirmed" {
		t.Fatal("fifth interrupted send retried without bound")
	}
}

func TestNotificationPermanentRejectionDoesNotRetry(t *testing.T) {
	s := open(t)
	installation, key := uuid.New(), uuid.New()
	ring, err := secret.NewKeyring(key, map[uuid.UUID][]byte{key: bytes.Repeat([]byte{11}, 32)})
	if err != nil {
		t.Fatal(err)
	}
	settings := notification.DefaultSettings()
	settings.URL = "generic+http://127.0.0.1:1/secret"
	if _, err := s.ConfigureNotifications(t.Context(), installation, ring, notification.Update{ID: uuid.New(), ExpectedRevision: 1, Settings: settings}, "admin", "configure"); err != nil {
		t.Fatal(err)
	}
	request := notification.TestRequest{ID: uuid.New(), ExpectedRevision: 2}
	if err := s.TestNotification(t.Context(), request, "admin", "test"); err != nil {
		t.Fatal(err)
	}
	delivery, err := s.ClaimNotification(t.Context(), installation, ring)
	if err != nil || delivery == nil {
		t.Fatal(err)
	}
	if err := s.CompleteNotification(t.Context(), delivery.ID, delivery.Attempt, notification.ErrRejected); err != nil {
		t.Fatal(err)
	}
	status, err := s.NotificationStatus(t.Context())
	if err != nil || len(status.Alerts) != 1 || status.Alerts[0].State != "failed" || status.Alerts[0].Attempts != 1 || status.Alerts[0].FailureCode != "delivery_rejected" {
		t.Fatal("permanent error queued for retry", err)
	}
}
