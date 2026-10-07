package store

import (
	"bytes"
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/destination"
	"github.com/sirrobot01/synod/internal/operation"
	"github.com/sirrobot01/synod/internal/repository"
	"github.com/sirrobot01/synod/internal/restore"
	"github.com/sirrobot01/synod/internal/secret"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestRestoreHistoricalAppHasDurableSessionBoundResults(t *testing.T) {
	path := filepath.Join(t.TempDir(), "controller.db")
	s, err := Open(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { s.Close() }()
	ca := authority(t)
	source, _ := join(t, s, ca)
	target, _ := join(t, s, ca)
	sourceSession, targetSession := uuid.New(), uuid.New()
	for node, session := range map[uuid.UUID]uuid.UUID{source: sourceSession, target: targetSession} {
		if err := s.RecordAgentSession(t.Context(), node, session, &agentv1.Hello{NodeId: node.String(), Architecture: "arm64"}); err != nil {
			t.Fatal(err)
		}
	}
	report := &agentv1.AppInventory{ObservedAt: timestamppb.Now(), Apps: []*agentv1.DiscoveredApp{{Key: "compose:lost", Name: "lost", Kind: "compose", Containers: []*agentv1.DiscoveredContainer{{Id: "container", Name: "lost", Image: "alpine"}}}}}
	if err := s.RecordInventory(t.Context(), source, sourceSession, report); err != nil {
		t.Fatal(err)
	}
	if err := s.RecordInventory(t.Context(), source, sourceSession, &agentv1.AppInventory{ObservedAt: timestamppb.New(time.Now().Add(time.Millisecond))}); err != nil {
		t.Fatal(err)
	}
	keyID := uuid.New()
	ring, err := secret.NewKeyring(keyID, map[uuid.UUID][]byte{keyID: bytes.Repeat([]byte{44}, 32)})
	if err != nil {
		t.Fatal(err)
	}
	destID := uuid.New()
	entry := destination.Entry{Destination: destination.Destination{ID: destID, Name: "restore", Location: repository.Location{Kind: "s3", S3: &repository.S3Location{Endpoint: "https://storage.example", Bucket: "backup"}}}, Credentials: repository.Credentials{Password: "private-restore-password", AccessKeyID: "key", SecretAccessKey: "secret"}}
	if _, err := s.AddDestination(t.Context(), ca.InstallationID(), ring, entry, "admin", "destination"); err != nil {
		t.Fatal(err)
	}
	input := restore.Submission{ID: uuid.New(), AppID: uuid.NewSHA1(source, []byte("compose:lost")), NodeID: target, Replace: true}
	if _, err := s.CreateRestore(t.Context(), ca.InstallationID(), input, "admin", "request"); code(err) != NotFound {
		t.Fatalf("unapproved target accepted: %v", err)
	}
	if _, err := s.ApproveNode(t.Context(), target, "admin", "approve"); err != nil {
		t.Fatal(err)
	}
	op, err := s.CreateRestore(t.Context(), ca.InstallationID(), input, "admin", "request")
	if err != nil {
		t.Fatal(err, errors.Unwrap(err))
	}
	if err := s.TransitionOperation(t.Context(), op.ID, operation.Pending, operation.Cancelled, ""); code(err) != OperationConflict {
		t.Fatalf("restore cancelled without recovery: %v", err)
	}
	if err := s.RemoveDestination(t.Context(), destID, "admin", "remove"); code(err) != OperationConflict {
		t.Fatalf("destination removed while restore can read it: %v", err)
	}
	changed := input
	changed.Replace = false
	if _, err := s.CreateRestore(t.Context(), ca.InstallationID(), changed, "admin", "changed"); code(err) != IdempotencyConflict {
		t.Fatalf("changed replacement accepted: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	again, err := s.CreateRestore(t.Context(), ca.InstallationID(), input, "admin", "retry")
	if err != nil || !bytes.Equal(op.Input, again.Input) {
		t.Fatal("restart changed immutable request")
	}
	jobs, err := s.RestoreRequests(t.Context())
	if err != nil || len(jobs) != 1 || jobs[0].Submission != input {
		t.Fatalf("lost request after restart: %v", err)
	}
	event := restore.Event{ID: input.ID, Status: "succeeded", Result: restore.Result{SnapshotID: uuid.New(), DestinationID: destID, SafetySnapshotID: uuid.New(), TargetAppID: uuid.New(), ProjectDirectory: "/srv/synod/restored", StartedAt: time.Now().UTC(), CompletedAt: time.Now().UTC()}}
	if err := s.RecordRestoreEvent(t.Context(), target, uuid.New(), event); !errors.Is(err, ErrStaleSession) {
		t.Fatalf("stale session accepted: %v", err)
	}
	if err := s.RecordRestoreEvent(t.Context(), source, sourceSession, event); !errors.Is(err, ErrStaleSession) {
		t.Fatalf("unapproved source submitted target result: %v", err)
	}
	for range 2 {
		if err := s.RecordRestoreEvent(t.Context(), target, targetSession, event); err != nil {
			t.Fatal(err, errors.Unwrap(err))
		}
	}
	op, err = s.GetOperation(t.Context(), input.ID)
	if err != nil || op.Status != operation.Succeeded || op.FinishedAt == nil {
		t.Fatal("lost completed restore")
	}
	event.Result.SnapshotID = uuid.New()
	if err := s.RecordRestoreEvent(t.Context(), target, targetSession, event); code(err) != OperationConflict {
		t.Fatalf("terminal result changed: %v", err)
	}
	jobs, err = s.RestoreRequests(t.Context())
	if err != nil || len(jobs) != 0 {
		t.Fatal("terminal restore was dispatched")
	}
	if err := s.RemoveDestination(t.Context(), destID, "admin", "remove"); err != nil {
		t.Fatal(err)
	}
	public, _ := json.Marshal(op)
	if bytes.Contains(public, []byte("private-")) || bytes.Contains(public, []byte("installation_id")) {
		t.Fatal("public result contains private input")
	}
}
