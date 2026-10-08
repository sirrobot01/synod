package store

import (
	"bytes"
	"encoding/json"
	"errors"
	"path/filepath"
	"reflect"
	"sync"
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

func TestBackupRequestsAndResultsSurviveRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "metadata.db")
	s, err := Open(t.Context(), path)
	if err != nil {
		t.Fatal(err, errors.Unwrap(err))
	}
	t.Cleanup(func() { s.Close() })
	ca := authority(t)
	node, _ := join(t, s, ca)
	session := uuid.New()
	if err := s.RecordAgentSession(t.Context(), node, session, &agentv1.Hello{NodeId: node.String(), Architecture: "arm64"}); err != nil {
		t.Fatal(err, errors.Unwrap(err))
	}
	report := &agentv1.AppInventory{ObservedAt: timestamppb.Now(), Apps: []*agentv1.DiscoveredApp{{Key: "compose:notes", Name: "notes", Kind: "compose", Containers: []*agentv1.DiscoveredContainer{{Id: "container", Name: "notes", Image: "alpine"}}}}}
	if err := s.RecordInventory(t.Context(), node, session, report); err != nil {
		t.Fatal(err, errors.Unwrap(err))
	}
	keyID := uuid.New()
	ring, err := secret.NewKeyring(keyID, map[uuid.UUID][]byte{keyID: bytes.Repeat([]byte{22}, 32)})
	if err != nil {
		t.Fatal(err, errors.Unwrap(err))
	}
	entry := destination.Entry{Destination: destination.Destination{ID: uuid.New(), Name: "Remote", Location: repository.Location{Kind: "s3", S3: &repository.S3Location{Endpoint: "https://storage.example", Bucket: "backups"}}}, Credentials: repository.Credentials{Password: "private-password", AccessKeyID: "private-key", SecretAccessKey: "private-secret"}}
	if _, err := s.AddDestination(t.Context(), ca.InstallationID(), ring, entry, "test", "destination"); err != nil {
		t.Fatal(err, errors.Unwrap(err))
	}
	input := backup.Submission{ID: uuid.New(), AppID: uuid.NewSHA1(node, []byte("compose:notes")), DestinationID: entry.Destination.ID, Method: backup.Stop}
	if _, err := s.CreateBackup(t.Context(), ca.InstallationID(), input, "test", "request"); code(err) != NotFound {
		t.Fatalf("unapproved source accepted: %v", err)
	}
	if _, err := s.ApproveNode(t.Context(), node, "test", "approve"); err != nil {
		t.Fatal(err, errors.Unwrap(err))
	}
	op, err := s.CreateBackup(t.Context(), ca.InstallationID(), input, "test", "request")
	if err != nil {
		t.Fatal(err, errors.Unwrap(err))
	}
	var request backup.Request
	if json.Unmarshal(op.Input, &request) != nil || !request.Initialize || request.Validate() != nil {
		t.Fatal("first request has no initialization assignment")
	}
	again, err := s.CreateBackup(t.Context(), ca.InstallationID(), input, "test", "retry")
	if err != nil || !bytes.Equal(again.Input, op.Input) {
		t.Fatalf("retry changed immutable input: %v", err)
	}
	changed := input
	changed.Method = backup.Live
	if _, err := s.CreateBackup(t.Context(), ca.InstallationID(), changed, "test", "changed"); code(err) != IdempotencyConflict {
		t.Fatalf("changed retry accepted: %v", err)
	}
	second := input
	second.ID = uuid.New()
	if _, err := s.CreateBackup(t.Context(), ca.InstallationID(), second, "test", "second"); code(err) != BackupInitializing {
		t.Fatalf("second initializer accepted: %v", err)
	}
	if err := s.RemoveDestination(t.Context(), input.DestinationID, "test", "remove"); code(err) != OperationConflict {
		t.Fatalf("removed destination during initialization: %v", err)
	}
	if err := s.TransitionOperation(t.Context(), op.ID, operation.Pending, operation.Cancelled, ""); code(err) != OperationConflict {
		t.Fatalf("initializer cancelled without agent acknowledgement: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err, errors.Unwrap(err))
	}
	s, err = Open(t.Context(), path)
	if err != nil {
		t.Fatal(err, errors.Unwrap(err))
	}
	jobs, err := s.BackupRequests(t.Context())
	if err != nil || len(jobs) != 1 || !reflect.DeepEqual(jobs[0], request) {
		t.Fatalf("saved request lost: %v", err)
	}
	progress := backup.Event{ID: input.ID, Status: "running", Result: backup.Result{DestinationID: input.DestinationID, StartedAt: time.Now().UTC(), RepositoryOpened: true}}
	if err := s.RecordBackupEvent(t.Context(), node, uuid.New(), progress); !errors.Is(err, ErrStaleSession) {
		t.Fatalf("old session accepted: %v", err)
	}
	if err := s.RecordBackupEvent(t.Context(), node, session, progress); err != nil {
		t.Fatal(err, errors.Unwrap(err))
	}
	op2, err := s.CreateBackup(t.Context(), ca.InstallationID(), second, "test", "second")
	if err != nil {
		t.Fatal(err, errors.Unwrap(err))
	}
	var job2 backup.Request
	if json.Unmarshal(op2.Input, &job2) != nil || job2.Initialize {
		t.Fatal("existing repository was assigned another initializer")
	}
	done := progress
	done.Status = "succeeded"
	done.Result.SnapshotID = done.ID
	done.Result.CompletedAt = time.Now().UTC()
	done.Result.FileBytes = 42
	for range 2 {
		if err := s.RecordBackupEvent(t.Context(), node, session, done); err != nil {
			t.Fatal(err, errors.Unwrap(err))
		}
	}
	op, err = s.GetOperation(t.Context(), input.ID)
	if err != nil || op.Status != operation.Succeeded || op.FinishedAt == nil || len(op.Result) == 0 {
		t.Fatalf("completion not saved: %v", err)
	}
	var result backup.Result
	if json.Unmarshal(op.Result, &result) != nil || result.SnapshotID != input.ID {
		t.Fatal("wrong saved result")
	}
	conflict := done
	conflict.Result.FileBytes++
	if err := s.RecordBackupEvent(t.Context(), node, session, conflict); code(err) != OperationConflict {
		t.Fatalf("terminal result changed: %v", err)
	}
	// A complete result may arrive after reconnect before a running event.
	failed := backup.Event{ID: second.ID, Status: "failed", Result: backup.Result{DestinationID: input.DestinationID, StartedAt: time.Now().UTC(), CompletedAt: time.Now().UTC(), FailureCode: "source_unavailable", RepositoryOpened: true}}
	if err := s.RecordBackupEvent(t.Context(), node, session, failed); err != nil {
		t.Fatal(err, errors.Unwrap(err))
	}
	if err := s.RemoveDestination(t.Context(), input.DestinationID, "test", "remove"); err != nil {
		t.Fatal(err, errors.Unwrap(err))
	}
	public, _ := json.Marshal(op)
	if bytes.Contains(public, []byte("private-")) || bytes.Contains(public, []byte("source_key")) {
		t.Fatal("public operation exposes its private input")
	}
	jobs, err = s.BackupRequests(t.Context())
	if err != nil || len(jobs) != 0 {
		t.Fatalf("terminal work was dispatched: %v", err)
	}
}

func TestOnlyOneRepositoryInitializerIsAdmittedConcurrently(t *testing.T) {
	s, ca := open(t), authority(t)
	node, _ := join(t, s, ca)
	session := uuid.New()
	if err := s.RecordAgentSession(t.Context(), node, session, &agentv1.Hello{NodeId: node.String(), Architecture: "amd64"}); err != nil {
		t.Fatal(err, errors.Unwrap(err))
	}
	if _, err := s.ApproveNode(t.Context(), node, "test", "approve"); err != nil {
		t.Fatal(err, errors.Unwrap(err))
	}
	if err := s.RecordInventory(t.Context(), node, session, &agentv1.AppInventory{ObservedAt: timestamppb.Now(), Apps: []*agentv1.DiscoveredApp{{Key: "compose:test", Name: "test", Kind: "compose", Containers: []*agentv1.DiscoveredContainer{{Id: "container", Name: "test", Image: "alpine"}}}}}); err != nil {
		t.Fatal(err, errors.Unwrap(err))
	}
	keyID := uuid.New()
	ring, err := secret.NewKeyring(keyID, map[uuid.UUID][]byte{keyID: bytes.Repeat([]byte{31}, 32)})
	if err != nil {
		t.Fatal(err, errors.Unwrap(err))
	}
	var choices []backup.Submission
	for _, name := range []string{"first", "alias"} {
		entry := destination.Entry{Destination: destination.Destination{ID: uuid.New(), Name: name, NodeID: node, Location: repository.Location{Kind: "local", Path: "/srv/backup"}}, Credentials: repository.Credentials{Password: "password"}}
		if _, err := s.AddDestination(t.Context(), ca.InstallationID(), ring, entry, "test", name); err != nil {
			t.Fatal(err, errors.Unwrap(err))
		}
		choices = append(choices, backup.Submission{ID: uuid.New(), AppID: uuid.NewSHA1(node, []byte("compose:test")), DestinationID: entry.Destination.ID, Method: backup.Live})
	}
	results := make(chan error, len(choices))
	var group sync.WaitGroup
	for _, input := range choices {
		group.Go(func() {
			_, err := s.CreateBackup(t.Context(), ca.InstallationID(), input, "test", "concurrent")
			results <- err
		})
	}
	group.Wait()
	close(results)
	accepted, blocked := 0, 0
	for err := range results {
		if err == nil {
			accepted++
		} else if code(err) == BackupInitializing {
			blocked++
		} else {
			t.Fatal(err, errors.Unwrap(err))
		}
	}
	if accepted != 1 || blocked != 1 {
		t.Fatalf("initializers: accepted=%d blocked=%d", accepted, blocked)
	}
}
