package store

import (
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/operation"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
)

func TestOperationSurvivesRestartAndKeepsItsResult(t *testing.T) {
	path := filepath.Join(t.TempDir(), "metadata.db")
	s, err := Open(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	node, _ := join(t, s, authority(t))
	op := operation.Operation{ID: uuid.New(), AppID: uuid.New(), NodeID: node, ActorID: "admin", Kind: "backup"}
	if _, err := s.CreateOperation(t.Context(), op); code(err) != OperationConflict {
		t.Fatalf("unapproved device accepted: %v", err)
	}
	if err := s.RecordAgentSession(t.Context(), node, uuid.New(), &agentv1.Hello{NodeId: node.String(), Architecture: "arm64"}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ApproveNode(t.Context(), node, "admin", "request"); err != nil {
		t.Fatal(err)
	}
	created, err := s.CreateOperation(t.Context(), op)
	if err != nil || created.Status != operation.Pending {
		t.Fatalf("create: %+v %v", created, err)
	}
	if err := s.TransitionOperation(t.Context(), op.ID, operation.Pending, operation.Running, ""); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	replayed, err := s.CreateOperation(t.Context(), op)
	if err != nil || replayed.Status != operation.Running || replayed.CreatedAt != created.CreatedAt {
		t.Fatalf("replay after restart: %+v %v", replayed, err)
	}
	changed := op
	changed.AppID = uuid.New()
	if _, err := s.CreateOperation(t.Context(), changed); code(err) != IdempotencyConflict {
		t.Fatalf("changed input accepted: %v", err)
	}
	if err := s.TransitionOperation(t.Context(), op.ID, operation.Pending, operation.Cancelled, ""); code(err) != OperationConflict {
		t.Fatalf("stale transition accepted: %v", err)
	}
	if err := s.TransitionOperation(t.Context(), op.ID, operation.Running, operation.Failed, "destination_unavailable"); err != nil {
		t.Fatal(err)
	}
	if err := s.TransitionOperation(t.Context(), op.ID, operation.Failed, operation.Running, ""); code(err) != InvalidRequest {
		t.Fatalf("terminal result restarted: %v", err)
	}
	read, err := s.GetOperation(t.Context(), op.ID)
	if err != nil || read.Status != operation.Failed || read.FinishedAt == nil || read.FailureCode != "destination_unavailable" {
		t.Fatalf("read result: %+v %v", read, err)
	}
	ops, err := s.ListOperations(t.Context())
	if err != nil || len(ops) != 1 || ops[0].ID != op.ID {
		t.Fatalf("list: %+v %v", ops, err)
	}
	if _, err := s.GetOperation(t.Context(), uuid.New()); code(err) != NotFound {
		t.Fatalf("missing operation: %v", err)
	}
}
