package store

import (
	"path/filepath"
	"strings"
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
	ops, err := s.ListOperations(t.Context(), 100, uuid.Nil)
	if err != nil || len(ops) != 1 || ops[0].ID != op.ID {
		t.Fatalf("list: %+v %v", ops, err)
	}
	if _, err := s.GetOperation(t.Context(), uuid.New()); code(err) != NotFound {
		t.Fatalf("missing operation: %v", err)
	}
}

// Activity pages by a cursor and hides finished work on request. Status history keeps it.
func TestActivityPagesAndRemovesFinishedOperations(t *testing.T) {
	s, err := Open(t.Context(), filepath.Join(t.TempDir(), "metadata.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	node, _ := join(t, s, authority(t))
	if err := s.RecordAgentSession(t.Context(), node, uuid.New(), &agentv1.Hello{NodeId: node.String(), Architecture: "arm64"}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ApproveNode(t.Context(), node, "admin", "request"); err != nil {
		t.Fatal(err)
	}
	var ids []uuid.UUID
	for i := range 5 {
		op := operation.Operation{ID: uuid.New(), AppID: uuid.New(), NodeID: node, ActorID: "admin", Kind: "backup"}
		if _, err := s.CreateOperation(t.Context(), op); err != nil {
			t.Fatal(err)
		}
		// The last operation stays pending, so it cannot be removed.
		if i < 4 {
			if err := s.TransitionOperation(t.Context(), op.ID, operation.Pending, operation.Cancelled, ""); err != nil {
				t.Fatal(err)
			}
		}
		ids = append(ids, op.ID)
	}
	var paged []uuid.UUID
	before := uuid.Nil
	for {
		page, err := s.ListOperations(t.Context(), 2, before)
		if err != nil {
			t.Fatal(err)
		}
		for _, op := range page {
			paged = append(paged, op.ID)
		}
		if len(page) < 2 {
			break
		}
		before = page[len(page)-1].ID
	}
	all, err := s.ListOperations(t.Context(), 200, uuid.Nil)
	if err != nil || len(all) != 5 || len(paged) != 5 {
		t.Fatalf("pages: %d of %d %v", len(paged), len(all), err)
	}
	for i := range all {
		if all[i].ID != paged[i] {
			t.Fatalf("pages differ from one list at %d", i)
		}
	}
	if _, err := s.ListOperations(t.Context(), 0, uuid.Nil); code(err) != InvalidRequest {
		t.Fatalf("limit 0: %v", err)
	}
	if err := s.RemoveOperation(t.Context(), ids[4]); code(err) != OperationActive {
		t.Fatalf("removed unfinished work: %v", err)
	}
	if err := s.RemoveOperation(t.Context(), uuid.New()); code(err) != NotFound {
		t.Fatalf("removed unknown work: %v", err)
	}
	if err := s.RemoveOperation(t.Context(), ids[0]); err != nil {
		t.Fatal(err)
	}
	if err := s.RemoveOperation(t.Context(), ids[0]); err != nil {
		t.Fatalf("a repeated removal failed: %v", err)
	}
	if listed, _ := s.ListOperations(t.Context(), 200, uuid.Nil); len(listed) != 4 {
		t.Fatalf("removed work is still listed: %d", len(listed))
	}
	if kept, err := s.GetOperation(t.Context(), ids[0]); err != nil || kept.ID != ids[0] {
		t.Fatal("removal deleted the record", err)
	}
	if removed, err := s.ClearOperations(t.Context()); err != nil || removed != 3 {
		t.Fatalf("clear: %d %v", removed, err)
	}
	if listed, _ := s.ListOperations(t.Context(), 200, uuid.Nil); len(listed) != 1 || listed[0].ID != ids[4] {
		t.Fatalf("after clear: %+v", listed)
	}
}

// A renamed device keeps its name when its agent reports its host name again.
func TestRenameNodeKeepsAUniqueName(t *testing.T) {
	s, err := Open(t.Context(), filepath.Join(t.TempDir(), "metadata.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	ca := authority(t)
	first, _ := join(t, s, ca)
	second, _ := join(t, s, ca)
	for _, node := range []uuid.UUID{first, second} {
		if err := s.RecordAgentSession(t.Context(), node, uuid.New(), &agentv1.Hello{NodeId: node.String(), Architecture: "arm64", Hostname: "06696e09d656"}); err != nil {
			t.Fatal(err)
		}
	}
	renamed, err := s.RenameNode(t.Context(), first, "nas")
	if err != nil || renamed.Name != "nas" {
		t.Fatalf("rename: %+v %v", renamed, err)
	}
	if _, err := s.RenameNode(t.Context(), second, "nas"); code(err) != NameTaken {
		t.Fatalf("duplicate name: %v", err)
	}
	for _, bad := range []string{"", "-nas", "two words", strings.Repeat("a", 64)} {
		if _, err := s.RenameNode(t.Context(), second, bad); code(err) != InvalidRequest {
			t.Fatalf("name %q: %v", bad, err)
		}
	}
	if _, err := s.RenameNode(t.Context(), uuid.New(), "other"); code(err) != NotFound {
		t.Fatalf("unknown device: %v", err)
	}
	if err := s.RecordAgentSession(t.Context(), first, uuid.New(), &agentv1.Hello{NodeId: first.String(), Architecture: "arm64", Hostname: "06696e09d656"}); err != nil {
		t.Fatal(err)
	}
	if node, err := s.GetNode(t.Context(), first); err != nil || node.Name != "nas" {
		t.Fatalf("the host name replaced the new name: %+v %v", node, err)
	}
}
