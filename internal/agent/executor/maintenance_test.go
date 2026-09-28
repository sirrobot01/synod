package executor

import (
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/agent/journal"
	"github.com/sirrobot01/synod/internal/operation"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/proto"
)

func TestRepeatedNativeCommandsAndScheduledCompaction(t *testing.T) {
	f := newFixture(t, nil)
	ctx, _ := localSession(t, f.e)
	activateLocal(t, f.e, ctx, f.epoch)
	var first *agentv1.Command
	for i := range 40 {
		command := ensureCommand(t, f.epoch)
		if i == 0 {
			first = command
		}
		ack, err := f.e.Handle(ctx, &agentv1.ControllerMessage{DeliveryAttemptId: uuid.NewString(), Message: &agentv1.ControllerMessage_Command{Command: command}})
		if err != nil || ack.GetAdmissionAck().State != "queued" {
			t.Fatalf("command %d: %v %v", i, ack, err)
		}
		f.e.execute(t.Context(), job{app: f.epoch.AppId, command: command.CommandId, session: f.e.session, gate: f.e.session.gates[f.epoch.AppId]})
		record, err := f.e.journal.Get(command.CommandId)
		if err != nil || record.State != journal.Terminal || record.Result.State != "succeeded" {
			t.Fatalf("command %d: %v %v", i, record, err)
		}
		if err := f.e.journal.Acknowledge(&agentv1.ResultAck{Epoch: command.Epoch, CommandId: command.CommandId, Sequence: record.Result.Sequence}); err != nil {
			t.Fatal(err)
		}
	}
	f.e.session = nil
	if err := f.e.schedule(t.Context(), nil); err != nil {
		t.Fatal(err)
	}
	if r, err := f.e.journal.Get(first.CommandId); err != nil || r.State != journal.Terminal {
		t.Fatal("compacted before recovery window", r, err)
	}
	if err := f.e.journal.Close(); err != nil {
		t.Fatal(err)
	}
	f.options.AllowCreate = false
	small := f.options
	small.MaxRetainedBytes = 64 << 20
	if j, err := journal.Open(f.path, small); err == nil {
		j.Close()
		t.Fatal("smaller budget accepted retained payloads")
	}
	f.options.RecoveryWindow = time.Nanosecond
	reopened, err := journal.Open(f.path, f.options)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	restarted, err := New(reopened, f.e.runtime, f.e.options)
	if err != nil {
		t.Fatal(err)
	}
	if err := restarted.schedule(t.Context(), nil); err != nil {
		t.Fatal(err)
	}
	if r, err := reopened.Get(first.CommandId); err != nil || r.State != journal.Compacted || r.Command != nil || r.Result != nil {
		t.Fatal("restart did not compact eligible payload", r, err)
	}
	if !restarted.nextCompaction.After(time.Now()) {
		t.Fatal("maintenance did not set next check")
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
	saved, err := journal.Open(f.path, f.options)
	if err != nil {
		t.Fatal(err)
	}
	defer saved.Close()
	if r, err := saved.Admit(first); err != nil || r.State != journal.Compacted {
		t.Fatal("lost replay tombstone", r, err)
	}
	conflict := proto.Clone(first).(*agentv1.Command)
	conflict.DesiredRevision++
	hash, err := operation.CommandHash(conflict)
	if err != nil {
		t.Fatal(err)
	}
	conflict.InputHash = hash[:]
	if _, err := saved.Admit(conflict); !errors.Is(err, operation.ErrCommandConflict) {
		t.Fatal("conflicting replay accepted", err)
	}
	if _, err := saved.Start(first.CommandId); !errors.Is(err, journal.ErrAlreadyCompleted) {
		t.Fatal("compacted command restarted", err)
	}
	next := proto.Clone(first.Epoch).(*agentv1.Epoch)
	next.ExecutionEpoch++
	if err := saved.Prepare(next); err != nil {
		t.Fatal(err)
	}
	if err := saved.Close(); err != nil {
		t.Fatal(err)
	}
	saved, err = journal.Open(f.path, f.options)
	if err != nil {
		t.Fatal(err)
	}
	defer saved.Close()
	if _, err := saved.Admit(first); !errors.Is(err, operation.ErrStaleOwner) {
		t.Fatal("old epoch accepted after compaction and reopen", err)
	}
	restarted.journal = saved
	restarted.nextCompaction = time.Time{}
	if err := saved.Close(); err != nil {
		t.Fatal(err)
	}
	if err := restarted.schedule(t.Context(), nil); !errors.Is(err, journal.ErrClosed) {
		t.Fatal("maintenance failure was hidden", err)
	}
}
