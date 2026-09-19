package journal

import (
	"errors"
	"testing"

	"github.com/sirrobot01/synod/internal/operation"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/proto"
)

func TestCloseAdmissionRequiresExactAuthority(t *testing.T) {
	j, options, command, path := fixture(t)
	wrong := proto.Clone(command.Epoch).(*agentv1.Epoch)
	wrong.ExecutionEpoch++
	if err := j.CloseAdmission(wrong); !errors.Is(err, operation.ErrStaleOwner) {
		t.Fatal(err)
	}
	if err := j.CloseAdmission(command.Epoch); err != nil {
		t.Fatal(err)
	}
	if _, err := j.Admit(command); !errors.Is(err, operation.ErrStaleOwner) {
		t.Fatal("closed admission accepted command", err)
	}
	if err := j.Activate(command.Epoch, true); !errors.Is(err, operation.ErrBarrierPending) {
		t.Fatal("reused old drain evidence", err)
	}
	if err := j.RecordBarrier(&agentv1.BarrierResult{Epoch: command.Epoch, AdmissionsClosed: true, Drained: true}); err != nil {
		t.Fatal(err)
	}
	if err := j.CloseAdmission(command.Epoch); err != nil {
		t.Fatal(err)
	}
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	options.AllowCreate = false
	reopened, err := Open(path, options)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	barrier, err := reopened.Barrier(command.Epoch)
	if err != nil || !barrier.Drained {
		t.Fatal("duplicate close erased fresh evidence", err)
	}
	if err := reopened.Activate(command.Epoch, true); err != nil {
		t.Fatal(err)
	}
}

func TestRecoveryRecordsRetainAcknowledgedNativeWork(t *testing.T) {
	j, options, command, path := fixture(t)
	if _, err := j.Admit(command); err != nil {
		t.Fatal(err)
	}
	if _, err := j.Start(command.CommandId); err != nil {
		t.Fatal(err)
	}
	effect := &agentv1.Effect{CommandId: command.CommandId, ResourceId: command.Input.ResourceId, NativeOperationId: "native-task", Active: true, OutcomeUnknown: true}
	result := &agentv1.TerminalResult{Epoch: command.Epoch, CommandId: command.CommandId, InputHash: command.InputHash, Sequence: 1, State: "failed", Effects: []*agentv1.Effect{effect}}
	if err := j.Complete(result); err != nil {
		t.Fatal(err)
	}
	if err := j.Acknowledge(&agentv1.ResultAck{Epoch: command.Epoch, CommandId: command.CommandId, Sequence: 1}); err != nil {
		t.Fatal(err)
	}
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	options.AllowCreate = false
	reopened, err := Open(path, options)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	records, err := reopened.RecoveryRecords()
	if err != nil || len(records) != 1 || !records[0].Acknowledged {
		t.Fatal("lost acknowledged native work", err)
	}
	records[0].Effects[0].Active, records[0].Effects[0].OutcomeUnknown = false, false
	still, err := reopened.RecoveryRecords()
	if err != nil || !still[0].Effects[0].Active {
		t.Fatal("returned journal-owned evidence", err)
	}
	if err := reopened.Observe(command.CommandId, records[0].Effects); err != nil {
		t.Fatal(err)
	}
	cleared, err := reopened.RecoveryRecords()
	if err != nil || len(cleared) != 0 {
		t.Fatal("scanned immutable result instead of current effects", err)
	}
}
