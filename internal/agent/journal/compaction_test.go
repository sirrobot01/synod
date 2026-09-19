package journal

import (
	"fmt"
	"testing"
	"time"

	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/proto"
)

func TestCompactRetainsUnresolvedBarrierEvidence(t *testing.T) {
	for _, nativeID := range []string{"", "native-task"} {
		for _, unknown := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/unknown=%t", nativeID, unknown), func(t *testing.T) {
				j, options, command, path := fixture(t)
				if _, err := j.Admit(command); err != nil {
					t.Fatal(err)
				}
				if _, err := j.Start(command.CommandId); err != nil {
					t.Fatal(err)
				}
				effect := &agentv1.Effect{CommandId: command.CommandId, ResourceId: command.Input.ResourceId, NativeOperationId: nativeID, Active: !unknown, OutcomeUnknown: unknown}
				if err := j.Observe(command.CommandId, []*agentv1.Effect{effect}); err != nil {
					t.Fatal(err)
				}
				next := proto.Clone(command.Epoch).(*agentv1.Epoch)
				next.ExecutionEpoch++
				if err := j.Prepare(next); err != nil {
					t.Fatal(err)
				}
				if err := j.RecordBarrier(&agentv1.BarrierResult{Epoch: next, AdmissionsClosed: true, Effects: []*agentv1.Effect{effect}}); err != nil {
					t.Fatal(err)
				}
				resolved := proto.Clone(effect).(*agentv1.Effect)
				resolved.Active, resolved.OutcomeUnknown, resolved.State = false, false, "inspected_inactive"
				if err := j.Complete(&agentv1.TerminalResult{Epoch: command.Epoch, CommandId: command.CommandId, InputHash: command.InputHash, Sequence: 1, State: "succeeded", Effects: []*agentv1.Effect{resolved}}); err != nil {
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
				if count, err := reopened.Compact(time.Now().Add(2 * options.RecoveryWindow)); err != nil || count != 0 {
					t.Fatalf("removed barrier evidence: count=%d err=%v", count, err)
				}
				record, err := reopened.Get(command.CommandId)
				if err != nil || record.State != Terminal || !record.Acknowledged || !proto.Equal(record.Command, command) || len(record.Effects) != 1 || !proto.Equal(record.Effects[0], resolved) {
					t.Fatalf("lost retained command evidence: %v", err)
				}
				// A fresh epoch must still resolve each identity from the retained command.
				next.ExecutionEpoch++
				if err := reopened.Prepare(next); err != nil {
					t.Fatal(err)
				}
				if err := reopened.RecordBarrier(&agentv1.BarrierResult{Epoch: next, AdmissionsClosed: true, Drained: true, Effects: record.Effects}); err != nil {
					t.Fatal(err)
				}
				if count, err := reopened.Compact(time.Now().Add(2 * options.RecoveryWindow)); err != nil || count != 1 {
					t.Fatalf("resolved barrier still pins command: count=%d err=%v", count, err)
				}
				if err := reopened.Activate(next, true); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}
