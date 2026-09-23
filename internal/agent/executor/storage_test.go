package executor

import (
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/agent/journal"
	"github.com/sirrobot01/synod/internal/operation"
	"github.com/sirrobot01/synod/internal/runtime"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
)

func TestStorageLostCompletesJournalAndReleasesMutationSlot(t *testing.T) {
	for _, recovering := range []bool{false, true} {
		t.Run(map[bool]string{false: "execution refusal", true: "recovered refusal"}[recovering], func(t *testing.T) {
			e := guard(t, pool(t, 8000000), 0)
			identity := e.journal.Identity()
			epoch := &agentv1.Epoch{InstallationId: identity.InstallationID, NodeId: identity.NodeID, RecoveryGeneration: identity.RecoveryGeneration, ClusterId: uuid.NewString(), ExecutionEpoch: 1}
			ctx, _ := localSession(t, e)
			activateLocal(t, e, ctx, epoch)
			command := volumeCommand(t, epoch)
			command.Input.Payload = &agentv1.CommandInput_StartContainer{StartContainer: &agentv1.StartContainer{}}
			hash, err := operation.CommandHash(command)
			if err != nil {
				t.Fatal(err)
			}
			command.InputHash = hash[:]
			if _, err := e.journal.Admit(command); err != nil {
				t.Fatal(err)
			}
			record, err := e.journal.Start(command.CommandId)
			if err != nil {
				t.Fatal(err)
			}
			result := runtime.Result{}
			if recovering {
				prior := &agentv1.Effect{CommandId: command.CommandId, ResourceId: command.Input.ResourceId, NativeOperationId: "docker-command:" + command.CommandId, Active: true, OutcomeUnknown: true, State: "unknown"}
				if err := e.journal.Observe(command.CommandId, []*agentv1.Effect{prior}); err != nil {
					t.Fatal(err)
				}
				result.Effects = []*agentv1.Effect{{CommandId: command.CommandId, ResourceId: command.Input.ResourceId, NativeOperationId: prior.NativeOperationId, State: "storage_lost"}}
			}
			err = &runtime.Error{Kind: runtime.ErrStorageLost}
			if !errors.Is(err, runtime.ErrConfigurationConflict) {
				t.Fatal("storage loss is not a configuration conflict")
			}
			e.recordOutcome(record, result, err)
			terminal, err := e.journal.Get(command.CommandId)
			if err != nil || terminal.State != journal.Terminal || terminal.Result == nil || terminal.Result.State != "failed" || terminal.Result.FailureCode != "storage_lost" {
				t.Fatalf("storage refusal remained unresolved: %+v %v", terminal, err)
			}
			if e.failure != nil {
				t.Fatal(e.failure)
			}
			next := volumeCommand(t, epoch)
			if _, err := e.journal.Admit(next); err != nil {
				t.Fatalf("known storage loss retained admission slot: %v", err)
			}
			if _, err := e.journal.Start(next.CommandId); err != nil {
				t.Fatalf("known storage loss retained mutation slot: %v", err)
			}
		})
	}
}
