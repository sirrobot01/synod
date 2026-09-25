package executor

import (
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/agent/journal"
	"github.com/sirrobot01/synod/internal/engine/postgres"
	"github.com/sirrobot01/synod/internal/operation"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/types/known/durationpb"
)

// A switchover that was running when the agent stopped left no native work. Recovery fails it
// without a new request, and the runtime is never asked.
func TestInterruptedSwitchoverFails(t *testing.T) {
	e, ctx, _ := challenged(t, &measuredRuntime{})
	identity := e.journal.Identity()
	epoch := &agentv1.Epoch{InstallationId: identity.InstallationID, NodeId: identity.NodeID, ClusterId: uuid.NewString(), RecoveryGeneration: identity.RecoveryGeneration, ExecutionEpoch: 1}
	activateLocal(t, e, ctx, epoch)
	command := &agentv1.Command{Epoch: epoch, OperationId: uuid.NewString(), StepId: uuid.NewString(), CommandId: uuid.NewString(), DesiredRevision: 1, PlanVersion: 1,
		Input: &agentv1.CommandInput{ResourceId: uuid.NewString(), PayloadVersion: 1, Timeout: durationpb.New(time.Minute),
			Secrets: []*agentv1.SecretReference{{CredentialId: uuid.NewString(), Version: 1, Purpose: "patroni_rest"}, {CredentialId: uuid.NewString(), Version: 1, Purpose: "patroni_member_tls"}},
			Payload: &agentv1.CommandInput_SwitchoverPatroni{SwitchoverPatroni: &agentv1.SwitchoverPatroni{Address: "192.0.2.10", RestPort: 8008}}}}
	hash, err := operation.CommandHash(command)
	if err != nil {
		t.Fatal(err)
	}
	command.InputHash = hash[:]
	if _, err := e.Handle(ctx, &agentv1.ControllerMessage{DeliveryAttemptId: uuid.NewString(), Message: &agentv1.ControllerMessage_Command{Command: command}}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.journal.Start(command.CommandId); err != nil {
		t.Fatal(err)
	}
	e.execute(t.Context(), job{cluster: epoch.ClusterId, command: command.CommandId})
	record, err := e.journal.Get(command.CommandId)
	if err != nil || record.State != journal.Terminal || record.Result.State != "failed" || record.Result.FailureCode != "patroni_switchover_interrupted" {
		t.Fatal("interrupted switchover was not failed", record.Result, err)
	}
}

func TestSwitchoverFailureCodes(t *testing.T) {
	for err, want := range map[error]string{
		errSwitchoverInterrupted:                                  "patroni_switchover_interrupted",
		postgres.ErrSwitchoverIneligible:                          "patroni_switchover_ineligible",
		fmt.Errorf("%w: HTTP 412", postgres.ErrSwitchoverRefused): "patroni_switchover_refused",
		postgres.ErrSwitchoverUnconfirmed:                         "patroni_switchover_unconfirmed",
	} {
		if code := failureCode(err); code != want {
			t.Errorf("%v: code %q, want %q", err, code, want)
		}
	}
}
