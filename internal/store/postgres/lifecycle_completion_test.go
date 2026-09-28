package postgres

import (
	"crypto/sha256"
	"testing"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/operation"
	"github.com/sirrobot01/synod/internal/placement"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/proto"
)

// completionFixture reserves a real app copy and installs its checked command plan.
func completionFixture(t *testing.T) (*placementHost, operation.Claim, *agentv1.ExecutionPlan) {
	t.Helper()
	host := placementFixture(t)
	claim := host.claimApp(t, "completed-app")
	request, decision, plan := host.place(t, claim, smallBudget, []placement.Port{{Address: "127.0.0.1", Transport: "tcp", Number: 15432}})
	if _, err := host.store.InstallPlacementPlan(t.Context(), claim, request, decision, plan); err != nil {
		t.Fatal(err)
	}
	reconcilePlan(t, host.store, claim, plan)
	if err := host.store.ActivateClaim(t.Context(), claim); err != nil {
		t.Fatal(err)
	}
	return host, claim, plan
}

// completeSteps delivers native receipts through dispatch and checked completion APIs.
func completeSteps(t *testing.T, s *Store, claim operation.Claim, plan *agentv1.ExecutionPlan) {
	t.Helper()
	for _, step := range plan.Steps {
		command := proto.Clone(step.Command).(*agentv1.Command)
		command.Epoch.ExecutionEpoch = claim.ExecutionEpoch
		command.Epoch.RecoveryGeneration = claim.RecoveryGeneration.String()
		if err := s.RecordDispatch(t.Context(), claim, command); err != nil {
			t.Fatal(err)
		}
		state := "running"
		switch step.Postcondition {
		case agentv1.Postcondition_POSTCONDITION_CONTAINER_EXISTS:
			state = "created"
		case agentv1.Postcondition_POSTCONDITION_CONTAINER_STOPPED:
			state = "exited"
		case agentv1.Postcondition_POSTCONDITION_CONTAINER_ABSENT:
			state = "absent"
		}
		result := &agentv1.TerminalResult{Epoch: proto.Clone(command.Epoch).(*agentv1.Epoch), CommandId: command.CommandId, InputHash: command.InputHash, Sequence: 1, State: "succeeded", Effects: []*agentv1.Effect{{CommandId: command.CommandId, ResourceId: command.Input.ResourceId, NativeOperationId: "docker-command:" + command.CommandId, State: state}}}
		if err := s.RecordAgentResult(t.Context(), claim.InstallationID, uuid.MustParse(command.Epoch.NodeId), result); err != nil {
			t.Fatal(err)
		}
		encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(result)
		if err != nil {
			t.Fatal(err)
		}
		if err := s.RecordStepCompletion(t.Context(), claim, uuid.MustParse(command.StepId), result.Epoch, sha256.Sum256(encoded)); err != nil {
			t.Fatal(err)
		}
	}
}

// Success commits the proved lifecycle. A plan whose last proof is missing stays open.
func TestLifecycleCompletionRequiresStoredProof(t *testing.T) {
	host, claim, plan := completionFixture(t)
	completeSteps(t, host.store, claim, &agentv1.ExecutionPlan{Version: 1, Steps: plan.Steps[:1]})
	if err := host.store.UpdateProgress(t.Context(), claim, ProgressUpdate{Sequence: 1, Status: operation.Succeeded, Reason: "All postconditions passed."}); err == nil {
		t.Fatal("success was recorded without the final proof")
	}
	completeSteps(t, host.store, claim, &agentv1.ExecutionPlan{Version: 1, Steps: plan.Steps[1:]})
	if err := host.store.UpdateProgress(t.Context(), claim, ProgressUpdate{Sequence: 1, Status: operation.Succeeded, Reason: "All postconditions passed."}); err != nil {
		t.Fatal(err)
	}
	if host.count(t, `SELECT count(*) FROM apps WHERE id=$1 AND observed_revision=1 AND observed_lifecycle='running'`, claim.AppID) != 1 {
		t.Fatal("success did not commit the proved app lifecycle")
	}
}
