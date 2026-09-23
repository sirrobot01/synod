package operation

import (
	"bytes"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestPlanRemovalRequiresStopOnTheSameNode(t *testing.T) {
	stop := commandFixture()
	stop.Input.Secrets = nil
	stop.Input.Payload = &agentv1.CommandInput_StopContainer{StopContainer: &agentv1.StopContainer{GracePeriod: durationpb.New(time.Second)}}
	hash, err := CommandHash(stop)
	if err != nil {
		t.Fatal(err)
	}
	stop.InputHash = bytes.Clone(hash[:])
	remove := proto.Clone(stop).(*agentv1.Command)
	remove.CommandId, remove.StepId = uuid.NewString(), uuid.NewString()
	remove.Input.Payload = &agentv1.CommandInput_RemoveContainer{RemoveContainer: &agentv1.RemoveContainer{}}
	hash, err = CommandHash(remove)
	if err != nil {
		t.Fatal(err)
	}
	remove.InputHash = hash[:]
	plan := &agentv1.ExecutionPlan{Version: 1, Steps: []*agentv1.PlanStep{
		{Command: stop, MaxAttempts: 1, CancellationBoundary: agentv1.CancellationBoundary_CANCELLATION_BOUNDARY_SAFE_STOP, Postcondition: agentv1.Postcondition_POSTCONDITION_CONTAINER_STOPPED},
		{Command: remove, MaxAttempts: 1, CancellationBoundary: agentv1.CancellationBoundary_CANCELLATION_BOUNDARY_BEFORE_EXECUTION, Postcondition: agentv1.Postcondition_POSTCONDITION_CONTAINER_ABSENT, DependsOn: []string{stop.StepId}},
	}}
	op := Operation{ID: uuid.MustParse(stop.OperationId), InstallationID: uuid.MustParse(stop.Epoch.InstallationId), ClusterID: uuid.MustParse(stop.Epoch.ClusterId), DesiredRevision: stop.DesiredRevision}
	if err := ValidatePlan(plan, op); err != nil {
		t.Fatal(err)
	}
	remove.Epoch.NodeId = uuid.NewString()
	hash, err = CommandHash(remove)
	if err != nil {
		t.Fatal(err)
	}
	remove.InputHash = hash[:]
	if err := ValidatePlan(plan, op); !errors.Is(err, ErrUnsupportedPlan) {
		t.Fatalf("a stop on another node proved this removal: %v", err)
	}
}

func TestSharedReceiptChecksRejectMissingPlanInput(t *testing.T) {
	for _, step := range []*agentv1.PlanStep{nil, {}, {Command: &agentv1.Command{}}, {Command: &agentv1.Command{Epoch: &agentv1.Epoch{}}}} {
		if err := CheckStepResult(step, &agentv1.TerminalResult{}); !errors.Is(err, ErrPostconditionUnproved) {
			t.Fatalf("missing step input accepted: %v", err)
		}
		if err := CheckStepEffects(step, nil); !errors.Is(err, ErrPostconditionUnproved) {
			t.Fatalf("missing step effects accepted: %v", err)
		}
	}
}
