package operation

import (
	"crypto/sha256"
	"testing"
	"time"

	"github.com/google/uuid"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestDatabaseSwitchoverContract(t *testing.T) {
	command := commandFixture()
	command.Input.Payload = &agentv1.CommandInput_SwitchDatabase{SwitchDatabase: &agentv1.SwitchDatabase{WriterEpoch: 4, TargetNodeId: uuid.NewString(), TargetHost: "192.0.2.10", TargetPort: 5432}}
	hash, err := CommandHash(command)
	if err != nil {
		t.Fatal(err)
	}
	command.InputHash = hash[:]
	step := &agentv1.PlanStep{Command: command, MaxAttempts: 1, CancellationBoundary: agentv1.CancellationBoundary_CANCELLATION_BOUNDARY_BEFORE_EXECUTION, Postcondition: agentv1.Postcondition_POSTCONDITION_DATABASE_SWITCHOVER_READY}
	op := Operation{ID: uuid.MustParse(command.OperationId), InstallationID: uuid.MustParse(command.Epoch.InstallationId), AppID: uuid.MustParse(command.Epoch.AppId), DesiredRevision: command.DesiredRevision}
	if err := ValidatePlan(&agentv1.ExecutionPlan{Version: 1, Steps: []*agentv1.PlanStep{step}}, op); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*agentv1.Command){
		"zero epoch":    func(c *agentv1.Command) { c.Input.GetSwitchDatabase().WriterEpoch = 0 },
		"same node":     func(c *agentv1.Command) { c.Input.GetSwitchDatabase().TargetNodeId = c.Epoch.NodeId },
		"loopback":      func(c *agentv1.Command) { c.Input.GetSwitchDatabase().TargetHost = "127.0.0.1" },
		"hostname":      func(c *agentv1.Command) { c.Input.GetSwitchDatabase().TargetHost = "target" },
		"port overflow": func(c *agentv1.Command) { c.Input.GetSwitchDatabase().TargetPort = 65536 },
		"long pause":    func(c *agentv1.Command) { c.Input.Timeout = durationpb.New(61 * time.Second) },
	} {
		t.Run(name, func(t *testing.T) {
			changed := proto.Clone(command).(*agentv1.Command)
			mutate(changed)
			if _, err := CommandHash(changed); err == nil {
				t.Fatal("unsafe switchover command accepted")
			}
		})
	}
	effect := &agentv1.Effect{CommandId: command.CommandId, ResourceId: command.Input.ResourceId, NativeOperationId: "database-switch:" + command.CommandId, State: "database_switchover_ready", DatabaseSwitchover: &agentv1.DatabaseSwitchoverReceipt{WriterEpoch: 4, TargetNodeId: command.Input.GetSwitchDatabase().TargetNodeId, PositionHash: make([]byte, sha256.Size)}}
	if err := CheckStepEffects(step, []*agentv1.Effect{effect}); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*agentv1.Effect){
		"missing proof":  func(e *agentv1.Effect) { e.DatabaseSwitchover = nil },
		"old epoch":      func(e *agentv1.Effect) { e.DatabaseSwitchover.WriterEpoch-- },
		"other target":   func(e *agentv1.Effect) { e.DatabaseSwitchover.TargetNodeId = uuid.NewString() },
		"no position":    func(e *agentv1.Effect) { e.DatabaseSwitchover.PositionHash = nil },
		"still running":  func(e *agentv1.Effect) { e.Active = true },
		"uncertain stop": func(e *agentv1.Effect) { e.OutcomeUnknown = true },
		"wrong effect":   func(e *agentv1.Effect) { e.NativeOperationId = "docker-command:" + command.CommandId },
	} {
		t.Run(name, func(t *testing.T) {
			changed := proto.Clone(effect).(*agentv1.Effect)
			mutate(changed)
			if err := CheckStepEffects(step, []*agentv1.Effect{changed}); err == nil {
				t.Fatal("unproved switchover accepted")
			}
		})
	}
}
