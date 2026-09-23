package executor

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/moby/moby/client"
	"github.com/sirrobot01/synod/internal/agent/journal"
	"github.com/sirrobot01/synod/internal/operation"
	"github.com/sirrobot01/synod/internal/runtime"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/proto"
)

func runningWorkload(t *testing.T, f *fixture) (*agentv1.Command, *agentv1.Command) {
	t.Helper()
	volume := volumeCommand(t, f.epoch)
	if _, err := f.e.runtime.Execute(t.Context(), volume, nil); err != nil {
		t.Fatal(err)
	}
	networkID := uuid.NewString()
	network := volumeCommand(t, f.epoch)
	network.Input.ResourceId = networkID
	network.Input.Payload = &agentv1.CommandInput_EnsureNetwork{EnsureNetwork: &agentv1.EnsureNetwork{NetworkId: networkID}}
	hash, err := operation.CommandHash(network)
	if err != nil {
		t.Fatal(err)
	}
	network.InputHash = hash[:]
	if _, err := f.e.runtime.Execute(t.Context(), network, nil); err != nil {
		t.Fatal(err)
	}
	ensure := volumeCommand(t, f.epoch)
	ensure.Input.Secrets = []*agentv1.SecretReference{{CredentialId: uuid.NewString(), Version: 1, Purpose: "superuser"}, {CredentialId: uuid.NewString(), Version: 1, Purpose: "application"}, {CredentialId: uuid.NewString(), Version: 1, Purpose: "replication"}}
	ensure.Input.Payload = &agentv1.CommandInput_EnsureContainer{EnsureContainer: &agentv1.EnsureContainer{
		ImageDigest: "redis@sha256:d30960f73a599496d8b2802c97758bab6b1cd421fd06337f837779c47a57e1f3", Engine: "redis", VolumeId: volume.Input.ResourceId, NetworkId: networkID, NetworkMode: agentv1.NetworkMode_NETWORK_MODE_BRIDGE, MemoryBytes: 64 << 20, CpuMillis: 250,
		Redis:     &agentv1.RedisContainerConfig{Port: 6379, Persistence: "none", MaxmemoryBytes: 48 << 20, EvictionPolicy: "noeviction"},
		SecretEnv: []*agentv1.SecretEnv{{Name: "REDIS_ADMIN_PASSWORD", Purpose: "superuser"}, {Name: "REDIS_APP_PASSWORD", Purpose: "application"}, {Name: "REDIS_REPLICATION_PASSWORD", Purpose: "replication"}},
	}}
	ensure.Input.GetEnsureContainer().Redis.Credentials = ensure.Input.Secrets
	hash, err = operation.CommandHash(ensure)
	if err != nil {
		t.Fatal(err)
	}
	ensure.InputHash = hash[:]
	if _, err := f.e.runtime.Execute(t.Context(), ensure, runtime.Secrets{"superuser": []byte("executor-test-admin"), "application": []byte("executor-test-app"), "replication": []byte("executor-test-repl")}); err != nil {
		t.Fatal(err)
	}
	start := proto.Clone(ensure).(*agentv1.Command)
	start.CommandId = uuid.NewString()
	start.StepId = uuid.NewString()
	start.Input.Secrets = nil
	// This is the workload's first start, so it initializes, as the plan does.
	start.Input.Payload = &agentv1.CommandInput_StartContainer{StartContainer: &agentv1.StartContainer{AllowInitialization: true}}
	hash, err = operation.CommandHash(start)
	if err != nil {
		t.Fatal(err)
	}
	start.InputHash = hash[:]
	if _, err := f.e.runtime.Execute(t.Context(), start, nil); err != nil {
		t.Fatal(err)
	}
	return ensure, volume
}

func TestBarrierDrainsUnjournaledExecAndShutdownPreservesWorkload(t *testing.T) {
	f := newFixture(t, nil)
	ensure, volume := runningWorkload(t, f)
	controller := startController(t, f, nil)
	native, err := f.api.ExecCreate(t.Context(), "synod-instance-"+ensure.Input.ResourceId, client.ExecCreateOptions{Cmd: []string{"sleep", "2"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.api.ExecStart(t.Context(), native.ID, client.ExecStartOptions{Detach: true}); err != nil {
		t.Fatal(err)
	}
	controller.send(t, &agentv1.ControllerMessage{Message: &agentv1.ControllerMessage_PrepareEpoch{PrepareEpoch: &agentv1.PrepareEpoch{Epoch: f.epoch}}})
	undrained := controller.wait(t, func(message *agentv1.AgentMessage) bool {
		return message.GetBarrierResult() != nil && slices.ContainsFunc(message.GetBarrierResult().Effects, func(effect *agentv1.Effect) bool { return effect.Active })
	}).GetBarrierResult()
	if undrained.Drained {
		t.Fatal("reported native task drained")
	}
	drained := controller.wait(t, func(message *agentv1.AgentMessage) bool {
		return message.GetBarrierResult() != nil && message.GetBarrierResult().Drained
	}).GetBarrierResult()
	for _, old := range undrained.Effects {
		if old.Active || old.OutcomeUnknown {
			if !slices.ContainsFunc(drained.Effects, func(effect *agentv1.Effect) bool {
				return effect.NativeOperationId == old.NativeOperationId && !effect.Active && !effect.OutcomeUnknown
			}) {
				t.Fatal("barrier omitted native resolution")
			}
		}
	}
	controller.send(t, &agentv1.ControllerMessage{Message: &agentv1.ControllerMessage_ActivateEpoch{ActivateEpoch: &agentv1.ActivateEpoch{Epoch: f.epoch}}})
	controller.cancel()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for {
		f.e.mu.Lock()
		running := f.e.running
		f.e.mu.Unlock()
		if !running {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("executor did not stop")
		case <-tick.C:
		}
	}
	container, err := f.api.ContainerInspect(t.Context(), "synod-instance-"+ensure.Input.ResourceId, client.ContainerInspectOptions{})
	if err != nil || !container.Container.State.Running {
		t.Fatal("shutdown stopped workload", err)
	}
	if _, err := f.api.VolumeInspect(t.Context(), "synod-volume-"+volume.Input.ResourceId, client.VolumeInspectOptions{}); err != nil {
		t.Fatal("shutdown removed data volume", err)
	}
}

func TestStartupRecoversAcknowledgedResidualEffects(t *testing.T) {
	f := newFixture(t, nil)
	ensure, _ := runningWorkload(t, f)
	ctx, _ := localSession(t, f.e)
	activateLocal(t, f.e, ctx, f.epoch)
	command := proto.Clone(ensure).(*agentv1.Command)
	command.CommandId, command.StepId = uuid.NewString(), uuid.NewString()
	command.Input.Payload = &agentv1.CommandInput_InspectContainer{InspectContainer: &agentv1.InspectContainer{}}
	hash, err := operation.CommandHash(command)
	if err != nil {
		t.Fatal(err)
	}
	command.InputHash = hash[:]
	native, err := f.api.ExecCreate(t.Context(), "synod-instance-"+ensure.Input.ResourceId, client.ExecCreateOptions{Cmd: []string{"sleep", "2"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.api.ExecStart(t.Context(), native.ID, client.ExecStartOptions{Detach: true}); err != nil {
		t.Fatal(err)
	}
	if _, err := f.e.journal.Admit(command); err != nil {
		t.Fatal(err)
	}
	if _, err := f.e.journal.Start(command.CommandId); err != nil {
		t.Fatal(err)
	}
	observation, err := f.e.runtime.Execute(t.Context(), command, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.ContainsFunc(observation.Effects, func(effect *agentv1.Effect) bool { return effect.Active }) {
		t.Fatal("native task was not active")
	}
	result := &agentv1.TerminalResult{Epoch: command.Epoch, CommandId: command.CommandId, InputHash: command.InputHash, Sequence: 1, State: "failed", FailureCode: "test_failure", Effects: observation.Effects}
	if err := f.e.journal.Complete(result); err != nil {
		t.Fatal(err)
	}
	if err := f.e.journal.Acknowledge(&agentv1.ResultAck{Epoch: command.Epoch, CommandId: command.CommandId, Sequence: 1}); err != nil {
		t.Fatal(err)
	}
	if err := f.e.journal.Close(); err != nil {
		t.Fatal(err)
	}
	f.options.AllowCreate = false
	reopened, err := journal.Open(f.path, f.options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { reopened.Close() })
	f.e, err = New(reopened, f.e.runtime, Options{PollInterval: 20 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	startController(t, f, nil)
	wait, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for {
		record, err := reopened.Get(command.CommandId)
		if err != nil {
			t.Fatal(err)
		}
		if !slices.ContainsFunc(record.Effects, func(effect *agentv1.Effect) bool { return effect.Active || effect.OutcomeUnknown }) {
			if !record.Acknowledged || !proto.Equal(record.Result, result) {
				t.Fatal("recovery changed acknowledged terminal identity")
			}
			break
		}
		select {
		case <-wait.Done():
			t.Fatal("acknowledged residual task was not recovered")
		case <-tick.C:
		}
	}
}
