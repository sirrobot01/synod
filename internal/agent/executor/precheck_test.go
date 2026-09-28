package executor

import (
	"errors"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"time"

	"github.com/containerd/errdefs"
	"github.com/google/uuid"
	"github.com/moby/moby/client"

	"github.com/sirrobot01/synod/internal/agent/capacity"
	"github.com/sirrobot01/synod/internal/agent/journal"
	"github.com/sirrobot01/synod/internal/operation"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/proto"
)

// pool writes a fixture proc tree and a real directory, so a precheck measures a
// known host rather than the machine running the test.
func pool(t *testing.T, availableKB int64) capacity.Host {
	t.Helper()
	dir := t.TempDir()
	meminfo := "MemTotal: 16000000 kB\nMemAvailable: " + strconv.FormatInt(availableKB, 10) + " kB\n"
	if err := os.WriteFile(filepath.Join(dir, "meminfo"), []byte(meminfo), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "stat"), []byte("cpu  1 0 1 1 1\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	return capacity.Host{ProcRoot: dir, StoragePath: t.TempDir()}
}

// guard builds an executor whose only job is to answer a precheck.
func guard(t *testing.T, host capacity.Host, floor int64) *Executor {
	t.Helper()
	identity := journal.Identity{InstallationID: uuid.NewString(), NodeID: uuid.NewString(), RecoveryGeneration: uuid.NewString()}
	j, err := journal.Open(filepath.Join(t.TempDir(), "journal"), journal.Options{Identity: identity, AllowCreate: true, EmptyInventory: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { j.Close() })
	e, err := New(j, &measuredRuntime{}, Options{
		Workers: 1, QueueSize: 1, PollInterval: time.Second,
		Host: host, BootID: uuid.NewString(), StorageID: "pool-a", RuntimeVersion: "26.1",
		MinFreeBytes: floor,
	})
	if err != nil {
		t.Fatal(err)
	}
	return e
}

func containerInput(memoryBytes int64, ports ...*agentv1.PortBinding) *agentv1.Command {
	return &agentv1.Command{Input: &agentv1.CommandInput{
		ResourceId: uuid.NewString(),
		Payload: &agentv1.CommandInput_EnsureContainer{EnsureContainer: &agentv1.EnsureContainer{
			MemoryBytes: memoryBytes, Ports: ports,
		}},
	}}
}

// A pool with room takes the work. A pool at its declared floor does not.
func TestPrecheckRefusesAPoolAtItsFloor(t *testing.T) {
	host := pool(t, 8000000)
	if err := guard(t, host, 0).precheck(containerInput(0)); err != nil {
		t.Fatalf("a pool with room refused the work: %v", err)
	}
	// A floor above the whole filesystem leaves nothing to give.
	err := guard(t, host, 1<<62).precheck(containerInput(0))
	if !errors.Is(err, capacity.ErrCapacityUnavailable) {
		t.Fatalf("err=%v, want ErrCapacityUnavailable", err)
	}
}

// Memory the host no longer has must refuse the container, not shrink it.
func TestPrecheckRefusesMemoryTheHostLacks(t *testing.T) {
	host := pool(t, 1000)
	available := int64(1000) * 1024
	if err := guard(t, host, 0).precheck(containerInput(available / 2)); err != nil {
		t.Fatalf("a container that fits was refused: %v", err)
	}
	err := guard(t, host, 0).precheck(containerInput(available * 4))
	if !errors.Is(err, capacity.ErrCapacityUnavailable) {
		t.Fatalf("err=%v, want ErrCapacityUnavailable", err)
	}
}

// A host port another process holds must refuse the container.
func TestPrecheckRefusesAnOccupiedHostPort(t *testing.T) {
	// A real listener, because the point is that anything on the host can hold a
	// port, not only something Synod placed.
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	_, portText, err := net.SplitHostPort(listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	taken, err := strconv.Atoi(portText)
	if err != nil {
		t.Fatal(err)
	}
	host := pool(t, 8000000)
	binding := &agentv1.PortBinding{HostAddress: "127.0.0.1", HostPort: uint32(taken), ContainerPort: 5432, Transport: "tcp"}
	if err := guard(t, host, 0).precheck(containerInput(0, binding)); !errors.Is(err, capacity.ErrPortOccupied) {
		t.Fatalf("err=%v, want ErrPortOccupied", err)
	}
	// The same binding is accepted once the holder lets go.
	listener.Close()
	if err := guard(t, host, 0).precheck(containerInput(0, binding)); err != nil {
		t.Fatalf("a free port was refused: %v", err)
	}
}

// Work that frees capacity must never be refused for lack of it.
func TestPrecheckLeavesReleasingWorkAlone(t *testing.T) {
	host := pool(t, 1)
	e := guard(t, host, 1<<62)
	for name, command := range map[string]*agentv1.Command{
		"stop":   {Input: &agentv1.CommandInput{Payload: &agentv1.CommandInput_StopContainer{StopContainer: &agentv1.StopContainer{}}}},
		"remove": {Input: &agentv1.CommandInput{Payload: &agentv1.CommandInput_RemoveContainer{RemoveContainer: &agentv1.RemoveContainer{}}}},
		"start":  {Input: &agentv1.CommandInput{Payload: &agentv1.CommandInput_StartContainer{StartContainer: &agentv1.StartContainer{}}}},
	} {
		t.Run(name, func(t *testing.T) {
			if err := e.precheck(command); err != nil {
				t.Fatalf("work that frees capacity was refused: %v", err)
			}
		})
	}
}

// An agent with no declared pool cannot measure one, so it must say so rather than
// pass the work through an unchecked path.
func TestPrecheckRefusesWithoutADeclaredPool(t *testing.T) {
	e := guard(t, capacity.Host{}, 0)
	if err := e.precheck(containerInput(0)); !errors.Is(err, capacity.ErrCapacityUnavailable) {
		t.Fatalf("err=%v, want a refusal rather than an unchecked effect", err)
	}
}

// The refusal must carry a stable code an operator can grep for.
func TestPrecheckRefusalsCarryStableCodes(t *testing.T) {
	for code, err := range map[string]error{
		"capacity_unavailable": capacity.ErrCapacityUnavailable,
		"host_port_occupied":   capacity.ErrPortOccupied,
	} {
		if found := failureCode(err); found != code {
			t.Fatalf("code=%q, want %q", found, code)
		}
	}
}

// A refused effect must leave the host exactly as it was. The check runs before the
// runtime is called, so there is nothing to clean up and nothing is cleaned up.
func TestRefusedEffectCreatesNoNativeObject(t *testing.T) {
	f := newFixture(t, nil)
	// The pool is declared but its floor is above the whole filesystem, so no new
	// work can fit. Nothing is deleted to make room.
	f.e.options.MinFreeBytes = 1 << 62
	ctx, _ := localSession(t, f.e)
	activateLocal(t, f.e, ctx, f.epoch)
	command := ensureCommand(t, f.epoch)
	if _, err := f.e.Handle(ctx, &agentv1.ControllerMessage{DeliveryAttemptId: uuid.NewString(), Message: &agentv1.ControllerMessage_Command{Command: command}}); err != nil {
		t.Fatal(err)
	}
	task := job{app: f.epoch.AppId, command: command.CommandId, session: f.e.session, gate: f.e.session.gates[f.epoch.AppId]}
	f.e.execute(t.Context(), task)

	record, err := f.e.journal.Get(command.CommandId)
	if err != nil {
		t.Fatal(err)
	}
	if record.Result == nil || record.Result.State != "failed" {
		t.Fatalf("result=%+v, want a refusal", record.Result)
	}
	if record.Result.FailureCode != "capacity_unavailable" {
		t.Fatalf("code=%q, want capacity_unavailable", record.Result.FailureCode)
	}
	// A refusal before the effect leaves no effect to report.
	if len(record.Result.Effects) != 0 {
		t.Fatalf("effects=%+v, want none from a refused command", record.Result.Effects)
	}
	if _, err := f.api.ContainerInspect(t.Context(), "synod-instance-"+command.Input.ResourceId, client.ContainerInspectOptions{}); !errdefs.IsNotFound(err) {
		t.Fatalf("a refused command created a native volume: %v", err)
	}
}

// A pool with room still runs the work, so the guard refuses scarcity and not
// everything.
func TestAMeasurablePoolStillRunsTheWork(t *testing.T) {
	f := newFixture(t, nil)
	ctx, _ := localSession(t, f.e)
	activateLocal(t, f.e, ctx, f.epoch)
	command := ensureCommand(t, f.epoch)
	if _, err := f.e.Handle(ctx, &agentv1.ControllerMessage{DeliveryAttemptId: uuid.NewString(), Message: &agentv1.ControllerMessage_Command{Command: command}}); err != nil {
		t.Fatal(err)
	}
	task := job{app: f.epoch.AppId, command: command.CommandId, session: f.e.session, gate: f.e.session.gates[f.epoch.AppId]}
	f.e.execute(t.Context(), task)
	record, err := f.e.journal.Get(command.CommandId)
	if err != nil || record.Result == nil || record.Result.State != "succeeded" {
		t.Fatalf("result=%+v err=%v, want the work to run", record.Result, err)
	}
	if _, err := f.api.ContainerInspect(t.Context(), "synod-instance-"+command.Input.ResourceId, client.ContainerInspectOptions{}); err != nil {
		t.Fatalf("the volume was not created: %v", err)
	}
}

// A host that cannot report memory must still run work whose pool has room. The
// pool floor is the check that always applies.
func TestPrecheckRunsWhenMemoryCannotBeMeasured(t *testing.T) {
	host := capacity.Host{ProcRoot: filepath.Join(t.TempDir(), "absent"), StoragePath: t.TempDir()}
	if err := guard(t, host, 0).precheck(containerInput(1 << 40)); err != nil {
		t.Fatalf("an unmeasurable memory figure refused the work: %v", err)
	}
	// The pool floor still refuses.
	if err := guard(t, host, 1<<62).precheck(containerInput(0)); !errors.Is(err, capacity.ErrCapacityUnavailable) {
		t.Fatalf("err=%v, want the pool floor to still apply", err)
	}
}

func TestCapacityFloorDoesNotBlockRecovery(t *testing.T) {
	f := newFixture(t, nil)
	ctx, _ := localSession(t, f.e)
	activateLocal(t, f.e, ctx, f.epoch)
	command := ensureCommand(t, f.epoch)
	if _, err := f.e.Handle(ctx, &agentv1.ControllerMessage{DeliveryAttemptId: uuid.NewString(), Message: &agentv1.ControllerMessage_Command{Command: command}}); err != nil {
		t.Fatal(err)
	}
	if _, err := f.e.journal.Start(command.CommandId); err != nil {
		t.Fatal(err)
	}
	if _, err := f.e.runtime.Execute(t.Context(), command, nil); err != nil {
		t.Fatal(err)
	}
	// Lose the result after the native effect. Recovery must inspect it even on a full pool.
	f.e.options.MinFreeBytes = 1 << 62
	f.e.execute(t.Context(), job{app: f.epoch.AppId, command: command.CommandId, session: f.e.session, gate: f.e.session.gates[f.epoch.AppId]})
	record, err := f.e.journal.Get(command.CommandId)
	if err != nil || record.Result == nil || record.Result.State != "succeeded" {
		t.Fatalf("full pool stranded prior effects: %+v %v", record.Result, err)
	}
}

func TestEnsureChecksExistingContainerBeforeHostCapacity(t *testing.T) {
	for _, name := range []string{"owned", "full_pool", "other_resource", "changed_config", "other_app"} {
		t.Run(name, func(t *testing.T) {
			f := newFixture(t, nil)
			ctx, _ := localSession(t, f.e)
			activateLocal(t, f.e, ctx, f.epoch)
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			port := listener.Addr().(*net.TCPAddr).Port
			listener.Close()
			original := ensureCommand(t, f.epoch)
			original.Input.GetEnsureContainer().Ports = []*agentv1.PortBinding{{HostAddress: "127.0.0.1", HostPort: uint32(port), ContainerPort: 6379, Transport: "tcp"}}
			hash, err := operation.CommandHash(original)
			if err != nil {
				t.Fatal(err)
			}
			original.InputHash = hash[:]
			if _, err := f.e.runtime.Execute(t.Context(), original, nil); err != nil {
				t.Fatal(err)
			}
			containerName := "synod-instance-" + original.Input.ResourceId
			if _, err := f.api.ContainerStart(t.Context(), containerName, client.ContainerStartOptions{}); err != nil {
				t.Fatal(err)
			}
			before, err := f.api.ContainerInspect(t.Context(), containerName, client.ContainerInspectOptions{})
			if err != nil {
				t.Fatal(err)
			}
			// Docker can run in a VM. Model its occupied host port at the agent boundary.
			previous := portFree
			portFree = func(string, string) bool { return false }
			t.Cleanup(func() { portFree = previous })
			command := proto.Clone(original).(*agentv1.Command)
			command.CommandId, command.OperationId, command.StepId = uuid.NewString(), uuid.NewString(), uuid.NewString()
			wantState, wantCode := "succeeded", ""
			switch name {
			case "full_pool":
				f.e.options.MinFreeBytes = 1 << 62
			case "other_resource":
				command.Input.ResourceId = uuid.NewString()
				wantState, wantCode = "failed", "host_port_occupied"
			case "changed_config":
				command.Input.GetEnsureContainer().CpuMillis++
				wantState, wantCode = "failed", "runtime_failed"
			case "other_app":
				command.Epoch.AppId = uuid.NewString()
				activateLocal(t, f.e, ctx, command.Epoch)
				wantState, wantCode = "failed", "runtime_failed"
			}
			hash, err = operation.CommandHash(command)
			if err != nil {
				t.Fatal(err)
			}
			command.InputHash = hash[:]
			if _, err := f.e.Handle(ctx, &agentv1.ControllerMessage{DeliveryAttemptId: uuid.NewString(), Message: &agentv1.ControllerMessage_Command{Command: command}}); err != nil {
				t.Fatal(err)
			}
			f.e.execute(t.Context(), job{app: command.Epoch.AppId, command: command.CommandId, session: f.e.session, gate: f.e.session.gates[command.Epoch.AppId]})
			record, err := f.e.journal.Get(command.CommandId)
			if err != nil {
				t.Fatal(err)
			}
			if record.Result == nil || record.Result.State != wantState || record.Result.FailureCode != wantCode {
				t.Fatalf("result=%+v, want %s %s", record.Result, wantState, wantCode)
			}
			after, err := f.api.ContainerInspect(t.Context(), containerName, client.ContainerInspectOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if after.Container.ID != before.Container.ID || !after.Container.State.Running || after.Container.State.StartedAt != before.Container.State.StartedAt {
				t.Fatal("ensure changed the existing container")
			}
			if name == "other_resource" {
				if _, err := f.api.ContainerInspect(t.Context(), "synod-instance-"+command.Input.ResourceId, client.ContainerInspectOptions{}); !errdefs.IsNotFound(err) {
					t.Fatalf("refused command created a container: %v", err)
				}
			}
		})
	}
}
