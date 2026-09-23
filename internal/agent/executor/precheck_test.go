package executor

import (
	"errors"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"testing"

	"github.com/containerd/errdefs"
	"github.com/google/uuid"
	"github.com/moby/moby/client"
	"time"

	"github.com/sirrobot01/synod/internal/agent/capacity"
	"github.com/sirrobot01/synod/internal/agent/journal"
	"github.com/sirrobot01/synod/internal/operation"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
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

func volumeInput() *agentv1.Command {
	id := uuid.NewString()
	return &agentv1.Command{Input: &agentv1.CommandInput{ResourceId: id, Payload: &agentv1.CommandInput_EnsureVolume{EnsureVolume: &agentv1.EnsureVolume{VolumeId: id}}}}
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
	if err := guard(t, host, 0).precheck(volumeInput()); err != nil {
		t.Fatalf("a pool with room refused the work: %v", err)
	}
	// A floor above the whole filesystem leaves nothing to give.
	err := guard(t, host, 1<<62).precheck(volumeInput())
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
		"stop":    {Input: &agentv1.CommandInput{Payload: &agentv1.CommandInput_StopContainer{StopContainer: &agentv1.StopContainer{}}}},
		"remove":  {Input: &agentv1.CommandInput{Payload: &agentv1.CommandInput_RemoveContainer{RemoveContainer: &agentv1.RemoveContainer{}}}},
		"inspect": {Input: &agentv1.CommandInput{Payload: &agentv1.CommandInput_InspectContainer{InspectContainer: &agentv1.InspectContainer{}}}},
		"network": {Input: &agentv1.CommandInput{Payload: &agentv1.CommandInput_EnsureNetwork{EnsureNetwork: &agentv1.EnsureNetwork{NetworkId: uuid.NewString()}}}},
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
	if err := e.precheck(volumeInput()); !errors.Is(err, capacity.ErrCapacityUnavailable) {
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
	command := volumeCommand(t, f.epoch)
	if _, err := f.e.Handle(ctx, &agentv1.ControllerMessage{DeliveryAttemptId: uuid.NewString(), Message: &agentv1.ControllerMessage_Command{Command: command}}); err != nil {
		t.Fatal(err)
	}
	task := job{cluster: f.epoch.ClusterId, command: command.CommandId, session: f.e.session, gate: f.e.session.gates[f.epoch.ClusterId]}
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
	if _, err := f.api.VolumeInspect(t.Context(), "synod-volume-"+command.Input.ResourceId, client.VolumeInspectOptions{}); !errdefs.IsNotFound(err) {
		t.Fatalf("a refused command created a native volume: %v", err)
	}
}

// A pool with room still runs the work, so the guard refuses scarcity and not
// everything.
func TestAMeasurablePoolStillRunsTheWork(t *testing.T) {
	f := newFixture(t, nil)
	ctx, _ := localSession(t, f.e)
	activateLocal(t, f.e, ctx, f.epoch)
	command := volumeCommand(t, f.epoch)
	if _, err := f.e.Handle(ctx, &agentv1.ControllerMessage{DeliveryAttemptId: uuid.NewString(), Message: &agentv1.ControllerMessage_Command{Command: command}}); err != nil {
		t.Fatal(err)
	}
	task := job{cluster: f.epoch.ClusterId, command: command.CommandId, session: f.e.session, gate: f.e.session.gates[f.epoch.ClusterId]}
	f.e.execute(t.Context(), task)
	record, err := f.e.journal.Get(command.CommandId)
	if err != nil || record.Result == nil || record.Result.State != "succeeded" {
		t.Fatalf("result=%+v err=%v, want the work to run", record.Result, err)
	}
	if _, err := f.api.VolumeInspect(t.Context(), "synod-volume-"+command.Input.ResourceId, client.VolumeInspectOptions{}); err != nil {
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

func TestPrecheckReplicaCloneRequiresStorageHeadroom(t *testing.T) {
	command := &agentv1.Command{Input: &agentv1.CommandInput{Payload: &agentv1.CommandInput_ClonePostgresReplica{ClonePostgresReplica: &agentv1.ClonePostgresReplica{RequiredFreeBytes: 1 << 30}}}}
	host := pool(t, 1)
	if err := guard(t, host, 1<<62).precheck(command); !errors.Is(err, capacity.ErrCapacityUnavailable) {
		t.Fatalf("clone ignored storage floor: %v", err)
	}
	if err := guard(t, capacity.Host{}, 0).precheck(command); !errors.Is(err, capacity.ErrCapacityUnavailable) {
		t.Fatalf("clone ignored missing pool: %v", err)
	}
}

func TestCloneSpacePreservesTheOperatorFloor(t *testing.T) {
	host := pool(t, 1)
	space, err := host.Free()
	if err != nil || space.DiskBytes < 1<<30 {
		t.Fatalf("fixture lacks space: %+v %v", space, err)
	}
	command := &agentv1.Command{Input: &agentv1.CommandInput{Payload: &agentv1.CommandInput_ClonePostgresReplica{ClonePostgresReplica: &agentv1.ClonePostgresReplica{RequiredFreeBytes: space.DiskBytes / 2}}}}
	if err := guard(t, host, 0).precheck(command); err != nil {
		t.Fatalf("available copy space refused: %v", err)
	}
	if err := guard(t, host, space.DiskBytes/4*3).precheck(command); !errors.Is(err, capacity.ErrCapacityUnavailable) {
		t.Fatalf("copy consumed floor: %v", err)
	}
	command.Input.GetClonePostgresReplica().RequiredFreeBytes = 1 << 50
	if err := guard(t, host, 0).precheck(command); !errors.Is(err, capacity.ErrCapacityUnavailable) {
		t.Fatalf("oversized copy passed: %v", err)
	}
	if err := guard(t, host, -1<<63).precheck(command); !errors.Is(err, capacity.ErrCapacityUnavailable) {
		t.Fatalf("negative floor bypassed capacity: %v", err)
	}
}

func TestCloneCapacityRefusalPrecedesDockerExecution(t *testing.T) {
	var requests atomic.Int64
	f := newFixture(t, func(request *http.Request, next http.RoundTripper) (*http.Response, error) {
		requests.Add(1)
		return next.RoundTrip(request)
	})
	ctx, _ := localSession(t, f.e)
	activateLocal(t, f.e, ctx, f.epoch)
	command := volumeCommand(t, f.epoch)
	app, repl, admin := uuid.NewString(), uuid.NewString(), uuid.NewString()
	command.Input.Secrets = []*agentv1.SecretReference{{CredentialId: app, Version: 1, Purpose: "application"}, {CredentialId: repl, Version: 1, Purpose: "replication"}}
	command.Input.Payload = &agentv1.CommandInput_ClonePostgresReplica{ClonePostgresReplica: &agentv1.ClonePostgresReplica{RequiredFreeBytes: 1 << 50, Postgres: &agentv1.PostgresContainerConfig{Port: 5432, SuperuserCredentialId: admin, SuperuserCredentialVersion: 1, ApplicationCredentialId: app, ApplicationCredentialVersion: 1, ReplicationCredentialId: repl, ReplicationCredentialVersion: 1, ReplicaSource: &agentv1.PostgresReplicaSource{Address: "127.0.0.1", Port: 5432, ResourceId: uuid.NewString()}}}}
	hash, err := operation.CommandHash(command)
	if err != nil {
		t.Fatal(err)
	}
	command.InputHash = hash[:]
	if _, err := f.e.Handle(ctx, &agentv1.ControllerMessage{DeliveryAttemptId: uuid.NewString(), Message: &agentv1.ControllerMessage_Command{Command: command}}); err != nil {
		t.Fatal(err)
	}
	before := requests.Load()
	f.e.execute(t.Context(), job{cluster: f.epoch.ClusterId, command: command.CommandId, session: f.e.session, gate: f.e.session.gates[f.epoch.ClusterId]})
	record, err := f.e.journal.Get(command.CommandId)
	if err != nil || record.Result == nil || record.Result.State != "failed" || record.Result.FailureCode != "capacity_unavailable" || len(record.Result.Effects) != 0 {
		t.Fatalf("copy refusal did not settle before effects: %+v %v", record.Result, err)
	}
	if requests.Load() != before {
		t.Fatal("capacity refusal contacted Docker")
	}
}

func TestCapacityFloorDoesNotBlockRecovery(t *testing.T) {
	f := newFixture(t, nil)
	ctx, _ := localSession(t, f.e)
	activateLocal(t, f.e, ctx, f.epoch)
	command := volumeCommand(t, f.epoch)
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
	f.e.execute(t.Context(), job{cluster: f.epoch.ClusterId, command: command.CommandId, session: f.e.session, gate: f.e.session.gates[f.epoch.ClusterId]})
	record, err := f.e.journal.Get(command.CommandId)
	if err != nil || record.Result == nil || record.Result.State != "succeeded" {
		t.Fatalf("full pool stranded prior effects: %+v %v", record.Result, err)
	}
}

func TestPatroniStartCapacityAdmission(t *testing.T) {
	host := pool(t, 8000000)
	command := &agentv1.Command{Input: &agentv1.CommandInput{Payload: &agentv1.CommandInput_StartPatroniMember{StartPatroniMember: &agentv1.StartPatroniMember{
		AllowInitialization: true, Container: &agentv1.EnsureContainer{MemoryBytes: 512 << 20, Postgres: &agentv1.PostgresContainerConfig{Patroni: &agentv1.PatroniContainerConfig{RequiredFreeBytes: 1 << 62}}},
	}}}}
	e := guard(t, host, 0)
	if err := e.precheck(command); !errors.Is(err, capacity.ErrCapacityUnavailable) {
		t.Fatal("initial copy allowance ignored", err)
	}
	command.Input.GetStartPatroniMember().AllowInitialization = false
	if err := e.precheck(command); err != nil {
		t.Fatal("restart charged initial copy space", err)
	}
	if err := guard(t, host, 1<<62).precheck(command); !errors.Is(err, capacity.ErrCapacityUnavailable) {
		t.Fatal("restart ignored host floor", err)
	}
	command.Input.GetStartPatroniMember().Container.MemoryBytes = 1 << 62
	if err := e.precheck(command); !errors.Is(err, capacity.ErrCapacityUnavailable) {
		t.Fatal("native start ignored memory pressure", err)
	}
}
