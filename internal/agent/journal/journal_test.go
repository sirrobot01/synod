package journal

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/operation"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

func fixture(t *testing.T) (*Journal, Options, *agentv1.Command, string) {
	t.Helper()
	options := Options{Identity: Identity{InstallationID: uuid.NewString(), NodeID: uuid.NewString(), RecoveryGeneration: uuid.NewString()}, AllowCreate: true, EmptyInventory: true, MaxCommands: 16, MaxResultBytes: 1024, MaxRetainedBytes: 1 << 20, RecoveryWindow: time.Minute}
	path := filepath.Join(t.TempDir(), "journal")
	j, err := Open(path, options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := j.Close(); err != nil {
			t.Error(err)
		}
	})
	resource := uuid.NewString()
	command := &agentv1.Command{
		Epoch:       &agentv1.Epoch{InstallationId: options.Identity.InstallationID, NodeId: options.Identity.NodeID, RecoveryGeneration: options.Identity.RecoveryGeneration, ClusterId: uuid.NewString(), ExecutionEpoch: 1},
		OperationId: uuid.NewString(), StepId: uuid.NewString(), CommandId: uuid.NewString(), DesiredRevision: 1, PlanVersion: 1,
		Input: &agentv1.CommandInput{ResourceId: resource, PayloadVersion: 1, Timeout: durationpb.New(time.Minute), Payload: &agentv1.CommandInput_EnsureVolume{EnsureVolume: &agentv1.EnsureVolume{VolumeId: resource}}},
	}
	hash, err := operation.CommandHash(command)
	if err != nil {
		t.Fatal(err)
	}
	command.InputHash = hash[:]
	activate(t, j, command.Epoch)
	return j, options, command, path
}

func activate(t *testing.T, j *Journal, epoch *agentv1.Epoch) {
	t.Helper()
	if err := j.Prepare(epoch); err != nil {
		t.Fatal(err)
	}
	if err := j.RecordBarrier(&agentv1.BarrierResult{Epoch: epoch, AdmissionsClosed: true, Drained: true}); err != nil {
		t.Fatal(err)
	}
	if err := j.Activate(epoch, true); err != nil {
		t.Fatal(err)
	}
}

func TestAdmissionReplayAndReopen(t *testing.T) {
	j, options, command, path := fixture(t)
	identity := j.Identity()
	first, err := j.Admit(command)
	if err != nil || first.State != Queued {
		t.Fatalf("admit: %v %v", first.State, err)
	}
	duplicate, err := j.Admit(proto.Clone(command).(*agentv1.Command))
	if err != nil || duplicate.State != Queued {
		t.Fatalf("duplicate: %v", err)
	}
	changed := proto.Clone(command).(*agentv1.Command)
	changed.DesiredRevision++
	hash, _ := operation.CommandHash(changed)
	changed.InputHash = hash[:]
	if _, err := j.Admit(changed); !errors.Is(err, operation.ErrCommandConflict) {
		t.Fatalf("changed input: %v", err)
	}
	first.Command.Input.ResourceId = uuid.NewString()
	read, err := j.Get(command.CommandId)
	if err != nil || !proto.Equal(read.Command, command) {
		t.Fatalf("caller changed durable command: %v", err)
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
	if reopened.Identity() != identity {
		t.Fatal("identity changed on reopen")
	}
	if _, err := reopened.Start(command.CommandId); err != nil {
		t.Fatal(err)
	}
	if _, err := reopened.Start(command.CommandId); !errors.Is(err, ErrInspectionRequired) {
		t.Fatalf("running command restarted: %v", err)
	}
}

func TestResultsAcknowledgementsAndCompaction(t *testing.T) {
	j, options, command, path := fixture(t)
	if _, err := j.Admit(command); err != nil {
		t.Fatal(err)
	}
	if _, err := j.Start(command.CommandId); err != nil {
		t.Fatal(err)
	}
	result := &agentv1.TerminalResult{Epoch: command.Epoch, CommandId: command.CommandId, InputHash: command.InputHash, Sequence: 2, State: "succeeded"}
	if err := j.Complete(result); err != nil {
		t.Fatal(err)
	}
	if err := j.Complete(proto.Clone(result).(*agentv1.TerminalResult)); err != nil {
		t.Fatal(err)
	}
	if _, err := j.Start(command.CommandId); !errors.Is(err, ErrAlreadyCompleted) {
		t.Fatalf("completed execution granted: %v", err)
	}
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path, options)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	results, err := reopened.Results()
	if err != nil || len(results) != 1 || !proto.Equal(results[0], result) {
		t.Fatalf("replay: %v %v", results, err)
	}
	if count, err := reopened.Compact(time.Now().Add(time.Hour)); err != nil || count != 0 {
		t.Fatalf("unacknowledged result compacted: %d %v", count, err)
	}
	ack := &agentv1.ResultAck{Epoch: command.Epoch, CommandId: command.CommandId, Sequence: 1}
	if err := reopened.Acknowledge(ack); !errors.Is(err, operation.ErrCommandConflict) {
		t.Fatalf("wrong sequence ack: %v", err)
	}
	ack.Sequence = 2
	if err := reopened.Acknowledge(ack); err != nil {
		t.Fatal(err)
	}
	if count, err := reopened.Compact(time.Now()); err != nil || count != 0 {
		t.Fatalf("early compaction: %d %v", count, err)
	}
	if count, err := reopened.Compact(time.Now().Add(2 * time.Minute)); err != nil || count != 1 {
		t.Fatalf("compaction: %d %v", count, err)
	}
	if err := reopened.Acknowledge(ack); err != nil {
		t.Fatalf("ack retry after compaction: %v", err)
	}
	record, err := reopened.Admit(command)
	if err != nil || record.State != Compacted || record.Command != nil {
		t.Fatalf("tombstone replay: %+v %v", record, err)
	}
	if _, err := reopened.Start(command.CommandId); !errors.Is(err, ErrAlreadyCompleted) {
		t.Fatalf("tombstone executed: %v", err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
	again, err := Open(path, options)
	if err != nil {
		t.Fatal(err)
	}
	defer again.Close()
	results, err = again.Results()
	if err != nil || len(results) != 0 {
		t.Fatalf("ack lost after reopen: %v", err)
	}
}

func TestPreparedBarrierRetainsActiveEffects(t *testing.T) {
	j, options, command, path := fixture(t)
	if _, err := j.Admit(command); err != nil {
		t.Fatal(err)
	}
	if _, err := j.Start(command.CommandId); err != nil {
		t.Fatal(err)
	}
	effect := &agentv1.Effect{CommandId: command.CommandId, ResourceId: command.Input.ResourceId, NativeOperationId: "native-backup-1", Active: true, OutcomeUnknown: true}
	if err := j.Observe(command.CommandId, []*agentv1.Effect{effect}); err != nil {
		t.Fatal(err)
	}
	next := proto.Clone(command.Epoch).(*agentv1.Epoch)
	next.ExecutionEpoch = 2
	if err := j.Prepare(next); err != nil {
		t.Fatal(err)
	}
	if _, err := j.Admit(command); !errors.Is(err, operation.ErrStaleOwner) {
		t.Fatalf("old admission after prepare: %v", err)
	}
	if err := j.RecordBarrier(&agentv1.BarrierResult{Epoch: next, AdmissionsClosed: true, Drained: true}); !errors.Is(err, operation.ErrBarrierPending) {
		t.Fatalf("active task drain accepted: %v", err)
	}
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path, options)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	barrier, err := reopened.Barrier(next)
	if err != nil || barrier.Drained || len(barrier.Effects) < 2 {
		t.Fatalf("active effects lost: %+v %v", barrier, err)
	}
	if err := reopened.Activate(next, true); !errors.Is(err, operation.ErrBarrierPending) {
		t.Fatalf("higher epoch alone activated: %v", err)
	}
	if err := reopened.Observe(command.CommandId, nil); !errors.Is(err, operation.ErrBarrierPending) {
		t.Fatalf("unresolved task omitted: %v", err)
	}
	result := &agentv1.TerminalResult{Epoch: command.Epoch, CommandId: command.CommandId, InputHash: command.InputHash, Sequence: 1, State: "cancelled", Effects: []*agentv1.Effect{effect}}
	if err := reopened.Complete(result); err != nil {
		t.Fatal(err)
	}
	barrier, err = reopened.Barrier(next)
	if err != nil || barrier.Drained {
		t.Fatalf("surviving task cleared by completion: %v", err)
	}
	stopped := proto.Clone(effect).(*agentv1.Effect)
	stopped.Active = false
	stopped.OutcomeUnknown = false
	stopped.State = "retained-artifact"
	if err := reopened.Observe(command.CommandId, []*agentv1.Effect{stopped}); err != nil {
		t.Fatal(err)
	}
	drained := &agentv1.BarrierResult{Epoch: next, AdmissionsClosed: true, Drained: true, Effects: []*agentv1.Effect{stopped}}
	if err := reopened.RecordBarrier(drained); err != nil {
		t.Fatal(err)
	}
	if err := reopened.Activate(next, false); !errors.Is(err, operation.ErrBarrierPending) {
		t.Fatalf("unreconciled effects activated: %v", err)
	}
	if err := reopened.Activate(next, true); err != nil {
		t.Fatal(err)
	}
	if err := reopened.Prepare(command.Epoch); !errors.Is(err, operation.ErrStaleOwner) {
		t.Fatalf("epoch high water lowered: %v", err)
	}
}

func TestQueuedTransferAndForeignAuthority(t *testing.T) {
	j, _, command, _ := fixture(t)
	if _, err := j.Admit(command); err != nil {
		t.Fatal(err)
	}
	next := proto.Clone(command.Epoch).(*agentv1.Epoch)
	next.ExecutionEpoch = 2
	if err := j.Prepare(next); err != nil {
		t.Fatal(err)
	}
	if _, err := j.Start(command.CommandId); !errors.Is(err, operation.ErrStaleOwner) {
		t.Fatal(err)
	}
	activate(t, j, next)
	transferred := proto.Clone(command).(*agentv1.Command)
	transferred.Epoch = next
	if _, err := j.Admit(transferred); err != nil {
		t.Fatal(err)
	}
	if _, err := j.Start(command.CommandId); err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"node", "installation", "recovery"} {
		bad := proto.Clone(transferred).(*agentv1.Command)
		switch field {
		case "node":
			bad.Epoch.NodeId = uuid.NewString()
		case "installation":
			bad.Epoch.InstallationId = uuid.NewString()
		case "recovery":
			bad.Epoch.RecoveryGeneration = uuid.NewString()
		}
		hash, _ := operation.CommandHash(bad)
		bad.InputHash = hash[:]
		if _, err := j.Admit(bad); !errors.Is(err, operation.ErrStaleOwner) {
			t.Fatalf("foreign %s admitted: %v", field, err)
		}
		if err := j.Prepare(bad.Epoch); !errors.Is(err, operation.ErrStaleOwner) {
			t.Fatalf("foreign %s prepared: %v", field, err)
		}
	}
}

func TestConcurrentAdmissionAndCapacity(t *testing.T) {
	j, _, command, _ := fixture(t)
	j.options.MaxCommands = 1
	var wg sync.WaitGroup
	var starts atomic.Int32
	for range 24 {
		wg.Go(func() {
			if _, err := j.Admit(command); err != nil {
				t.Error(err)
				return
			}
			if _, err := j.Start(command.CommandId); err == nil {
				starts.Add(1)
			} else if !errors.Is(err, ErrInspectionRequired) {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	if starts.Load() != 1 {
		t.Fatalf("execution granted %d times", starts.Load())
	}
	other := proto.Clone(command).(*agentv1.Command)
	other.CommandId = uuid.NewString()
	other.Epoch.ClusterId = uuid.NewString()
	hash, _ := operation.CommandHash(other)
	other.InputHash = hash[:]
	activate(t, j, other.Epoch)
	if _, err := j.Admit(other); !errors.Is(err, ErrBusy) {
		t.Fatalf("queue bound ignored: %v", err)
	}
	result := &agentv1.TerminalResult{Epoch: command.Epoch, CommandId: command.CommandId, InputHash: command.InputHash, Sequence: 1, State: "succeeded"}
	if err := j.Complete(result); err != nil {
		t.Fatal(err)
	}
	if _, err := j.Admit(other); !errors.Is(err, ErrBusy) {
		t.Fatalf("unacknowledged result evicted: %v", err)
	}
	if err := j.Acknowledge(&agentv1.ResultAck{Epoch: command.Epoch, CommandId: command.CommandId, Sequence: 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := j.Admit(other); err != nil {
		t.Fatal(err)
	}
}

func TestMissingCorruptAndIdentityMismatch(t *testing.T) {
	j, options, _, path := fixture(t)
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	wrong := options
	wrong.Identity.NodeID = uuid.NewString()
	if opened, err := Open(path, wrong); !errors.Is(err, ErrRecoveryRequired) || opened != nil {
		t.Fatalf("wrong identity opened: %v", err)
	}
	missing := filepath.Join(t.TempDir(), "absent")
	options.EmptyInventory = false
	if opened, err := Open(missing, options); !errors.Is(err, ErrRecoveryRequired) || opened != nil {
		t.Fatalf("missing journal initialized with inventory: %v", err)
	}
	options.EmptyInventory = true
	unknown := t.TempDir()
	if err := os.WriteFile(filepath.Join(unknown, "unexpected"), []byte("retained"), 0600); err != nil {
		t.Fatal(err)
	}
	if opened, err := Open(unknown, options); !errors.Is(err, ErrRecoveryRequired) || opened != nil {
		t.Fatalf("inconsistent path initialized: %v", err)
	}
	db, err := pebble.Open(path, &pebble.Options{ErrorIfNotExists: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Set(stateKey, []byte(`{"version":99}`), pebble.Sync); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if opened, err := Open(path, options); !errors.Is(err, ErrRecoveryRequired) || opened != nil {
		t.Fatalf("corruption accepted: %v", err)
	}
}

func TestProcessInterruption(t *testing.T) {
	if os.Getenv("SYNOD_JOURNAL_CHILD") == "1" {
		var options Options
		if err := json.Unmarshal([]byte(os.Getenv("SYNOD_JOURNAL_OPTIONS")), &options); err != nil {
			t.Fatal(err)
		}
		j, err := Open(os.Getenv("SYNOD_JOURNAL_PATH"), options)
		if err != nil {
			t.Fatal(err)
		}
		r, err := j.Start(os.Getenv("SYNOD_JOURNAL_COMMAND"))
		if err != nil {
			t.Fatal(err)
		}
		effect := &agentv1.Effect{CommandId: r.CommandID, ResourceId: r.Command.Input.ResourceId, NativeOperationId: "surviving-native-task", Active: true, OutcomeUnknown: true}
		if err := j.Observe(r.CommandID, []*agentv1.Effect{effect}); err != nil {
			t.Fatal(err)
		}
		next := proto.Clone(r.Epoch).(*agentv1.Epoch)
		next.ExecutionEpoch++
		if err := j.Prepare(next); err != nil {
			t.Fatal(err)
		}
		fmt.Println("DURABLE")
		time.Sleep(time.Hour)
		return
	}
	j, options, command, path := fixture(t)
	if _, err := j.Admit(command); err != nil {
		t.Fatal(err)
	}
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	encoded, _ := json.Marshal(options)
	child := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestProcessInterruption$")
	child.Env = append(os.Environ(), "SYNOD_JOURNAL_CHILD=1", "SYNOD_JOURNAL_OPTIONS="+string(encoded), "SYNOD_JOURNAL_PATH="+path, "SYNOD_JOURNAL_COMMAND="+command.CommandId)
	stdout, err := child.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	var stderr bytes.Buffer
	child.Stderr = &stderr
	if err := child.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if child.ProcessState == nil {
			_ = child.Process.Kill()
			_ = child.Wait()
		}
	})
	scanner := bufio.NewScanner(stdout)
	if !scanner.Scan() || scanner.Text() != "DURABLE" {
		t.Fatalf("child did not reach durable boundary: %s %s", scanner.Text(), stderr.String())
	}
	if err := child.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	if err := child.Wait(); err == nil {
		t.Fatal("child exited normally")
	}
	reopened, err := Open(path, options)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	record, err := reopened.Get(command.CommandId)
	if err != nil || record.State != Running || len(record.Effects) != 1 || !record.Effects[0].Active {
		t.Fatalf("crash state: %+v %v", record, err)
	}
	if _, err := reopened.Start(command.CommandId); !errors.Is(err, ErrInspectionRequired) {
		t.Fatalf("crash repeated execution: %v", err)
	}
	if _, err := reopened.Admit(command); !errors.Is(err, operation.ErrStaleOwner) {
		t.Fatalf("prepared epoch lost on crash: %v", err)
	}
}

func TestUntrackedEffectsSurviveFurtherTakeover(t *testing.T) {
	j, _, command, _ := fixture(t)
	next := proto.Clone(command.Epoch).(*agentv1.Epoch)
	next.ExecutionEpoch = 2
	if err := j.Prepare(next); err != nil {
		t.Fatal(err)
	}
	effect := &agentv1.Effect{CommandId: uuid.NewString(), ResourceId: uuid.NewString(), NativeOperationId: "orphan-task", Active: true, OutcomeUnknown: true}
	if err := j.RecordBarrier(&agentv1.BarrierResult{Epoch: next, AdmissionsClosed: true, Effects: []*agentv1.Effect{effect}}); err != nil {
		t.Fatal(err)
	}
	later := proto.Clone(next).(*agentv1.Epoch)
	later.ExecutionEpoch = 3
	if err := j.Prepare(later); err != nil {
		t.Fatal(err)
	}
	barrier, err := j.Barrier(later)
	if err != nil || len(barrier.Effects) != 1 || !barrier.Effects[0].Active {
		t.Fatalf("untracked effect lost on takeover: %+v %v", barrier, err)
	}
	if err := j.RecordBarrier(&agentv1.BarrierResult{Epoch: later, AdmissionsClosed: true, Drained: true}); !errors.Is(err, operation.ErrBarrierPending) {
		t.Fatalf("untracked effect omitted: %v", err)
	}
	effect.Active = false
	effect.OutcomeUnknown = false
	if err := j.RecordBarrier(&agentv1.BarrierResult{Epoch: later, AdmissionsClosed: true, Drained: true, Effects: []*agentv1.Effect{effect}}); err != nil {
		t.Fatal(err)
	}
	if err := j.Activate(later, true); err != nil {
		t.Fatal(err)
	}
}

func TestByteBoundsAndRetainedPayloads(t *testing.T) {
	j, _, command, _ := fixture(t)
	j.options.MaxCommandBytes = proto.Size(command) - 1
	if _, err := j.Admit(command); !errors.Is(err, ErrBusy) {
		t.Fatalf("oversize command admitted: %v", err)
	}
	j.options.MaxCommandBytes = 1 << 20
	j.options.MaxRetainedBytes = int64(proto.Size(command)+2*j.options.MaxResultBytes) - 1
	if _, err := j.Admit(command); !errors.Is(err, ErrBusy) {
		t.Fatalf("result reservation ignored: %v", err)
	}
	j.options.MaxRetainedBytes = 1 << 20
	if _, err := j.Admit(command); err != nil {
		t.Fatal(err)
	}
	if _, err := j.Start(command.CommandId); err != nil {
		t.Fatal(err)
	}
	result := &agentv1.TerminalResult{Epoch: command.Epoch, CommandId: command.CommandId, InputHash: command.InputHash, Sequence: 1, State: "succeeded"}
	if err := j.Complete(result); err != nil {
		t.Fatal(err)
	}
	if err := j.Acknowledge(&agentv1.ResultAck{Epoch: command.Epoch, CommandId: command.CommandId, Sequence: 1}); err != nil {
		t.Fatal(err)
	}
	other := proto.Clone(command).(*agentv1.Command)
	other.CommandId = uuid.NewString()
	other.Epoch.ClusterId = uuid.NewString()
	hash, _ := operation.CommandHash(other)
	other.InputHash = hash[:]
	activate(t, j, other.Epoch)
	j.options.MaxRetainedBytes = int64(proto.Size(other) + 2*j.options.MaxResultBytes)
	if _, err := j.Admit(other); !errors.Is(err, ErrBusy) {
		t.Fatalf("acknowledged recovery-window payloads ignored: %v", err)
	}
	if _, err := j.Compact(time.Now().Add(2 * time.Minute)); err != nil {
		t.Fatal(err)
	}
	if _, err := j.Admit(other); err != nil {
		t.Fatal(err)
	}
}

func TestPhysicalJournalCorruptionFailsClosed(t *testing.T) {
	j, options, _, path := fixture(t)
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	manifests, err := filepath.Glob(filepath.Join(path, "MANIFEST-*"))
	if err != nil || len(manifests) == 0 {
		t.Fatalf("find real Pebble manifest: %v", err)
	}
	for _, manifest := range manifests {
		if err := os.WriteFile(manifest, []byte("corrupted durable manifest"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	if opened, err := Open(path, options); !errors.Is(err, ErrRecoveryRequired) || opened != nil {
		t.Fatalf("physical corruption accepted: %v", err)
	}
}

func TestStartRejectsLateNativeEffect(t *testing.T) {
	j, options, command, path := fixture(t)
	if _, err := j.Admit(command); err != nil {
		t.Fatal(err)
	}
	if _, err := j.Start(command.CommandId); err != nil {
		t.Fatal(err)
	}
	if err := j.Complete(&agentv1.TerminalResult{Epoch: command.Epoch, CommandId: command.CommandId, InputHash: command.InputHash, Sequence: 1, State: "succeeded"}); err != nil {
		t.Fatal(err)
	}
	next := proto.Clone(command).(*agentv1.Command)
	next.CommandId = uuid.NewString()
	hash, err := operation.CommandHash(next)
	if err != nil {
		t.Fatal(err)
	}
	next.InputHash = hash[:]
	if _, err := j.Admit(next); err != nil {
		t.Fatal(err)
	}
	effect := &agentv1.Effect{CommandId: command.CommandId, ResourceId: command.Input.ResourceId, NativeOperationId: "late-native-task", Active: true, OutcomeUnknown: true}
	if err := j.Observe(command.CommandId, []*agentv1.Effect{effect}); err != nil {
		t.Fatal(err)
	}
	if _, err := j.Start(next.CommandId); !errors.Is(err, ErrBusy) {
		t.Fatalf("late active task allowed execution: %v", err)
	}
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path, options)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if _, err := reopened.Start(next.CommandId); !errors.Is(err, ErrBusy) {
		t.Fatalf("reopen lost native exclusion: %v", err)
	}
	effect.Active = false
	if err := reopened.Observe(command.CommandId, []*agentv1.Effect{effect}); err != nil {
		t.Fatal(err)
	}
	if _, err := reopened.Start(next.CommandId); !errors.Is(err, ErrBusy) {
		t.Fatalf("unknown outcome allowed execution: %v", err)
	}
	effect.OutcomeUnknown = false
	if err := reopened.Observe(command.CommandId, []*agentv1.Effect{effect}); err != nil {
		t.Fatal(err)
	}
	if _, err := reopened.Start(next.CommandId); err != nil {
		t.Fatalf("resolved observation blocked execution: %v", err)
	}
}
