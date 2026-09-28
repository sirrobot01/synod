package journal

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/operation"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/proto"
)

// Legacy fixtures use the real previous layout in the same Pebble database.
func writeLegacy(t *testing.T, j *Journal, path string) {
	t.Helper()
	legacy := j.state
	legacy.Version = 1
	legacy.Commands = maps.Clone(legacy.Commands)
	legacy.Apps = maps.Clone(legacy.Apps)
	for id, record := range legacy.Commands {
		record.CommandLimit, record.ResultLimit, record.ReservedBytes = 0, 0, 0
		legacy.Commands[id] = record
	}
	for id, app := range legacy.Apps {
		app.InventoryLimit, app.ReservedBytes = 0, 0
		legacy.Apps[id] = app
	}
	data, err := json.Marshal(legacy)
	if err != nil {
		t.Fatal(err)
	}
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	db, err := pebble.Open(path, &pebble.Options{ErrorIfNotExists: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	batch := db.NewBatch()
	defer batch.Close()
	if err := batch.DeleteRange([]byte("synod/"), []byte("synoe/"), nil); err != nil {
		t.Fatal(err)
	}
	if err := batch.Set(stateKey, data, nil); err != nil {
		t.Fatal(err)
	}
	if err := batch.Commit(pebble.Sync); err != nil {
		t.Fatal(err)
	}
}

func TestCompletionReservationSurvivesReopen(t *testing.T) {
	j, _, command, path := fixture(t)
	u, err := j.Usage()
	if err != nil {
		t.Fatal(err)
	}
	j.options.MaxStoredBytes = u.ReservedBytes + commandReservation(j.options.MaxCommandBytes, j.options.MaxResultBytes)
	j.options.MaxRecords = 1
	options := j.options
	if _, err := j.Admit(command); err != nil {
		t.Fatal(err)
	}
	if _, err := j.Start(command.CommandId); err != nil {
		t.Fatal(err)
	}
	u, _ = j.Usage()
	if u.ReservedBytes != options.MaxStoredBytes {
		t.Fatalf("reservation: %+v", u)
	}
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	// Lower limits apply to new work. This command keeps its admitted limits.
	options.MaxCommandBytes, options.MaxResultBytes, options.MaxInventoryBytes, options.MaxRecordBytes = 128, 128, 128, 8192
	reopened, err := Open(path, options)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if record, err := reopened.Admit(command); err != nil || record.State != Running {
		t.Fatalf("old admission limits lost: %v", err)
	}
	result := &agentv1.TerminalResult{Epoch: command.Epoch, CommandId: command.CommandId, InputHash: command.InputHash, Sequence: 1, State: "succeeded", FailureCode: strings.Repeat("x", 500)}
	if err := reopened.Complete(result); err != nil {
		t.Fatalf("reserved completion: %v", err)
	}
	if err := reopened.Acknowledge(&agentv1.ResultAck{Epoch: command.Epoch, CommandId: command.CommandId, Sequence: 1}); err != nil {
		t.Fatalf("reserved acknowledgement: %v", err)
	}
	u, _ = reopened.Usage()
	if u.ReservedBytes != options.MaxStoredBytes || u.LogicalStoredBytes >= u.ReservedBytes {
		t.Fatalf("accounting: %+v", u)
	}
	if _, err := reopened.Compact(time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	u, _ = reopened.Usage()
	if u.ReservedBytes >= options.MaxStoredBytes || u.Records != 1 {
		t.Fatalf("compaction lost reservation or tombstone: %+v", u)
	}
	other := proto.Clone(command).(*agentv1.Command)
	other.CommandId = uuid.NewString()
	hash, _ := operation.CommandHash(other)
	other.InputHash = hash[:]
	reopened.options.MaxCommandBytes = 1 << 20
	if _, err := reopened.Admit(other); !errors.Is(err, ErrBusy) {
		t.Fatalf("record limit ignored: %v", err)
	}
	if record, err := reopened.Admit(command); err != nil || record.State != Compacted {
		t.Fatalf("capacity deleted replay evidence: %+v %v", record, err)
	}
}

func TestInventoryAndNativeCapacityFailClosed(t *testing.T) {
	for _, boundary := range []string{"observation", "result", "inventory", "aggregate"} {
		t.Run(boundary, func(t *testing.T) {
			j, _, command, path := fixture(t)
			options := j.options
			if _, err := j.Admit(command); err != nil {
				t.Fatal(err)
			}
			if _, err := j.Start(command.CommandId); err != nil {
				t.Fatal(err)
			}
			var capacityErr error
			effect := &agentv1.Effect{CommandId: command.CommandId, ResourceId: command.Input.ResourceId, NativeOperationId: "task", Active: true, State: strings.Repeat("x", 2048)}
			switch boundary {
			case "observation":
				capacityErr = j.Observe(command.CommandId, []*agentv1.Effect{effect})
			case "result":
				capacityErr = j.Complete(&agentv1.TerminalResult{Epoch: command.Epoch, CommandId: command.CommandId, InputHash: command.InputHash, Sequence: 1, State: "failed", Effects: []*agentv1.Effect{effect}})
			case "inventory":
				epoch := proto.Clone(command.Epoch).(*agentv1.Epoch)
				epoch.ExecutionEpoch++
				if err := j.Prepare(epoch); err != nil {
					t.Fatal(err)
				}
				effect.State = strings.Repeat("x", options.MaxInventoryBytes)
				capacityErr = j.RecordBarrier(&agentv1.BarrierResult{Epoch: epoch, AdmissionsClosed: true, Effects: []*agentv1.Effect{effect}})
			case "aggregate":
				// Each input fits. Combining retained inventory and running evidence does not.
				epoch := proto.Clone(command.Epoch).(*agentv1.Epoch)
				epoch.ExecutionEpoch++
				if err := j.Prepare(epoch); err != nil {
					t.Fatal(err)
				}
				effect.State = ""
				report := &agentv1.BarrierResult{Epoch: epoch, AdmissionsClosed: true, Effects: []*agentv1.Effect{effect}}
				effect.State = strings.Repeat("x", options.MaxInventoryBytes-proto.Size(report)-32)
				if err := j.RecordBarrier(report); err != nil {
					t.Fatal(err)
				}
				_, capacityErr = j.Barrier(epoch)
			}
			if !errors.Is(capacityErr, ErrBusy) || !errors.Is(capacityErr, ErrRecoveryRequired) {
				t.Fatalf("capacity did not close execution: %v", capacityErr)
			}
			if err := j.Close(); err != nil {
				t.Fatal(err)
			}
			reopened, err := Open(path, options)
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			u, _ := reopened.Usage()
			if !u.ExecutionBlocked {
				t.Fatal("execution block lost on reopen")
			}
			if _, err := reopened.Start(command.CommandId); !errors.Is(err, ErrRecoveryRequired) {
				t.Fatalf("execution allowed: %v", err)
			}
			if err := reopened.Activate(command.Epoch, true); !errors.Is(err, ErrRecoveryRequired) {
				t.Fatalf("activation allowed: %v", err)
			}
			if _, err := reopened.Admit(command); !errors.Is(err, ErrRecoveryRequired) {
				t.Fatalf("admission allowed: %v", err)
			}
			if err := reopened.Complete(&agentv1.TerminalResult{Epoch: command.Epoch, CommandId: command.CommandId, InputHash: command.InputHash, Sequence: 1, State: "failed"}); err != nil {
				t.Fatalf("blocked reserved completion: %v", err)
			}
			if err := reopened.Acknowledge(&agentv1.ResultAck{Epoch: command.Epoch, CommandId: command.CommandId, Sequence: 1}); err != nil {
				t.Fatalf("blocked reserved acknowledgement: %v", err)
			}
		})
	}
}

func TestMigrationPreservesReplayAndAccounting(t *testing.T) {
	j, _, command, path := fixture(t)
	options := j.options
	var commands []*agentv1.Command
	for _, state := range []State{Compacted, Terminal, Running, Queued} {
		c := proto.Clone(command).(*agentv1.Command)
		c.CommandId, c.Epoch.AppId = uuid.NewString(), uuid.NewString()
		hash, _ := operation.CommandHash(c)
		c.InputHash = hash[:]
		activate(t, j, c.Epoch)
		if _, err := j.Admit(c); err != nil {
			t.Fatal(err)
		}
		if state != Queued {
			if _, err := j.Start(c.CommandId); err != nil {
				t.Fatal(err)
			}
		}
		if state == Terminal || state == Compacted {
			if err := j.Complete(&agentv1.TerminalResult{Epoch: c.Epoch, CommandId: c.CommandId, InputHash: c.InputHash, Sequence: 3, State: "succeeded"}); err != nil {
				t.Fatal(err)
			}
		}
		if state == Compacted {
			if err := j.Acknowledge(&agentv1.ResultAck{Epoch: c.Epoch, CommandId: c.CommandId, Sequence: 3}); err != nil {
				t.Fatal(err)
			}
			if _, err := j.Compact(time.Now().Add(time.Hour)); err != nil {
				t.Fatal(err)
			}
		}
		if state == Running {
			if err := j.Observe(c.CommandId, []*agentv1.Effect{{CommandId: c.CommandId, ResourceId: c.Input.ResourceId, NativeOperationId: "retained-task", Active: true, OutcomeUnknown: true}}); err != nil {
				t.Fatal(err)
			}
			higher := proto.Clone(c.Epoch).(*agentv1.Epoch)
			higher.ExecutionEpoch++
			if err := j.Prepare(higher); err != nil {
				t.Fatal(err)
			}
		}
		commands = append(commands, c)
	}
	before := j.state
	writeLegacy(t, j, path)
	// Migration capacity failure must leave the old snapshot intact.
	tooSmall := options
	tooSmall.MaxRecords = 1
	if opened, err := Open(path, tooSmall); opened != nil || !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("oversize migration: %v", err)
	}
	db, err := pebble.Open(path, &pebble.Options{ErrorIfNotExists: true})
	if err != nil {
		t.Fatal(err)
	}
	_, closer, err := db.Get(stateKey)
	if err != nil {
		t.Fatalf("failed migration deleted legacy: %v", err)
	}
	if err := closer.Close(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	migrated, err := Open(path, options)
	if err != nil {
		t.Fatal(err)
	}
	defer migrated.Close()
	if migrated.state.Version != 2 || migrated.Identity() != before.Identity {
		t.Fatal("migration changed identity")
	}
	for _, c := range commands {
		want, _ := before.Commands[c.CommandId].view()
		got, err := migrated.Get(c.CommandId)
		if err != nil {
			t.Fatal(err)
		}
		wantJSON, _ := json.Marshal(want)
		gotJSON, _ := json.Marshal(got)
		if !bytes.Equal(wantJSON, gotJSON) {
			t.Fatalf("migration changed record %s", c.CommandId)
		}
	}
	if _, closer, err := migrated.db.Get(stateKey); !errors.Is(err, pebble.ErrNotFound) {
		if closer != nil {
			_ = closer.Close()
		}
		t.Fatalf("legacy key retained: %v", err)
	}
	u, _ := migrated.Usage()
	iter, err := migrated.db.NewIter(nil)
	if err != nil {
		t.Fatal(err)
	}
	var actual int64
	for iter.First(); iter.Valid(); iter.Next() {
		actual += int64(len(iter.Key()) + len(iter.Value()))
	}
	if err := iter.Error(); err != nil {
		t.Fatal(err)
	}
	if err := iter.Close(); err != nil {
		t.Fatal(err)
	}
	if actual != u.LogicalStoredBytes || u.Records != 4 || u.ReservedBytes < u.LogicalStoredBytes {
		t.Fatalf("accounting differs from Pebble: actual=%d %+v", actual, u)
	}
}

func TestStorageProcessInterruption(t *testing.T) {
	if stage := os.Getenv("SYNOD_STORAGE_CHILD"); stage != "" {
		var options Options
		if err := json.Unmarshal([]byte(os.Getenv("SYNOD_STORAGE_OPTIONS")), &options); err != nil {
			t.Fatal(err)
		}
		path := os.Getenv("SYNOD_STORAGE_PATH")
		if stage == "migration-before" {
			db, err := pebble.Open(path, &pebble.Options{ErrorIfNotExists: true})
			if err != nil {
				t.Fatal(err)
			}
			value, closer, err := db.Get(stateKey)
			if err != nil {
				t.Fatal(err)
			}
			j := &Journal{db: db, options: options, sizes: make(map[string]int64)}
			if err := decodeRecord(value, &j.state); err != nil {
				t.Fatal(err)
			}
			if err := closer.Close(); err != nil {
				t.Fatal(err)
			}
			if err := j.validate(); err != nil {
				t.Fatal(err)
			}
			if err := j.prepareMigration(); err != nil {
				t.Fatal(err)
			}
			batch, err := j.makeBatch(j.state.Commands, j.state.Apps, true)
			if err != nil {
				t.Fatal(err)
			}
			if err := batch.batch.Delete(stateKey, nil); err != nil {
				t.Fatal(err)
			}
			// The full migration batch exists, but Commit has not started.
		} else {
			j, err := Open(path, options)
			if err != nil {
				t.Fatal(err)
			}
			if stage != "migration-after" {
				r, err := j.Get(os.Getenv("SYNOD_STORAGE_COMMAND"))
				if err != nil {
					t.Fatal(err)
				}
				if stage == "terminal" {
					err = j.Complete(&agentv1.TerminalResult{Epoch: r.Epoch, CommandId: r.CommandID, InputHash: r.InputHash, Sequence: 7, State: "succeeded"})
				} else {
					err = j.Acknowledge(&agentv1.ResultAck{Epoch: r.Epoch, CommandId: r.CommandID, Sequence: 7})
				}
				if err != nil {
					t.Fatal(err)
				}
			}
		}
		fmt.Println("DURABLE")
		time.Sleep(time.Hour)
		return
	}
	for _, stage := range []string{"terminal", "acknowledgement", "migration-before", "migration-after"} {
		t.Run(stage, func(t *testing.T) {
			j, _, command, path := fixture(t)
			options := j.options
			if _, err := j.Admit(command); err != nil {
				t.Fatal(err)
			}
			if _, err := j.Start(command.CommandId); err != nil {
				t.Fatal(err)
			}
			if stage != "terminal" {
				if err := j.Complete(&agentv1.TerminalResult{Epoch: command.Epoch, CommandId: command.CommandId, InputHash: command.InputHash, Sequence: 7, State: "succeeded"}); err != nil {
					t.Fatal(err)
				}
			}
			if strings.HasPrefix(stage, "migration") {
				writeLegacy(t, j, path)
			} else {
				if err := j.Close(); err != nil {
					t.Fatal(err)
				}
			}
			encoded, _ := json.Marshal(options)
			child := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestStorageProcessInterruption$")
			child.Env = append(os.Environ(), "SYNOD_STORAGE_CHILD="+stage, "SYNOD_STORAGE_OPTIONS="+string(encoded), "SYNOD_STORAGE_PATH="+path, "SYNOD_STORAGE_COMMAND="+command.CommandId)
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
				t.Fatalf("boundary failed: %s %s", scanner.Text(), stderr.String())
			}
			if err := child.Process.Kill(); err != nil {
				t.Fatal(err)
			}
			if err := child.Wait(); err == nil {
				t.Fatal("child exited normally")
			}
			if stage == "migration-before" {
				db, err := pebble.Open(path, &pebble.Options{ErrorIfNotExists: true})
				if err != nil {
					t.Fatal(err)
				}
				_, closer, err := db.Get(stateKey)
				if err != nil {
					t.Fatalf("uncommitted migration lost old layout: %v", err)
				}
				_ = closer.Close()
				if _, closer, err := db.Get([]byte(metadataKey)); !errors.Is(err, pebble.ErrNotFound) {
					if closer != nil {
						_ = closer.Close()
					}
					t.Fatalf("partial migration visible: %v", err)
				}
				if err := db.Close(); err != nil {
					t.Fatal(err)
				}
			}
			reopened, err := Open(path, options)
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			r, err := reopened.Get(command.CommandId)
			if err != nil || r.State != Terminal || r.Result.Sequence != 7 || r.Acknowledged != (stage == "acknowledgement") {
				t.Fatalf("crash lost result/ack: %+v %v", r, err)
			}
			if _, err := reopened.Start(command.CommandId); !errors.Is(err, ErrAlreadyCompleted) {
				t.Fatalf("crash repeated effect: %v", err)
			}
			results, err := reopened.Results()
			if err != nil {
				t.Fatal(err)
			}
			if (len(results) == 0) != (stage == "acknowledgement") {
				t.Fatalf("crash changed delivery state: %d", len(results))
			}
		})
	}
}

func TestRecordGrowth(t *testing.T) {
	j, _, template, path := fixture(t)
	options := j.options
	t.Log("completed,logical_bytes,reserved_bytes,physical_bytes,max_transition_bytes,max_transition_records")
	maxWrite, maxRecords := 0, 0
	for i := range 1000 {
		command := proto.Clone(template).(*agentv1.Command)
		command.CommandId = uuid.NewString()
		hash, _ := operation.CommandHash(command)
		command.InputHash = hash[:]
		if _, err := j.Admit(command); err != nil {
			t.Fatal(err)
		}
		u, _ := j.Usage()
		maxWrite = max(maxWrite, u.LastWriteBytes)
		maxRecords = max(maxRecords, u.LastWriteRecords)
		if _, err := j.Start(command.CommandId); err != nil {
			t.Fatal(err)
		}
		if err := j.Complete(&agentv1.TerminalResult{Epoch: command.Epoch, CommandId: command.CommandId, InputHash: command.InputHash, Sequence: 1, State: "succeeded"}); err != nil {
			t.Fatal(err)
		}
		u, _ = j.Usage()
		maxWrite = max(maxWrite, u.LastWriteBytes)
		maxRecords = max(maxRecords, u.LastWriteRecords)
		if err := j.Acknowledge(&agentv1.ResultAck{Epoch: command.Epoch, CommandId: command.CommandId, Sequence: 1}); err != nil {
			t.Fatal(err)
		}
		u, _ = j.Usage()
		maxWrite = max(maxWrite, u.LastWriteBytes)
		maxRecords = max(maxRecords, u.LastWriteRecords)
		if count, err := j.Compact(time.Now().Add(time.Hour)); err != nil || count != 1 {
			t.Fatalf("compact: %d %v", count, err)
		}
		if i == 0 || i == 99 || i == 499 || i == 999 {
			u, _ = j.Usage()
			t.Logf("%d,%d,%d,%d,%d,%d", i+1, u.LogicalStoredBytes, u.ReservedBytes, u.PhysicalDiskBytes, maxWrite, maxRecords)
		}
	}
	if maxRecords != 1 || maxWrite > 4096 {
		t.Fatalf("write work grew with history: records=%d bytes=%d", maxRecords, maxWrite)
	}
	u, _ := j.Usage()
	if u.Records != 1000 {
		t.Fatalf("growth lost tombstones: %+v", u)
	}
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path, options)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	after, _ := reopened.Usage()
	if after.Records != 1000 || after.LogicalStoredBytes != u.LogicalStoredBytes || after.ReservedBytes != u.ReservedBytes {
		t.Fatalf("growth reopen changed accounting: before=%+v after=%+v", u, after)
	}
}

func TestStoredAndAppCapacity(t *testing.T) {
	j, _, command, _ := fixture(t)
	u, _ := j.Usage()
	j.options.MaxStoredBytes = u.ReservedBytes + commandReservation(j.options.MaxCommandBytes, j.options.MaxResultBytes) + 4096
	j.options.MaxApps = 1
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
	hash, _ := operation.CommandHash(other)
	other.InputHash = hash[:]
	if _, err := j.Admit(other); !errors.Is(err, ErrBusy) {
		t.Fatalf("stored capacity ignored: %v", err)
	}
	u, _ = j.Usage()
	if u.ExecutionBlocked || u.Records != 1 {
		t.Fatalf("admission failure changed evidence: %+v", u)
	}
	foreign := proto.Clone(command.Epoch).(*agentv1.Epoch)
	foreign.AppId = uuid.NewString()
	if err := j.Prepare(foreign); !errors.Is(err, ErrBusy) {
		t.Fatalf("app capacity ignored: %v", err)
	}
	if err := j.Complete(result); err != nil {
		t.Fatalf("capacity blocked result replay: %v", err)
	}
	if _, err := j.Compact(time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if _, err := j.Admit(other); err != nil {
		t.Fatalf("compaction did not release reserved growth: %v", err)
	}
	if r, err := j.Get(command.CommandId); err != nil || r.State != Compacted {
		t.Fatal("capacity removed tombstone")
	}
}

func TestUnsupportedFormatAndReservationCorruption(t *testing.T) {
	for _, which := range []string{"version", "reservation", "mixed-empty"} {
		t.Run(which, func(t *testing.T) {
			j, options, command, path := fixture(t)
			if _, err := j.Admit(command); err != nil {
				t.Fatal(err)
			}
			if which == "mixed-empty" {
				if err := j.db.Set(stateKey, nil, pebble.Sync); err != nil {
					t.Fatal(err)
				}
			} else if which == "version" {
				data, _ := json.Marshal(metadata{Version: 3, Identity: j.Identity()})
				if err := j.db.Set([]byte(metadataKey), data, pebble.Sync); err != nil {
					t.Fatal(err)
				}
			} else {
				record := j.state.Commands[command.CommandId]
				record.ReservedBytes--
				data, _ := json.Marshal(storedCommand{Record: record})
				if err := j.db.Set([]byte(commandPrefix+command.CommandId), data, pebble.Sync); err != nil {
					t.Fatal(err)
				}
			}
			if err := j.Close(); err != nil {
				t.Fatal(err)
			}
			if opened, err := Open(path, options); opened != nil || !errors.Is(err, ErrRecoveryRequired) {
				t.Fatalf("invalid %s opened: %v", which, err)
			}
		})
	}
}

func TestCompactionBatchLimit(t *testing.T) {
	j, _, template, _ := fixture(t)
	for range 257 {
		command := proto.Clone(template).(*agentv1.Command)
		command.CommandId = uuid.NewString()
		hash, _ := operation.CommandHash(command)
		command.InputHash = hash[:]
		if _, err := j.Admit(command); err != nil {
			t.Fatal(err)
		}
		if _, err := j.Start(command.CommandId); err != nil {
			t.Fatal(err)
		}
		if err := j.Complete(&agentv1.TerminalResult{Epoch: command.Epoch, CommandId: command.CommandId, InputHash: command.InputHash, Sequence: 1, State: "succeeded"}); err != nil {
			t.Fatal(err)
		}
		if err := j.Acknowledge(&agentv1.ResultAck{Epoch: command.Epoch, CommandId: command.CommandId, Sequence: 1}); err != nil {
			t.Fatal(err)
		}
	}
	if count, err := j.Compact(time.Now().Add(time.Hour)); err != nil || count != 256 {
		t.Fatalf("first bounded batch: count=%d err=%v", count, err)
	}
	u, _ := j.Usage()
	if u.LastWriteRecords != 256 || u.Records != 257 {
		t.Fatalf("compaction batch/evidence: %+v", u)
	}
	if count, err := j.Compact(time.Now().Add(time.Hour)); err != nil || count != 1 {
		t.Fatalf("second bounded batch: count=%d err=%v", count, err)
	}
	if count, err := j.Compact(time.Now().Add(time.Hour)); err != nil || count != 0 {
		t.Fatalf("repeat compaction: count=%d err=%v", count, err)
	}
}
