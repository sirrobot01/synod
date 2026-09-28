package executor

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/agent/replica"
	"github.com/sirrobot01/synod/internal/agent/replication"
	"github.com/sirrobot01/synod/internal/operation"
	"github.com/sirrobot01/synod/internal/runtime"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestFinalSnapshotKeepsProofAcrossLostReplyAndRestart(t *testing.T) {
	appID, source, target, resource := uuid.NewString(), uuid.NewString(), uuid.NewString(), uuid.NewString()
	table := &replication.Table{}
	table.Set([]*agentv1.AppReplication{{AppId: appID, Role: "active", Epoch: 3, Peers: []*agentv1.ReplicaPeer{{NodeId: target, Address: "192.0.2.1:8445"}}}})
	native := &writerTestRuntime{app: runtime.AppContainer{AppID: appID, ResourceID: resource, Running: true}}
	e := &Executor{runtime: native, options: Options{AppsDir: t.TempDir(), Replication: table, BootTime: func() (time.Duration, error) { return 0, nil }}, leases: map[string]writerLease{appID: {epoch: 3, expires: time.Minute}}}
	root := filepath.Join(e.options.AppsDir, appID)
	if err := os.MkdirAll(filepath.Join(root, "live", "config"), 0700); err != nil {
		t.Fatal(err)
	}
	for name, value := range map[string]string{"epoch": "3", "sequence": "7", "live/config/app.db": "every committed write"} {
		if err := os.WriteFile(filepath.Join(root, name), []byte(value), 0600); err != nil {
			t.Fatal(err)
		}
	}
	command := &agentv1.Command{Epoch: &agentv1.Epoch{InstallationId: uuid.NewString(), NodeId: source, AppId: appID, RecoveryGeneration: uuid.NewString(), ExecutionEpoch: 1}, OperationId: uuid.NewString(), CommandId: uuid.NewString(), StepId: uuid.NewString(), DesiredRevision: 2, PlanVersion: 1, Input: &agentv1.CommandInput{ResourceId: resource, PayloadVersion: 1, Timeout: durationpb.New(time.Minute), Payload: &agentv1.CommandInput_FinalAppSnapshot{FinalAppSnapshot: &agentv1.FinalAppSnapshot{WriterEpoch: 3, TargetNodeId: target}}}}
	hash, err := operation.CommandHash(command)
	if err != nil {
		t.Fatal(err)
	}
	command.InputHash = hash[:]
	receiver := replica.Replica{Root: t.TempDir()}
	calls := 0
	e.options.PushSnapshot = func(_ context.Context, peer *agentv1.ReplicaPeer, app string, m *replica.Manifest, path string) (*agentv1.PushResult, error) {
		calls++
		if peer.NodeId != target || app != appID {
			t.Fatal("wrong transfer target")
		}
		if calls == 1 {
			if err := receiver.Apply(m, func(ref replica.BlockRef) ([]byte, error) { return os.ReadFile(filepath.Join(path, ref.Path)) }); err != nil {
				t.Fatal(err)
			}
			return nil, errors.New("reply lost")
		}
		return &agentv1.PushResult{Epoch: m.Epoch, Sequence: m.Sequence}, nil
	}
	if _, err := e.finalAppSnapshot(t.Context(), command); !errors.Is(err, runtime.ErrBusy) || calls != 0 {
		t.Fatalf("running app snapshot: %v %d", err, calls)
	}
	native.app.Running = false
	result, err := e.finalAppSnapshot(t.Context(), command)
	if err != nil || calls != 2 {
		t.Fatalf("final copy: %+v %v calls=%d", result, err, calls)
	}
	step := &agentv1.PlanStep{Command: command, Postcondition: agentv1.Postcondition_POSTCONDITION_APP_SNAPSHOT_TRANSFERRED}
	if err := operation.CheckStepEffects(step, result.Effects); err != nil {
		t.Fatal(err)
	}
	if got, err := os.ReadFile(filepath.Join(receiver.Root, "replica", "config", "app.db")); err != nil || string(got) != "every committed write" {
		t.Fatalf("lost write: %q %v", got, err)
	}
	restarted := &Executor{runtime: native, options: Options{AppsDir: e.options.AppsDir}}
	recovered, err := restarted.recoverAppData(t.Context(), command)
	if err != nil || !proto.Equal(result.Effects[0], recovered.Effects[0]) {
		t.Fatalf("lost durable proof: %+v %v", recovered, err)
	}
	changed := proto.Clone(command).(*agentv1.Command)
	changed.InputHash[0] ^= 1
	if _, err := restarted.recoverAppData(t.Context(), changed); !errors.Is(err, operation.ErrCommandConflict) {
		t.Fatalf("changed command used receipt: %v", err)
	}
	cleanup := proto.Clone(command).(*agentv1.Command)
	cleanup.CommandId = uuid.NewString()
	cleanup.Input.Payload = &agentv1.CommandInput_RemoveAppData{RemoveAppData: &agentv1.RemoveAppData{WriterEpoch: 3}}
	if _, err := e.removeAppData(t.Context(), cleanup); !errors.Is(err, runtime.ErrBusy) {
		t.Fatalf("assigned app removed: %v", err)
	}
	table.Set(nil)
	if _, err := e.removeAppData(t.Context(), cleanup); !errors.Is(err, runtime.ErrBusy) {
		t.Fatalf("container data removed: %v", err)
	}
	native.app = runtime.AppContainer{}
	external := filepath.Join(t.TempDir(), "keep")
	if err := os.WriteFile(external, []byte("external"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(external, filepath.Join(root, "live", "config", "external")); err != nil {
		t.Fatal(err)
	}
	if _, err := e.removeAppData(t.Context(), cleanup); err != nil {
		t.Fatal(err)
	}
	if _, err := restarted.recoverAppData(t.Context(), cleanup); err != nil {
		t.Fatal(err)
	}
	if got, err := os.ReadFile(external); err != nil || string(got) != "external" {
		t.Fatalf("external file changed: %q %v", got, err)
	}
}

func TestStaleLiveWaitsForCompleteCurrentReplica(t *testing.T) {
	appID, peer := uuid.NewString(), uuid.NewString()
	table := &replication.Table{}
	table.Set([]*agentv1.AppReplication{{AppId: appID, Role: "standby", Epoch: 2, Peers: []*agentv1.ReplicaPeer{{NodeId: peer, Address: "192.0.2.1:8445"}}}})
	native := &writerTestRuntime{app: runtime.AppContainer{AppID: appID, Running: false}}
	e := &Executor{runtime: native, options: Options{AppsDir: t.TempDir(), Replication: table}}
	root := filepath.Join(e.options.AppsDir, appID)
	if err := os.MkdirAll(filepath.Join(root, "live", "config"), 0700); err != nil {
		t.Fatal(err)
	}
	for name, data := range map[string]string{"epoch": "1", "live/config/state": "old"} {
		if err := os.WriteFile(filepath.Join(root, name), []byte(data), 0600); err != nil {
			t.Fatal(err)
		}
	}
	if err := e.discardStaleLive(t.Context(), appID); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, "live", "config", "state")); err != nil {
		t.Fatal("old data removed without a replica")
	}
	snapshot := t.TempDir()
	manifest, err := (&replica.Snapshotter{Live: filepath.Join(root, "live"), Snapshot: snapshot}).TakeStopped(t.Context(), 2, 1)
	if err != nil {
		t.Fatal(err)
	}
	store := replica.Replica{Root: root}
	if err := store.Apply(manifest, func(ref replica.BlockRef) ([]byte, error) { return os.ReadFile(filepath.Join(snapshot, ref.Path)) }); err != nil {
		t.Fatal(err)
	}
	native.app.Running = true
	if err := e.discardStaleLive(t.Context(), appID); !errors.Is(err, runtime.ErrBusy) {
		t.Fatalf("removed running data: %v", err)
	}
	native.app.Running = false
	if err := e.discardStaleLive(t.Context(), appID); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, "live")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stale live remains: %v", err)
	}
	if err := store.Verify(t.Context()); err != nil {
		t.Fatalf("cleanup damaged replica: %v", err)
	}
}

func TestDatabaseRebuildRequiresAbsentContainerAndRemovesOldEpoch(t *testing.T) {
	appID := uuid.NewString()
	native := &writerTestRuntime{app: runtime.AppContainer{AppID: appID}}
	e := &Executor{runtime: native, options: Options{AppsDir: t.TempDir(), Replication: &replication.Table{}}}
	root := filepath.Join(e.options.AppsDir, appID)
	if err := os.MkdirAll(filepath.Join(root, "engine", "data"), 0700); err != nil {
		t.Fatal(err)
	}
	for name, value := range map[string]string{"database-primary-epoch": "2", "database-write-pause": "2", "engine/data/divergent": "old primary writes"} {
		if err := os.WriteFile(filepath.Join(root, name), []byte(value), 0600); err != nil {
			t.Fatal(err)
		}
	}
	command := &agentv1.Command{Epoch: &agentv1.Epoch{AppId: appID}, CommandId: uuid.NewString(), Input: &agentv1.CommandInput{ResourceId: uuid.NewString(), Payload: &agentv1.CommandInput_RemoveAppData{RemoveAppData: &agentv1.RemoveAppData{WriterEpoch: 2}}}}
	if _, err := e.removeAppData(t.Context(), command); !errors.Is(err, runtime.ErrBusy) {
		t.Fatalf("removed mounted database data: %v", err)
	}
	native.app = runtime.AppContainer{}
	command.Input.GetRemoveAppData().WriterEpoch = 1
	if _, err := e.removeAppData(t.Context(), command); !errors.Is(err, operation.ErrStaleOwner) {
		t.Fatalf("old command removed new data: %v", err)
	}
	command.Input.GetRemoveAppData().WriterEpoch = 2
	if _, err := e.removeAppData(t.Context(), command); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(root); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("old primary data remains: %v", err)
	}
	if _, err := e.recoverAppData(t.Context(), command); err != nil {
		t.Fatalf("lost removal reply: %v", err)
	}
}
