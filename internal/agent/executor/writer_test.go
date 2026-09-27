package executor

import (
	"context"
	"crypto/sha256"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/moby/moby/client"
	"github.com/sirrobot01/synod/internal/agent/journal"
	"github.com/sirrobot01/synod/internal/agent/replica"
	"github.com/sirrobot01/synod/internal/agent/replication"
	"github.com/sirrobot01/synod/internal/controlstream"
	"github.com/sirrobot01/synod/internal/operation"
	"github.com/sirrobot01/synod/internal/runtime"
	"github.com/sirrobot01/synod/internal/runtime/docker"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/proto"
)

type writerTestRuntime struct {
	runtime.Runtime
	mu      sync.Mutex
	app     runtime.AppContainer
	stopped chan struct{}
	once    sync.Once
}

func (r *writerTestRuntime) AppContainers(context.Context) ([]runtime.AppContainer, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return []runtime.AppContainer{r.app}, nil
}
func (r *writerTestRuntime) FenceApp(_ context.Context, _ runtime.AppContainer) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.app.Running = false
	r.once.Do(func() { close(r.stopped) })
	return nil
}

type writerTestFreezer struct {
	err   error
	thaws int
}

func (f *writerTestFreezer) Freeze(context.Context) error { return f.err }
func (f *writerTestFreezer) Thaw(context.Context) error   { f.thaws++; return nil }

func TestWriterLeaseFencesAfterSuspendAndDisconnect(t *testing.T) {
	var now atomic.Int64
	appID := uuid.NewString()
	table := &replication.Table{}
	table.Set([]*agentv1.AppReplication{{AppId: appID, Epoch: 1, Role: replication.RoleActive}})
	native := &writerTestRuntime{app: runtime.AppContainer{AppID: appID, ResourceID: uuid.NewString(), Running: true}, stopped: make(chan struct{})}
	e := &Executor{runtime: native, options: Options{AppsDir: t.TempDir(), Replication: table, BootTime: func() (time.Duration, error) { return time.Duration(now.Load()), nil }, InspectionTimeout: time.Second}}
	if err := os.MkdirAll(filepath.Join(e.options.AppsDir, appID), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(e.options.AppsDir, appID, "epoch"), []byte("1"), 0o600); err != nil {
		t.Fatal(err)
	}
	session, disconnect := localSession(t, e, replication.Capability)
	grant := &agentv1.ControllerMessage{Message: &agentv1.ControllerMessage_WriterLease{WriterLease: &agentv1.WriterLease{SessionId: e.session.id, AppId: appID, Epoch: 1, Seconds: 30, ExpiresAtBootNanos: uint64(30 * time.Second)}}}
	if _, err := e.Handle(session, grant); err != nil {
		t.Fatal(err)
	}
	now.Store(int64(20 * time.Second))
	grant.GetWriterLease().ExpiresAtBootNanos = uint64(50 * time.Second)
	if _, err := e.Handle(session, grant); err != nil {
		t.Fatal(err)
	}
	disconnect()
	now.Store(int64(35 * time.Second))
	if err := e.checkWriter(appID); err != nil {
		t.Fatal("renewal was not retained across disconnect", err)
	}
	if _, err := e.Handle(session, grant); !errors.Is(err, controlstream.ErrDisconnected) {
		t.Fatal("disconnected session renewed its lease", err)
	}
	// Simulate suspend without waiting for wall time. CLOCK_BOOTTIME includes this interval.
	now.Store(int64(51 * time.Second))
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() { defer close(done); e.watchWriters(ctx) }()
	select {
	case <-native.stopped:
	case <-time.After(2 * time.Second):
		t.Fatal("expired writer was not fenced")
	}
	cancel()
	<-done
	if err := e.checkWriter(appID); !errors.Is(err, errWriterLease) {
		t.Fatalf("expired lease: %v", err)
	}
}

func TestWriterLeaseRejectsWrongAssignmentAndSession(t *testing.T) {
	table := &replication.Table{}
	appID := uuid.NewString()
	assignment := &agentv1.AppReplication{AppId: appID, Epoch: 2, Role: replication.RoleActive}
	table.Set([]*agentv1.AppReplication{assignment})
	e := &Executor{options: Options{Replication: table, BootTime: func() (time.Duration, error) { return 0, nil }}}
	ctx, _ := localSession(t, e, replication.Capability)
	valid := &agentv1.WriterLease{SessionId: e.session.id, AppId: appID, Epoch: 2, Seconds: 30, ExpiresAtBootNanos: uint64(30 * time.Second)}
	for name, mutate := range map[string]func(*agentv1.WriterLease){
		"session": func(l *agentv1.WriterLease) { l.SessionId = uuid.NewString() },
		"app":     func(l *agentv1.WriterLease) { l.AppId = uuid.NewString() },
		"epoch":   func(l *agentv1.WriterLease) { l.Epoch = 1 },
		"long":    func(l *agentv1.WriterLease) { l.Seconds = 31 },
	} {
		t.Run(name, func(t *testing.T) {
			l := proto.Clone(valid).(*agentv1.WriterLease)
			mutate(l)
			if _, err := e.Handle(ctx, &agentv1.ControllerMessage{Message: &agentv1.ControllerMessage_WriterLease{WriterLease: l}}); err == nil {
				t.Fatal("invalid grant accepted")
			}
		})
	}
	assignment.Role = replication.RoleStandby
	table.Set([]*agentv1.AppReplication{assignment})
	if _, err := e.Handle(ctx, &agentv1.ControllerMessage{Message: &agentv1.ControllerMessage_WriterLease{WriterLease: valid}}); err == nil {
		t.Fatal("standby received writer lease")
	}
}

func TestWriterPromotionRequiresCompletePreviousEpoch(t *testing.T) {
	for _, mode := range []string{"missing", "stale", "complete", "crash-after-rename"} {
		t.Run(mode, func(t *testing.T) {
			appID := uuid.NewString()
			table := &replication.Table{}
			table.Set([]*agentv1.AppReplication{{AppId: appID, Epoch: 3, Role: replication.RoleActive}})
			e := &Executor{options: Options{AppsDir: t.TempDir(), Replication: table, BootTime: func() (time.Duration, error) { return 0, nil }}, leases: map[string]writerLease{appID: {epoch: 3, expires: time.Minute}}}
			root := filepath.Join(e.options.AppsDir, appID)
			if err := os.MkdirAll(root, 0o700); err != nil {
				t.Fatal(err)
			}
			if mode != "missing" {
				content := []byte("kept data")
				epoch := uint64(2)
				if mode == "stale" {
					epoch = 1
				}
				manifest := &replica.Manifest{Epoch: epoch, Sequence: 7, CreatedAt: time.Now(), Files: []replica.File{{Path: "config", Type: replica.TypeDir, Mode: 0o700, UID: os.Getuid(), GID: os.Getgid()}, {Path: "config/state", Type: replica.TypeFile, Mode: 0o600, UID: os.Getuid(), GID: os.Getgid(), Size: int64(len(content)), Blocks: []replica.Hash{sha256.Sum256(content)}}}}
				store := replica.Replica{Root: root}
				if err := store.Apply(manifest, func(replica.BlockRef) ([]byte, error) { return content, nil }); err != nil {
					t.Fatal(err)
				}
				if mode == "crash-after-rename" {
					if err := os.WriteFile(filepath.Join(root, "promoting"), []byte("3"), 0o600); err != nil {
						t.Fatal(err)
					}
					if err := os.Rename(filepath.Join(root, "replica"), filepath.Join(root, "live")); err != nil {
						t.Fatal(err)
					}
				}
			}
			command := &agentv1.Command{Epoch: &agentv1.Epoch{ClusterId: appID}, Input: &agentv1.CommandInput{Payload: &agentv1.CommandInput_StartContainer{StartContainer: &agentv1.StartContainer{}}}}
			mounts := []*agentv1.Mount{{Type: "replicated", Source: "config", Target: "/config"}}
			err := e.prepareWriter(t.Context(), command, mounts)
			if mode == "missing" || mode == "stale" {
				if err == nil {
					t.Fatal("unsafe promotion accepted")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			data, err := os.ReadFile(filepath.Join(root, "live", "config", "state"))
			if err != nil || string(data) != "kept data" {
				t.Fatalf("promoted data: %q %v", data, err)
			}
			if err := e.prepareWriter(t.Context(), command, mounts); err != nil {
				t.Fatal("promotion replay", err)
			}
			if err := os.RemoveAll(filepath.Join(root, "live", "config")); err != nil {
				t.Fatal(err)
			}
			if err := e.prepareWriter(t.Context(), command, mounts); !errors.Is(err, runtime.ErrStorageLost) {
				t.Fatal("lost live volume accepted", err)
			}
		})
	}
}

func TestWriterSnapshotReportsFailuresAndPersistsSequence(t *testing.T) {
	appID := uuid.NewString()
	table := &replication.Table{}
	assignment := &agentv1.AppReplication{AppId: appID, Epoch: 1, Role: replication.RoleActive, IntervalSeconds: 10, Peers: []*agentv1.ReplicaPeer{{NodeId: uuid.NewString()}}}
	table.Set([]*agentv1.AppReplication{assignment})
	e := &Executor{options: Options{AppsDir: t.TempDir(), Replication: table, ReplicaStatus: &replication.Status{}, BootTime: func() (time.Duration, error) { return 0, nil }}, leases: map[string]writerLease{appID: {epoch: 1, expires: time.Minute}}}
	root := filepath.Join(e.options.AppsDir, appID)
	command := &agentv1.Command{Epoch: &agentv1.Epoch{ClusterId: appID}, Input: &agentv1.CommandInput{Payload: &agentv1.CommandInput_EnsureContainer{EnsureContainer: &agentv1.EnsureContainer{}}}}
	if err := e.prepareWriter(t.Context(), command, []*agentv1.Mount{{Type: "replicated", Source: "config", Target: "/config"}}); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "live/config/data"), []byte("snapshot content"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(root, "snapshot"), 0o700); err != nil {
		t.Fatal(err)
	}
	freezer := &writerTestFreezer{}
	snapshot := &replica.Snapshotter{Live: filepath.Join(root, "live"), Snapshot: filepath.Join(root, "snapshot"), Freezer: freezer}
	standby := replica.Replica{Root: t.TempDir()}
	pushed := 0
	e.options.PushSnapshot = func(ctx context.Context, peer *agentv1.ReplicaPeer, id string, m *replica.Manifest, path string) (*agentv1.PushResult, error) {
		pushed++
		err := standby.Apply(m, func(ref replica.BlockRef) ([]byte, error) { return os.ReadFile(filepath.Join(path, ref.Path)) })
		return &agentv1.PushResult{Epoch: m.Epoch, Sequence: m.Sequence}, err
	}
	e.takeSnapshot(t.Context(), appID, assignment, snapshot)
	got := e.options.ReplicaStatus.List()[0]
	if got.Sequence != 1 || got.FailureCode != "" || pushed != 1 || freezer.thaws != 1 {
		t.Fatalf("success: %v pushes=%d thaws=%d", got, pushed, freezer.thaws)
	}
	data, err := os.ReadFile(filepath.Join(standby.Root, "replica/config/data"))
	if err != nil || string(data) != "snapshot content" {
		t.Fatalf("standby: %q %v", data, err)
	}
	freezer.err = replica.ErrFreezeTimeout
	e.takeSnapshot(t.Context(), appID, assignment, snapshot)
	got = e.options.ReplicaStatus.List()[0]
	if got.FailureCode != replication.FailureFreeze || got.Sequence != 1 || pushed != 1 || got.SkippedSnapshots != 1 {
		t.Fatalf("freeze: %v pushes=%d", got, pushed)
	}
	freezer.err = nil
	e.options.PushSnapshot = func(context.Context, *agentv1.ReplicaPeer, string, *replica.Manifest, string) (*agentv1.PushResult, error) {
		return nil, errors.New("network lost")
	}
	e.takeSnapshot(t.Context(), appID, assignment, snapshot)
	got = e.options.ReplicaStatus.List()[0]
	if got.FailureCode != replication.FailurePush || got.Sequence != 3 {
		t.Fatalf("push: %v", got)
	}
	// A new Snapshotter and empty status represent an agent restart. Counters continue from disk.
	e.options.ReplicaStatus = &replication.Status{}
	snapshot = &replica.Snapshotter{Live: snapshot.Live, Snapshot: snapshot.Snapshot, Freezer: freezer}
	e.takeSnapshot(t.Context(), appID, assignment, snapshot)
	if got := e.options.ReplicaStatus.List()[0]; got.Sequence != 4 || got.SkippedSnapshots != 1 || got.MaxPauseNanos < got.LastPauseNanos || got.MaxPauseNanos == 0 {
		t.Fatalf("restart sequence: %v", got)
	}
}

func TestReplicationEpochSurvivesRestart(t *testing.T) {
	appID := uuid.NewString()
	dir := t.TempDir()
	first := &Executor{options: Options{AppsDir: dir, Replication: &replication.Table{}}}
	assignment := &agentv1.AppReplication{AppId: appID, Epoch: 4, Role: replication.RoleStandby, Peers: []*agentv1.ReplicaPeer{{NodeId: uuid.NewString(), Address: "127.0.0.1:8445"}}}
	if err := first.recordReplicationEpochs([]*agentv1.AppReplication{assignment}); err != nil {
		t.Fatal(err)
	}
	restarted := &Executor{options: Options{AppsDir: dir, Replication: &replication.Table{}}}
	ctx, _ := localSession(t, restarted, replication.Capability)
	assignment.Epoch = 3
	message := &agentv1.ControllerMessage{Message: &agentv1.ControllerMessage_Replication{Replication: &agentv1.ReplicationTable{SessionId: restarted.session.id, Revision: 1, Apps: []*agentv1.AppReplication{assignment}}}}
	ack, err := restarted.Handle(ctx, message)
	if err != nil || ack.GetReplicationAck().FailureCode != "stale_replication_epoch" {
		t.Fatalf("old assignment: %v %v", ack, err)
	}
	if _, ok := restarted.options.Replication.Lookup(appID); ok {
		t.Fatal("old assignment installed")
	}
}

func TestWriterLeaseStopsDockerContainer(t *testing.T) {
	f := newFixture(t, nil)
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	native, err := docker.New(f.api, docker.Options{InstallationID: uuid.MustParse(f.epoch.InstallationId), NodeID: uuid.MustParse(f.epoch.NodeId), AppsDir: root})
	if err != nil {
		t.Fatal(err)
	}
	var now atomic.Int64
	options := f.e.options
	options.AppsDir = root
	options.BootTime = func() (time.Duration, error) { return time.Duration(now.Load()), nil }
	options.Replication = &replication.Table{}
	options.Replication.Set([]*agentv1.AppReplication{{AppId: f.epoch.ClusterId, Epoch: 1, Role: replication.RoleActive}})
	f.e, err = New(f.e.journal, native, options)
	if err != nil {
		t.Fatal(err)
	}
	session, disconnect := localSession(t, f.e, replication.Capability)
	activateLocal(t, f.e, session, f.epoch)
	if _, err := f.e.Handle(session, &agentv1.ControllerMessage{Message: &agentv1.ControllerMessage_WriterLease{WriterLease: &agentv1.WriterLease{SessionId: f.e.session.id, AppId: f.epoch.ClusterId, Epoch: 1, Seconds: 30, ExpiresAtBootNanos: uint64(30 * time.Second)}}}); err != nil {
		t.Fatal(err)
	}
	image := "redis@sha256:d30960f73a599496d8b2802c97758bab6b1cd421fd06337f837779c47a57e1f3"
	command := volumeCommand(t, f.epoch)
	command.Input.Payload = &agentv1.CommandInput_EnsureContainer{EnsureContainer: &agentv1.EnsureContainer{ImageDigest: image, Engine: "generic", NetworkMode: agentv1.NetworkMode_NETWORK_MODE_BRIDGE, Generic: &agentv1.GenericContainerConfig{Image: image, AppName: "leased-writer", Mounts: []*agentv1.Mount{{Type: "replicated", Source: "config", Target: "/data"}}}}}
	for step := range 2 {
		if step == 1 {
			command.CommandId = uuid.NewString()
			command.StepId = uuid.NewString()
			command.Input.Payload = &agentv1.CommandInput_StartContainer{StartContainer: &agentv1.StartContainer{}}
		}
		hash, err := operation.CommandHash(command)
		if err != nil {
			t.Fatal(err)
		}
		command.InputHash = hash[:]
		ack, err := f.e.Handle(session, &agentv1.ControllerMessage{DeliveryAttemptId: uuid.NewString(), Message: &agentv1.ControllerMessage_Command{Command: command}})
		if err != nil || ack.GetAdmissionAck().State != "queued" {
			t.Fatalf("admission: %v %v", ack, err)
		}
		f.e.execute(t.Context(), job{cluster: f.epoch.ClusterId, command: command.CommandId, session: f.e.session, gate: f.e.session.gates[f.epoch.ClusterId]})
		record, err := f.e.journal.Get(command.CommandId)
		if err != nil || record.State != journal.Terminal || record.Result.State != "succeeded" {
			t.Fatalf("step %d: %+v %v", step, record, err)
		}
	}
	disconnect()
	now.Store(int64(time.Minute))
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() { defer close(done); f.e.watchWriters(ctx) }()
	defer func() { cancel(); <-done }()
	deadline := time.Now().Add(5 * time.Second)
	for {
		detail, err := f.api.ContainerInspect(t.Context(), "synod-instance-"+command.Input.ResourceId, client.ContainerInspectOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if !detail.Container.State.Running {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("lease expiry left the Docker writer running")
		}
		time.Sleep(20 * time.Millisecond)
	}
	if _, err := os.Stat(filepath.Join(root, f.epoch.ClusterId, "live", "config")); err != nil {
		t.Fatal("fencing removed data", err)
	}
}

func TestWriterRefusesSecondLiveCopy(t *testing.T) {
	appID := uuid.NewString()
	native := &writerTestRuntime{app: runtime.AppContainer{AppID: appID, ResourceID: uuid.NewString(), Running: true}}
	e := &Executor{runtime: native}
	command := &agentv1.Command{Epoch: &agentv1.Epoch{ClusterId: appID}, Input: &agentv1.CommandInput{ResourceId: uuid.NewString(), Payload: &agentv1.CommandInput_EnsureContainer{EnsureContainer: &agentv1.EnsureContainer{Generic: &agentv1.GenericContainerConfig{Mounts: []*agentv1.Mount{{Type: "replicated", Source: "config", Target: "/config"}}}}}}}
	if _, err := e.executeWriter(t.Context(), command, nil); !errors.Is(err, runtime.ErrBusy) {
		t.Fatalf("second live copy: %v", err)
	}
}

func TestDelayedWriterGrantCannotExtendItsDeadline(t *testing.T) {
	var now atomic.Int64
	table := &replication.Table{}
	appID := uuid.NewString()
	table.Set([]*agentv1.AppReplication{{AppId: appID, Epoch: 1, Role: replication.RoleActive}})
	e := &Executor{options: Options{Replication: table, BootTime: func() (time.Duration, error) { return time.Duration(now.Load()), nil }}}
	session, cancel := localSession(t, e, replication.Capability)
	defer cancel()
	grant := &agentv1.ControllerMessage{Message: &agentv1.ControllerMessage_WriterLease{WriterLease: &agentv1.WriterLease{SessionId: e.session.id, AppId: appID, Epoch: 1, Seconds: 30, ExpiresAtBootNanos: uint64(30 * time.Second)}}}
	now.Store(int64(25 * time.Second))
	if _, err := e.Handle(session, grant); err != nil {
		t.Fatal(err)
	}
	if _, err := e.writerAssignment(appID); err != nil {
		t.Fatal(err)
	}
	now.Store(int64(31 * time.Second))
	if _, err := e.Handle(session, grant); err != nil {
		t.Fatal(err)
	}
	if _, err := e.writerAssignment(appID); !errors.Is(err, errWriterLease) {
		t.Fatalf("delayed lease was renewed: %v", err)
	}
	grant.GetWriterLease().ExpiresAtBootNanos = uint64(62 * time.Second)
	if _, err := e.Handle(session, grant); !errors.Is(err, controlstream.ErrMessage) {
		t.Fatalf("excessive clock deadline: %v", err)
	}
}

type cleanupWriterRuntime struct {
	*writerTestRuntime
	standby runtime.AppContainer
}

func (r *cleanupWriterRuntime) AppContainers(ctx context.Context) ([]runtime.AppContainer, error) {
	apps, err := r.writerTestRuntime.AppContainers(ctx)
	return append([]runtime.AppContainer{r.standby}, apps...), err
}

func TestWriterFencesWhileAnotherAppWaitsForCleanup(t *testing.T) {
	standby, active := uuid.NewString(), uuid.NewString()
	table := &replication.Table{}
	table.Set([]*agentv1.AppReplication{{AppId: standby, Epoch: 2, Role: replication.RoleStandby}, {AppId: active, Epoch: 1, Role: replication.RoleActive}})
	unlock, err := table.LockApp(t.Context(), standby)
	if err != nil {
		t.Fatal(err)
	}
	defer unlock()
	native := &cleanupWriterRuntime{
		writerTestRuntime: &writerTestRuntime{app: runtime.AppContainer{AppID: active, ResourceID: uuid.NewString(), Running: true}, stopped: make(chan struct{})},
		standby:           runtime.AppContainer{AppID: standby, ResourceID: uuid.NewString()},
	}
	e := &Executor{runtime: native, options: Options{AppsDir: t.TempDir(), Replication: table, BootTime: func() (time.Duration, error) { return 0, nil }, InspectionTimeout: 10 * time.Second}}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() { defer close(done); e.watchWriters(ctx) }()
	defer func() { cancel(); <-done }()
	select {
	case <-native.stopped:
	case <-time.After(2 * time.Second):
		t.Fatal("standby cleanup delayed fencing another writer")
	}
}
