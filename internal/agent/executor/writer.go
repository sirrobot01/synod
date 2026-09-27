package executor

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/sirrobot01/synod/internal/agent/replica"
	"github.com/sirrobot01/synod/internal/agent/replication"
	"github.com/sirrobot01/synod/internal/operation"
	"github.com/sirrobot01/synod/internal/runtime"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var errWriterLease = errors.New("writer lease is absent or expired")

type writerLease struct {
	epoch   uint64
	expires time.Duration
}

type writerRuntime interface {
	AppContainers(context.Context) ([]runtime.AppContainer, error)
	FenceApp(context.Context, runtime.AppContainer) error
}

func (e *Executor) writerAssignment(appID string) (*agentv1.AppReplication, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.options.Replication == nil || e.options.BootTime == nil {
		return nil, errWriterLease
	}
	assignment, ok := e.options.Replication.Lookup(appID)
	lease := e.leases[appID]
	now, err := e.options.BootTime()
	if err != nil || !ok || assignment.Role != replication.RoleActive || lease.epoch != assignment.Epoch || now >= lease.expires {
		return nil, errWriterLease
	}
	return assignment, nil
}

// prepareWriter refuses empty data after the first writer epoch.
func (e *Executor) prepareWriter(ctx context.Context, command *agentv1.Command, mounts []*agentv1.Mount) error {
	assignment, err := e.writerAssignment(command.Epoch.ClusterId)
	if err != nil {
		return err
	}
	if !filepath.IsAbs(e.options.AppsDir) {
		return runtime.ErrUnsupportedConfiguration
	}
	rootPath := filepath.Join(e.options.AppsDir, assignment.AppId)
	if err := os.MkdirAll(rootPath, 0o700); err != nil {
		return err
	}
	unlock, err := e.options.Replication.LockApp(ctx, assignment.AppId)
	if err != nil {
		return err
	}
	defer unlock()
	root, err := os.OpenRoot(rootPath)
	if err != nil {
		return err
	}
	defer root.Close()
	saved, err := root.ReadFile("epoch")
	var epoch uint64
	if err == nil {
		epoch, err = strconv.ParseUint(strings.TrimSpace(string(saved)), 10, 64)
	}
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if epoch > assignment.Epoch {
		return errWriterLease
	}
	live := filepath.Join(rootPath, "live")
	if epoch != assignment.Epoch {
		store := replica.Replica{Root: rootPath}
		if err := store.Recover(); err != nil {
			return err
		}
		manifest, err := store.Current()
		if err != nil {
			return err
		}
		pending, pendingErr := root.ReadFile("promoting")
		recovering := pendingErr == nil && strings.TrimSpace(string(pending)) == strconv.FormatUint(assignment.Epoch, 10)
		_, liveErr := root.Lstat("live")
		_, replicaErr := root.Lstat("replica")
		if recovering && liveErr == nil && errors.Is(replicaErr, os.ErrNotExist) {
			if err := root.Remove("replica.manifest"); err != nil && !errors.Is(err, os.ErrNotExist) {
				return err
			}
		} else if manifest != nil {
			if manifest.Epoch != assignment.Epoch-1 {
				return errWriterLease
			}
			if err := root.RemoveAll("live"); err != nil {
				return err
			}
			if err := writeWriterFile(root, "promoting", strconv.FormatUint(assignment.Epoch, 10)); err != nil {
				return err
			}
			if err := store.Promote(live); err != nil {
				return err
			}
		} else if recovering {
			if _, err := root.Stat("live"); err != nil {
				return replica.ErrNoReplica
			}
		} else if assignment.Epoch == 1 && epoch == 0 && command.Input.GetEnsureContainer() != nil {
			if err := root.MkdirAll("live", 0o700); err != nil {
				return err
			}
			for _, m := range mounts {
				if m.Type == "replicated" {
					if err := root.MkdirAll(filepath.Join("live", m.Source), 0o700); err != nil {
						return err
					}
				}
			}
		} else {
			return replica.ErrNoReplica
		}
		if err := writeWriterFile(root, "epoch", strconv.FormatUint(assignment.Epoch, 10)); err != nil {
			return err
		}
		if err := root.Remove("promoting"); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	for _, m := range mounts {
		if m.Type != "replicated" {
			continue
		}
		// A volume root must be a real directory. Docker must never follow an app-created link here.
		info, err := root.Lstat(filepath.Join("live", m.Source))
		if err != nil || !info.IsDir() {
			return runtime.ErrStorageLost
		}
	}
	_, err = e.writerAssignment(assignment.AppId)
	return err
}

func writeWriterFile(root *os.Root, name, value string) error {
	file, err := root.OpenFile(name+".new", os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	_, writeErr := file.WriteString(value)
	err = errors.Join(writeErr, file.Sync(), file.Close())
	if err != nil {
		return err
	}
	if err := root.Rename(name+".new", name); err != nil {
		return err
	}
	dir, err := root.Open(".")
	if err != nil {
		return err
	}
	return errors.Join(dir.Sync(), dir.Close())
}

func (e *Executor) executeWriter(ctx context.Context, command *agentv1.Command, secrets runtime.Secrets) (runtime.Result, error) {
	if command.Input.GetFinalAppSnapshot() != nil {
		return e.finalAppSnapshot(ctx, command)
	}
	if command.Input.GetRemoveAppData() != nil {
		return e.removeAppData(ctx, command)
	}
	if command.Input.Timeout != nil {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, command.Input.Timeout.AsDuration())
		defer cancel()
	}
	var mounts []*agentv1.Mount
	if ensure := command.Input.GetEnsureContainer(); ensure != nil && ensure.Generic != nil {
		mounts = ensure.Generic.Mounts
	} else if command.Input.GetStartContainer() != nil {
		if native, ok := e.runtime.(writerRuntime); ok {
			apps, err := native.AppContainers(ctx)
			if err != nil {
				return runtime.Result{}, err
			}
			for _, app := range apps {
				if app.ResourceID == command.Input.ResourceId {
					mounts = app.Mounts
					break
				}
			}
		}
	}
	if !slices.ContainsFunc(mounts, func(m *agentv1.Mount) bool { return m.Type == "replicated" }) {
		return e.runtime.Execute(ctx, command, secrets)
	}
	if native, ok := e.runtime.(writerRuntime); ok {
		apps, err := native.AppContainers(ctx)
		if err != nil {
			return runtime.Result{}, err
		}
		for _, app := range apps {
			if app.AppID == command.Epoch.ClusterId && app.Running {
				if app.ResourceID != command.Input.ResourceId {
					return runtime.Result{}, runtime.ErrBusy
				}
				if err := e.checkWriter(app.AppID); err != nil {
					if err := native.FenceApp(ctx, app); err != nil {
						return runtime.Result{}, err
					}
					return runtime.Result{}, runtime.ErrBusy
				}
			}
		}
	}
	if err := e.prepareWriter(ctx, command, mounts); err != nil {
		return runtime.Result{}, err
	}
	result, err := e.runtime.Execute(ctx, command, secrets)
	if _, leaseErr := e.writerAssignment(command.Epoch.ClusterId); leaseErr != nil {
		if native, ok := e.runtime.(writerRuntime); ok {
			stop, cancel := context.WithTimeout(context.WithoutCancel(ctx), e.options.InspectionTimeout)
			fenceErr := native.FenceApp(stop, runtime.AppContainer{AppID: command.Epoch.ClusterId, ResourceID: command.Input.ResourceId})
			cancel()
			return result, errors.Join(err, leaseErr, fenceErr)
		}
	}
	return result, err
}

func (e *Executor) fenceWriters() {
	native, ok := e.runtime.(writerRuntime)
	if !ok {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), e.options.InspectionTimeout)
	defer cancel()
	apps, err := native.AppContainers(ctx)
	if err != nil {
		e.fail(err)
		return
	}
	for _, app := range apps {
		if err := native.FenceApp(ctx, app); err != nil {
			e.fail(err)
		}
	}
}

func (e *Executor) watchWriters(ctx context.Context) {
	native, ok := e.runtime.(writerRuntime)
	if !ok {
		return
	}
	type snapshotJob struct {
		pid    int
		cancel context.CancelFunc
		epoch  uint64
		done   chan struct{}
	}
	jobs := map[string]snapshotJob{}
	cleanups := map[string]chan struct{}{}
	var workers sync.WaitGroup
	defer func() {
		for _, job := range jobs {
			job.cancel()
		}
		e.fenceWriters()
		workers.Wait()
	}()
	for {
		read, cancel := context.WithTimeout(ctx, e.options.InspectionTimeout)
		apps, err := native.AppContainers(read)
		cancel()
		present := map[string]bool{}
		if err == nil {
			for _, app := range apps {
				present[app.AppID] = true
				assignment, leaseErr := e.writerAssignment(app.AppID)
				unleased := leaseErr != nil || e.checkWriter(app.AppID) != nil
				if unleased || !app.Running {
					if !app.Running && e.options.Replication != nil {
						if done := cleanups[app.AppID]; done != nil {
							select {
							case <-done:
								delete(cleanups, app.AppID)
							default:
							}
						}
						if assigned, ok := e.options.Replication.Lookup(app.AppID); ok && assigned.Role == replication.RoleStandby && cleanups[app.AppID] == nil {
							done := make(chan struct{})
							cleanups[app.AppID] = done
							workers.Go(func() {
								defer close(done)
								clean, cancel := context.WithTimeout(ctx, e.options.InspectionTimeout)
								defer cancel()
								_ = e.discardStaleLive(clean, app.AppID)
							})
						}
					}
					if job, ok := jobs[app.AppID]; ok {
						job.cancel()
					}
					if unleased && app.Running {
						stop, cancel := context.WithTimeout(context.WithoutCancel(ctx), e.options.InspectionTimeout)
						err := native.FenceApp(stop, app)
						cancel()
						if err != nil {
							e.fail(err)
						}
					}
					continue
				}
				if job, ok := jobs[app.AppID]; ok {
					if job.epoch != assignment.Epoch || job.pid != app.PID {
						job.cancel()
					}
					select {
					case <-job.done:
						delete(jobs, app.AppID)
					default:
						continue
					}
				}
				if e.options.PushSnapshot != nil && e.options.ReplicaStatus != nil {
					snapshotCtx, cancel := context.WithCancel(ctx)
					done := make(chan struct{})
					jobs[app.AppID] = snapshotJob{pid: app.PID, cancel: cancel, epoch: assignment.Epoch, done: done}
					workers.Go(func() { defer close(done); e.snapshotWriter(snapshotCtx, app, assignment.Epoch) })
				}
			}
		}
		for appID, job := range jobs {
			if !present[appID] {
				job.cancel()
				select {
				case <-job.done:
					delete(jobs, appID)
				default:
				}
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(250 * time.Millisecond):
		}
	}
}

func (e *Executor) snapshotWriter(ctx context.Context, app runtime.AppContainer, epoch uint64) {
	root := filepath.Join(e.options.AppsDir, app.AppID)
	path := filepath.Join(root, "snapshot")
	status := e.options.ReplicaStatus
	if err := os.MkdirAll(path, 0o700); err != nil {
		status.Fail(app.AppID, replication.RoleActive, replication.FailureSnapshot)
		return
	}
	dir, err := replica.CgroupDir(app.PID)
	if err != nil {
		status.Fail(app.AppID, replication.RoleActive, replication.FailureSnapshot)
		return
	}
	snapshotter := &replica.Snapshotter{Live: filepath.Join(root, "live"), Snapshot: path, Freezer: replica.CgroupFreezer{Dir: dir, Limit: 2 * time.Second}}
	for {
		assignment, err := e.writerAssignment(app.AppID)
		if err != nil || assignment.Epoch != epoch || ctx.Err() != nil {
			return
		}
		e.takeSnapshot(ctx, app.AppID, assignment, snapshotter)

		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Duration(assignment.IntervalSeconds) * time.Second):
		}
	}
}

func (e *Executor) checkWriter(appID string) error {
	assignment, err := e.writerAssignment(appID)
	if err != nil {
		return err
	}
	data, err := os.ReadFile(filepath.Join(e.options.AppsDir, appID, "epoch"))
	if err != nil {
		return errWriterLease
	}
	epoch, err := strconv.ParseUint(strings.TrimSpace(string(data)), 10, 64)
	if err != nil || epoch != assignment.Epoch {
		return errWriterLease
	}
	return nil
}

type snapshotMetrics struct {
	LastPauseNanos uint64 `json:"last_pause_nanos"`
	MaxPauseNanos  uint64 `json:"max_pause_nanos"`
	Skipped        uint64 `json:"skipped"`
}

func (e *Executor) takeSnapshot(ctx context.Context, appID string, assignment *agentv1.AppReplication, snapshotter *replica.Snapshotter) {
	status := e.options.ReplicaStatus
	root := filepath.Join(e.options.AppsDir, appID)
	unlock, err := e.options.Replication.LockApp(ctx, appID)
	if err != nil {
		return
	}
	defer unlock()
	current, err := e.writerAssignment(appID)
	if err != nil || current.Epoch != assignment.Epoch || e.checkWriter(appID) != nil || ctx.Err() != nil {
		return
	}
	// Reserve each sequence on disk before use. A restart must not reuse a sequence.
	base, err := os.OpenRoot(root)
	if err != nil {
		status.Fail(appID, replication.RoleActive, replication.FailureSnapshot)
		return
	}
	defer base.Close()
	var metrics snapshotMetrics
	data, err := base.ReadFile("snapshot_metrics")
	if err == nil {
		err = json.Unmarshal(data, &metrics)
	}
	if (err != nil && !errors.Is(err, os.ErrNotExist)) || metrics.LastPauseNanos > metrics.MaxPauseNanos || metrics.MaxPauseNanos > math.MaxInt64 || metrics.Skipped > math.MaxInt64 {
		status.Fail(appID, replication.RoleActive, replication.FailureSnapshot)
		return
	}
	data, err = base.ReadFile("sequence")
	var sequence uint64
	if err == nil {
		sequence, err = strconv.ParseUint(strings.TrimSpace(string(data)), 10, 64)
	}
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		status.Fail(appID, replication.RoleActive, replication.FailureSnapshot)
		return
	}
	sequence++
	err = writeWriterFile(base, "sequence", strconv.FormatUint(sequence, 10))
	if err != nil {
		status.Fail(appID, replication.RoleActive, replication.FailureSnapshot)
		return
	}
	manifest, err := snapshotter.Take(ctx, assignment.Epoch, sequence)
	if err != nil {
		metrics.Skipped = min(metrics.Skipped+1, uint64(math.MaxInt64))
	} else {
		metrics.LastPauseNanos = uint64(max(manifest.Frozen, 0))
		metrics.MaxPauseNanos = max(metrics.MaxPauseNanos, metrics.LastPauseNanos)
	}
	encoded, _ := json.Marshal(metrics)
	if writeErr := writeWriterFile(base, "snapshot_metrics", string(encoded)); writeErr != nil {
		status.Fail(appID, replication.RoleActive, replication.FailureSnapshot)
		return
	}
	defer status.SnapshotMetrics(appID, metrics.LastPauseNanos, metrics.MaxPauseNanos, metrics.Skipped)
	switch {
	case errors.Is(err, replica.ErrFreezeTimeout):
		status.Fail(appID, replication.RoleActive, replication.FailureFreeze)
	case err != nil:
		status.Fail(appID, replication.RoleActive, replication.FailureSnapshot)
	default:
		status.Record(&agentv1.ReplicaStatus{AppId: appID, Role: replication.RoleActive, Epoch: assignment.Epoch, Sequence: sequence, CompletedAt: timestamppb.Now(), LastPauseNanos: metrics.LastPauseNanos, MaxPauseNanos: metrics.MaxPauseNanos, SkippedSnapshots: metrics.Skipped})
		for _, peer := range assignment.Peers {
			current, err := e.writerAssignment(appID)
			if err != nil || current.Epoch != assignment.Epoch || ctx.Err() != nil {
				return
			}
			push, cancel := context.WithTimeout(ctx, time.Duration(assignment.IntervalSeconds)*time.Second)
			result, err := e.options.PushSnapshot(push, peer, appID, manifest, snapshotter.Snapshot)
			cancel()
			if err != nil || result == nil || result.FailureCode != "" || result.Epoch != assignment.Epoch || result.Sequence != sequence {
				status.Fail(appID, replication.RoleActive, replication.FailurePush)
			}
		}
	}
}

// recordReplicationEpochs prevents an old session from restoring an old writer role.
// Handle holds e.mu while this runs.
func (e *Executor) recordReplicationEpochs(apps []*agentv1.AppReplication) error {
	for _, app := range apps {
		if previous, ok := e.options.Replication.Lookup(app.AppId); ok && app.Epoch < previous.Epoch {
			return operation.ErrStaleOwner
		}
		if e.options.AppsDir == "" {
			continue
		}
		path := filepath.Join(e.options.AppsDir, app.AppId)
		if err := os.MkdirAll(path, 0o700); err != nil {
			return err
		}
		root, err := os.OpenRoot(path)
		if err != nil {
			return err
		}
		for _, name := range []string{"epoch", "assigned_epoch"} {
			data, err := root.ReadFile(name)
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			if err != nil {
				root.Close()
				return err
			}
			epoch, err := strconv.ParseUint(strings.TrimSpace(string(data)), 10, 64)
			if err != nil {
				root.Close()
				return err
			}
			if epoch > app.Epoch {
				root.Close()
				return operation.ErrStaleOwner
			}
		}
		err = writeWriterFile(root, "assigned_epoch", strconv.FormatUint(app.Epoch, 10))
		root.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

// WriterClock supplies the same suspend-aware clock that enforces writer leases.
func (e *Executor) WriterClock() uint64 {
	if e.options.BootTime == nil {
		return 0
	}
	now, err := e.options.BootTime()
	if err != nil || now <= 0 {
		return 0
	}
	return uint64(now)
}
