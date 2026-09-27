package executor

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	"github.com/sirrobot01/synod/internal/agent/replica"
	"github.com/sirrobot01/synod/internal/operation"
	"github.com/sirrobot01/synod/internal/runtime"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
)

type finalSnapshotProof struct {
	InputHash []byte                      `json:"input_hash"`
	Receipt   *agentv1.AppSnapshotReceipt `json:"receipt"`
}

func (e *Executor) finalAppSnapshot(ctx context.Context, command *agentv1.Command) (runtime.Result, error) {
	if e.options.Replication == nil || e.options.PushSnapshot == nil || !filepath.IsAbs(e.options.AppsDir) {
		return runtime.Result{}, runtime.ErrUnsupportedConfiguration
	}
	ctx, cancel := context.WithTimeout(ctx, command.Input.Timeout.AsDuration())
	defer cancel()
	appID, input := command.Epoch.ClusterId, command.Input.GetFinalAppSnapshot()
	unlock, err := e.options.Replication.LockApp(ctx, appID)
	if err != nil {
		return runtime.Result{}, err
	}
	defer unlock()
	assignment, err := e.writerAssignment(appID)
	if err != nil || assignment.Epoch != input.WriterEpoch {
		return runtime.Result{}, errWriterLease
	}
	at := slices.IndexFunc(assignment.Peers, func(p *agentv1.ReplicaPeer) bool { return p.NodeId == input.TargetNodeId })
	if at < 0 {
		return runtime.Result{}, operation.ErrCommandConflict
	}
	native, ok := e.runtime.(writerRuntime)
	if !ok {
		return runtime.Result{}, runtime.ErrUnsupportedConfiguration
	}
	containers, err := native.AppContainers(ctx)
	if err != nil {
		return runtime.Result{}, err
	}
	found := false
	for _, container := range containers {
		if container.AppID != appID {
			continue
		}
		if container.Running {
			return runtime.Result{}, runtime.ErrBusy
		}
		if container.ResourceID == command.Input.ResourceId {
			found = true
		}
	}
	if !found {
		return runtime.Result{}, runtime.ErrStorageLost
	}
	root, err := os.OpenRoot(filepath.Join(e.options.AppsDir, appID))
	if err != nil {
		return runtime.Result{}, err
	}
	defer root.Close()
	proofName := "final-" + command.CommandId + ".json"
	if _, err := root.Stat(proofName); err == nil {
		return e.recoverAppData(ctx, command)
	} else if !errors.Is(err, os.ErrNotExist) {
		return runtime.Result{}, err
	}
	data, err := root.ReadFile("epoch")
	if err != nil {
		return runtime.Result{}, err
	}
	epoch, err := strconv.ParseUint(strings.TrimSpace(string(data)), 10, 64)
	if err != nil || epoch != input.WriterEpoch {
		return runtime.Result{}, operation.ErrStaleOwner
	}
	data, err = root.ReadFile("sequence")
	var sequence uint64
	if err == nil {
		sequence, err = strconv.ParseUint(strings.TrimSpace(string(data)), 10, 64)
	}
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return runtime.Result{}, err
	}
	if sequence >= math.MaxInt64 {
		return runtime.Result{}, operation.ErrCommandConflict
	}
	sequence++
	if err := writeWriterFile(root, "sequence", strconv.FormatUint(sequence, 10)); err != nil {
		return runtime.Result{}, err
	}
	if err := root.MkdirAll("final-snapshot", 0o700); err != nil {
		return runtime.Result{}, err
	}
	snapshot := &replica.Snapshotter{Live: filepath.Join(e.options.AppsDir, appID, "live"), Snapshot: filepath.Join(e.options.AppsDir, appID, "final-snapshot")}
	manifest, err := snapshot.TakeStopped(ctx, epoch, sequence)
	if err != nil {
		return runtime.Result{}, err
	}
	encoded, err := json.Marshal(manifest)
	if err != nil {
		return runtime.Result{}, err
	}
	digest := sha256.Sum256(encoded)
	var result *agentv1.PushResult
	for range 2 {
		result, err = e.options.PushSnapshot(ctx, assignment.Peers[at], appID, manifest, snapshot.Snapshot)
		if err == nil || ctx.Err() != nil {
			break
		}
	}
	if err != nil {
		return runtime.Result{}, err
	}
	if result == nil || result.FailureCode != "" || result.Epoch != epoch || result.Sequence != sequence {
		return runtime.Result{}, runtime.ErrStorageLost
	}
	current, err := e.writerAssignment(appID)
	if err != nil || current.Epoch != epoch {
		return runtime.Result{}, errWriterLease
	}
	proof := finalSnapshotProof{InputHash: bytes.Clone(command.InputHash), Receipt: &agentv1.AppSnapshotReceipt{WriterEpoch: epoch, Sequence: sequence, TargetNodeId: input.TargetNodeId, ManifestHash: digest[:]}}
	encoded, err = json.Marshal(proof)
	if err != nil {
		return runtime.Result{}, err
	}
	if err := writeWriterFile(root, proofName, string(encoded)); err != nil {
		return runtime.Result{}, err
	}
	return runtime.Result{Effects: []*agentv1.Effect{{CommandId: command.CommandId, ResourceId: command.Input.ResourceId, NativeOperationId: "app-data:" + command.CommandId, State: "snapshot_transferred", AppSnapshot: proof.Receipt}}}, nil
}

func (e *Executor) removeAppData(ctx context.Context, command *agentv1.Command) (runtime.Result, error) {
	if e.options.Replication == nil || !filepath.IsAbs(e.options.AppsDir) {
		return runtime.Result{}, runtime.ErrUnsupportedConfiguration
	}
	appID := command.Epoch.ClusterId
	unlock, err := e.options.Replication.LockApp(ctx, appID)
	if err != nil {
		return runtime.Result{}, err
	}
	defer unlock()
	if _, assigned := e.options.Replication.Lookup(appID); assigned {
		return runtime.Result{}, runtime.ErrBusy
	}
	native, ok := e.runtime.(writerRuntime)
	if !ok {
		return runtime.Result{}, runtime.ErrUnsupportedConfiguration
	}
	containers, err := native.AppContainers(ctx)
	if err != nil {
		return runtime.Result{}, err
	}
	for _, container := range containers {
		if container.AppID == appID {
			return runtime.Result{}, runtime.ErrBusy
		}
	}
	if err := os.MkdirAll(e.options.AppsDir, 0700); err != nil {
		return runtime.Result{}, err
	}
	root, err := os.OpenRoot(e.options.AppsDir)
	if err != nil {
		return runtime.Result{}, err
	}
	defer root.Close()
	for _, name := range []string{"epoch", "assigned_epoch"} {
		data, err := root.ReadFile(filepath.Join(appID, name))
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return runtime.Result{}, err
		}
		epoch, err := strconv.ParseUint(strings.TrimSpace(string(data)), 10, 64)
		if err != nil || epoch > command.Input.GetRemoveAppData().WriterEpoch {
			return runtime.Result{}, operation.ErrStaleOwner
		}
	}
	if err := root.RemoveAll(appID); err != nil {
		return runtime.Result{}, err
	}
	dir, err := root.Open(".")
	if err != nil {
		return runtime.Result{}, err
	}
	defer dir.Close()
	if err := dir.Sync(); err != nil {
		return runtime.Result{}, err
	}
	return e.recoverAppData(ctx, command)
}

// recoverAppData reads durable evidence. It never repeats a transfer or deletes files.
func (e *Executor) recoverAppData(ctx context.Context, command *agentv1.Command) (runtime.Result, error) {
	if err := ctx.Err(); err != nil {
		return runtime.Result{}, err
	}
	if !filepath.IsAbs(e.options.AppsDir) {
		return runtime.Result{}, runtime.ErrUnsupportedConfiguration
	}
	effect := &agentv1.Effect{CommandId: command.CommandId, ResourceId: command.Input.ResourceId, NativeOperationId: "app-data:" + command.CommandId}
	if command.Input.GetRemoveAppData() != nil {
		native, ok := e.runtime.(writerRuntime)
		if !ok {
			return runtime.Result{}, runtime.ErrUnsupportedConfiguration
		}
		containers, err := native.AppContainers(ctx)
		if err != nil {
			return runtime.Result{}, err
		}
		for _, container := range containers {
			if container.AppID == command.Epoch.ClusterId {
				return runtime.Result{}, runtime.ErrBusy
			}
		}
	}
	root, err := os.OpenRoot(e.options.AppsDir)
	if err != nil {
		if command.Input.GetRemoveAppData() != nil && errors.Is(err, os.ErrNotExist) {
			effect.State = "app_data_absent"
			return runtime.Result{Effects: []*agentv1.Effect{effect}}, nil
		}
		return runtime.Result{}, err
	}
	defer root.Close()
	if command.Input.GetRemoveAppData() != nil {
		if _, err := root.Lstat(command.Epoch.ClusterId); !errors.Is(err, os.ErrNotExist) {
			return runtime.Result{}, runtime.ErrStorageLost
		}
		effect.State = "app_data_absent"
	} else {
		data, err := root.ReadFile(filepath.Join(command.Epoch.ClusterId, "final-"+command.CommandId+".json"))
		if err != nil {
			return runtime.Result{}, runtime.ErrStorageLost
		}
		var proof finalSnapshotProof
		if json.Unmarshal(data, &proof) != nil || !bytes.Equal(proof.InputHash, command.InputHash) {
			return runtime.Result{}, operation.ErrCommandConflict
		}
		effect.State, effect.AppSnapshot = "snapshot_transferred", proof.Receipt
		if err := operation.CheckStepEffects(&agentv1.PlanStep{Command: command, Postcondition: agentv1.Postcondition_POSTCONDITION_APP_SNAPSHOT_TRANSFERRED}, []*agentv1.Effect{effect}); err != nil {
			return runtime.Result{}, err
		}
	}
	return runtime.Result{Effects: []*agentv1.Effect{effect}}, nil
}

// discardStaleLive keeps old data until the replacement replica is complete.
func (e *Executor) discardStaleLive(ctx context.Context, appID string) error {
	unlock, err := e.options.Replication.LockApp(ctx, appID)
	if err != nil {
		return err
	}
	defer unlock()
	assignment, ok := e.options.Replication.Lookup(appID)
	if !ok || assignment.Role != "standby" {
		return nil
	}
	native, ok := e.runtime.(writerRuntime)
	if !ok {
		return runtime.ErrUnsupportedConfiguration
	}
	containers, err := native.AppContainers(ctx)
	if err != nil {
		return err
	}
	for _, container := range containers {
		if container.AppID == appID && container.Running {
			return runtime.ErrBusy
		}
	}
	rootPath := filepath.Join(e.options.AppsDir, appID)
	root, err := os.OpenRoot(rootPath)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	defer root.Close()
	if _, err := root.Lstat("live"); errors.Is(err, os.ErrNotExist) {
		return nil
	} else if err != nil {
		return err
	}
	data, err := root.ReadFile("epoch")
	if err != nil {
		return err
	}
	epoch, err := strconv.ParseUint(strings.TrimSpace(string(data)), 10, 64)
	if err != nil {
		return err
	}
	if epoch >= assignment.Epoch {
		return nil
	}
	store := replica.Replica{Root: rootPath}
	manifest, err := store.Current()
	if err != nil {
		return err
	}
	if manifest == nil || manifest.Epoch != assignment.Epoch {
		return nil
	}
	if err := store.Verify(ctx); err != nil {
		return err
	}
	for _, name := range []string{"live", "snapshot", "final-snapshot"} {
		if err := root.RemoveAll(name); err != nil {
			return err
		}
	}
	dir, err := root.Open(".")
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}
