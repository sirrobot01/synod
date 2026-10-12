package restore

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/client"
	agentbackup "github.com/sirrobot01/synod/internal/agent/backup"
	"github.com/sirrobot01/synod/internal/agent/journal"
	"github.com/sirrobot01/synod/internal/backup"
	"github.com/sirrobot01/synod/internal/proof"
	"github.com/sirrobot01/synod/internal/repository"
	"github.com/sirrobot01/synod/internal/restore"
	"github.com/sirrobot01/synod/internal/runtime/compose"
)

// ErrRecoveryPending means a replaced app is not running again yet. The worker retries recovery.
var ErrRecoveryPending = errors.New("replaced app recovery is pending; restore Docker access so the agent can start the app again")

const (
	phasePreparing = "preparing"
	phaseSafety    = "safety"
	phaseReplacing = "replacing"
	phaseStarting  = "starting"
	phaseStarted   = "started"
)

// Worker restores one app at a time on this device. It shares the journal and identity of the backup worker.
// The caller must not run it at the same time as the backup worker.
type Worker struct {
	Backup *agentbackup.Worker
	// Directory holds one private directory for each restore operation.
	Directory string
}

type progress struct {
	Version          int                         `json:"version"`
	RequestID        uuid.UUID                   `json:"request_id"`
	StartedAt        time.Time                   `json:"started_at"`
	Phase            string                      `json:"phase"`
	SnapshotID       uuid.UUID                   `json:"snapshot_id,omitzero"`
	DestinationID    uuid.UUID                   `json:"destination_id,omitzero"`
	Project          string                      `json:"project,omitempty"`
	ReplacedKey      string                      `json:"replaced_key,omitempty"`
	Replaced         []container.InspectResponse `json:"replaced,omitempty"`
	SafetySnapshotID uuid.UUID                   `json:"safety_snapshot_id,omitzero"`
	FailureCode      string                      `json:"failure_code,omitempty"`
	Resource         string                      `json:"resource,omitempty"`
	Warnings         []string                    `json:"warnings,omitempty"`
	Checks           []proof.Check               `json:"checks,omitempty"`
}

// Execute starts admitted work once. A running record is recovered and never copied again.
func (w *Worker) Execute(ctx context.Context, request restore.Request) (restore.Result, error) {
	b := w.Backup
	if request.Validate() != nil || request.NodeID != b.NodeID || request.InstallationID != b.InstallationID || !filepath.IsAbs(w.Directory) {
		return restore.Result{}, restore.ErrInvalid
	}
	input, _ := json.Marshal(request)
	record, err := b.Journal.Admit(ctx, request.ID, input)
	if err != nil {
		return restore.Result{}, err
	}
	if record.State == journal.Terminal {
		var result restore.Result
		if len(record.Result) == 0 {
			return result, nil
		} // The controller already acknowledged this result.
		if err := json.Unmarshal(record.Result, &result); err != nil {
			return result, journal.ErrConflict
		}
		w.releaseSafety(ctx, request)
		return result, nil
	}
	state := progress{Version: 1, RequestID: request.ID, StartedAt: time.Now().UTC(), Phase: phasePreparing}
	if record.State == journal.Running {
		if len(record.Progress) > 0 && (json.Unmarshal(record.Progress, &state) != nil || state.Version != 1 || state.RequestID != request.ID) {
			return restore.Result{}, journal.ErrConflict
		}
		if state.Phase == phaseStarted {
			return w.finish(ctx, request, &state, nil)
		}
		return w.finish(ctx, request, &state, &failure{Code: "agent_interrupted"})
	}
	raw, err := json.Marshal(state)
	if err != nil {
		return restore.Result{}, journal.ErrConflict
	}
	if err := b.Journal.StartWithProgress(ctx, request.ID, raw); err != nil {
		return restore.Result{}, err
	}
	return w.finish(ctx, request, &state, w.run(ctx, request, &state))
}

func (w *Worker) save(ctx context.Context, state *progress) error {
	raw, err := json.Marshal(state)
	if err != nil {
		return journal.ErrConflict
	}
	return w.Backup.Journal.SaveProgress(ctx, state.RequestID, raw)
}

// run changes the existing app only after every isolated step has passed.
func (w *Worker) run(ctx context.Context, request restore.Request, state *progress) error {
	api := w.Backup.Docker
	r, saved, destinationID, unavailable, err := w.openSnapshot(ctx, request.AppID, request.SnapshotID, request.DestinationID)
	if err != nil {
		return err
	}
	state.SnapshotID, state.DestinationID = saved.ID, destinationID
	if unavailable {
		state.Warnings = append(state.Warnings, "destination_unavailable")
	}
	defer r.Close(context.Background())
	if err := w.save(ctx, state); err != nil {
		return err
	}
	directory := filepath.Join(w.Directory, request.ID.String())
	p, err := makePlan(ctx, r, saved, request.ID, directory, w.Backup.Architecture)
	if err != nil {
		return err
	}
	state.Warnings = append(state.Warnings, p.Warnings...)
	state.Project = p.Definition.Name
	if err := w.save(ctx, state); err != nil {
		return err
	}
	if err := dockerCLI(ctx, api, "compose", "version"); err != nil {
		return &failure{Code: "compose_unavailable", Cause: err}
	}
	replaced, key, err := findConflicts(ctx, api, p, request.Replace)
	if err != nil {
		return err
	}
	if err := loadImages(ctx, api, r, p, w.Backup.Architecture); err != nil {
		return err
	}
	if err := createNetworks(ctx, api, p, request.ID); err != nil {
		return err
	}
	if err := os.MkdirAll(w.Directory, 0o700); err != nil {
		return &failure{Code: "file_restore_failed", Resource: w.Directory, Cause: err}
	}
	if err := os.Mkdir(directory, 0o700); err != nil {
		return &failure{Code: "file_restore_failed", Resource: directory, Cause: err}
	}
	if err := os.Mkdir(filepath.Join(directory, "data"), 0o700); err != nil {
		return &failure{Code: "file_restore_failed", Resource: directory, Cause: err}
	}
	helper, err := prepareData(ctx, api, r, p, request.ID, directory)
	if helper != "" {
		if _, removeErr := api.ContainerRemove(context.WithoutCancel(ctx), helper, client.ContainerRemoveOptions{Force: true, RemoveVolumes: true}); removeErr != nil && err == nil {
			err = &failure{Code: "file_restore_failed", Cause: removeErr}
		}
	}
	if err != nil {
		return err
	}
	state.Checks = checkData(ctx, api, r, p, directory)
	if err := prepareDatabases(ctx, api, r, p, request.ID); err != nil {
		return err
	}
	definition, err := compose.Encode(p.Definition)
	if err != nil {
		return &failure{Code: "definition_unsupported", Resource: p.Definition.Name, Cause: err}
	}
	file := filepath.Join(directory, "compose.json")
	if err := os.WriteFile(file, definition, 0o600); err != nil {
		return &failure{Code: "file_restore_failed", Resource: file, Cause: err}
	}
	if len(replaced) > 0 {
		if err := w.replace(ctx, request, state, p, replaced, key); err != nil {
			return err
		}
	} else if _, _, err := findConflicts(ctx, api, p, false); err != nil {
		return err
	}
	state.Phase = phaseStarting
	if err := w.save(ctx, state); err != nil {
		return err
	}
	if err := dockerCLI(ctx, api, "compose", "--project-name", p.Definition.Name, "--project-directory", directory, "--file", file, "up", "--detach", "--no-build", "--pull", "never"); err != nil {
		return &failure{Code: "compose_start_failed", Resource: p.Definition.Name, Cause: err}
	}
	if err := checkStarted(ctx, api, p, request.ID); err != nil {
		return err
	}
	state.Checks = append(state.Checks, checkApp(ctx, api, p, drillTargets(p))...)
	if slices.ContainsFunc(state.Checks, func(check proof.Check) bool { return check.Status == proof.CheckFailed }) {
		state.Warnings = append(state.Warnings, "restore_check_failed")
	}
	state.Phase = phaseStarted
	return w.save(ctx, state)
}

// replace takes the safety snapshot, then stops and removes the existing containers.
// Volumes and bind paths of the existing app stay in place.
func (w *Worker) replace(ctx context.Context, request restore.Request, state *progress, p *plan, replaced []string, key string) error {
	api := w.Backup.Docker
	state.ReplacedKey = key
	for _, id := range replaced {
		response, err := api.ContainerInspect(ctx, id, client.ContainerInspectOptions{})
		if err != nil || response.Container.Config == nil || response.Container.HostConfig == nil || response.Container.State == nil {
			return &failure{Code: "app_changed_during_restore", Resource: id, Cause: err}
		}
		state.Replaced = append(state.Replaced, response.Container)
	}
	state.Phase = phaseSafety
	if err := w.save(ctx, state); err != nil {
		return err
	}
	result, err := w.Backup.Execute(ctx, safetyRequest(request, state, w.Backup.NodeID))
	if err != nil || result.FailureCode != "" {
		return &failure{Code: "safety_backup_failed", Resource: key, Cause: err}
	}
	state.SafetySnapshotID = result.SnapshotID
	current, _, err := findConflicts(ctx, api, p, true)
	if err != nil {
		return err
	}
	slices.Sort(current)
	if !slices.Equal(current, slices.Sorted(slices.Values(replaced))) {
		return &failure{Code: "app_changed_during_restore", Resource: key}
	}
	for i, old := range state.Replaced {
		response, err := api.ContainerInspect(ctx, old.ID, client.ContainerInspectOptions{})
		if err != nil || response.Container.State == nil {
			return &failure{Code: "app_changed_during_restore", Resource: old.ID, Cause: err}
		}
		// The safety backup restarts what it stopped. Keep the state that the app has now.
		state.Replaced[i].State = response.Container.State
	}
	state.Phase = phaseReplacing
	if err := w.save(ctx, state); err != nil {
		return err
	}
	for _, old := range state.Replaced {
		timeout := 10
		if old.Config.StopTimeout != nil {
			timeout = *old.Config.StopTimeout
		}
		stop, cancel := context.WithTimeout(ctx, time.Duration(timeout+30)*time.Second)
		_, err := api.ContainerStop(stop, old.ID, client.ContainerStopOptions{Timeout: new(timeout)})
		cancel()
		if err != nil {
			return &failure{Code: "app_stop_failed", Resource: strings.TrimPrefix(old.Name, "/"), Cause: err}
		}
	}
	for _, old := range state.Replaced {
		if _, err := api.ContainerRemove(ctx, old.ID, client.ContainerRemoveOptions{}); err != nil {
			return &failure{Code: "app_stop_failed", Resource: strings.TrimPrefix(old.Name, "/"), Cause: err}
		}
	}
	return nil
}

func safetyRequest(request restore.Request, state *progress, nodeID uuid.UUID) backup.Request {
	return backup.Request{ID: backup.SafetyID(request.ID), InstallationID: request.InstallationID, AppID: uuid.NewSHA1(nodeID, []byte(state.ReplacedKey)), NodeID: nodeID, SourceKey: state.ReplacedKey, DestinationID: state.DestinationID, Method: backup.Auto, RequestedAt: request.RequestedAt, RestoreID: request.ID}
}

// releaseSafety drops the delivered safety record. The restore result reports its snapshot.
func (w *Worker) releaseSafety(ctx context.Context, request restore.Request) {
	if request.Replace {
		_ = w.Backup.Journal.Acknowledge(ctx, backup.SafetyID(request.ID))
	}
}

// finish recovers a replaced app and removes isolated resources before it saves a failure.
func (w *Worker) finish(ctx context.Context, request restore.Request, state *progress, cause error) (restore.Result, error) {
	cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 15*time.Minute)
	defer cancel()
	result := restore.Result{SnapshotID: state.SnapshotID, DestinationID: state.DestinationID, SafetySnapshotID: state.SafetySnapshotID, StartedAt: state.StartedAt}
	if cause != nil || state.Phase != phaseStarted {
		if state.FailureCode == "" {
			state.FailureCode, state.Resource = "agent_interrupted", ""
			if failed, ok := errors.AsType[*failure](cause); ok {
				state.FailureCode, state.Resource = failed.Code, failed.Resource
			}
			if err := w.save(cleanup, state); err != nil {
				return restore.Result{}, err
			}
		}
		if state.Phase == phaseSafety {
			// A lost safety backup restarts the containers that it stopped.
			if _, err := w.Backup.Execute(cleanup, safetyRequest(request, state, w.Backup.NodeID)); err != nil {
				return restore.Result{}, err
			}
		}
		if err := removeLabelled(cleanup, w.Backup.Docker, request.ID, false); err != nil {
			return restore.Result{}, err
		}
		if state.Phase == phaseReplacing || state.Phase == phaseStarting || state.Phase == phaseStarted {
			if err := recoverReplaced(cleanup, w.Backup.Docker, state.Replaced); err != nil {
				return restore.Result{}, errors.Join(ErrRecoveryPending, err)
			}
		}
		if err := errors.Join(removeLabelled(cleanup, w.Backup.Docker, request.ID, true), os.RemoveAll(filepath.Join(w.Directory, request.ID.String()))); err != nil {
			state.Warnings = append(state.Warnings, "cleanup_pending")
		}
		result.FailureCode, result.Resource = state.FailureCode, state.Resource
	} else {
		result.TargetAppID = uuid.NewSHA1(w.Backup.NodeID, []byte("compose:"+state.Project))
		result.ProjectDirectory = filepath.Join(w.Directory, request.ID.String())
		result.Checks = state.Checks
	}
	for _, warning := range state.Warnings {
		if !slices.Contains(result.WarningCodes, warning) {
			result.WarningCodes = append(result.WarningCodes, warning)
		}
	}
	result.CompletedAt = time.Now().UTC()
	raw, _ := json.Marshal(result)
	if err := w.Backup.Journal.Complete(cleanup, request.ID, raw); err != nil {
		return restore.Result{}, err
	}
	w.releaseSafety(cleanup, request)
	return result, nil
}

// WaitingResult exposes only safe progress fields while restore work runs or waits for recovery.
func WaitingResult(record journal.Record) (restore.Result, error) {
	var state progress
	if len(record.Progress) == 0 {
		return restore.Result{}, nil
	}
	if json.Unmarshal(record.Progress, &state) != nil || state.Version != 1 || state.RequestID != record.ID {
		return restore.Result{}, journal.ErrConflict
	}
	result := restore.Result{SnapshotID: state.SnapshotID, DestinationID: state.DestinationID, SafetySnapshotID: state.SafetySnapshotID, StartedAt: state.StartedAt}
	if state.Phase == phaseReplacing || state.Phase == phaseStarting {
		for _, old := range state.Replaced {
			result.RecoveryPending = append(result.RecoveryPending, old.ID)
		}
	}
	return result, nil
}

// openSnapshot opens the requested snapshot, or the newest snapshot in every available destination.
// It reports whether a destination could not be read.
func (w *Worker) openSnapshot(ctx context.Context, appID, snapshotID, destinationID uuid.UUID) (*repository.Repository, backup.Snapshot, uuid.UUID, bool, error) {
	b := w.Backup
	plan, err := b.Journal.DestinationPlan(ctx, b.Keyring)
	if err != nil {
		return nil, backup.Snapshot{}, uuid.Nil, false, &failure{Code: "destination_unavailable", Cause: err}
	}
	var candidates []uuid.UUID
	for _, entry := range plan.Destinations {
		if destinationID == uuid.Nil || entry.Destination.ID == destinationID {
			candidates = append(candidates, entry.Destination.ID)
		}
	}
	var best *repository.Repository
	var selected backup.Snapshot
	var selectedDestination uuid.UUID
	unavailable, opened := false, false
	code := "destination_unavailable"
	for _, id := range candidates {
		location, credentials, err := plan.Connection(id, b.Identity)
		if err != nil {
			unavailable = true
			continue
		}
		r, err := repository.Open(ctx, location, credentials)
		if err != nil {
			unavailable, code = true, "repository_open_failed"
			continue
		}
		opened = true
		var saved backup.Snapshot
		if snapshotID != uuid.Nil {
			saved, err = r.Snapshot(ctx, snapshotID)
			if errors.Is(err, repository.ErrSnapshotMissing) {
				r.Close(context.Background())
				continue
			}
			if err == nil && (saved.InstallationID != b.InstallationID || saved.App.ID != appID) {
				r.Close(context.Background())
				if best != nil {
					best.Close(context.Background())
				}
				return nil, backup.Snapshot{}, uuid.Nil, false, &failure{Code: "snapshot_invalid"}
			}
		} else {
			var list []backup.Snapshot
			list, err = r.Snapshots(ctx, b.InstallationID, appID)
			if err == nil && len(list) == 0 {
				r.Close(context.Background())
				continue
			}
			if err == nil {
				saved = list[0]
			}
		}
		if err != nil {
			r.Close(context.Background())
			unavailable = true
			continue
		}
		if best == nil || saved.CompletedAt.After(selected.CompletedAt) {
			if best != nil {
				best.Close(context.Background())
			}
			best, selected, selectedDestination = r, saved, id
			continue
		}
		r.Close(context.Background())
	}
	if best == nil {
		if opened {
			code = "snapshot_not_found"
		}
		return nil, backup.Snapshot{}, uuid.Nil, false, &failure{Code: code}
	}
	return best, selected, selectedDestination, unavailable, nil
}
