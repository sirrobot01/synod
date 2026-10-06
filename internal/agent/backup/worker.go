package backup

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/kopia/kopia/snapshot"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/client"
	"github.com/sirrobot01/synod/internal/agent/discovery"
	"github.com/sirrobot01/synod/internal/agent/journal"
	"github.com/sirrobot01/synod/internal/backup"
	"github.com/sirrobot01/synod/internal/inventory"
	"github.com/sirrobot01/synod/internal/repository"
	"github.com/sirrobot01/synod/internal/secret"
)

var ErrRestartPending = errors.New("app restart is pending; restore Docker access so the agent can resume the app")

// Worker executes one job at a time. The journal owns admission and crash recovery.
type Worker struct {
	Docker                 *client.Client
	Journal                *journal.Journal
	Keyring                *secret.Keyring
	InstallationID, NodeID uuid.UUID
	NodeName, Architecture string
	Identity               *repository.DeviceIdentity
	PrivatePaths           []string
}

type restartIntent struct {
	ID        string `json:"id"`
	Confirmed bool   `json:"confirmed"`
	Timeout   int    `json:"timeout"`
}

type progress struct {
	Version          int                `json:"version"`
	RequestID        uuid.UUID          `json:"request_id"`
	StartedAt        time.Time          `json:"started_at"`
	StoppingAt       time.Time          `json:"stopping_at,omitzero"`
	Restart          []restartIntent    `json:"restart,omitempty"`
	RepositoryOpened bool               `json:"repository_opened,omitzero"`
	FailureCode      string             `json:"failure_code,omitempty"`
	Prepared         *snapshot.Manifest `json:"prepared,omitempty"`
	Metadata         backup.Snapshot    `json:"metadata"`
}

// Execute starts admitted work once. A running record is recovered without another copy.
// The caller must not execute jobs concurrently on this worker.
func (w *Worker) Execute(ctx context.Context, request backup.Request) (backup.Result, error) {
	if request.Validate() != nil || request.NodeID != w.NodeID || request.InstallationID != w.InstallationID {
		return backup.Result{}, backup.ErrInvalid
	}
	input, _ := json.Marshal(request)
	record, err := w.Journal.Admit(ctx, request.ID, input)
	if err != nil {
		return backup.Result{}, err
	}
	if record.State == journal.Terminal {
		var result backup.Result
		if len(record.Result) == 0 {
			return result, nil
		} // The controller already acknowledged this result.
		if err := json.Unmarshal(record.Result, &result); err != nil {
			return result, journal.ErrConflict
		}
		return result, nil
	}
	state := progress{Version: 1, RequestID: request.ID, StartedAt: time.Now().UTC()}
	if record.State == journal.Running {
		if len(record.Progress) > 0 && (json.Unmarshal(record.Progress, &state) != nil || state.Version != 1 || state.RequestID != request.ID) {
			return backup.Result{}, journal.ErrConflict
		}
		if state.Prepared == nil && state.FailureCode == "" {
			state.FailureCode = "agent_interrupted"
		}
		return w.finish(ctx, request, &state, nil)
	}
	if err := w.Journal.Start(ctx, request.ID); err != nil {
		return backup.Result{}, err
	}
	if err := w.save(ctx, &state); err != nil {
		return backup.Result{}, err
	}
	plan, err := w.Journal.DestinationPlan(ctx, w.Keyring)
	if err != nil {
		state.FailureCode = "destination_unavailable"
		return w.finish(ctx, request, &state, nil)
	}
	location, credentials, err := plan.Connection(request.DestinationID, w.Identity)
	if err != nil {
		state.FailureCode = "destination_unavailable"
		return w.finish(ctx, request, &state, nil)
	}
	r, err := repository.Connect(ctx, location, credentials, request.Initialize)
	if err != nil {
		state.FailureCode = "repository_open_failed"
		return w.finish(ctx, request, &state, nil)
	}
	defer r.Close(context.Background())
	state.RepositoryOpened = true
	if err := w.save(ctx, &state); err != nil {
		return backup.Result{}, err
	}
	filters := make(client.Filters)
	if name, ok := strings.CutPrefix(request.SourceKey, "compose:"); ok {
		filters.Add("label", "com.docker.compose.project="+name)
	} else {
		filters.Add("id", strings.TrimPrefix(request.SourceKey, "container:"))
	}
	report, err := discovery.Scan(ctx, w.Docker, filters)
	if err != nil || len(report.Apps) != 1 || report.Apps[0].Key != request.SourceKey || inventory.Validate(report) != nil {
		state.FailureCode = "app_scan_failed"
		return w.finish(ctx, request, &state, r)
	}
	app := report.Apps[0]
	for _, item := range app.Containers {
		if item.Database != "" {
			state.FailureCode = "database_dump_unavailable"
			return w.finish(ctx, request, &state, r)
		}
	}
	if len(app.Issues) > 0 {
		state.FailureCode = "app_definition_incomplete"
		return w.finish(ctx, request, &state, r)
	}
	var inspected []container.InspectResponse
	for _, item := range app.Containers {
		response, err := w.Docker.ContainerInspect(ctx, item.Id, client.ContainerInspectOptions{})
		if err != nil || response.Container.Config == nil || response.Container.HostConfig == nil || response.Container.State == nil {
			state.FailureCode = "app_scan_failed"
			return w.finish(ctx, request, &state, r)
		}
		c := response.Container
		if c.State.Restarting || c.State.Dead || c.State.Status == "removing" || (request.Method == backup.Stop && (c.HostConfig.AutoRemove || (c.Config.StopTimeout != nil && (*c.Config.StopTimeout < 0 || *c.Config.StopTimeout > 600)))) {
			state.FailureCode = "app_state_unsafe"
			return w.finish(ctx, request, &state, r)
		}
		inspected = append(inspected, c)
	}
	raw, err := json.Marshal(inspected)
	if err != nil || len(raw) > 4<<20 {
		state.FailureCode = "app_definition_limit"
		return w.finish(ctx, request, &state, r)
	}
	definitions := map[string][]byte{"inspect.json": raw}
	if app.Kind == "container" {
		compose, err := StandaloneCompose(inspected[0])
		if err != nil {
			state.FailureCode = "standalone_definition_unsupported"
			return w.finish(ctx, request, &state, r)
		}
		definitions["compose.json"] = compose
	}
	excluded := make([]backup.Exclusion, 0, len(w.PrivatePaths)+1)
	for _, path := range w.PrivatePaths {
		excluded = append(excluded, backup.Exclusion{Source: path, Reason: "Synod identity and journal files are not app data."})
	}
	if location.Kind == "local" || (location.Kind == "device" && location.Device.NodeID == w.NodeID) {
		excluded = append(excluded, backup.Exclusion{Source: location.Path, Reason: "The destination must not be copied into itself."})
	}
	tree, err := NewTree(ctx, app, excluded)
	if err != nil {
		state.FailureCode = "source_unavailable"
		return w.finish(ctx, request, &state, r)
	}
	defer tree.Close()
	if app.Kind == "compose" && !slices.ContainsFunc(tree.Paths, func(path backup.Path) bool { return path.Kind == "compose" }) {
		state.FailureCode = "app_definition_excluded"
		return w.finish(ctx, request, &state, r)
	}
	state.Metadata = backup.Snapshot{
		ID:             request.ID,
		InstallationID: w.InstallationID,
		DestinationID:  request.DestinationID,
		App: inventory.App{
			ID:         request.AppID,
			NodeID:     w.NodeID,
			NodeName:   w.NodeName,
			ObservedAt: report.ObservedAt.AsTime(),
			Present:    true,
			Definition: app,
		},
		Method:       request.Method,
		Architecture: w.Architecture,
		StartedAt:    state.StartedAt,
		Paths:        tree.Paths,
	}
	source := snapshot.SourceInfo{Host: w.NodeID.String(), UserName: "synod", Path: request.AppID.String()}
	if request.Method == backup.Stop {
		root, err := tree.Root(ctx, definitions)
		if err == nil {
			_, err = r.UploadFiles(ctx, root, source, true)
		}
		if err != nil {
			state.FailureCode = "first_copy_failed"
			return w.finish(ctx, request, &state, r)
		}
		for _, c := range inspected {
			if !c.State.Running || c.State.Paused {
				continue
			}
			timeout := 10
			if c.Config.StopTimeout != nil {
				timeout = *c.Config.StopTimeout
			}
			if state.StoppingAt.IsZero() {
				state.StoppingAt = time.Now().UTC()
			}
			state.Restart = append(state.Restart, restartIntent{ID: c.ID, Timeout: timeout})
			if err := w.save(ctx, &state); err != nil {
				state.Restart = state.Restart[:len(state.Restart)-1]
				state.FailureCode = "journal_save_failed"
				return w.finish(ctx, request, &state, r)
			}
			stop, cancel := context.WithTimeout(ctx, time.Duration(timeout+30)*time.Second)
			_, err := w.Docker.ContainerStop(stop, c.ID, client.ContainerStopOptions{Timeout: new(timeout)})
			cancel()
			if err != nil {
				state.FailureCode = "app_stop_failed"
				return w.finish(ctx, request, &state, r)
			}
			state.Restart[len(state.Restart)-1].Confirmed = true
			if err := w.save(ctx, &state); err != nil {
				return w.finish(ctx, request, &state, r)
			}
		}
	}
	if request.Method == backup.Stop {
		if err := w.checkStopped(ctx, inspected); err != nil {
			state.FailureCode = "app_changed_during_backup"
			return w.finish(ctx, request, &state, r)
		}
	}
	final, cancel := context.WithTimeout(ctx, 30*time.Minute)
	defer cancel()
	root, err := tree.Root(final, definitions)
	if err == nil {
		state.Prepared, err = r.UploadFiles(final, root, source, false)
	}
	if err == nil && request.Method == backup.Stop {
		err = w.checkStopped(final, inspected)
	}
	if err != nil {
		state.Prepared = nil
		state.FailureCode = "final_copy_failed"
	} else {
		state.Metadata.Exclusions = tree.Exclusions()
	}
	durable, cancelSave := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	saveErr := w.save(durable, &state)
	cancelSave()
	if saveErr != nil {
		state.FailureCode = "journal_save_failed"
		state.Prepared = nil
	}
	return w.finish(ctx, request, &state, r)
}

func (w *Worker) save(ctx context.Context, state *progress) error {
	raw, err := json.Marshal(state)
	if err != nil {
		return journal.ErrConflict
	}
	return w.Journal.SaveProgress(ctx, state.RequestID, raw)
}

// finish restores each recorded container before it publishes a success or failure.
func (w *Worker) finish(ctx context.Context, request backup.Request, state *progress, r *repository.Repository) (backup.Result, error) {
	cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 15*time.Minute)
	defer cancel()
	var pending []restartIntent
	for _, intent := range state.Restart {
		if !intent.Confirmed {
			// A lost stop response is ambiguous. Finish the stop before starting this container.
			stop, cancelStop := context.WithTimeout(cleanup, time.Duration(intent.Timeout+30)*time.Second)
			_, err := w.Docker.ContainerStop(stop, intent.ID, client.ContainerStopOptions{Timeout: new(intent.Timeout)})
			cancelStop()
			if err != nil {
				pending = append(pending, intent)
				continue
			}
			intent.Confirmed = true
		}
		start, cancelStart := context.WithTimeout(cleanup, 30*time.Second)
		_, err := w.Docker.ContainerStart(start, intent.ID, client.ContainerStartOptions{})
		if err == nil {
			response, inspectErr := w.Docker.ContainerInspect(start, intent.ID, client.ContainerInspectOptions{})
			if inspectErr != nil || response.Container.State == nil || !response.Container.State.Running {
				err = ErrRestartPending
			}
		}
		cancelStart()
		if err != nil {
			pending = append(pending, intent)
		}
	}
	state.Restart = pending
	if err := w.save(cleanup, state); err != nil {
		return backup.Result{}, err
	}
	if len(pending) > 0 {
		return backup.Result{}, ErrRestartPending
	}
	if state.Metadata.CompletedAt.IsZero() {
		state.Metadata.CompletedAt = time.Now().UTC()
		if !state.StoppingAt.IsZero() {
			state.Metadata.DowntimeMillis = max(0, time.Since(state.StoppingAt).Milliseconds())
		}
	}
	if err := w.save(cleanup, state); err != nil {
		return backup.Result{}, err
	}
	if state.Prepared != nil && state.FailureCode == "" {
		if r == nil {
			plan, err := w.Journal.DestinationPlan(cleanup, w.Keyring)
			if err != nil {
				return backup.Result{}, err
			}
			location, credentials, err := plan.Connection(request.DestinationID, w.Identity)
			if err != nil {
				return backup.Result{}, err
			}
			r, err = repository.Open(cleanup, location, credentials)
			if err != nil {
				return backup.Result{}, err
			}
			defer r.Close(context.Background())
		}
		saved, err := r.PublishSnapshot(cleanup, state.Prepared, state.Metadata)
		if err != nil {
			state.FailureCode = "snapshot_publish_failed"
		} else {
			state.Metadata = saved
		}
	}
	if state.Prepared == nil && state.FailureCode == "" {
		state.FailureCode = "agent_interrupted"
	}
	result := backup.Result{DestinationID: request.DestinationID, StartedAt: state.StartedAt, CompletedAt: state.Metadata.CompletedAt, DowntimeMillis: state.Metadata.DowntimeMillis, RepositoryOpened: state.RepositoryOpened, FailureCode: state.FailureCode}
	if result.FailureCode == "" {
		result.SnapshotID = state.Metadata.ID
		result.FileBytes = state.Metadata.FileBytes
	}
	raw, _ := json.Marshal(result)
	if err := w.Journal.Complete(cleanup, request.ID, raw); err != nil {
		return backup.Result{}, err
	}
	return result, nil
}

// WaitingResult exposes only safe progress fields while durable recovery is pending.
func WaitingResult(record journal.Record) (backup.Result, error) {
	var state progress
	if len(record.Progress) == 0 {
		return backup.Result{}, nil
	}
	if json.Unmarshal(record.Progress, &state) != nil || state.Version != 1 || state.RequestID != record.ID {
		return backup.Result{}, journal.ErrConflict
	}
	result := backup.Result{StartedAt: state.StartedAt, RepositoryOpened: state.RepositoryOpened}
	for _, intent := range state.Restart {
		result.RestartPending = append(result.RestartPending, intent.ID)
	}
	return result, nil
}

// checkStopped refuses a Stop snapshot if a container resumed during the copy.
func (w *Worker) checkStopped(ctx context.Context, containers []container.InspectResponse) error {
	for _, original := range containers {
		current, err := w.Docker.ContainerInspect(ctx, original.ID, client.ContainerInspectOptions{})
		if err != nil || current.Container.State == nil {
			return ErrSource
		}
		state := current.Container.State
		if state.Running && !state.Paused || state.Restarting || state.Dead {
			return ErrSource
		}
	}
	return nil
}
