package backup

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"log/slog"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"
	kfs "github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/snapshot"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/network"
	"github.com/moby/moby/client"
	"github.com/sirrobot01/synod/internal/agent/discovery"
	"github.com/sirrobot01/synod/internal/agent/journal"
	"github.com/sirrobot01/synod/internal/backup"
	"github.com/sirrobot01/synod/internal/inventory"
	"github.com/sirrobot01/synod/internal/repository"
	"github.com/sirrobot01/synod/internal/runtime/compose"
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
	Logger                 *slog.Logger
	// CacheDirectory holds one Kopia metadata cache for each destination. Empty disables caching.
	CacheDirectory string
	images         sync.Map
	// catalogs maps a destination to the newest catalog generation known to be stored there.
	catalogs sync.Map
	// connections keeps one open repository for each destination between backups.
	connectionsMu sync.Mutex
	connections   map[uuid.UUID]connection
}

type connection struct {
	key  [32]byte
	r    *repository.Repository
	used time.Time
}

// ConnectionIdle is how long an unused repository connection stays open.
const ConnectionIdle = 10 * time.Minute

// connect reuses an idle connection opened with the same settings, or opens a new one.
func (w *Worker) connect(ctx context.Context, destinationID uuid.UUID, location repository.Location, credentials repository.Credentials, initialize bool) (*repository.Repository, [32]byte, error) {
	settings, _ := json.Marshal(struct {
		Location    repository.Location
		Credentials repository.Credentials
		Identity    *repository.DeviceIdentity
		Cache       string
	}{location, credentials, credentials.DeviceIdentity, credentials.CacheDirectory})
	key := sha256.Sum256(settings)
	clear(settings)
	w.connectionsMu.Lock()
	open, ok := w.connections[destinationID]
	delete(w.connections, destinationID)
	w.connectionsMu.Unlock()
	if ok {
		if open.key == key && time.Since(open.used) < ConnectionIdle {
			return open.r, key, nil
		}
		open.r.Close(context.WithoutCancel(ctx))
	}
	r, err := repository.Connect(ctx, location, credentials, initialize)
	return r, key, err
}

// release keeps a healthy connection for the next backup, or closes it.
func (w *Worker) release(destinationID uuid.UUID, r *repository.Repository, key [32]byte, healthy bool) {
	if !healthy {
		r.Close(context.Background())
		return
	}
	w.connectionsMu.Lock()
	defer w.connectionsMu.Unlock()
	if w.connections == nil {
		w.connections = make(map[uuid.UUID]connection)
	}
	if open, ok := w.connections[destinationID]; ok {
		open.r.Close(context.Background())
	}
	w.connections[destinationID] = connection{key: key, r: r, used: time.Now()}
}

// CloseIdle closes connections unused for at least idle. Zero closes all of them.
func (w *Worker) CloseIdle(idle time.Duration) {
	w.connectionsMu.Lock()
	defer w.connectionsMu.Unlock()
	for id, open := range w.connections {
		if time.Since(open.used) >= idle {
			open.r.Close(context.Background())
			delete(w.connections, id)
		}
	}
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
	// phases holds the milliseconds of each backup step for the backup_timing log.
	phases []any
	mark   time.Time
}

func (p *progress) phase(name string) {
	now := time.Now()
	if !p.mark.IsZero() {
		p.phases = append(p.phases, name, now.Sub(p.mark).Milliseconds())
	}
	p.mark = now
}

// Execute starts admitted work once. A running record is recovered without another copy.
// The caller must not execute jobs concurrently on this worker.
func (w *Worker) Execute(ctx context.Context, request backup.Request) (backup.Result, error) {
	if request.Validate() != nil || request.NodeID != w.NodeID || request.InstallationID != w.InstallationID {
		return backup.Result{}, backup.ErrInvalid
	}
	method := request.Method
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
	state.phase("")
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
	credentials.CacheDirectory = w.cache(request.DestinationID)
	r, key, err := w.connect(ctx, request.DestinationID, location, credentials, request.Initialize)
	if err != nil {
		state.FailureCode = "repository_open_failed"
		return w.finish(ctx, request, &state, nil)
	}
	// A failed backup can leave a broken connection. Only a clean result keeps it open.
	defer func() { w.release(request.DestinationID, r, key, state.FailureCode == "") }()
	state.RepositoryOpened = true
	if err := w.save(ctx, &state); err != nil {
		return backup.Result{}, err
	}
	if err := r.CheckInstallation(ctx, w.InstallationID); err != nil {
		state.FailureCode = "repository_installation_conflict"
		return w.finish(ctx, request, &state, r)
	}
	filters := make(client.Filters)
	if name, ok := strings.CutPrefix(request.SourceKey, "compose:"); ok {
		filters.Add("label", "com.docker.compose.project="+name)
	} else {
		filters.Add("id", strings.TrimPrefix(request.SourceKey, "container:"))
	}
	state.phase("open_repository")
	report, err := discovery.Scan(ctx, w.Docker, filters)
	state.phase("discover_app")
	if err != nil || len(report.Apps) != 1 || report.Apps[0].Key != request.SourceKey || inventory.Validate(report) != nil {
		state.FailureCode = "app_scan_failed"
		return w.finish(ctx, request, &state, r)
	}
	app := report.Apps[0]
	databases := make(map[string]string)
	for _, item := range app.Containers {
		if item.Database != "" {
			databases[item.Id] = item.Database
		}
	}
	if method == backup.Auto {
		method = backup.Stop
		if len(databases) > 0 {
			method = backup.Dump
		}
	}
	if method == backup.Dump && len(databases) == 0 {
		state.FailureCode = "database_not_detected"
		return w.finish(ctx, request, &state, r)
	}
	if len(app.Issues) > 0 {
		state.FailureCode = "app_definition_incomplete"
		return w.finish(ctx, request, &state, r)
	}
	var inspected, fileContainers []container.InspectResponse
	for _, item := range app.Containers {
		response, err := w.Docker.ContainerInspect(ctx, item.Id, client.ContainerInspectOptions{})
		if err != nil || response.Container.Config == nil || response.Container.HostConfig == nil || response.Container.State == nil {
			state.FailureCode = "app_scan_failed"
			return w.finish(ctx, request, &state, r)
		}
		c := response.Container
		if databases[c.ID] != "" && (!c.State.Running || c.State.Paused) {
			state.FailureCode = "database_unavailable"
			return w.finish(ctx, request, &state, r)
		}
		if c.State.Restarting || c.State.Dead || c.State.Status == "removing" || (method == backup.Stop && (c.HostConfig.AutoRemove || (c.Config.StopTimeout != nil && (*c.Config.StopTimeout < 0 || *c.Config.StopTimeout > 600)))) {
			state.FailureCode = "app_state_unsafe"
			return w.finish(ctx, request, &state, r)
		}
		inspected = append(inspected, c)
		if databases[c.ID] == "" {
			fileContainers = append(fileContainers, c)
		}
	}
	raw, err := json.Marshal(inspected)
	if err != nil || len(raw) > 4<<20 {
		state.FailureCode = "app_definition_limit"
		return w.finish(ctx, request, &state, r)
	}
	definitions := map[string][]byte{"inspect.json": raw}
	networks := make(map[string]network.Network)
	for _, c := range inspected {
		if c.NetworkSettings == nil {
			state.FailureCode = "app_scan_failed"
			return w.finish(ctx, request, &state, r)
		}
		for name := range c.NetworkSettings.Networks {
			if _, ok := networks[name]; ok || slices.Contains([]string{"bridge", "host", "none"}, name) {
				continue
			}
			response, err := w.Docker.NetworkInspect(ctx, name, client.NetworkInspectOptions{})
			if err != nil {
				state.FailureCode = "app_scan_failed"
				return w.finish(ctx, request, &state, r)
			}
			// Container endpoints can describe other apps. Keep only network settings.
			networks[name] = response.Network.Network
		}
	}
	raw, err = json.Marshal(networks)
	if err != nil || len(raw) > 4<<20 {
		state.FailureCode = "app_definition_limit"
		return w.finish(ctx, request, &state, r)
	}
	definitions["networks.json"] = raw
	state.phase("inspect_containers")
	// A registry lookup can take seconds. It runs during the copy and joins before the app stops.
	type imageResult struct {
		images []backup.Image
		err    error
	}
	resolved := make(chan imageResult, 1)
	go func() {
		images, err := w.imageEvidence(ctx, app)
		resolved <- imageResult{images, err}
	}()
	joinImages := func() bool {
		result := <-resolved
		state.phase("wait_images")
		state.Metadata.Images = result.images
		return result.err == nil
	}
	if app.Kind == "container" {
		definition, err := compose.FromContainer(inspected[0])
		if err != nil {
			state.FailureCode = "standalone_definition_unsupported"
			return w.finish(ctx, request, &state, r)
		}
		encoded, err := compose.Encode(definition)
		if err != nil {
			state.FailureCode = "standalone_definition_unsupported"
			return w.finish(ctx, request, &state, r)
		}
		definitions["compose.json"] = encoded
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
	tree.live = method == backup.Live || method == backup.Dump
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
		Method:       method,
		Architecture: w.Architecture,
		StartedAt:    state.StartedAt,
		Paths:        tree.Paths,
	}
	for _, path := range app.Paths {
		if path.Kind != "bind" || path.Status == "Never" || slices.ContainsFunc(state.Metadata.MountSources, func(source backup.MountSource) bool { return source.Source == path.Source }) {
			continue
		}
		info, err := os.Stat(path.Source)
		if err != nil {
			continue
		} // An unavailable excluded source remains explicitly absent.
		owner := info.Sys().(*syscall.Stat_t)
		state.Metadata.MountSources = append(state.Metadata.MountSources, backup.MountSource{Source: path.Source, Mode: uint32(info.Mode()), UserID: owner.Uid, GroupID: owner.Gid})
	}
	state.phase("prepare_tree")
	source := snapshot.SourceInfo{Host: w.NodeID.String(), UserName: "synod", Path: request.AppID.String()}
	// Without a readable previous snapshot, the backup reads every file.
	previous, err := r.LatestFiles(ctx, w.InstallationID, request.AppID, request.DestinationID)
	if err != nil {
		previous = nil
	}
	state.phase("read_previous")
	if method == backup.Stop {
		var copied *snapshot.Manifest
		root, err := tree.Root(ctx, definitions)
		if err == nil {
			copied, err = r.UploadFiles(ctx, root, source, true, previous)
		}
		// The stopped pass reads only files that changed after the live pass.
		previous = copied
		state.phase("first_copy")
		if !joinImages() {
			state.FailureCode = "image_inspection_failed"
			return w.finish(ctx, request, &state, r)
		}
		if err != nil {
			state.FailureCode = "first_copy_failed"
			return w.finish(ctx, request, &state, r)
		}
		for _, c := range fileContainers {
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
	if method == backup.Stop {
		state.phase("stop_app")
		if err := w.checkStopped(ctx, fileContainers); err != nil {
			state.FailureCode = "app_changed_during_backup"
			return w.finish(ctx, request, &state, r)
		}
	}
	final, cancel := context.WithTimeout(ctx, 30*time.Minute)
	defer cancel()
	var dumps []kfs.Entry
	var streams []*dumpFile
	for _, c := range inspected {
		if database := databases[c.ID]; database != "" {
			stream, metadata, err := newDump(w.Docker, c, database)
			if err != nil {
				state.FailureCode = "database_dump_failed"
				return w.finish(ctx, request, &state, r)
			}
			dumps = append(dumps, stream)
			streams = append(streams, stream)
			state.Metadata.Dumps = append(state.Metadata.Dumps, metadata)
		}
	}
	root, err := tree.Root(final, definitions, dumps...)
	if err == nil {
		state.Prepared, err = r.UploadFiles(final, root, source, false, previous)
	}
	if err == nil && method == backup.Stop {
		err = w.checkStopped(final, fileContainers)
	}
	state.phase("final_copy")
	if method != backup.Stop && !joinImages() {
		state.Prepared = nil
		state.FailureCode = "image_inspection_failed"
		return w.finish(ctx, request, &state, r)
	}
	tree.sqliteMu.Lock()
	sqliteErr := tree.sqliteErr
	sqliteFiles := slices.Sorted(maps.Keys(tree.sqlite))
	tree.sqliteMu.Unlock()
	if err != nil {
		state.Prepared = nil
		state.FailureCode = "final_copy_failed"
		if sqliteErr != nil {
			state.FailureCode = "sqlite_copy_failed"
		}
		if errors.Is(sqliteErr, ErrSQLitePlatform) {
			state.FailureCode = "sqlite_copy_unavailable"
		}
		for _, stream := range streams {
			if stream.failed.Load() {
				state.FailureCode = "database_dump_failed"
			}
		}
	} else {
		state.Metadata.Exclusions = tree.Exclusions()
		state.Metadata.Paths = tree.Paths
		state.Metadata.SQLite = sqliteFiles
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

func (w *Worker) cache(destinationID uuid.UUID) string {
	if w.CacheDirectory == "" {
		return ""
	}
	return filepath.Join(w.CacheDirectory, destinationID.String())
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
	state.phase("restart_app")
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
			credentials.CacheDirectory = w.cache(request.DestinationID)
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
		state.phase("publish")
	}
	if state.Prepared == nil && state.FailureCode == "" {
		state.FailureCode = "agent_interrupted"
	}
	result := backup.Result{DestinationID: request.DestinationID, StartedAt: state.StartedAt, CompletedAt: state.Metadata.CompletedAt, DowntimeMillis: state.Metadata.DowntimeMillis, RepositoryOpened: state.RepositoryOpened, FailureCode: state.FailureCode}
	if result.FailureCode == "" {
		result.SnapshotID = state.Metadata.ID
		result.FileBytes = state.Metadata.FileBytes
	}
	if r != nil && state.RepositoryOpened {
		plan, catalogErr := w.Journal.DestinationPlan(cleanup, w.Keyring)
		var catalog repository.Catalog
		if catalogErr == nil {
			catalog, catalogErr = repository.DecodeCatalog(plan.Catalog)
		}
		if catalogErr == nil && catalog.InstallationID != w.InstallationID {
			catalogErr = repository.ErrCatalogConflict
		}
		if stored, ok := w.catalogs.Load(request.DestinationID); catalogErr == nil && ok && stored.(uint64) >= catalog.Generation {
			// This destination already holds this catalog or a newer one.
		} else if catalogErr == nil {
			previous, err := r.ReadCatalog(cleanup)
			if err != nil && !errors.Is(err, repository.ErrCatalogMissing) {
				catalogErr = err
			} else if err == nil && previous.InstallationID != w.InstallationID {
				catalogErr = repository.ErrCatalogConflict
			} else if previous.Generation <= catalog.Generation {
				catalogErr = r.WriteCatalog(cleanup, catalog)
				// Another agent can commit a newer generation between the read and write.
				if errors.Is(catalogErr, repository.ErrCatalogConflict) {
					latest, err := r.ReadCatalog(cleanup)
					if err == nil && latest.InstallationID == w.InstallationID && latest.Generation > catalog.Generation {
						catalogErr = nil
					}
				}
			}
		}
		if catalogErr != nil {
			result.WarningCodes = append(result.WarningCodes, "catalog_write_failed")
		} else if stored, ok := w.catalogs.Load(request.DestinationID); !ok || stored.(uint64) < catalog.Generation {
			w.catalogs.Store(request.DestinationID, catalog.Generation)
		}
		state.phase("write_catalog")
		if result.FailureCode == "" && request.Retention != nil {
			if _, err := r.ApplyRetention(cleanup, w.InstallationID, request.AppID, request.DestinationID, *request.Retention, result.SnapshotID); err != nil {
				result.WarningCodes = append(result.WarningCodes, "retention_failed")
			}
			state.phase("apply_retention")
		}
	}
	if w.Logger != nil && len(state.phases) > 0 {
		counts := []any{"operation_id", request.ID, "method", state.Metadata.Method}
		if state.Prepared != nil {
			counts = append(counts, "files_reused", state.Prepared.Stats.CachedFiles, "files_read", state.Prepared.Stats.NonCachedFiles)
		}
		w.Logger.Info("backup_timing", append(counts, state.phases...)...)
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
