package restore

import (
	"context"
	"encoding/json"
	"errors"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/moby/moby/client"
	"github.com/sirrobot01/synod/internal/agent/journal"
	"github.com/sirrobot01/synod/internal/proof"
	"github.com/sirrobot01/synod/internal/repository"
	"github.com/sirrobot01/synod/internal/runtime/compose"
)

var ErrProofCleanupPending = errors.New("drill cleanup is pending; restore Docker access so the agent can remove its temporary resources")

type proofProgress struct {
	Version   int          `json:"version"`
	RequestID uuid.UUID    `json:"request_id"`
	Result    proof.Result `json:"result"`
}

// ExecuteProof shares the backup queue. A drill is complete only after cleanup succeeds.
func (w *Worker) ExecuteProof(ctx context.Context, request proof.Request) (proof.Result, error) {
	b := w.Backup
	if request.Validate() != nil || request.NodeID != b.NodeID || request.InstallationID != b.InstallationID || !filepath.IsAbs(w.Directory) {
		return proof.Result{}, proof.ErrInvalid
	}
	input, _ := json.Marshal(request)
	record, err := b.Journal.Admit(ctx, request.ID, input)
	if err != nil {
		return proof.Result{}, err
	}
	state := proofProgress{Version: 1, RequestID: request.ID, Result: proof.Result{StartedAt: time.Now().UTC()}}
	if record.State == journal.Terminal {
		if len(record.Result) == 0 {
			return proof.Result{}, nil
		}
		if json.Unmarshal(record.Result, &state.Result) != nil {
			return proof.Result{}, journal.ErrConflict
		}
		return state.Result, nil
	}
	if record.State == journal.Running {
		if len(record.Progress) > 0 && (json.Unmarshal(record.Progress, &state) != nil || state.Version != 1 || state.RequestID != request.ID) {
			return proof.Result{}, journal.ErrConflict
		}
		state.Result.FailureCode = "agent_interrupted"
	} else {
		raw, _ := json.Marshal(state)
		if err := b.Journal.StartWithProgress(ctx, request.ID, raw); err != nil {
			return proof.Result{}, err
		}
		limit := 2 * time.Hour
		if request.Kind == proof.Maintenance {
			limit = repository.MaintenanceRunLimit
		}
		run, cancel := context.WithTimeout(ctx, limit)
		err := w.runProof(run, request, &state.Result)
		cancel()
		if err != nil {
			state.Result.FailureCode = "agent_interrupted"
			if failed, ok := errors.AsType[*failure](err); ok {
				state.Result.FailureCode, state.Result.Resource = failed.Code, failed.Resource
			}
		}
	}
	cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Minute)
	defer cancel()
	if request.Kind == proof.Drill {
		if err := removeLabelled(cleanup, b.Docker, request.ID, true); err != nil {
			return proof.Result{}, errors.Join(ErrProofCleanupPending, err)
		}
		if err := os.RemoveAll(filepath.Join(w.Directory, request.ID.String())); err != nil {
			return proof.Result{}, errors.Join(ErrProofCleanupPending, err)
		}
	}
	state.Result.CompletedAt = time.Now().UTC()
	raw, _ := json.Marshal(state.Result)
	if err := b.Journal.Complete(cleanup, request.ID, raw); err != nil {
		return proof.Result{}, err
	}
	return state.Result, nil
}

func (w *Worker) runProof(ctx context.Context, request proof.Request, result *proof.Result) error {
	b := w.Backup
	if request.Kind == proof.Maintenance {
		plan, err := b.Journal.DestinationPlan(ctx, b.Keyring)
		if err != nil {
			return &failure{Code: "destination_unavailable", Cause: err}
		}
		// A newer plan can give the repository to another device after this job was admitted.
		if !slices.ContainsFunc(plan.Proofs, func(s proof.Schedule) bool {
			return s.Kind == proof.Maintenance && s.DestinationID == request.DestinationID && s.NodeID == b.NodeID
		}) {
			return &failure{Code: "maintenance_not_owner"}
		}
		location, credentials, err := plan.Connection(request.DestinationID, b.Identity)
		if err != nil {
			return &failure{Code: "destination_unavailable", Cause: err}
		}
		result.DestinationID = request.DestinationID
		state, err := repository.Maintain(ctx, location, credentials, request.InstallationID, b.NodeID, plan.Revision)
		if errors.Is(err, repository.ErrMaintenanceNotOwner) {
			return &failure{Code: "maintenance_not_owner", Cause: err}
		}
		if err != nil {
			return &failure{Code: "maintenance_failed", Cause: err}
		}
		result.Maintenance = string(state)
		return nil
	}
	if request.Kind == proof.Integrity {
		plan, err := b.Journal.DestinationPlan(ctx, b.Keyring)
		if err != nil {
			return &failure{Code: "destination_unavailable", Cause: err}
		}
		location, credentials, err := plan.Connection(request.DestinationID, b.Identity)
		if err != nil {
			return &failure{Code: "destination_unavailable", Cause: err}
		}
		r, err := repository.Open(ctx, location, credentials)
		if err != nil {
			return &failure{Code: "repository_open_failed", Cause: err}
		}
		defer r.Close(context.Background())
		result.DestinationID = request.DestinationID
		checked, err := r.Check(ctx, request.InstallationID, 1)
		result.Contents, result.Read, result.BytesRead = checked.Contents, checked.Read, checked.BytesRead
		if err != nil {
			return &failure{Code: "integrity_check_failed", Cause: err}
		}
		return nil
	}
	r, saved, destinationID, _, err := w.openSnapshot(ctx, request.AppID, request.SnapshotID, request.DestinationID)
	if err != nil {
		return err
	}
	defer r.Close(context.Background())
	result.SnapshotID, result.DestinationID = saved.ID, destinationID
	directory := filepath.Join(w.Directory, request.ID.String())
	p, err := makePlan(ctx, r, saved, request.ID, directory, b.Architecture, true)
	if err != nil {
		return err
	}
	targets := drillTargets(p)
	if err := isolateDrill(p, request.ID, directory); err != nil {
		return err
	}
	if err := dockerCLI(ctx, b.Docker, "compose", "version"); err != nil {
		return &failure{Code: "compose_unavailable", Cause: err}
	}
	if err := loadImages(ctx, b.Docker, r, p, b.Architecture); err != nil {
		return err
	}
	if err := os.MkdirAll(w.Directory, 0o700); err != nil {
		return &failure{Code: "file_restore_failed", Cause: err}
	}
	if err := os.Mkdir(directory, 0o700); err != nil {
		return &failure{Code: "file_restore_failed", Cause: err}
	}
	if err := os.Mkdir(filepath.Join(directory, "data"), 0o700); err != nil {
		return &failure{Code: "file_restore_failed", Cause: err}
	}
	helper, err := prepareData(ctx, b.Docker, r, p, request.ID, directory)
	if helper != "" {
		_, removeErr := b.Docker.ContainerRemove(context.WithoutCancel(ctx), helper, client.ContainerRemoveOptions{Force: true, RemoveVolumes: true})
		err = errors.Join(err, removeErr)
	}
	if err != nil {
		return err
	}
	// The data check runs before any app container can change the restored files.
	result.Checks = checkData(ctx, b.Docker, r, p, directory)
	if err := prepareDatabases(ctx, b.Docker, r, p, request.ID); err != nil {
		return err
	}
	definition, err := compose.Encode(p.Definition)
	if err != nil {
		return &failure{Code: "definition_unsupported", Cause: err}
	}
	file := filepath.Join(directory, "compose.json")
	if err := os.WriteFile(file, definition, 0o600); err != nil {
		return &failure{Code: "file_restore_failed", Cause: err}
	}
	if err := dockerCLI(ctx, b.Docker, "compose", "--project-name", p.Definition.Name, "--project-directory", directory, "--file", file, "up", "--detach", "--no-build", "--pull", "never"); err != nil {
		return &failure{Code: "compose_start_failed", Cause: err}
	}
	for until := time.Now().Add(30 * time.Second); ; {
		for _, key := range slices.Sorted(maps.Keys(p.Definition.Services)) {
			name := p.Definition.Services[key].ContainerName
			inspected, err := b.Docker.ContainerInspect(ctx, name, client.ContainerInspectOptions{})
			c := inspected.Container
			if err != nil || c.Config == nil || c.HostConfig == nil || c.State == nil || c.NetworkSettings == nil || c.Config.Labels[operationLabel] != request.ID.String() || len(c.HostConfig.PortBindings) != 0 || len(c.NetworkSettings.Networks) != 1 || c.NetworkSettings.Networks[drillNetwork(request.ID)] == nil {
				return &failure{Code: "drill_isolation_failed", Resource: key, Cause: err}
			}
			if !c.State.Running || c.State.Restarting || c.RestartCount != 0 || c.State.OOMKilled || (c.State.Health != nil && c.State.Health.Status == "unhealthy") {
				return &failure{Code: "drill_container_stopped", Resource: key}
			}
		}
		if !time.Now().Before(until) {
			break
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}
	network, err := b.Docker.NetworkInspect(ctx, drillNetwork(request.ID), client.NetworkInspectOptions{})
	if err != nil || !network.Network.Internal || network.Network.Labels[operationLabel] != request.ID.String() {
		return &failure{Code: "drill_isolation_failed", Resource: drillNetwork(request.ID), Cause: err}
	}
	result.Containers = len(p.Definition.Services)
	result.Checks = append(result.Checks, checkApp(ctx, b.Docker, p, targets)...)
	for _, check := range result.Checks {
		if check.Status == proof.CheckFailed {
			return &failure{Code: "drill_check_failed", Resource: check.Kind + " " + check.Resource}
		}
	}
	return nil
}

func drillNetwork(id uuid.UUID) string { return "synod-drill-" + id.String() }

// isolateDrill permits only copied mounts and ordinary container namespaces.
// Rebuild each service from the allowed fields so new Compose fields cannot weaken isolation.
// All services join one internal network. It has no route out and publishes no ports.
// Each service keeps its service name, container name, and network aliases on it, so the
// app's services find each other as on the source.
func isolateDrill(p *plan, id uuid.UUID, directory string) error {
	p.Definition.Name = drillNetwork(id)
	p.Definition.Networks = map[string]compose.Network{"drill": {Name: drillNetwork(id), Internal: true, Labels: map[string]string{operationLabel: id.String()}}}
	names := make(map[string]string)
	for _, c := range p.Containers {
		names[p.Services[c.ID]] = strings.TrimPrefix(c.Name, "/")
	}
	for key, service := range p.Definition.Services {
		aliases := []string{key}
		if names[key] != "" {
			aliases = append(aliases, names[key])
		}
		for _, attachment := range service.Networks {
			aliases = append(aliases, attachment.Aliases...)
		}
		slices.Sort(aliases)
		aliases = slices.Compact(aliases)
		if service.Privileged || service.PID != "" || (service.IPC != "" && service.IPC != "private" && service.IPC != "shareable") || service.UTS != "" || service.UserNS != "" || len(service.Devices)+len(service.DeviceCgroupRules)+len(service.CapAdd)+len(service.SecurityOpt) != 0 || (service.Runtime != "" && service.Runtime != "runc") {
			return &failure{Code: "drill_isolation_failed", Resource: key}
		}
		for _, mount := range service.Volumes {
			copied := mount.Type == "bind" && strings.HasPrefix(filepath.Clean(mount.Source), directory+string(filepath.Separator))
			if mount.Type == "volume" {
				name := p.Definition.Volumes[mount.Source].Name
				copied = slices.ContainsFunc(p.Data, func(root dataRoot) bool { return root.Volume == name && name != "" })
			}
			if !copied {
				return &failure{Code: "drill_isolation_failed", Resource: mount.Target}
			}
		}
		p.Definition.Services[key] = compose.Service{
			Image: service.Image, Platform: service.Platform, ContainerName: p.Definition.Name + "-" + key,
			Hostname: service.Hostname, User: service.User, WorkingDir: service.WorkingDir,
			Command: service.Command, Entrypoint: service.Entrypoint, Environment: service.Environment,
			Labels: map[string]string{operationLabel: id.String(), "io.synod.proof": "true"},
			Init:   service.Init, StopSignal: service.StopSignal, StopGracePeriod: service.StopGracePeriod,
			Healthcheck: service.Healthcheck, Restart: "no", ReadOnly: service.ReadOnly,
			Networks: map[string]compose.Attachment{"drill": {Aliases: aliases}}, Volumes: service.Volumes, Tmpfs: service.Tmpfs,
			GroupAdd: service.GroupAdd, CapDrop: service.CapDrop, ShmSize: service.ShmSize,
			CPUs: service.CPUs, Memory: service.Memory, MemorySwap: service.MemorySwap, PidsLimit: service.PidsLimit,
			Logging: &compose.Logging{Driver: "none"},
		}
	}
	return nil
}

func WaitingProofResult(record journal.Record) (proof.Result, error) {
	if len(record.Progress) == 0 {
		return proof.Result{}, nil
	}
	var state proofProgress
	if json.Unmarshal(record.Progress, &state) != nil || state.Version != 1 || state.RequestID != record.ID {
		return proof.Result{}, journal.ErrConflict
	}
	return state.Result, nil
}
