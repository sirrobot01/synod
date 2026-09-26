package docker

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/containerd/errdefs"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/client"
	"github.com/sirrobot01/synod/internal/engine/postgres"
	"github.com/sirrobot01/synod/internal/runtime"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
)

type patroniIdentity struct {
	Installation string `json:"installation_id"`
	Cluster      string `json:"cluster_id"`
	Resource     string `json:"resource_id"`
	Volume       string `json:"volume_id"`
	Provision    string `json:"provision_id"`
	Format       int    `json:"format"`
}

type patroniCompletion struct {
	Identity patroniIdentity `json:"identity"`
	SystemID string          `json:"system_id"`
}

func (r *Runtime) startPatroni(ctx context.Context, command *agentv1.Command) (runtime.Result, error) {
	p := command.Input.GetStartPatroniMember()
	item, err := r.api.ContainerInspect(ctx, "synod-instance-"+command.Input.ResourceId, client.ContainerInspectOptions{})
	if err != nil {
		return runtime.Result{}, &runtime.Error{Kind: runtime.ErrOutcomeUnknown, Cause: err}
	}
	if err := r.checkContainer(ctx, command, item.Container, p.Container); err != nil {
		return runtime.Result{}, err
	}
	if item.Container.State == nil {
		return runtime.Result{}, runtime.ErrOutcomeUnknown
	}
	if item.Container.State.Running || item.Container.State.Restarting {
		return r.Recover(ctx, command, nil)
	}
	if len(item.Container.ExecIDs) != 0 {
		return r.containerResult(command, item.Container), runtime.ErrBusy
	}
	if err := r.inspectPatroniSource(ctx, item.Container); err != nil {
		return runtime.Result{}, err
	}
	if err := r.checkPatroniStartStorage(ctx, command, item.Container, p.Container, p.AllowInitialization); err != nil {
		return runtime.Result{}, err
	}
	_, mutationErr := r.api.ContainerUpdate(ctx, item.Container.ID, client.ContainerUpdateOptions{RestartPolicy: &container.RestartPolicy{Name: container.RestartPolicyUnlessStopped}})
	if mutationErr == nil {
		_, mutationErr = r.api.ContainerStart(ctx, item.Container.ID, client.ContainerStartOptions{})
	}
	read, cancel := context.WithTimeout(context.WithoutCancel(ctx), r.options.InspectionTimeout)
	defer cancel()
	result, err := r.Recover(read, command, nil)
	if err != nil {
		return result, &runtime.Error{Kind: runtime.ErrOutcomeUnknown, Cause: errors.Join(err, mutationErr)}
	}
	return result, nil
}

// checkPatroniStartStorage checks the storage a start would use. Initial requires untouched
// empty storage. Otherwise it is the restart check, which a failure proof also runs.
func (r *Runtime) checkPatroniStartStorage(ctx context.Context, command *agentv1.Command, item container.InspectResponse, stored *agentv1.EnsureContainer, initial bool) error {
	started, err := time.Parse(time.RFC3339Nano, item.State.StartedAt)
	if err != nil {
		return runtime.ErrOutcomeUnknown
	}
	if initial {
		if !started.IsZero() || item.State.Status != container.StateCreated || !stored.Postgres.Patroni.AllowInitialization {
			return runtime.ErrConfigurationConflict
		}
		for _, filename := range []string{postgres.DataDirectory, "/var/lib/synod-patroni"} {
			copied, err := r.api.CopyFromContainer(ctx, item.ID, client.CopyFromContainerOptions{SourcePath: filename})
			if errdefs.IsNotFound(err) && filename != "/var/lib/synod-patroni" {
				return r.missingPatroniStorage(ctx, item.ID)
			}
			if errdefs.IsNotFound(err) {
				continue
			}
			if err != nil {
				return runtime.ErrOutcomeUnknown
			}
			data, readErr := io.ReadAll(io.LimitReader(copied.Content, 65537))
			copied.Content.Close()
			if readErr != nil || len(data) > 65536 {
				return runtime.ErrOutcomeUnknown
			}
			reader := tar.NewReader(bytes.NewReader(data))
			header, err := reader.Next()
			if err != nil || !copied.Stat.Mode.IsDir() || copied.Stat.LinkTarget != "" || header.Typeflag != tar.TypeDir || header.Linkname != "" || strings.TrimSuffix(header.Name, "/") != path.Base(filename) {
				return runtime.ErrConfigurationConflict
			}
			if _, err := reader.Next(); !errors.Is(err, io.EOF) {
				return runtime.ErrConfigurationConflict
			}
		}
		// A missing path is also returned for a missing container. Recheck its native identity.
		current, err := r.api.ContainerInspect(ctx, item.ID, client.ContainerInspectOptions{})
		if err != nil || current.Container.State == nil || current.Container.State.Running || current.Container.State.Restarting || current.Container.State.StartedAt != item.State.StartedAt {
			return runtime.ErrOutcomeUnknown
		}
		return nil
	}
	expected := patroniIdentity{Installation: command.Epoch.InstallationId, Cluster: command.Epoch.ClusterId, Resource: command.Input.ResourceId, Volume: stored.VolumeId, Provision: stored.Postgres.Patroni.ProvisionId, Format: 1}
	for _, filename := range []string{postgres.DataDirectory + "/.synod-patroni-identity.json", "/var/lib/synod-patroni/identity.json"} {
		var found patroniIdentity
		if err := r.patroniRecord(ctx, item.ID, filename, &found); err != nil {
			return err
		}
		if found != expected {
			return runtime.ErrConfigurationConflict
		}
	}
	var completed patroniCompletion
	if err := r.patroniRecord(ctx, item.ID, "/var/lib/synod-patroni/complete.json", &completed); err != nil {
		return err
	}
	var inside patroniCompletion
	if err := r.patroniRecord(ctx, item.ID, postgres.DataDirectory+"/.synod-patroni-complete.json", &inside); err != nil {
		return err
	}
	systemID, err := strconv.ParseUint(completed.SystemID, 10, 64)
	if err != nil || systemID == 0 || strconv.FormatUint(systemID, 10) != completed.SystemID || completed.Identity != expected || completed != inside {
		return runtime.ErrConfigurationConflict
	}
	var attempt struct {
		Identity patroniIdentity `json:"identity"`
		Method   string          `json:"method"`
	}
	if err := r.patroniRecord(ctx, item.ID, "/var/lib/synod-patroni/attempt.json", &attempt); err != nil {
		return err
	}
	if attempt.Identity != expected || attempt.Method != "init" && attempt.Method != "clone" {
		return runtime.ErrConfigurationConflict
	}
	version, err := r.containerRecord(ctx, item.ID, postgres.PatroniDataDirectory+"/PG_VERSION")
	if errdefs.IsNotFound(err) {
		return r.missingPatroniStorage(ctx, item.ID)
	}
	if err != nil {
		return &runtime.Error{Kind: runtime.ErrOutcomeUnknown, Cause: err}
	}
	if strings.TrimSpace(version) != "17" {
		return runtime.ErrConfigurationConflict
	}
	control, err := r.api.ContainerStatPath(ctx, item.ID, client.ContainerStatPathOptions{Path: postgres.PatroniDataDirectory + "/global/pg_control"})
	if errdefs.IsNotFound(err) {
		return r.missingPatroniStorage(ctx, item.ID)
	}
	if err != nil {
		return runtime.ErrOutcomeUnknown
	}
	if !control.Stat.Mode.IsRegular() || control.Stat.Size == 0 || control.Stat.LinkTarget != "" {
		return runtime.ErrStorageLost
	}
	return nil
}

// patroniRecord reads one bounded private native record without changing its container.
func (r *Runtime) patroniRecord(ctx context.Context, containerID, filename string, target any) error {
	copied, err := r.api.CopyFromContainer(ctx, containerID, client.CopyFromContainerOptions{SourcePath: filename})
	if errdefs.IsNotFound(err) {
		return r.missingPatroniStorage(ctx, containerID)
	}
	if err != nil {
		return runtime.ErrOutcomeUnknown
	}
	defer copied.Content.Close()
	if !copied.Stat.Mode.IsRegular() || copied.Stat.Mode.Perm() != 0600 || copied.Stat.LinkTarget != "" || copied.Stat.Size < 1 || copied.Stat.Size > 65536 {
		return runtime.ErrConfigurationConflict
	}
	reader := tar.NewReader(io.LimitReader(copied.Content, 80<<10))
	header, err := reader.Next()
	if err != nil || header.Name != path.Base(filename) || header.Typeflag != tar.TypeReg || header.Linkname != "" || header.Mode != 0600 || header.Size != copied.Stat.Size {
		return runtime.ErrConfigurationConflict
	}
	data, err := io.ReadAll(reader)
	if err != nil || int64(len(data)) != copied.Stat.Size {
		return runtime.ErrOutcomeUnknown
	}
	if _, err := reader.Next(); !errors.Is(err, io.EOF) {
		return runtime.ErrConfigurationConflict
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return runtime.ErrConfigurationConflict
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return runtime.ErrConfigurationConflict
	}
	return nil
}

// missingPatroniStorage distinguishes an absent file from an absent or active container.
func (r *Runtime) missingPatroniStorage(ctx context.Context, containerID string) error {
	current, err := r.api.ContainerInspect(ctx, containerID, client.ContainerInspectOptions{})
	if err != nil || current.Container.State == nil || current.Container.State.Running || current.Container.State.Restarting || len(current.Container.ExecIDs) != 0 {
		return runtime.ErrOutcomeUnknown
	}
	return runtime.ErrStorageLost
}
