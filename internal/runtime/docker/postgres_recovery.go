package docker

import (
	"archive/tar"
	"context"
	"encoding/base64"
	"errors"
	"io"
	"path"
	"time"

	"github.com/containerd/errdefs"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/client"
	"github.com/sirrobot01/synod/internal/engine/postgres"
	"github.com/sirrobot01/synod/internal/runtime"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/proto"
)

func (r *Runtime) postgresRecoveryContainer(ctx context.Context, command *agentv1.Command) (container.InspectResponse, *agentv1.EnsureContainer, error) {
	item, err := r.api.ContainerInspect(ctx, "synod-instance-"+command.Input.ResourceId, client.ContainerInspectOptions{})
	if err != nil {
		return item.Container, nil, &runtime.Error{Kind: runtime.ErrOutcomeUnknown, Cause: err}
	}
	if err := r.checkContainer(ctx, command, item.Container, nil); err != nil {
		return item.Container, nil, err
	}
	encoded, err := base64.RawStdEncoding.DecodeString(item.Container.Config.Labels[labelConfig])
	if err != nil {
		return item.Container, nil, runtime.ErrConfigurationConflict
	}
	var stored agentv1.EnsureContainer
	if proto.Unmarshal(encoded, &stored) != nil || stored.Engine != "postgres" || stored.Postgres == nil || !proto.Equal(stored.Postgres, command.Input.GetRecoverPostgresSetup().Postgres) {
		return item.Container, nil, runtime.ErrConfigurationConflict
	}
	return item.Container, &stored, nil
}

func (r *Runtime) executePostgresRecovery(ctx context.Context, command *agentv1.Command, secrets runtime.Secrets) (runtime.Result, error) {
	item, stored, err := r.postgresRecoveryContainer(ctx, command)
	if err != nil {
		return runtime.Result{}, err
	}
	phase, err := postgresPhase(item)
	if err != nil {
		return r.containerResult(command, item), err
	}
	if phase != "bootstrap_failed" || item.State == nil || !item.State.Running || item.State.Restarting {
		return runtime.Result{}, runtime.ErrConfigurationConflict
	}
	quiet, stopWaiting := context.WithTimeout(ctx, r.options.InspectionTimeout)
	defer stopWaiting()
	for len(item.ExecIDs) != 0 {
		select {
		case <-quiet.Done():
			return runtime.Result{}, runtime.ErrBusy
		case <-time.After(25 * time.Millisecond):
		}
		item, stored, err = r.postgresRecoveryContainer(quiet, command)
		if err != nil {
			if quiet.Err() != nil {
				return runtime.Result{}, runtime.ErrBusy
			}
			return runtime.Result{}, err
		}
		phase, err = postgresPhase(item)
		if err != nil {
			return r.containerResult(command, item), err
		}
		if phase != "bootstrap_failed" {
			return runtime.Result{}, runtime.ErrConfigurationConflict
		}
	}
	storageCommand := proto.Clone(command).(*agentv1.Command)
	storageCommand.Input.Payload = &agentv1.CommandInput_StartContainer{StartContainer: &agentv1.StartContainer{}}
	if err := r.checkStartStorage(ctx, storageCommand, item); err != nil {
		return runtime.Result{}, err
	}
	environment, err := secretEnvironment(nil, stored, secrets)
	if err != nil {
		return runtime.Result{}, err
	}
	program, err := postgres.RecoveryCommand(stored.Postgres, command.Epoch.InstallationId, command.Epoch.ClusterId, command.Input.ResourceId, command.CommandId)
	if err != nil {
		return runtime.Result{}, runtime.ErrConfigurationConflict
	}
	prior := []*agentv1.Effect{{CommandId: command.CommandId, ResourceId: command.Input.ResourceId, NativeOperationId: "docker-command:" + command.CommandId, Active: true, OutcomeUnknown: true, State: "setup_recovery_unknown"}}
	native, mutationErr := r.api.ExecCreate(ctx, item.ID, client.ExecCreateOptions{User: "postgres", Cmd: program, Env: environment})
	clear(environment)
	if native.ID != "" {
		prior = append(prior, &agentv1.Effect{CommandId: command.CommandId, ResourceId: command.Input.ResourceId, NativeOperationId: "docker-exec:" + item.ID + ":" + native.ID, Active: true, OutcomeUnknown: true, State: "native_exec_unknown"})
	}
	if mutationErr == nil {
		_, mutationErr = r.api.ExecStart(ctx, native.ID, client.ExecStartOptions{Detach: true})
	}
	inspect, cancel := context.WithTimeout(context.WithoutCancel(ctx), r.options.InspectionTimeout)
	defer cancel()
	result, err := r.Recover(inspect, command, prior)
	if err != nil {
		return result, &runtime.Error{Kind: err, Cause: mutationErr}
	}
	return result, nil
}

func (r *Runtime) inspectPostgresRecovery(ctx context.Context, command *agentv1.Command) (runtime.Result, error) {
	result := runtime.Result{Effects: []*agentv1.Effect{{CommandId: command.CommandId, ResourceId: command.Input.ResourceId, NativeOperationId: "docker-command:" + command.CommandId, Active: true, OutcomeUnknown: true, State: "setup_recovery_unknown"}}}
	item, _, err := r.postgresRecoveryContainer(ctx, command)
	if err != nil {
		return result, err
	}
	for _, id := range item.ExecIDs {
		result.Effects = append(result.Effects, &agentv1.Effect{CommandId: command.CommandId, ResourceId: command.Input.ResourceId, NativeOperationId: "docker-exec:" + item.ID + ":" + id, Active: true, State: "native_exec_active"})
	}
	if item.State == nil || item.State.Restarting || item.State.Dead || len(item.ExecIDs) != 0 {
		return result, runtime.ErrOutcomeUnknown
	}
	if _, err := postgresPhase(item); err != nil {
		return result, err
	}
	receiptPath := postgres.InitializationDirectory + "/recovery-" + command.CommandId
	receipt, err := r.api.CopyFromContainer(ctx, item.ID, client.CopyFromContainerOptions{SourcePath: receiptPath})
	if err != nil {
		return result, &runtime.Error{Kind: runtime.ErrOutcomeUnknown, Cause: err}
	}
	defer receipt.Content.Close()
	if !receipt.Stat.Mode.IsRegular() || receipt.Stat.LinkTarget != "" || receipt.Stat.Size < 1 || receipt.Stat.Size > 128 {
		return result, runtime.ErrOutcomeUnknown
	}
	archive := tar.NewReader(io.LimitReader(receipt.Content, 8192))
	header, err := archive.Next()
	if err != nil || header.Typeflag != tar.TypeReg || header.Name != path.Base(receiptPath) || header.Linkname != "" || header.Size != receipt.Stat.Size {
		return result, runtime.ErrOutcomeUnknown
	}
	content, err := io.ReadAll(io.LimitReader(archive, 129))
	if err != nil || int64(len(content)) != header.Size {
		return result, runtime.ErrOutcomeUnknown
	}
	if _, err := archive.Next(); !errors.Is(err, io.EOF) {
		return result, runtime.ErrOutcomeUnknown
	}
	tombstone, err := r.api.ContainerStatPath(ctx, item.ID, client.ContainerStatPathOptions{Path: postgres.BootstrapFailurePath})
	absent := errdefs.IsNotFound(err)
	if err != nil && !absent {
		return result, &runtime.Error{Kind: runtime.ErrOutcomeUnknown, Cause: err}
	}
	if !absent && (!tombstone.Stat.Mode.IsRegular() || tombstone.Stat.LinkTarget != "" || tombstone.Stat.Size < 1) {
		return result, runtime.ErrOutcomeUnknown
	}
	current, _, err := r.postgresRecoveryContainer(ctx, command)
	if err != nil {
		return result, err
	}
	for _, id := range current.ExecIDs {
		result.Effects = append(result.Effects, &agentv1.Effect{CommandId: command.CommandId, ResourceId: command.Input.ResourceId, NativeOperationId: "docker-exec:" + current.ID + ":" + id, Active: true, State: "native_exec_active"})
	}
	if current.ID != item.ID || current.State == nil || current.State.StartedAt != item.State.StartedAt || current.State.Restarting || current.State.Dead || len(current.ExecIDs) != 0 {
		return result, runtime.ErrOutcomeUnknown
	}
	if _, err := postgresPhase(current); err != nil {
		return result, err
	}
	outcome := string(content)
	if outcome == "validated:"+command.CommandId+"\n" && absent {
		result.Effects[0].State = "setup_recovered"
	} else if outcome == "refused:"+command.CommandId+"\n" && !absent {
		result.Effects[0].State = "setup_refused"
	} else {
		return result, runtime.ErrOutcomeUnknown
	}
	result.Effects[0].Active, result.Effects[0].OutcomeUnknown = false, false
	if result.Effects[0].State == "setup_refused" {
		return result, runtime.ErrConfigurationConflict
	}
	return result, nil
}
