package docker

import (
	"archive/tar"
	"context"
	"encoding/base64"
	"errors"
	"io"
	"path"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/containerd/errdefs"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/client"
	"github.com/sirrobot01/synod/internal/engine/postgres"
	"github.com/sirrobot01/synod/internal/runtime"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/proto"
)

func (r *Runtime) postgresReplicaContainer(ctx context.Context, command *agentv1.Command) (container.InspectResponse, *agentv1.EnsureContainer, error) {
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
	if proto.Unmarshal(encoded, &stored) != nil || stored.Engine != "postgres" || stored.Postgres == nil || stored.Postgres.ReplicaSource == nil {
		return item.Container, nil, runtime.ErrConfigurationConflict
	}
	if clone := command.Input.GetClonePostgresReplica(); clone != nil && !proto.Equal(clone.Postgres, stored.Postgres) {
		return item.Container, nil, runtime.ErrConfigurationConflict
	}
	if verify := command.Input.GetVerifyPostgresReplicaClone(); verify != nil && !proto.Equal(verify.Postgres, stored.Postgres) {
		return item.Container, nil, runtime.ErrConfigurationConflict
	}
	if verify := command.Input.GetVerifyPostgresReplicaFailure(); verify != nil && !proto.Equal(verify.Postgres, stored.Postgres) {
		return item.Container, nil, runtime.ErrConfigurationConflict
	}
	return item.Container, &stored, nil
}

func (r *Runtime) executePostgresPrepare(ctx context.Context, command *agentv1.Command) (runtime.Result, error) {
	item, _, err := r.postgresReplicaContainer(ctx, command)
	if err != nil {
		return runtime.Result{}, err
	}
	if item.State == nil {
		return runtime.Result{}, runtime.ErrOutcomeUnknown
	}
	if item.State.Running {
		return r.inspectPostgresReplica(ctx, command)
	}
	started, err := time.Parse(time.RFC3339Nano, item.State.StartedAt)
	if err != nil {
		return runtime.Result{}, runtime.ErrOutcomeUnknown
	}
	if !started.IsZero() || item.State.Status != container.StateCreated || len(item.ExecIDs) != 0 {
		return runtime.Result{}, runtime.ErrConfigurationConflict
	}
	// Keep the prepared container inert across a host restart.
	_, mutationErr := r.api.ContainerUpdate(ctx, item.ID, client.ContainerUpdateOptions{RestartPolicy: &container.RestartPolicy{Name: container.RestartPolicyDisabled}})
	if mutationErr == nil {
		_, mutationErr = r.api.ContainerStart(ctx, item.ID, client.ContainerStartOptions{})
	}
	read, cancel := context.WithTimeout(context.WithoutCancel(ctx), r.options.InspectionTimeout)
	defer cancel()
	result, err := r.Recover(read, command, nil)
	if err != nil {
		return result, &runtime.Error{Kind: err, Cause: mutationErr}
	}
	return result, nil
}

func (r *Runtime) executePostgresClone(ctx context.Context, command *agentv1.Command, secrets runtime.Secrets) (runtime.Result, error) {
	item, stored, err := r.postgresReplicaContainer(ctx, command)
	if err != nil {
		return runtime.Result{}, err
	}
	phase, err := postgresPhase(item)
	if err != nil {
		return r.containerResult(command, item), err
	}
	if phase != "replica_prepared" || item.State == nil || !item.State.Running || item.State.Restarting {
		return runtime.Result{}, runtime.ErrConfigurationConflict
	}
	quiet, cancelQuiet := context.WithTimeout(ctx, r.options.InspectionTimeout)
	defer cancelQuiet()
	for len(item.ExecIDs) != 0 {
		select {
		case <-quiet.Done():
			return r.containerResult(command, item), runtime.ErrBusy
		case <-time.After(25 * time.Millisecond):
		}
		item, stored, err = r.postgresReplicaContainer(quiet, command)
		if err != nil {
			return runtime.Result{}, err
		}
		phase, err = postgresPhase(item)
		if err != nil {
			return r.containerResult(command, item), err
		}
		if phase != "replica_prepared" {
			return runtime.Result{}, runtime.ErrConfigurationConflict
		}
	}
	environment, err := secretEnvironment(nil, stored, secrets)
	if err != nil {
		return runtime.Result{}, err
	}
	source := stored.Postgres.ReplicaSource
	program, err := postgres.CloneReplicaCommand(stored.Postgres, postgres.ReplicaSource{Address: source.Address, Port: uint16(source.Port), ResourceID: source.ResourceId}, command.Epoch.InstallationId, command.Epoch.ClusterId, command.Input.ResourceId, command.CommandId)
	if err != nil {
		return runtime.Result{}, runtime.ErrConfigurationConflict
	}
	prior := []*agentv1.Effect{{CommandId: command.CommandId, ResourceId: command.Input.ResourceId, NativeOperationId: "docker-command:" + command.CommandId, Active: true, OutcomeUnknown: true, State: "replica_clone_unknown"}}
	native, mutationErr := r.api.ExecCreate(ctx, item.ID, client.ExecCreateOptions{User: "postgres", Cmd: program, Env: environment})
	clear(environment)
	if native.ID != "" {
		prior = append(prior, &agentv1.Effect{CommandId: command.CommandId, ResourceId: command.Input.ResourceId, NativeOperationId: "docker-exec:" + item.ID + ":" + native.ID, Active: true, OutcomeUnknown: true, State: "native_exec_unknown"})
	}
	if mutationErr == nil {
		_, mutationErr = r.api.ExecStart(ctx, native.ID, client.ExecStartOptions{Detach: true})
	}
	read, cancel := context.WithTimeout(context.WithoutCancel(ctx), r.options.InspectionTimeout)
	defer cancel()
	result, err := r.Recover(read, command, prior)
	if err != nil {
		return result, &runtime.Error{Kind: err, Cause: mutationErr}
	}
	return result, nil
}

// postgresReplicaRecord rejects links, large files, and extra archive entries.
func (r *Runtime) postgresReplicaRecord(ctx context.Context, containerID, filename string) (string, error) {
	copy, err := r.api.CopyFromContainer(ctx, containerID, client.CopyFromContainerOptions{SourcePath: filename})
	if err != nil {
		return "", err
	}
	defer copy.Content.Close()
	if !copy.Stat.Mode.IsRegular() || copy.Stat.LinkTarget != "" || copy.Stat.Size < 1 || copy.Stat.Size > 256 {
		return "", runtime.ErrOutcomeUnknown
	}
	archive := tar.NewReader(io.LimitReader(copy.Content, 8192))
	header, err := archive.Next()
	if err != nil || header.Typeflag != tar.TypeReg || header.Name != path.Base(filename) || header.Linkname != "" || header.Size != copy.Stat.Size {
		return "", runtime.ErrOutcomeUnknown
	}
	content, err := io.ReadAll(io.LimitReader(archive, 257))
	if err != nil || int64(len(content)) != header.Size {
		return "", runtime.ErrOutcomeUnknown
	}
	if _, err := archive.Next(); !errors.Is(err, io.EOF) {
		return "", runtime.ErrOutcomeUnknown
	}
	return string(content), nil
}

func (r *Runtime) inspectPostgresReplica(ctx context.Context, command *agentv1.Command) (runtime.Result, error) {
	result := runtime.Result{Effects: []*agentv1.Effect{{CommandId: command.CommandId, ResourceId: command.Input.ResourceId, NativeOperationId: "docker-command:" + command.CommandId, Active: true, OutcomeUnknown: true, State: "replica_unknown"}}}
	item, stored, err := r.postgresReplicaContainer(ctx, command)
	if err != nil {
		return result, err
	}
	for _, id := range item.ExecIDs {
		result.Effects = append(result.Effects, &agentv1.Effect{CommandId: command.CommandId, ResourceId: command.Input.ResourceId, NativeOperationId: "docker-exec:" + item.ID + ":" + id, Active: true, State: "native_exec_active"})
	}
	if item.State == nil || item.State.Restarting || item.State.Dead || len(item.ExecIDs) != 0 {
		return result, runtime.ErrOutcomeUnknown
	}
	phase, err := postgresPhase(item)
	if err != nil {
		return result, err
	}
	if command.Input.GetPreparePostgresReplica() != nil && phase == "bootstrap_failed" {
		result.Effects[0].State, result.Effects[0].Active, result.Effects[0].OutcomeUnknown = "replica_refused", false, false
		return result, runtime.ErrConfigurationConflict
	}
	program, err := postgresCommand(stored.Postgres, command.Epoch.InstallationId, command.Epoch.ClusterId, command.Input.ResourceId)
	if err != nil {
		return result, runtime.ErrConfigurationConflict
	}
	digest := program[len(program)-1]
	config, err := r.postgresReplicaRecord(ctx, item.ID, postgres.InitializationDirectory+"/replica-config")
	if err != nil || config != digest+"\n" {
		return result, runtime.ErrOutcomeUnknown
	}
	failure, failureErr := r.postgresReplicaRecord(ctx, item.ID, postgres.InitializationDirectory+"/replica-failed")
	if failureErr != nil && !errdefs.IsNotFound(failureErr) {
		return result, runtime.ErrOutcomeUnknown
	}
	outcome := ""
	if command.Input.GetPreparePostgresReplica() != nil {
		if phase != "replica_prepared" || !item.State.Running || failureErr == nil {
			return result, runtime.ErrOutcomeUnknown
		}
		for _, filename := range []string{postgres.InitializationDirectory + "/replica-attempt", postgres.DataDirectory + "/.synod-clone-intent", postgres.ReplicaReceiptFile} {
			_, err := r.api.ContainerStatPath(ctx, item.ID, client.ContainerStatPathOptions{Path: filename})
			if !errdefs.IsNotFound(err) {
				return result, runtime.ErrOutcomeUnknown
			}
		}
		outcome = "replica_prepared"
	} else {
		cloneID := command.CommandId
		if verify := command.Input.GetVerifyPostgresReplicaClone(); verify != nil {
			cloneID = verify.CloneCommandId
		}
		if verify := command.Input.GetVerifyPostgresReplicaFailure(); verify != nil {
			cloneID = verify.CloneCommandId
		}
		attempt, err := r.postgresReplicaRecord(ctx, item.ID, postgres.InitializationDirectory+"/replica-attempt")
		expected := "synod-clone-v1\n" + digest + "\n" + cloneID + "\n"
		if err != nil || attempt != expected {
			return result, runtime.ErrOutcomeUnknown
		}
		if failureErr == nil {
			if !slices.Contains([]string{"failed:configuration\n", "failed:copy\n", "failed:archive\n", "failed:backup_verification\n", "failed:native_verification\n", "failed:publication\n"}, failure) || (item.State.Running && phase != "bootstrap_failed") {
				return result, runtime.ErrOutcomeUnknown
			}
			outcome = "replica_refused"
		} else {
			intent, err := r.postgresReplicaRecord(ctx, item.ID, postgres.DataDirectory+"/.synod-clone-intent")
			if err != nil || intent != expected {
				return result, runtime.ErrOutcomeUnknown
			}
			receipt, err := r.postgresReplicaRecord(ctx, item.ID, postgres.ReplicaReceiptFile)
			prefix := "synod-replica-v1\n" + digest + "\n" + cloneID + "\n"
			identity, matched := strings.CutPrefix(receipt, prefix)
			number, parseErr := strconv.ParseUint(strings.TrimSuffix(identity, "\n"), 10, 64)
			if err != nil || !matched || parseErr != nil || number == 0 || identity != strconv.FormatUint(number, 10)+"\n" {
				return result, runtime.ErrOutcomeUnknown
			}
			if item.State.Running && phase != "replica_cloned" && phase != "bootstrap_ready" {
				return result, runtime.ErrOutcomeUnknown
			}
			outcome = "replica_cloned"
		}
	}
	current, _, err := r.postgresReplicaContainer(ctx, command)
	if err != nil {
		return result, err
	}
	if current.ID != item.ID || current.State == nil || current.State.StartedAt != item.State.StartedAt || current.State.Running != item.State.Running || current.State.Restarting || current.State.Dead || len(current.ExecIDs) != 0 {
		return result, runtime.ErrOutcomeUnknown
	}
	currentPhase, err := postgresPhase(current)
	if err != nil || phase != currentPhase {
		return result, runtime.ErrOutcomeUnknown
	}
	result.Effects[0].State, result.Effects[0].Active, result.Effects[0].OutcomeUnknown = outcome, false, false
	if command.Input.GetVerifyPostgresReplicaFailure() != nil {
		if outcome == "replica_refused" {
			return result, nil
		}
		return result, runtime.ErrConfigurationConflict
	}
	if outcome == "replica_refused" {
		return result, runtime.ErrConfigurationConflict
	}
	return result, nil
}
