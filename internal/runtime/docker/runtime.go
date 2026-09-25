// Package docker executes typed actions on exactly owned Docker resources.
package docker

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/netip"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/containerd/errdefs"
	"github.com/google/uuid"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/mount"
	"github.com/moby/moby/api/types/network"
	"github.com/moby/moby/client"
	"github.com/sirrobot01/synod/internal/engine/postgres"
	redisengine "github.com/sirrobot01/synod/internal/engine/redis"
	"github.com/sirrobot01/synod/internal/operation"
	"github.com/sirrobot01/synod/internal/runtime"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/proto"
)

const labelInstallation = "io.synod.installation"
const labelNode = "io.synod.node"
const labelCluster = "io.synod.cluster"
const labelResource = "io.synod.resource"
const labelKind = "io.synod.kind"
const labelConfig = "io.synod.configuration"
const labelHash = "io.synod.configuration-hash"

// Runtime implements the node runtime contract on the Docker Engine API.
var _ runtime.Runtime = (*Runtime)(nil)

// Options binds a runtime to one node and bounds its inspections.
type Options struct {
	InstallationID     uuid.UUID
	NodeID             uuid.UUID
	AllowBridgeFixture bool
	InspectionTimeout  time.Duration
}

// Runtime performs owned Docker actions for one node.
type Runtime struct {
	api     *client.Client
	options Options
}

// New borrows the API client. The caller owns its connection and journal lifetime.
func New(api *client.Client, options Options) (*Runtime, error) {
	if api == nil || options.InstallationID == uuid.Nil || options.NodeID == uuid.Nil {
		return nil, runtime.ErrUnsupportedConfiguration
	}
	if options.InspectionTimeout == 0 {
		options.InspectionTimeout = 5 * time.Second
	}
	if options.InspectionTimeout < time.Second || options.InspectionTimeout > time.Minute {
		return nil, runtime.ErrUnsupportedConfiguration
	}
	return &Runtime{api: api, options: options}, nil
}

func (r *Runtime) validate(command *agentv1.Command) error {
	if err := operation.ValidateCommand(command); err != nil {
		return err
	}
	if command.Epoch.InstallationId != r.options.InstallationID.String() || command.Epoch.NodeId != r.options.NodeID.String() {
		return operation.ErrStaleOwner
	}
	return nil
}

func (r *Runtime) labels(command *agentv1.Command, kind, resourceID string) map[string]string {
	return map[string]string{labelInstallation: r.options.InstallationID.String(), labelNode: r.options.NodeID.String(), labelCluster: command.Epoch.ClusterId, labelResource: resourceID, labelKind: kind}
}

func (r *Runtime) checkLabels(command *agentv1.Command, labels map[string]string, kind, resourceID string) error {
	for key, want := range r.labels(command, kind, resourceID) {
		if labels[key] != want {
			return runtime.ErrOwnershipConflict
		}
	}
	return nil
}

// Execute requires durable journal admission and Start before this call.
// The journal owner must serialize commands for each cluster and enforce plan dependencies.
func (r *Runtime) Execute(ctx context.Context, command *agentv1.Command, secrets runtime.Secrets) (runtime.Result, error) {
	if err := r.validate(command); err != nil {
		return runtime.Result{}, err
	}
	ctx, cancel := context.WithTimeout(ctx, command.Input.Timeout.AsDuration())
	defer cancel()
	id := command.Input.ResourceId
	var mutationErr error
	switch payload := command.Input.Payload.(type) {
	case *agentv1.CommandInput_StartPatroniMember:
		return r.startPatroni(ctx, command)
	case *agentv1.CommandInput_RecreatePatroniMember:
		return r.recreatePatroni(ctx, command, secrets)
	case *agentv1.CommandInput_VerifyContainerAbsent, *agentv1.CommandInput_VerifyPatroniStartFailure,
		*agentv1.CommandInput_VerifyPatroniMemberNotServing, *agentv1.CommandInput_VerifyPatroniMemberFailed:
		return r.inspectCommand(ctx, command)
	case *agentv1.CommandInput_RecoverPostgresSetup:
		return r.executePostgresRecovery(ctx, command, secrets)
	case *agentv1.CommandInput_PreparePostgresReplica:
		return r.executePostgresPrepare(ctx, command)
	case *agentv1.CommandInput_ClonePostgresReplica:
		return r.executePostgresClone(ctx, command, secrets)
	case *agentv1.CommandInput_VerifyPostgresReplicaClone, *agentv1.CommandInput_VerifyPostgresReplicaFailure:
		return r.inspectPostgresReplica(ctx, command)
	case *agentv1.CommandInput_EnsureVolume:
		name := "synod-volume-" + id
		current, err := r.api.VolumeInspect(ctx, name, client.VolumeInspectOptions{})
		if err == nil {
			// An existing volume must be ours and must still be where it says it
			// is, before this command treats it as the one to keep.
			if err := checkVolumePath(current.Volume); err != nil {
				return runtime.Result{}, err
			}
			if err := r.checkLabels(command, current.Volume.Labels, "volume", id); err != nil {
				return runtime.Result{}, err
			}
		} else if !errdefs.IsNotFound(err) {
			return runtime.Result{}, &runtime.Error{Kind: runtime.ErrOutcomeUnknown, Cause: err}
		}

		_, mutationErr = r.api.VolumeCreate(ctx, client.VolumeCreateOptions{Name: name, Driver: "local", Labels: r.labels(command, "volume", id)})
	case *agentv1.CommandInput_EnsureNetwork:
		if !r.options.AllowBridgeFixture {
			return runtime.Result{}, runtime.ErrUnsupportedConfiguration
		}
		name := "synod-network-" + id
		current, err := r.api.NetworkInspect(ctx, name, client.NetworkInspectOptions{})
		if err == nil {
			if err := r.checkLabels(command, current.Network.Labels, "network", id); err != nil {
				return runtime.Result{}, err
			}
			if current.Network.Driver != "bridge" {
				return runtime.Result{}, runtime.ErrConfigurationConflict
			}
		} else if !errdefs.IsNotFound(err) {
			return runtime.Result{}, &runtime.Error{Kind: runtime.ErrOutcomeUnknown, Cause: err}
		}

		_, mutationErr = r.api.NetworkCreate(ctx, name, client.NetworkCreateOptions{Driver: "bridge", Labels: r.labels(command, "network", id)})
	case *agentv1.CommandInput_EnsureContainer:
		p := payload.EnsureContainer
		if p.Postgres != nil && p.Postgres.Patroni != nil {
			return r.preparePatroni(ctx, command, secrets)
		}
		if p.Route != nil {
			return r.prepareRoute(ctx, command, secrets)
		}
		options, err := r.containerOptions(ctx, command, p, true)
		if err != nil {
			return runtime.Result{}, err
		}
		// The credentials go in here, after the payload has already been encoded
		// into the configuration label and hashed. A value applied at this point
		// cannot reach the label, the hash, an inventory read, or a drift report.
		options.Config.Env, err = secretEnvironment(options.Config.Env, p, secrets)
		if err != nil {
			return runtime.Result{}, err
		}
		current, err := r.api.ContainerInspect(ctx, options.Name, client.ContainerInspectOptions{})
		if err == nil {
			if err := r.checkContainer(ctx, command, current.Container, p); err != nil {
				return runtime.Result{}, err
			}
		} else if !errdefs.IsNotFound(err) {
			return runtime.Result{}, &runtime.Error{Kind: runtime.ErrOutcomeUnknown, Cause: err}
		}

		_, mutationErr = r.api.ContainerCreate(ctx, options)
	case *agentv1.CommandInput_StartContainer, *agentv1.CommandInput_StopContainer, *agentv1.CommandInput_RemoveContainer, *agentv1.CommandInput_InspectContainer:
		current, err := r.api.ContainerInspect(ctx, "synod-instance-"+id, client.ContainerInspectOptions{})
		if errdefs.IsNotFound(err) {
			// A missing container runs nothing, so it is stopped as well as absent.
			switch payload.(type) {
			case *agentv1.CommandInput_RemoveContainer, *agentv1.CommandInput_StopContainer:
				return runtime.Result{Effects: []*agentv1.Effect{{CommandId: command.CommandId, ResourceId: id, NativeOperationId: "docker-command:" + command.CommandId, State: "absent"}}}, nil
			}
			return runtime.Result{}, &runtime.Error{Kind: runtime.ErrOutcomeUnknown, Cause: err}
		}
		if err != nil {
			return runtime.Result{}, &runtime.Error{Kind: runtime.ErrOutcomeUnknown, Cause: err}
		}
		if err := r.checkContainer(ctx, command, current.Container, nil); err != nil {
			return runtime.Result{}, err
		}
		if current.Container.State == nil {
			return runtime.Result{}, runtime.ErrOutcomeUnknown
		}
		switch p := payload.(type) {
		case *agentv1.CommandInput_StartContainer:
			if phase, err := postgresPhase(current.Container); err == nil && (phase == "replica_prepared" || phase == "replica_cloned") {
				result := r.containerResult(command, current.Container)
				result.Effects[0].State = phase
				return result, runtime.ErrConfigurationConflict
			}
			if err := r.checkStartStorage(ctx, command, current.Container); err != nil {
				if current.Container.State.Running || current.Container.State.Restarting || len(current.Container.ExecIDs) != 0 {
					result := r.containerResult(command, current.Container)
					result.Effects[0].Active, result.Effects[0].OutcomeUnknown = true, true
					return result, &runtime.Error{Kind: runtime.ErrOutcomeUnknown, Cause: err}
				}
				return runtime.Result{}, err
			}
			_, mutationErr = r.api.ContainerUpdate(ctx, current.Container.ID, client.ContainerUpdateOptions{RestartPolicy: &container.RestartPolicy{Name: container.RestartPolicyUnlessStopped}})
			if mutationErr == nil {
				_, mutationErr = r.api.ContainerStart(ctx, current.Container.ID, client.ContainerStartOptions{})
			}
		case *agentv1.CommandInput_StopContainer:
			// Disable restart before stopping. A daemon restart must retain stopped intent.
			_, mutationErr = r.api.ContainerUpdate(ctx, current.Container.ID, client.ContainerUpdateOptions{RestartPolicy: &container.RestartPolicy{Name: container.RestartPolicyDisabled}})
			if mutationErr == nil {
				grace := int((p.StopContainer.GracePeriod.AsDuration() + time.Second - 1) / time.Second)
				_, mutationErr = r.api.ContainerStop(ctx, current.Container.ID, client.ContainerStopOptions{Timeout: &grace})
			}
		case *agentv1.CommandInput_RemoveContainer:
			if current.Container.State.Running || len(current.Container.ExecIDs) > 0 {
				return runtime.Result{}, runtime.ErrBusy
			}
			_, mutationErr = r.api.ContainerRemove(ctx, current.Container.ID, client.ContainerRemoveOptions{Force: false, RemoveVolumes: false})
		case *agentv1.CommandInput_InspectContainer:
			if current.Container.Config.Labels[labelPatroniSource] != "" {
				return r.Recover(ctx, command, nil)
			}
			return r.containerResult(command, current.Container), nil
		}
	default:
		return runtime.Result{}, runtime.ErrUnsupportedConfiguration
	}
	// A lost reply can hide success. Inspect with a fresh bounded context.
	inspectCtx, inspectCancel := context.WithTimeout(context.WithoutCancel(ctx), r.options.InspectionTimeout)
	defer inspectCancel()
	result, err := r.Recover(inspectCtx, command, nil)
	if err != nil {
		if runtimeErr, ok := errors.AsType[*runtime.Error](err); ok {
			return result, &runtime.Error{Kind: runtimeErr.Kind, Cause: errors.Join(runtimeErr.Cause, mutationErr)}
		}
		return result, &runtime.Error{Kind: err, Cause: mutationErr}
	}
	return result, nil
}

func (r *Runtime) inspectCommand(ctx context.Context, command *agentv1.Command) (runtime.Result, error) {
	if err := r.validate(command); err != nil {
		return runtime.Result{}, err
	}
	id := command.Input.ResourceId
	result := runtime.Result{Effects: []*agentv1.Effect{{CommandId: command.CommandId, ResourceId: id, NativeOperationId: "docker-command:" + command.CommandId, OutcomeUnknown: true, Active: true, State: "unknown"}}}
	switch p := command.Input.Payload.(type) {
	case *agentv1.CommandInput_VerifyContainerAbsent:
		_, err := r.api.ContainerInspect(ctx, "synod-instance-"+id, client.ContainerInspectOptions{})
		if errdefs.IsNotFound(err) {
			result.Effects[0].State = "absent"
			break
		}
		if err != nil {
			return result, &runtime.Error{Kind: runtime.ErrOutcomeUnknown, Cause: err}
		}
		result.Effects[0].State = "present"
		result.Effects[0].Active, result.Effects[0].OutcomeUnknown = false, false
		return result, runtime.ErrConfigurationConflict
	case *agentv1.CommandInput_VerifyPatroniStartFailure:
		return r.verifyPatroniStartFailure(ctx, command, result)
	case *agentv1.CommandInput_VerifyPatroniMemberNotServing, *agentv1.CommandInput_VerifyPatroniMemberFailed:
		return r.verifyPatroniMember(ctx, command, result)
	case *agentv1.CommandInput_RecreatePatroniMember:
		return r.inspectRecreate(ctx, command, result)
	case *agentv1.CommandInput_RecoverPostgresSetup:
		return r.inspectPostgresRecovery(ctx, command)
	case *agentv1.CommandInput_PreparePostgresReplica, *agentv1.CommandInput_ClonePostgresReplica, *agentv1.CommandInput_VerifyPostgresReplicaClone, *agentv1.CommandInput_VerifyPostgresReplicaFailure:
		return r.inspectPostgresReplica(ctx, command)
	case *agentv1.CommandInput_EnsureVolume:
		if err := r.verifyVolume(ctx, command, id); err != nil {
			return result, err
		}
		result.Effects[0].State = "present"
	case *agentv1.CommandInput_EnsureNetwork:
		if !r.options.AllowBridgeFixture {
			return result, runtime.ErrUnsupportedConfiguration
		}
		item, err := r.api.NetworkInspect(ctx, "synod-network-"+id, client.NetworkInspectOptions{})
		if err != nil {
			return result, &runtime.Error{Kind: runtime.ErrOutcomeUnknown, Cause: err}
		}
		if err := r.checkLabels(command, item.Network.Labels, "network", id); err != nil {
			return result, err
		}
		if item.Network.Driver != "bridge" {
			return result, runtime.ErrConfigurationConflict
		}
		result.Effects[0].State = "present"
	default:
		item, err := r.api.ContainerInspect(ctx, "synod-instance-"+id, client.ContainerInspectOptions{})
		if errdefs.IsNotFound(err) {
			switch p.(type) {
			case *agentv1.CommandInput_RemoveContainer, *agentv1.CommandInput_StopContainer:
				result.Effects[0].State = "absent"
			}
			if result.Effects[0].State == "absent" {
				break
			}
		}
		if err != nil {
			return result, &runtime.Error{Kind: runtime.ErrOutcomeUnknown, Cause: err}
		}
		var expected *agentv1.EnsureContainer
		if ensure, ok := p.(*agentv1.CommandInput_EnsureContainer); ok {
			expected = ensure.EnsureContainer
		}
		if start := command.Input.GetStartPatroniMember(); start != nil {
			expected = start.Container
		}
		if err := r.checkContainer(ctx, command, item.Container, expected); err != nil {
			return result, err
		}
		result = r.containerResult(command, item.Container)
		sourceCheck := command.Input.GetEnsureContainer() != nil || command.Input.GetInspectContainer() != nil || command.Input.GetStartPatroniMember() != nil || command.Input.GetStartContainer() != nil
		if item.Container.Config != nil && item.Container.Config.Labels[labelRouteSource] != "" && sourceCheck {
			stored, err := storedConfig(item.Container)
			if err == nil && stored.Route != nil {
				err = r.inspectSource(ctx, item.Container, routeSource(stored.Route))
			} else if err == nil {
				err = runtime.ErrConfigurationConflict
			}
			if err != nil {
				result.Effects[0].State = "route_configuration_unknown"
				result.Effects[0].Active, result.Effects[0].OutcomeUnknown = true, true
				return result, &runtime.Error{Kind: runtime.ErrOutcomeUnknown, Cause: err}
			}
		}
		if item.Container.Config != nil && item.Container.Config.Labels[labelPatroniSource] != "" && (command.Input.GetEnsureContainer() != nil || command.Input.GetInspectContainer() != nil || command.Input.GetStartPatroniMember() != nil) {
			if err := r.inspectPatroniSource(ctx, item.Container); err != nil {
				result.Effects[0].State = "patroni_configuration_unknown"
				result.Effects[0].Active, result.Effects[0].OutcomeUnknown = true, true
				return result, &runtime.Error{Kind: runtime.ErrOutcomeUnknown, Cause: err}
			}
		}
		state := item.Container.State
		if state == nil {
			return result, runtime.ErrOutcomeUnknown
		}
		phase, phaseErr := postgresPhase(item.Container)
		if command.Input.GetStartContainer() != nil && phaseErr == nil && (phase == "replica_prepared" || phase == "replica_cloned") {
			result.Effects[0].State = phase
			return result, runtime.ErrConfigurationConflict
		}
		matched := false
		switch p.(type) {
		case *agentv1.CommandInput_EnsureContainer, *agentv1.CommandInput_InspectContainer:
			matched = true
		case *agentv1.CommandInput_StartPatroniMember:
			matched = state.Running && !state.Restarting && !state.Dead && item.Container.HostConfig.RestartPolicy.Name == container.RestartPolicyUnlessStopped
		case *agentv1.CommandInput_StartContainer:
			// The first start can be running while initdb still creates the data files.
			replica := len(item.Container.Config.Cmd) > 3 && item.Container.Config.Cmd[3] == "synod-postgres-replica"
			if replica || !command.Input.GetStartContainer().AllowInitialization || !state.Running || state.Restarting || state.Dead {
				if err := r.checkStartStorage(ctx, command, item.Container); err != nil {
					if errors.Is(err, runtime.ErrStorageLost) && !state.Running && !state.Restarting && len(item.Container.ExecIDs) == 0 {
						result.Effects[0].State = "storage_lost"
						return result, err
					}
					result.Effects[0].Active, result.Effects[0].OutcomeUnknown = true, true
					return result, &runtime.Error{Kind: runtime.ErrOutcomeUnknown, Cause: err}
				}
			}
			matched = state.Running && !state.Restarting && !state.Dead && item.Container.HostConfig.RestartPolicy.Name == container.RestartPolicyUnlessStopped
		case *agentv1.CommandInput_StopContainer:
			matched = !state.Running && item.Container.HostConfig.RestartPolicy.Name == container.RestartPolicyDisabled
		}
		if !matched {
			result.Effects[0].OutcomeUnknown = true
			result.Effects[0].Active = true
			return result, runtime.ErrOutcomeUnknown
		}
		if phaseErr != nil {
			result.Effects[0].State = phase
			result.Effects[0].Active, result.Effects[0].OutcomeUnknown = true, true
			return result, phaseErr
		}
		if (phase == "bootstrap_failed" || phase == "bootstrap_recovered" || phase == "replica_prepared" || phase == "replica_cloned") && command.Input.GetStartContainer() != nil {
			result.Effects[0].State = phase
			result.Effects[0].Active, result.Effects[0].OutcomeUnknown = false, false
			return result, runtime.ErrConfigurationConflict
		}
	}
	result.Effects[0].OutcomeUnknown = false
	result.Effects[0].Active = false
	return result, nil
}

func (r *Runtime) containerResult(command *agentv1.Command, item container.InspectResponse) runtime.Result {
	state := "unknown"
	if item.State != nil {
		state = string(item.State.Status)
	}
	result := runtime.Result{Effects: []*agentv1.Effect{{CommandId: command.CommandId, ResourceId: command.Input.ResourceId, NativeOperationId: "docker-command:" + command.CommandId, State: state, OutcomeUnknown: item.State == nil}}}
	if phase, err := postgresPhase(item); err != nil {
		result.Effects[0].State = phase
		result.Effects[0].Active, result.Effects[0].OutcomeUnknown = true, true
	}
	for _, execID := range item.ExecIDs {
		result.Effects = append(result.Effects, &agentv1.Effect{CommandId: command.CommandId, ResourceId: command.Input.ResourceId, NativeOperationId: "docker-exec:" + item.ID + ":" + execID, Active: true, State: "native_exec_active"})
	}
	return result
}

func (r *Runtime) containerOptions(ctx context.Context, command *agentv1.Command, p *agentv1.EnsureContainer, allowPull bool) (client.ContainerCreateOptions, error) {
	// Stored labels must pass the same typed checks as a new container command.
	validation := proto.Clone(command).(*agentv1.Command)
	validation.Input.Payload = &agentv1.CommandInput_EnsureContainer{EnsureContainer: p}
	if p.Postgres != nil {
		validation.Input.Secrets = []*agentv1.SecretReference{
			{CredentialId: p.Postgres.SuperuserCredentialId, Version: p.Postgres.SuperuserCredentialVersion, Purpose: "superuser"},
			{CredentialId: p.Postgres.ApplicationCredentialId, Version: p.Postgres.ApplicationCredentialVersion, Purpose: "application"},
			{CredentialId: p.Postgres.ReplicationCredentialId, Version: p.Postgres.ReplicationCredentialVersion, Purpose: "replication"},
		}
		if p.Postgres.ReplicaSource != nil {
			validation.Input.Secrets = validation.Input.Secrets[1:]
		}
		if p.Postgres.Patroni != nil {
			validation.Input.Secrets = p.Postgres.Patroni.Credentials
		}
	}
	if p.Redis != nil {
		validation.Input.Secrets = p.Redis.Credentials
	}
	if p.Route != nil {
		validation.Input.Secrets = p.Route.Credentials
	}
	if _, err := operation.CommandHash(validation); err != nil {
		return client.ContainerCreateOptions{}, &runtime.Error{Kind: runtime.ErrConfigurationConflict, Cause: err}
	}
	if p.NetworkMode == agentv1.NetworkMode_NETWORK_MODE_BRIDGE && !r.options.AllowBridgeFixture {
		return client.ContainerCreateOptions{}, runtime.ErrUnsupportedConfiguration
	}
	if p.NetworkMode != agentv1.NetworkMode_NETWORK_MODE_HOST && p.NetworkMode != agentv1.NetworkMode_NETWORK_MODE_BRIDGE {
		return client.ContainerCreateOptions{}, runtime.ErrUnsupportedConfiguration
	}
	if len(p.Ports) > 64 {
		return client.ContainerCreateOptions{}, runtime.ErrUnsupportedConfiguration
	}
	// The engine is about to read this volume, so its identity is proved first. A
	// missing volume must never be created here by the daemon and initialized over.
	// A route container holds no data and mounts no volume.
	var mounts []mount.Mount
	if p.Route == nil {
		if err := r.verifyVolume(ctx, command, p.VolumeId); err != nil {
			return client.ContainerCreateOptions{}, err
		}
		volume, err := r.api.VolumeInspect(ctx, "synod-volume-"+p.VolumeId, client.VolumeInspectOptions{})
		if err != nil {
			return client.ContainerCreateOptions{}, &runtime.Error{Kind: runtime.ErrOutcomeUnknown, Cause: err}
		}
		mounts = []mount.Mount{{Type: mount.TypeVolume, Source: volume.Volume.Name, Target: dataTarget(p)}}
	}
	mode := container.NetworkMode("host")
	if p.NetworkMode == agentv1.NetworkMode_NETWORK_MODE_BRIDGE {
		mode = container.NetworkMode("synod-network-" + p.NetworkId)
		net, err := r.api.NetworkInspect(ctx, string(mode), client.NetworkInspectOptions{})
		if err != nil {
			return client.ContainerCreateOptions{}, &runtime.Error{Kind: runtime.ErrOutcomeUnknown, Cause: err}
		}
		if err := r.checkLabels(command, net.Network.Labels, "network", p.NetworkId); err != nil {
			return client.ContainerCreateOptions{}, err
		}
		if net.Network.Driver != "bridge" {
			return client.ContainerCreateOptions{}, runtime.ErrConfigurationConflict
		}
	}
	image, err := r.api.ImageInspect(ctx, p.ImageDigest)
	if errdefs.IsNotFound(err) && allowPull {
		pull, pullErr := r.api.ImagePull(ctx, p.ImageDigest, client.ImagePullOptions{})
		if pullErr != nil {
			return client.ContainerCreateOptions{}, &runtime.Error{Kind: runtime.ErrOutcomeUnknown, Cause: pullErr}
		}
		pullErr = errors.Join(pull.Wait(ctx), pull.Close())
		if pullErr != nil {
			return client.ContainerCreateOptions{}, &runtime.Error{Kind: runtime.ErrOutcomeUnknown, Cause: fmt.Errorf("pull %s: %w", p.ImageDigest, pullErr)}
		}
		image, err = r.api.ImageInspect(ctx, p.ImageDigest)
	}
	if err != nil {
		return client.ContainerCreateOptions{}, &runtime.Error{Kind: runtime.ErrOutcomeUnknown, Cause: err}
	}
	target := dataTarget(p)
	if image.Config == nil {
		return client.ContainerCreateOptions{}, runtime.ErrUnsupportedConfiguration
	}
	for declared := range image.Config.Volumes {
		if declared != target {
			return client.ContainerCreateOptions{}, runtime.ErrUnsupportedConfiguration
		}
	}
	encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(p)
	if err != nil {
		return client.ContainerCreateOptions{}, err
	}
	hash := sha256.Sum256(encoded)
	labels := r.labels(command, "container", command.Input.ResourceId)
	labels[labelConfig] = base64.RawStdEncoding.EncodeToString(encoded)
	labels[labelHash] = hex.EncodeToString(hash[:])
	labels["io.synod.image-id"] = image.ID
	memory, cpu := containerLimits(p)
	if memory < 16<<20 || cpu < 1 || cpu > 1000000 {
		return client.ContainerCreateOptions{}, runtime.ErrUnsupportedConfiguration
	}
	host := &container.HostConfig{NetworkMode: mode, RestartPolicy: container.RestartPolicy{Name: container.RestartPolicyDisabled}, Resources: container.Resources{Memory: memory, NanoCPUs: cpu * 1000000, PidsLimit: new(int64(256))}, Mounts: mounts, LogConfig: container.LogConfig{Type: "local", Config: map[string]string{"max-size": "1m", "max-file": "2"}}, PortBindings: network.PortMap{}}
	// Pin the Linux namespace and path protections instead of inheriting daemon overrides.
	host.IpcMode = "private"
	host.CgroupnsMode = "private"
	host.Runtime = "runc"
	host.ShmSize = 64 << 20
	host.MaskedPaths = []string{"/proc/acpi", "/proc/asound", "/proc/interrupts", "/proc/kcore", "/proc/keys", "/proc/latency_stats", "/proc/timer_list", "/proc/timer_stats", "/proc/sched_debug", "/proc/scsi", "/sys/firmware", "/sys/devices/virtual/powercap"}
	host.ReadonlyPaths = []string{"/proc/bus", "/proc/fs", "/proc/irq", "/proc/sys", "/proc/sysrq-trigger"}
	// The non-secret environment goes in here, where a verification rebuild can
	// reproduce it. The credentials are applied by Execute, after this payload has
	// been encoded into the configuration label.
	plain, err := plainEnvironment(image.Config.Env, p)
	if err != nil {
		return client.ContainerCreateOptions{}, err
	}
	config := &container.Config{Image: p.ImageDigest, Labels: labels, StopTimeout: new(10), ExposedPorts: network.PortSet{}, Cmd: image.Config.Cmd, Entrypoint: image.Config.Entrypoint, Env: plain, User: image.Config.User, WorkingDir: image.Config.WorkingDir}
	if p.Redis != nil {
		config.Cmd = redisengine.ContainerCommand(p.Redis)
		host.Tmpfs = map[string]string{redisengine.ConfigDirectory: "rw,noexec,nosuid,size=1m"}
	}
	if p.Postgres != nil {
		config.Cmd, err = postgresCommand(p.Postgres, command.Epoch.InstallationId, command.Epoch.ClusterId, command.Input.ResourceId)
		if err != nil {
			return client.ContainerCreateOptions{}, runtime.ErrConfigurationConflict
		}
		config.Healthcheck = postgresHealthcheck(p.Postgres.ReplicaSource != nil)
		config.StopSignal = image.Config.StopSignal
		host.Tmpfs = map[string]string{postgres.ConfigDirectory: postgresTmpfs}
		if p.Postgres.Patroni != nil {
			config.Healthcheck = patroniHealthcheck()
			host.Tmpfs = map[string]string{postgres.PatroniDirectory: patroniTmpfs}
		}
	}
	if p.Route != nil {
		config.Cmd, host.Tmpfs = routeShape(p.Route)
		config.StopSignal = image.Config.StopSignal
	}
	for _, binding := range p.Ports {
		address, err := netip.ParseAddr(binding.HostAddress)
		if err != nil || !address.IsLoopback() {
			return client.ContainerCreateOptions{}, runtime.ErrUnsupportedConfiguration
		}
		port, err := network.ParsePort(strconv.Itoa(int(binding.ContainerPort)) + "/" + binding.Transport)
		if err != nil {
			return client.ContainerCreateOptions{}, err
		}
		config.ExposedPorts[port] = struct{}{}
		host.PortBindings[port] = append(host.PortBindings[port], network.PortBinding{HostIP: address, HostPort: strconv.Itoa(int(binding.HostPort))})
	}
	return client.ContainerCreateOptions{Name: "synod-instance-" + command.Input.ResourceId, Config: config, HostConfig: host}, nil
}

// containerLimits applies the driver's defaults to one container request. The drift
// scan compares a live container against these same numbers, so both callers must
// read them from one place.
func containerLimits(p *agentv1.EnsureContainer) (memory, cpu int64) {
	memory, cpu = p.MemoryBytes, p.CpuMillis
	if memory == 0 {
		memory = 64 << 20
	}
	if cpu == 0 {
		cpu = 250
	}
	return memory, cpu
}

func (r *Runtime) checkContainer(ctx context.Context, command *agentv1.Command, item container.InspectResponse, expected *agentv1.EnsureContainer) error {
	if item.Config == nil || item.HostConfig == nil {
		return runtime.ErrConfigurationConflict
	}
	if err := r.checkLabels(command, item.Config.Labels, "container", command.Input.ResourceId); err != nil {
		return err
	}
	if strings.TrimPrefix(item.Name, "/") != "synod-instance-"+command.Input.ResourceId {
		return runtime.ErrOwnershipConflict
	}
	encoded, err := base64.RawStdEncoding.DecodeString(item.Config.Labels[labelConfig])
	if err != nil {
		return &runtime.Error{Kind: runtime.ErrConfigurationConflict, Cause: err}
	}
	hash := sha256.Sum256(encoded)
	if item.Config.Labels[labelHash] != hex.EncodeToString(hash[:]) {
		return runtime.ErrConfigurationConflict
	}
	var stored agentv1.EnsureContainer
	if err := proto.Unmarshal(encoded, &stored); err != nil {
		return &runtime.Error{Kind: runtime.ErrConfigurationConflict, Cause: err}
	}
	if expected != nil && !proto.Equal(expected, &stored) {
		return runtime.ErrConfigurationConflict
	}
	if stored.Postgres != nil && stored.Postgres.Patroni != nil || stored.Route != nil {
		label := labelPatroniSource
		if stored.Route != nil {
			label = labelRouteSource
		}
		source, err := hex.DecodeString(item.Config.Labels[label])
		if err != nil || len(source) != sha256.Size || hex.EncodeToString(source) != item.Config.Labels[label] {
			return runtime.ErrConfigurationConflict
		}
	}
	options, err := r.containerOptions(ctx, command, &stored, false)
	if err != nil {
		return err
	}
	if stored.Postgres != nil && (!reflect.DeepEqual(item.Config.Healthcheck, options.Config.Healthcheck) || item.Config.StopSignal != options.Config.StopSignal) {
		return runtime.ErrConfigurationConflict
	}
	if item.Image != options.Config.Labels["io.synod.image-id"] || !reflect.DeepEqual(item.Config.Cmd, options.Config.Cmd) || !reflect.DeepEqual(item.Config.Entrypoint, options.Config.Entrypoint) || !sameEnvironment(item.Config.Env, options.Config.Env, &stored) || item.Config.User != options.Config.User || item.Config.WorkingDir != options.Config.WorkingDir || item.HostConfig.PidsLimit == nil || *item.HostConfig.PidsLimit != 256 || item.Config.Image != options.Config.Image || item.HostConfig.NetworkMode != options.HostConfig.NetworkMode || item.HostConfig.Memory != options.HostConfig.Memory || item.HostConfig.NanoCPUs != options.HostConfig.NanoCPUs || !reflect.DeepEqual(item.HostConfig.PortBindings, options.HostConfig.PortBindings) || !reflect.DeepEqual(item.HostConfig.LogConfig, options.HostConfig.LogConfig) {
		return runtime.ErrConfigurationConflict
	}
	// A route container mounts nothing. Every other container mounts its one data volume.
	if len(item.Mounts) != len(options.HostConfig.Mounts) {
		return runtime.ErrConfigurationConflict
	}
	for _, mounted := range item.Mounts {
		switch mounted.Type {
		case mount.TypeVolume:
			if mounted.Name != options.HostConfig.Mounts[0].Source || mounted.Destination != options.HostConfig.Mounts[0].Target || !mounted.RW {
				return runtime.ErrConfigurationConflict
			}
		default:
			return runtime.ErrConfigurationConflict
		}
	}
	host := item.HostConfig
	if host.Privileged || len(host.Binds) != 0 || len(host.Devices) != 0 || len(host.DeviceRequests) != 0 || len(host.DeviceCgroupRules) != 0 ||
		len(host.CapAdd) != 0 || len(host.CapDrop) != 0 || len(host.SecurityOpt) != 0 || len(host.GroupAdd) != 0 ||
		host.PidMode != "" || host.UTSMode != "" || host.UsernsMode != "" || host.IpcMode != options.HostConfig.IpcMode || host.CgroupnsMode != options.HostConfig.CgroupnsMode ||
		host.Runtime != options.HostConfig.Runtime || host.CgroupParent != "" || host.Cgroup != "" || len(host.Annotations) != 0 || host.Isolation != "" ||
		len(host.Sysctls) != 0 || !reflect.DeepEqual(host.Tmpfs, options.HostConfig.Tmpfs) || len(host.StorageOpt) != 0 || len(host.VolumesFrom) != 0 || host.ContainerIDFile != "" ||
		host.AutoRemove || host.PublishAllPorts || host.ReadonlyRootfs || host.Umask != nil || host.ShmSize != options.HostConfig.ShmSize ||
		!slices.Equal(host.MaskedPaths, options.HostConfig.MaskedPaths) || !slices.Equal(host.ReadonlyPaths, options.HostConfig.ReadonlyPaths) {
		return runtime.ErrConfigurationConflict
	}
	return nil
}

// Logs returns bounded Docker stream bytes. The caller must authorize and redact diagnostics.
func (r *Runtime) Logs(ctx context.Context, command *agentv1.Command, maxBytes int64) ([]byte, error) {
	if maxBytes < 1 || maxBytes > 1<<20 {
		return nil, runtime.ErrUnsupportedConfiguration
	}
	if err := r.validate(command); err != nil {
		return nil, err
	}
	item, err := r.api.ContainerInspect(ctx, "synod-instance-"+command.Input.ResourceId, client.ContainerInspectOptions{})
	if err != nil {
		return nil, &runtime.Error{Kind: runtime.ErrOutcomeUnknown, Cause: err}
	}
	if err := r.checkContainer(ctx, command, item.Container, nil); err != nil {
		return nil, err
	}
	logs, err := r.api.ContainerLogs(ctx, item.Container.ID, client.ContainerLogsOptions{ShowStdout: true, ShowStderr: true, Tail: "1000"})
	if err != nil {
		return nil, &runtime.Error{Kind: runtime.ErrOutcomeUnknown, Cause: err}
	}
	defer logs.Close()
	return io.ReadAll(io.LimitReader(logs, maxBytes))
}

// dataTarget is where a container's data volume is mounted.
func dataTarget(p *agentv1.EnsureContainer) string {
	if p.Engine == "postgres" {
		return postgres.DataDirectory
	}
	return "/data"
}

// routeTmpfs covers the image data volume of a route container. The image declares one,
// and without a mount Docker would create an anonymous volume that nothing owns.
const routeTmpfs = "rw,noexec,nosuid,nodev,size=1m"

// routeShape is the command and tmpfs set of a pooler or router container.
func routeShape(route *agentv1.PostgresRouteConfig) ([]string, map[string]string) {
	if route.Pooler != nil {
		return postgres.PoolerCommand(), map[string]string{postgres.PoolerDirectory: patroniTmpfs, postgres.DataDirectory: routeTmpfs}
	}
	return postgres.RouterCommand(), map[string]string{postgres.RouterDirectory: patroniTmpfs, postgres.DataDirectory: routeTmpfs}
}
