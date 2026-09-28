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
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/containerd/errdefs"
	"github.com/google/uuid"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/mount"
	"github.com/moby/moby/api/types/network"
	"github.com/moby/moby/client"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/sirrobot01/synod/internal/operation"
	"github.com/sirrobot01/synod/internal/runtime"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/proto"
)

const labelInstallation = "io.synod.installation"
const labelNode = "io.synod.node"
const labelApp = "io.synod.app"
const labelResource = "io.synod.resource"
const labelKind = "io.synod.kind"
const labelConfig = "io.synod.configuration"
const labelHash = "io.synod.configuration-hash"

// Runtime implements the node runtime contract on the Docker Engine API.
var _ runtime.Runtime = (*Runtime)(nil)

// Options binds a runtime to one node and bounds its inspections.
type Options struct {
	AppsDir           string
	InstallationID    uuid.UUID
	NodeID            uuid.UUID
	InspectionTimeout time.Duration
}

// Runtime performs owned Docker actions for one node.
type Runtime struct {
	writerLeaseCheck func(string) error
	api              *client.Client
	options          Options
	healthMu         sync.Mutex
	health           map[string]genericHealth
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
	return &Runtime{api: api, options: options, health: map[string]genericHealth{}}, nil
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
	return map[string]string{labelInstallation: r.options.InstallationID.String(), labelNode: r.options.NodeID.String(), labelApp: command.Epoch.AppId, labelResource: resourceID, labelKind: kind}
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
// The journal owner must serialize commands for each app and enforce plan dependencies.
func (r *Runtime) Execute(ctx context.Context, command *agentv1.Command, secrets runtime.Secrets) (runtime.Result, error) {
	if err := r.validate(command); err != nil {
		return runtime.Result{}, err
	}
	ctx, cancel := context.WithTimeout(ctx, command.Input.Timeout.AsDuration())
	defer cancel()
	id := command.Input.ResourceId
	var mutationErr error
	switch payload := command.Input.Payload.(type) {
	case *agentv1.CommandInput_EnsureContainer:
		p := payload.EnsureContainer
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
	case *agentv1.CommandInput_StartContainer, *agentv1.CommandInput_StopContainer, *agentv1.CommandInput_RemoveContainer:
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
			if current.Container.Config.Labels["synod.tier"] == "2" {
				if r.writerLeaseCheck == nil {
					return runtime.Result{}, runtime.ErrUnsupportedConfiguration
				}
				if err := r.writerLeaseCheck(command.Epoch.AppId); err != nil {
					return runtime.Result{}, err
				}
			}
			// A tier 1 or tier 2 copy must not restart without Synod after a host reboot.
			policy := container.RestartPolicyUnlessStopped
			if tier := current.Container.Config.Labels["synod.tier"]; tier == "1" || tier == "2" {
				policy = container.RestartPolicyDisabled
			}
			_, mutationErr = r.api.ContainerUpdate(ctx, current.Container.ID, client.ContainerUpdateOptions{RestartPolicy: &container.RestartPolicy{Name: policy}})
			if mutationErr == nil && current.Container.Config.Labels["synod.tier"] == "2" {
				mutationErr = r.writerLeaseCheck(command.Epoch.AppId)
			}
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
	default:
		item, err := r.api.ContainerInspect(ctx, "synod-instance-"+id, client.ContainerInspectOptions{})
		if errdefs.IsNotFound(err) {
			switch p.(type) {
			case *agentv1.CommandInput_RemoveContainer, *agentv1.CommandInput_StopContainer:
				result.Effects[0].State = "absent"
			case *agentv1.CommandInput_EnsureContainer:
				// The container name is fixed by the resource, so a missing container
				// proves that this ensure created nothing. A later attempt adopts a late
				// create under the same name.
				result.Effects[0].State = "absent"
				result.Effects[0].Active, result.Effects[0].OutcomeUnknown = false, false
				return result, runtime.ErrNotApplied
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
		if err := r.checkContainer(ctx, command, item.Container, expected); err != nil {
			return result, err
		}
		result = r.containerResult(command, item.Container)
		state := item.Container.State
		if state == nil {
			return result, runtime.ErrOutcomeUnknown
		}
		matched := false
		switch p.(type) {
		case *agentv1.CommandInput_EnsureContainer:
			matched = true
		case *agentv1.CommandInput_StartContainer:
			policy := container.RestartPolicyUnlessStopped
			if tier := item.Container.Config.Labels["synod.tier"]; tier == "1" || tier == "2" {
				policy = container.RestartPolicyDisabled
			}
			matched = state.Running && !state.Restarting && !state.Dead && item.Container.HostConfig.RestartPolicy.Name == policy
		case *agentv1.CommandInput_StopContainer:
			matched = !state.Running && item.Container.HostConfig.RestartPolicy.Name == container.RestartPolicyDisabled
		}
		if !matched {
			result.Effects[0].OutcomeUnknown = true
			result.Effects[0].Active = true
			return result, runtime.ErrOutcomeUnknown
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
	for _, execID := range item.ExecIDs {
		result.Effects = append(result.Effects, &agentv1.Effect{CommandId: command.CommandId, ResourceId: command.Input.ResourceId, NativeOperationId: "docker-exec:" + item.ID + ":" + execID, Active: true, State: "native_exec_active"})
	}
	return result
}

func (r *Runtime) containerOptions(ctx context.Context, command *agentv1.Command, p *agentv1.EnsureContainer, allowPull bool) (client.ContainerCreateOptions, error) {
	// A saved configuration has no command credential references. The original
	// ensure command passes full validation before Docker stores it.
	if err := operation.ValidateGenericContainer(p); err != nil {
		return client.ContainerCreateOptions{}, &runtime.Error{Kind: runtime.ErrConfigurationConflict, Cause: err}
	}
	if len(p.Ports) > 64 {
		return client.ContainerCreateOptions{}, runtime.ErrUnsupportedConfiguration
	}
	var mounts []mount.Mount
	for _, m := range p.Generic.Mounts {
		kind, source := mount.Type(m.Type), m.Source
		if m.Type == "replicated" || m.Type == "engine" {
			if !filepath.IsAbs(r.options.AppsDir) {
				return client.ContainerCreateOptions{}, runtime.ErrUnsupportedConfiguration
			}
			directory := "live"
			if m.Type == "engine" {
				directory = "engine"
			}
			kind, source = mount.TypeBind, filepath.Join(r.options.AppsDir, command.Epoch.AppId, directory, m.Source)
		}
		mounts = append(mounts, mount.Mount{Type: kind, Source: source, Target: m.Target, ReadOnly: m.ReadOnly})
	}
	version, err := r.api.ServerVersion(ctx, client.ServerVersionOptions{})
	if err != nil {
		return client.ContainerCreateOptions{}, &runtime.Error{Kind: runtime.ErrOutcomeUnknown, Cause: err}
	}
	if version.Os != "linux" || (version.Arch != "amd64" && version.Arch != "arm64") {
		return client.ContainerCreateOptions{}, runtime.ErrUnsupportedConfiguration
	}
	platform := &ocispec.Platform{OS: "linux", Architecture: version.Arch}
	image, err := r.api.ImageInspect(ctx, p.ImageDigest)
	if errdefs.IsNotFound(err) && allowPull {
		pull, pullErr := r.api.ImagePull(ctx, p.ImageDigest, client.ImagePullOptions{Platforms: []ocispec.Platform{*platform}})
		// A pull creates no container, so a failed pull is a definite outcome.
		if pullErr != nil {
			return client.ContainerCreateOptions{}, &runtime.Error{Kind: runtime.ErrNotApplied, Cause: pullErr}
		}
		pullErr = errors.Join(pull.Wait(ctx), pull.Close())
		if pullErr != nil {
			return client.ContainerCreateOptions{}, &runtime.Error{Kind: runtime.ErrNotApplied, Cause: fmt.Errorf("pull %s: %w", p.ImageDigest, pullErr)}
		}
		image, err = r.api.ImageInspect(ctx, p.ImageDigest)
		if err != nil {
			return client.ContainerCreateOptions{}, &runtime.Error{Kind: runtime.ErrNotApplied, Cause: err}
		}
	}
	if err != nil {
		return client.ContainerCreateOptions{}, &runtime.Error{Kind: runtime.ErrOutcomeUnknown, Cause: err}
	}
	if image.Os != platform.OS || image.Architecture != platform.Architecture || image.Config == nil {
		return client.ContainerCreateOptions{}, runtime.ErrUnsupportedConfiguration
	}
	for declared := range image.Config.Volumes {
		if !slices.ContainsFunc(mounts, func(m mount.Mount) bool { return m.Target == declared }) {
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
	g := p.Generic
	labels["synod.managed"] = "true"
	labels["synod.app_id"] = command.Epoch.AppId
	labels["synod.app_name"] = g.AppName
	labels["synod.copy_index"] = strconv.FormatUint(uint64(g.CopyIndex), 10)
	labels["synod.tier"] = "3"
	if slices.ContainsFunc(g.Mounts, func(m *agentv1.Mount) bool { return m.Type == "replicated" }) {
		labels["synod.tier"] = "2"
	}
	if slices.ContainsFunc(g.Mounts, func(m *agentv1.Mount) bool { return m.Type == "engine" }) {
		labels["synod.tier"] = "1"
	}
	memory, cpu := containerLimits(p)
	if memory < 16<<20 || cpu < 1 || cpu > 1000000 {
		return client.ContainerCreateOptions{}, runtime.ErrUnsupportedConfiguration
	}
	host := &container.HostConfig{NetworkMode: "bridge", RestartPolicy: container.RestartPolicy{Name: container.RestartPolicyDisabled}, Resources: container.Resources{Memory: memory, NanoCPUs: cpu * 1000000, PidsLimit: new(int64(256))}, Mounts: mounts, LogConfig: container.LogConfig{Type: "local", Config: map[string]string{"max-size": "1m", "max-file": "2"}}, PortBindings: network.PortMap{}}
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
	config := &container.Config{Image: p.ImageDigest, Labels: labels, StopTimeout: new(10), ExposedPorts: network.PortSet{}, Cmd: image.Config.Cmd, Entrypoint: image.Config.Entrypoint, Env: plainEnvironment(image.Config.Env, p), User: image.Config.User, WorkingDir: image.Config.WorkingDir}
	if g.CommandOverride {
		config.Cmd = append([]string{}, g.Command...)
	}
	if g.EntrypointOverride {
		config.Entrypoint = append([]string{}, g.Entrypoint...)
		if !g.CommandOverride {
			config.Cmd = []string{}
		}
	}
	if len(config.Cmd) == 0 && len(config.Entrypoint) == 0 {
		return client.ContainerCreateOptions{}, runtime.ErrUnsupportedConfiguration
	}
	config.StopSignal = image.Config.StopSignal
	config.Healthcheck = image.Config.Healthcheck
	if h := g.HealthCheck; h != nil {
		config.Healthcheck = &container.HealthConfig{}
		if image.Config.Healthcheck != nil {
			*config.Healthcheck = *image.Config.Healthcheck
		}
		config.Healthcheck.Test = []string{"NONE"}
		if h.Type == "exec" {
			interval, timeout, retries := h.IntervalSeconds, h.TimeoutSeconds, h.UnhealthyThreshold
			if interval == 0 {
				interval = 30
			}
			if timeout == 0 {
				timeout = 30
			}
			if retries == 0 {
				retries = 3
			}
			config.Healthcheck.Test = append([]string{"CMD"}, h.Command...)
			config.Healthcheck.Interval = time.Duration(interval) * time.Second
			config.Healthcheck.Timeout = time.Duration(timeout) * time.Second
			config.Healthcheck.Retries = int(retries)
		}
	}
	for _, binding := range p.Ports {
		address, err := netip.ParseAddr(binding.HostAddress)
		if err != nil {
			return client.ContainerCreateOptions{}, runtime.ErrUnsupportedConfiguration
		}
		port, err := network.ParsePort(strconv.Itoa(int(binding.ContainerPort)) + "/" + binding.Transport)
		if err != nil {
			return client.ContainerCreateOptions{}, err
		}
		config.ExposedPorts[port] = struct{}{}
		host.PortBindings[port] = append(host.PortBindings[port], network.PortBinding{HostIP: address, HostPort: strconv.Itoa(int(binding.HostPort))})
	}
	return client.ContainerCreateOptions{Name: "synod-instance-" + command.Input.ResourceId, Config: config, HostConfig: host, Platform: platform}, nil
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
	options, err := r.containerOptions(ctx, command, &stored, false)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(item.Config.Healthcheck, options.Config.Healthcheck) || item.Config.StopSignal != options.Config.StopSignal {
		return runtime.ErrConfigurationConflict
	}
	if item.Image != options.Config.Labels["io.synod.image-id"] || !slices.Equal(item.Config.Cmd, options.Config.Cmd) || !slices.Equal(item.Config.Entrypoint, options.Config.Entrypoint) || !sameEnvironment(item.Config.Env, options.Config.Env, &stored) || item.Config.User != options.Config.User || item.Config.WorkingDir != options.Config.WorkingDir || item.HostConfig.PidsLimit == nil || *item.HostConfig.PidsLimit != 256 || item.Config.Image != options.Config.Image || item.HostConfig.NetworkMode != options.HostConfig.NetworkMode || item.HostConfig.Memory != options.HostConfig.Memory || item.HostConfig.NanoCPUs != options.HostConfig.NanoCPUs || !reflect.DeepEqual(item.HostConfig.PortBindings, options.HostConfig.PortBindings) || !reflect.DeepEqual(item.HostConfig.LogConfig, options.HostConfig.LogConfig) {
		return runtime.ErrConfigurationConflict
	}
	if tier := options.Config.Labels["synod.tier"]; (tier == "1" || tier == "2") && item.HostConfig.RestartPolicy.Name != container.RestartPolicyDisabled {
		return runtime.ErrConfigurationConflict
	}
	if item.HostConfig.RestartPolicy.Name != container.RestartPolicyDisabled && item.HostConfig.RestartPolicy.Name != container.RestartPolicyUnlessStopped {
		return runtime.ErrConfigurationConflict
	}
	for _, key := range []string{"synod.managed", "synod.app_id", "synod.app_name", "synod.copy_index", "synod.tier"} {
		if item.Config.Labels[key] != options.Config.Labels[key] {
			return runtime.ErrOwnershipConflict
		}
	}
	if len(item.Mounts) != len(options.HostConfig.Mounts) {
		return runtime.ErrConfigurationConflict
	}
	for _, mounted := range item.Mounts {
		index := slices.IndexFunc(options.HostConfig.Mounts, func(m mount.Mount) bool { return m.Target == mounted.Destination })
		if index < 0 {
			return runtime.ErrConfigurationConflict
		}
		wanted := options.HostConfig.Mounts[index]
		if mounted.Type != wanted.Type || mounted.RW == wanted.ReadOnly {
			return runtime.ErrConfigurationConflict
		}
		switch mounted.Type {
		case mount.TypeVolume:
			if mounted.Name != wanted.Source {
				return runtime.ErrConfigurationConflict
			}
		case mount.TypeBind:
			if mounted.Propagation != mount.PropagationRPrivate {
				return runtime.ErrConfigurationConflict
			}
			if mounted.Source != wanted.Source {
				// Docker Desktop maps shared host paths into its Linux VM.
				if mounted.Source != "/host_mnt"+wanted.Source {
					return runtime.ErrConfigurationConflict
				}
				info, err := r.api.Info(ctx, client.InfoOptions{})
				if err != nil || info.Info.OperatingSystem != "Docker Desktop" {
					return runtime.ErrConfigurationConflict
				}
			}
		case mount.TypeTmpfs:
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
