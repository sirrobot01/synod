package docker

import (
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/client"
	"github.com/sirrobot01/synod/internal/operation"
	"github.com/sirrobot01/synod/internal/runtime"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestGenericContainerLifecycle(t *testing.T) {
	r, api, appID := runtimeFixture(t)
	id := uuid.NewString()
	source, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	command := runtimeCommand(t, r, appID, &agentv1.CommandInput{ResourceId: id, Payload: &agentv1.CommandInput_EnsureContainer{EnsureContainer: &agentv1.EnsureContainer{
		ImageDigest: redisFixture, NetworkMode: agentv1.NetworkMode_NETWORK_MODE_BRIDGE, MemoryBytes: 64 << 20, CpuMillis: 250,
		Generic: &agentv1.GenericContainerConfig{Image: redisFixture, AppName: "redis-web", CopyIndex: 2, CommandOverride: true, Command: []string{"redis-server", "--save", "", "--appendonly", "no"}, EntrypointOverride: true, Mounts: []*agentv1.Mount{{Type: "tmpfs", Target: "/data"}}, HealthCheck: &agentv1.HealthCheckConfig{Type: "exec", Command: []string{"redis-cli", "ping"}, IntervalSeconds: 1, TimeoutSeconds: 1, UnhealthyThreshold: 1}},
		Ports:   []*agentv1.PortBinding{{ContainerPort: 6379, HostPort: 0, HostAddress: "127.0.0.1", Transport: "tcp"}}, PlainEnv: []*agentv1.PlainEnv{{Name: "lowercase", Value: ""}, {Name: "REDIS_VERSION", Value: "override"}},
	}}})
	command.Input.GetEnsureContainer().Generic.Mounts = append(command.Input.GetEnsureContainer().Generic.Mounts, &agentv1.Mount{Type: "bind", Source: source, Target: "/synod-fixture", ReadOnly: true})
	hash, err := operation.CommandHash(command)
	if err != nil {
		t.Fatal(err)
	}
	command.InputHash = hash[:]
	if _, err := r.Execute(t.Context(), command, nil); err != nil {
		if detail, inspectErr := api.ContainerInspect(t.Context(), "synod-instance-"+id, client.ContainerInspectOptions{}); inspectErr == nil {
			t.Logf("mounts: %+v; requested: %+v", detail.Container.Mounts, detail.Container.HostConfig.Mounts)
		}
		t.Fatalf("create: %v / %v", err, errors.Unwrap(err))
	}
	current, err := api.ContainerInspect(t.Context(), "synod-instance-"+id, client.ContainerInspectOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if current.Container.Config.Labels["synod.app_id"] != appID.String() || current.Container.Config.Labels["synod.copy_index"] != "2" || len(current.Container.Config.Entrypoint) != 0 || !slices.Contains(current.Container.Config.Env, "lowercase=") || !slices.Contains(current.Container.Config.Env, "REDIS_VERSION=override") {
		t.Fatalf("generic config: %+v", current.Container.Config)
	}
	// Replay resolves the existing object without a second container.
	if _, err := r.Execute(t.Context(), command, nil); err != nil {
		t.Fatalf("replay: %v / %v", err, errors.Unwrap(err))
	}
	for _, input := range []*agentv1.CommandInput{
		{ResourceId: id, Payload: &agentv1.CommandInput_StartContainer{StartContainer: &agentv1.StartContainer{}}},
	} {
		if _, err := r.Execute(t.Context(), runtimeCommand(t, r, appID, input), nil); err != nil {
			t.Fatalf("lifecycle: %v / %v", err, errors.Unwrap(err))
		}
	}
	current, err = api.ContainerInspect(t.Context(), "synod-instance-"+id, client.ContainerInspectOptions{})
	if err != nil || !current.Container.State.Running {
		t.Fatalf("running: %+v %v", current.Container.State, err)
	}
	if len(current.Container.NetworkSettings.Ports) == 0 {
		t.Fatal("no dynamic backend port")
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		resources, err := r.Inventory(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		index := slices.IndexFunc(resources, func(item runtime.Resource) bool { return item.ResourceID.String() == id })
		if index < 0 {
			t.Fatal("app copy missing from inventory")
		}
		observed := resources[index]
		if observed.Health == "healthy" {
			if len(observed.Ports) != 1 || observed.Ports[0].HostPort == 0 || observed.State != "running" {
				t.Fatalf("incorrect app observation: %+v", observed)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("app did not become healthy: %+v", observed)
		}
		time.Sleep(100 * time.Millisecond)
	}
	drift, err := r.Drift(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	for _, item := range drift {
		if item.ResourceID.String() == id {
			t.Fatalf("fresh generic container drift: %+v", item)
		}
	}
	changed := proto.Clone(command).(*agentv1.Command)
	changed.Input.GetEnsureContainer().Generic.Command = []string{"other"}
	hash, err = operation.CommandHash(changed)
	if err != nil {
		t.Fatal(err)
	}
	changed.InputHash = hash[:]
	if _, err := r.Execute(t.Context(), changed, nil); !errors.Is(err, runtime.ErrConfigurationConflict) {
		t.Fatalf("configuration change accepted: %v", err)
	}
	foreign := runtimeCommand(t, r, uuid.New(), &agentv1.CommandInput{ResourceId: id, Payload: &agentv1.CommandInput_StopContainer{StopContainer: &agentv1.StopContainer{GracePeriod: durationpb.New(time.Second)}}})
	if _, err := r.Execute(t.Context(), foreign, nil); !errors.Is(err, runtime.ErrOwnershipConflict) {
		t.Fatalf("foreign stop accepted: %v", err)
	}
	stop := runtimeCommand(t, r, appID, &agentv1.CommandInput{ResourceId: id, Payload: &agentv1.CommandInput_StopContainer{StopContainer: &agentv1.StopContainer{GracePeriod: durationpb.New(time.Second)}}})
	if _, err := r.Execute(t.Context(), stop, nil); err != nil {
		t.Fatal(err)
	}
	remove := runtimeCommand(t, r, appID, &agentv1.CommandInput{ResourceId: id, Payload: &agentv1.CommandInput_RemoveContainer{RemoveContainer: &agentv1.RemoveContainer{}}})
	if _, err := r.Execute(t.Context(), remove, nil); err != nil {
		t.Fatal(err)
	}
	missing := proto.Clone(command.Input).(*agentv1.CommandInput)
	missing.ResourceId = uuid.NewString()
	missing.GetEnsureContainer().Generic.Mounts[1].Source = filepath.Join(source, "absent")
	if _, err := r.Execute(t.Context(), runtimeCommand(t, r, appID, missing), nil); err == nil {
		t.Fatal("missing bind source accepted")
	}
	if _, err := os.Stat(filepath.Join(source, "absent")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("runtime created the missing bind source: %v", err)
	}
}

func TestTier2ContainerUsesLiveStorageAndFences(t *testing.T) {
	r, api, appID := runtimeFixture(t)
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	r.options.AppsDir = root
	live := filepath.Join(root, appID.String(), "live", "config")
	if err := os.MkdirAll(live, 0o700); err != nil {
		t.Fatal(err)
	}
	id := uuid.NewString()
	command := runtimeCommand(t, r, appID, &agentv1.CommandInput{ResourceId: id, Payload: &agentv1.CommandInput_EnsureContainer{EnsureContainer: &agentv1.EnsureContainer{ImageDigest: redisFixture, NetworkMode: agentv1.NetworkMode_NETWORK_MODE_BRIDGE, Generic: &agentv1.GenericContainerConfig{Image: redisFixture, AppName: "writer", CommandOverride: true, Command: []string{"redis-server", "--save", "", "--appendonly", "no"}, Mounts: []*agentv1.Mount{{Type: "replicated", Source: "config", Target: "/data"}}}}}})
	if _, err := r.Execute(t.Context(), command, nil); err != nil {
		t.Fatalf("ensure: %v / %v", err, errors.Unwrap(err))
	}
	current, err := api.ContainerInspect(t.Context(), "synod-instance-"+id, client.ContainerInspectOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if current.Container.Config.Labels["synod.tier"] != "2" || len(current.Container.Mounts) != 1 || current.Container.Mounts[0].Source != live {
		t.Fatalf("mounts: %+v", current.Container.Mounts)
	}
	start := runtimeCommand(t, r, appID, &agentv1.CommandInput{ResourceId: id, Payload: &agentv1.CommandInput_StartContainer{StartContainer: &agentv1.StartContainer{}}})
	if _, err := r.Execute(t.Context(), start, nil); err == nil {
		t.Fatal("writer started without lease guard")
	}
	r.SetWriterLeaseCheck(func(got string) error {
		if got != appID.String() {
			return errors.New("wrong app")
		}
		return nil
	})
	if _, err := r.Execute(t.Context(), start, nil); err != nil {
		t.Fatal(err)
	}
	current, err = api.ContainerInspect(t.Context(), "synod-instance-"+id, client.ContainerInspectOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if current.Container.HostConfig.RestartPolicy.Name != "no" || !current.Container.State.Running {
		t.Fatalf("writer restart policy/state: %+v %+v", current.Container.HostConfig.RestartPolicy, current.Container.State)
	}
	apps, err := r.AppContainers(t.Context())
	if err != nil || len(apps) != 1 || apps[0].PID <= 0 || !apps[0].Running {
		t.Fatalf("writer inventory: %+v %v", apps, err)
	}
	foreign := apps[0]
	foreign.AppID = uuid.NewString()
	if err := r.FenceApp(t.Context(), foreign); !errors.Is(err, runtime.ErrOwnershipConflict) {
		t.Fatal("foreign fence accepted", err)
	}
	if err := r.FenceApp(t.Context(), apps[0]); err != nil {
		t.Fatal(err)
	}
	current, err = api.ContainerInspect(t.Context(), "synod-instance-"+id, client.ContainerInspectOptions{})
	if err != nil || current.Container.State.Running {
		t.Fatalf("writer remains running: %+v %v", current.Container.State, err)
	}
	if _, err := os.Stat(live); err != nil {
		t.Fatal("fence removed live data", err)
	}
}

func TestFailedPullIsNotAppliedAndRecoversAsAbsent(t *testing.T) {
	r, _, appID := runtimeFixture(t)
	// No registry listens on port 1, so the pull fails without creating anything.
	image := "127.0.0.1:1/synod-missing@sha256:" + strings.Repeat("0", 64)
	id := uuid.NewString()
	command := runtimeCommand(t, r, appID, &agentv1.CommandInput{ResourceId: id, Payload: &agentv1.CommandInput_EnsureContainer{EnsureContainer: &agentv1.EnsureContainer{
		ImageDigest: image, NetworkMode: agentv1.NetworkMode_NETWORK_MODE_BRIDGE, MemoryBytes: 64 << 20, CpuMillis: 250,
		Generic: &agentv1.GenericContainerConfig{Image: image, AppName: "missing", Mounts: []*agentv1.Mount{{Type: "tmpfs", Target: "/data"}}},
	}}})
	result, err := r.Execute(t.Context(), command, nil)
	if !errors.Is(err, runtime.ErrNotApplied) || slices.ContainsFunc(result.Effects, func(effect *agentv1.Effect) bool { return effect.Active || effect.OutcomeUnknown }) {
		t.Fatalf("failed pull left an open outcome: %+v %v", result, err)
	}
	// An agent that restarted during the pull recovers the command from the missing container.
	result, err = r.Recover(t.Context(), command, nil)
	if !errors.Is(err, runtime.ErrNotApplied) || len(result.Effects) != 1 || result.Effects[0].State != "absent" || result.Effects[0].Active || result.Effects[0].OutcomeUnknown {
		t.Fatalf("missing container did not resolve the ensure: %+v %v", result, err)
	}
}

func TestTier1ContainerUsesEngineStorageAndRunsEngineSteps(t *testing.T) {
	r, api, appID := runtimeFixture(t)
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	r.options.AppsDir = root
	data := filepath.Join(root, appID.String(), "engine", "data")
	if err := os.MkdirAll(data, 0o700); err != nil {
		t.Fatal(err)
	}
	id := uuid.NewString()
	ensure := &agentv1.EnsureContainer{ImageDigest: redisFixture, NetworkMode: agentv1.NetworkMode_NETWORK_MODE_BRIDGE, SecretEnv: []*agentv1.SecretEnv{{Name: "SYNOD_TEST_SECRET", Purpose: "superuser"}},
		Generic: &agentv1.GenericContainerConfig{Image: redisFixture, AppName: "cache", CommandOverride: true, Command: []string{"redis-server", "--save", "", "--appendonly", "no"}, Mounts: []*agentv1.Mount{{Type: "engine", Source: "data", Target: "/data"}}}}
	command := runtimeCommand(t, r, appID, &agentv1.CommandInput{ResourceId: id, Secrets: []*agentv1.SecretReference{{CredentialId: uuid.NewString(), Version: 1, Purpose: "superuser"}}, Payload: &agentv1.CommandInput_EnsureContainer{EnsureContainer: ensure}})
	secrets := runtime.Secrets{"superuser": []byte("private-value")}
	if _, err := r.Execute(t.Context(), command, secrets); err != nil {
		t.Fatalf("ensure: %v / %v", err, errors.Unwrap(err))
	}
	current, err := api.ContainerInspect(t.Context(), "synod-instance-"+id, client.ContainerInspectOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if current.Container.Config.Labels["synod.tier"] != "1" || len(current.Container.Mounts) != 1 || current.Container.Mounts[0].Source != data {
		t.Fatalf("mounts: %+v labels: %v", current.Container.Mounts, current.Container.Config.Labels)
	}
	// A database copy starts without a writer lease. Standbys run too.
	start := runtimeCommand(t, r, appID, &agentv1.CommandInput{ResourceId: id, Payload: &agentv1.CommandInput_StartContainer{StartContainer: &agentv1.StartContainer{}}})
	if _, err := r.Execute(t.Context(), start, secrets); err != nil {
		t.Fatalf("start: %v / %v", err, errors.Unwrap(err))
	}
	current, err = api.ContainerInspect(t.Context(), "synod-instance-"+id, client.ContainerInspectOptions{})
	if err != nil || !current.Container.State.Running || current.Container.HostConfig.RestartPolicy.Name != container.RestartPolicyDisabled {
		t.Fatalf("running copy: %+v %v", current.Container.HostConfig.RestartPolicy, err)
	}
	out, err := r.EngineExec(t.Context(), appID, []string{"SYNOD_STEP=step"}, "from-stdin", "sh", "-c", `cat; echo " $SYNOD_STEP $SYNOD_TEST_SECRET"`)
	if err != nil || string(out) != "from-stdin step private-value\n" {
		t.Fatalf("engine step: %q %v", out, err)
	}
	if _, err := r.EngineExec(t.Context(), appID, nil, "", "sh", "-c", "echo broken >&2; exit 3"); err == nil || !strings.Contains(err.Error(), "broken") {
		t.Fatalf("failed step: %v", err)
	}
	if _, err := r.EngineExec(t.Context(), uuid.New(), nil, "", "true"); !errors.Is(err, runtime.ErrNotApplied) {
		t.Fatalf("step for an absent copy: %v", err)
	}
	apps, err := r.EngineApps(t.Context())
	if err != nil || !slices.Equal(apps, []uuid.UUID{appID}) {
		t.Fatalf("running database inventory: %v %v", apps, err)
	}
	blocked := make(chan error, 1)
	go func() {
		_, err := r.EngineExec(t.Context(), appID, nil, "", "sh", "-c", "touch /tmp/fence-test; sleep 60")
		blocked <- err
	}()
	started := false
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); {
		if _, err := r.EngineExec(t.Context(), appID, nil, "", "test", "-e", "/tmp/fence-test"); err == nil {
			started = true
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !started {
		t.Fatal("engine command did not start")
	}
	if err := r.FenceEngine(t.Context(), appID); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-blocked:
		if err == nil {
			t.Fatal("killed engine command reported success")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("fencing waited for the engine command")
	}
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); {
		apps, err = r.EngineApps(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		if len(apps) == 0 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("stopped database remains active: %v", apps)
}
