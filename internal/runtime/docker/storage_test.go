package docker

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"os"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/moby/moby/api/pkg/stdcopy"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/mount"
	"github.com/moby/moby/client"
	"github.com/sirrobot01/synod/internal/runtime"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/types/known/durationpb"
)

func postgresStorageFixture(t *testing.T, r *Runtime, api *client.Client, clusterID uuid.UUID, material ...runtime.Secrets) *agentv1.Command {
	t.Helper()
	info, err := api.Info(t.Context(), client.InfoOptions{})
	if err != nil {
		t.Fatal(err)
	}
	var image string
	switch info.Info.Architecture {
	case "aarch64", "arm64":
		image = "postgres@sha256:051f7b7b3abdd564d5d1bd1e8c4b9c1b6e77087d1dd22020ede611c096a272e0"
	case "x86_64", "amd64":
		image = "postgres@sha256:f4c66b820c6f974249089d3d16d86a3698eae11e8746eb6644b2271031e91232"
	default:
		t.Fatalf("no pinned PostgreSQL fixture for %s", info.Info.Architecture)
	}
	volumeID, networkID := uuid.NewString(), uuid.NewString()
	for _, input := range []*agentv1.CommandInput{
		{ResourceId: volumeID, Payload: &agentv1.CommandInput_EnsureVolume{EnsureVolume: &agentv1.EnsureVolume{VolumeId: volumeID}}},
		{ResourceId: networkID, Payload: &agentv1.CommandInput_EnsureNetwork{EnsureNetwork: &agentv1.EnsureNetwork{NetworkId: networkID}}},
	} {
		if _, err := r.Execute(t.Context(), runtimeCommand(t, r, clusterID, input), nil); err != nil {
			t.Fatal(err)
		}
	}
	adminID, appID, replicationID := uuid.NewString(), uuid.NewString(), uuid.NewString()
	command := runtimeCommand(t, r, clusterID, &agentv1.CommandInput{
		ResourceId: uuid.NewString(), Secrets: []*agentv1.SecretReference{{CredentialId: adminID, Version: 1, Purpose: "superuser"}, {CredentialId: appID, Version: 1, Purpose: "application"}, {CredentialId: replicationID, Version: 1, Purpose: "replication"}},
		Payload: &agentv1.CommandInput_EnsureContainer{EnsureContainer: &agentv1.EnsureContainer{
			ImageDigest: image, Engine: "postgres", VolumeId: volumeID, NetworkId: networkID,
			NetworkMode: agentv1.NetworkMode_NETWORK_MODE_BRIDGE, MemoryBytes: 128 << 20, CpuMillis: 1000,
			Postgres:  &agentv1.PostgresContainerConfig{Port: 5432, SuperuserCredentialId: adminID, SuperuserCredentialVersion: 1, ApplicationCredentialId: appID, ApplicationCredentialVersion: 1, ReplicationCredentialId: replicationID, ReplicationCredentialVersion: 1},
			SecretEnv: []*agentv1.SecretEnv{{Name: "POSTGRES_PASSWORD", Purpose: "superuser"}, {Name: "SYNOD_APP_VERIFIER", Purpose: "application"}, {Name: "SYNOD_REPL_VERIFIER", Purpose: "replication"}},
		}},
	})
	secrets := runtime.Secrets{"superuser": []byte(uuid.NewString()), "application": []byte(uuid.NewString()), "replication": []byte(uuid.NewString())}
	if len(material) > 0 {
		secrets = material[0]
	}
	if _, err := r.Execute(t.Context(), command, secrets); err != nil {
		t.Fatal(err)
	}
	return command
}

func startPostgresFixture(t *testing.T, r *Runtime, command *agentv1.Command) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	result, err := r.Execute(ctx, command, nil)
	for errors.Is(err, runtime.ErrOutcomeUnknown) {
		select {
		case <-ctx.Done():
			t.Fatal("PostgreSQL bootstrap did not finish")
		case <-time.After(50 * time.Millisecond):
		}
		result, err = r.Recover(ctx, command, result.Effects)
	}
	if err != nil {
		t.Fatalf("PostgreSQL startup: %v", err)
	}
}

func storageExec(t *testing.T, api *client.Client, name string, args ...string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	native, err := api.ExecCreate(ctx, name, client.ExecCreateOptions{Cmd: args, AttachStdout: true, AttachStderr: true})
	if err != nil {
		t.Fatal(err)
	}
	attached, err := api.ExecAttach(ctx, native.ID, client.ExecAttachOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer attached.Close()
	stop := context.AfterFunc(ctx, func() { attached.Close() })
	defer stop()
	var output bytes.Buffer
	_, err = stdcopy.StdCopy(&output, &output, io.LimitReader(attached.Reader, 4096))
	if err != nil {
		t.Fatal(err)
	}
	status, err := api.ExecInspect(ctx, native.ID, client.ExecInspectOptions{})
	if err != nil || status.Running || status.ExitCode != 0 {
		t.Fatalf("fixture exec: %+v %v output=%q", status, err, output.String())
	}
	return strings.TrimSpace(output.String())
}

func TestPostgresFirstStartAndRestartPreserveData(t *testing.T) {
	r, api, clusterID := runtimeFixture(t)
	ensure := postgresStorageFixture(t, r, api, clusterID)
	id, name := ensure.Input.ResourceId, "synod-instance-"+ensure.Input.ResourceId
	later := runtimeCommand(t, r, clusterID, &agentv1.CommandInput{ResourceId: id, Payload: &agentv1.CommandInput_StartContainer{StartContainer: &agentv1.StartContainer{}}})
	if _, err := r.Execute(t.Context(), later, nil); !errors.Is(err, runtime.ErrStorageLost) {
		t.Fatalf("unmarked empty storage initialized: %v", err)
	}
	initial := runtimeCommand(t, r, clusterID, &agentv1.CommandInput{ResourceId: id, Payload: &agentv1.CommandInput_StartContainer{StartContainer: &agentv1.StartContainer{AllowInitialization: true}}})
	startPostgresFixture(t, r, initial)
	storageExec(t, api, name, "sh", "-c", "until pg_isready -h 127.0.0.1 -q; do sleep 0.1; done")
	storageExec(t, api, name, "psql", "-h", "/run/synod-postgres", "-U", "synod_admin", "-d", "app", "-v", "ON_ERROR_STOP=1", "-c", "CREATE TABLE storage_test (value integer); INSERT INTO storage_test VALUES (86);")
	stop := runtimeCommand(t, r, clusterID, &agentv1.CommandInput{ResourceId: id, Payload: &agentv1.CommandInput_StopContainer{StopContainer: &agentv1.StopContainer{GracePeriod: durationpb.New(10 * time.Second)}}})
	if _, err := r.Execute(t.Context(), stop, nil); err != nil {
		t.Fatal(err)
	}
	restart := runtimeCommand(t, r, clusterID, &agentv1.CommandInput{ResourceId: id, Payload: &agentv1.CommandInput_StartContainer{StartContainer: &agentv1.StartContainer{}}})
	startPostgresFixture(t, r, restart)
	got := storageExec(t, api, name, "sh", "-c", "until pg_isready -h 127.0.0.1 -q; do sleep 0.1; done; psql -h /run/synod-postgres -U synod_admin -d app -At -c 'SELECT value FROM storage_test'")
	if got != "86" {
		t.Fatalf("restarted data=%q", got)
	}
}

func TestPostgresInitialStartRecoveryAllowsInitializationWindow(t *testing.T) {
	r, api, clusterID := runtimeFixture(t)
	ensure := postgresStorageFixture(t, r, api, clusterID)
	var inspected atomic.Int32
	faulty, err := client.New(client.WithHost(os.Getenv("SYNOD_TEST_DOCKER_HOST")), client.WithResponseHook(func(response *http.Response) {
		if response.Request.Method == http.MethodHead && strings.HasSuffix(response.Request.URL.Path, "/archive") {
			inspected.Add(1)
			response.StatusCode = http.StatusNotFound
			response.Status = "404 Not Found"
		}
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer faulty.Close()
	injected, err := New(faulty, r.options)
	if err != nil {
		t.Fatal(err)
	}
	initial := runtimeCommand(t, r, clusterID, &agentv1.CommandInput{ResourceId: ensure.Input.ResourceId, Payload: &agentv1.CommandInput_StartContainer{StartContainer: &agentv1.StartContainer{AllowInitialization: true}}})
	if _, err := injected.Execute(t.Context(), initial, nil); err != nil && !errors.Is(err, runtime.ErrOutcomeUnknown) {
		t.Fatalf("initial running process required PG_VERSION before initdb completed: %v", err)
	}
	if inspected.Load() != 0 {
		t.Fatal("first start inspected a marker that initdb has not yet created")
	}
	later := runtimeCommand(t, r, clusterID, &agentv1.CommandInput{ResourceId: ensure.Input.ResourceId, Payload: &agentv1.CommandInput_StartContainer{StartContainer: &agentv1.StartContainer{}}})
	recovered, err := injected.Recover(t.Context(), later, nil)
	if !errors.Is(err, runtime.ErrOutcomeUnknown) || len(recovered.Effects) == 0 || !recovered.Effects[0].OutcomeUnknown || inspected.Load() == 0 {
		t.Fatalf("running storage loss settled as a known refusal: %+v %v", recovered, err)
	}
	// Native work must remain visible even when the main process is already running.
	native, err := api.ExecCreate(t.Context(), "synod-instance-"+ensure.Input.ResourceId, client.ExecCreateOptions{Cmd: []string{"sleep", "30"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := api.ExecStart(t.Context(), native.ID, client.ExecStartOptions{Detach: true}); err != nil {
		t.Fatal(err)
	}
	recovered, err = injected.Recover(t.Context(), later, nil)
	if !errors.Is(err, runtime.ErrOutcomeUnknown) || !slices.ContainsFunc(recovered.Effects, func(effect *agentv1.Effect) bool {
		return effect.Active && strings.HasSuffix(effect.NativeOperationId, ":"+native.ID)
	}) {
		t.Fatalf("storage refusal discarded active native work: %+v %v", recovered, err)
	}
}

func TestPostgresLostStorageRefusesStartAndReplay(t *testing.T) {
	for name, damage := range map[string]string{
		"missing marker":   "rm /data/PG_VERSION",
		"empty marker":     ": > /data/PG_VERSION",
		"directory marker": "rm /data/PG_VERSION; mkdir /data/PG_VERSION",
		"symlink marker":   "rm /data/PG_VERSION; ln -s /etc/passwd /data/PG_VERSION",
		"empty directory":  "find /data -mindepth 1 -delete",
	} {
		t.Run(name, func(t *testing.T) {
			r, api, clusterID := runtimeFixture(t)
			ensure := postgresStorageFixture(t, r, api, clusterID)
			id, containerName := ensure.Input.ResourceId, "synod-instance-"+ensure.Input.ResourceId
			initial := runtimeCommand(t, r, clusterID, &agentv1.CommandInput{ResourceId: id, Payload: &agentv1.CommandInput_StartContainer{StartContainer: &agentv1.StartContainer{AllowInitialization: true}}})
			startPostgresFixture(t, r, initial)
			storageExec(t, api, containerName, "sh", "-c", "until pg_isready -h 127.0.0.1 -q; do sleep 0.1; done")
			stop := runtimeCommand(t, r, clusterID, &agentv1.CommandInput{ResourceId: id, Payload: &agentv1.CommandInput_StopContainer{StopContainer: &agentv1.StopContainer{GracePeriod: durationpb.New(10 * time.Second)}}})
			if _, err := r.Execute(t.Context(), stop, nil); err != nil {
				t.Fatal(err)
			}
			before, err := api.ContainerInspect(t.Context(), containerName, client.ContainerInspectOptions{})
			if err != nil {
				t.Fatal(err)
			}
			// Only this test's stopped, owned volume is mounted in the fault container.
			tool, err := api.ContainerCreate(t.Context(), client.ContainerCreateOptions{
				Config:     &container.Config{Image: ensure.Input.GetEnsureContainer().ImageDigest, Entrypoint: []string{"sh", "-c"}, Cmd: []string{damage}, Labels: map[string]string{labelInstallation: r.options.InstallationID.String()}},
				HostConfig: &container.HostConfig{NetworkMode: "none", Mounts: []mount.Mount{{Type: mount.TypeVolume, Source: "synod-volume-" + ensure.Input.GetEnsureContainer().VolumeId, Target: "/data"}}},
			})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := api.ContainerStart(t.Context(), tool.ID, client.ContainerStartOptions{}); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
			defer cancel()
			wait := api.ContainerWait(ctx, tool.ID, client.ContainerWaitOptions{})
			select {
			case result := <-wait.Result:
				if result.StatusCode != 0 || result.Error != nil {
					t.Fatalf("storage fault failed: %+v", result)
				}
			case err := <-wait.Error:
				t.Fatal(err)
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			if _, err := api.ContainerRemove(t.Context(), tool.ID, client.ContainerRemoveOptions{RemoveVolumes: true}); err != nil {
				t.Fatal(err)
			}
			restart := runtimeCommand(t, r, clusterID, &agentv1.CommandInput{ResourceId: id, Payload: &agentv1.CommandInput_StartContainer{StartContainer: &agentv1.StartContainer{}}})
			for _, command := range []*agentv1.Command{restart, initial} {
				if _, err := r.Execute(t.Context(), command, nil); !errors.Is(err, runtime.ErrStorageLost) || !errors.Is(err, runtime.ErrConfigurationConflict) {
					t.Fatalf("lost storage accepted: %v", err)
				}
				recovered, err := r.Recover(t.Context(), command, nil)
				if !errors.Is(err, runtime.ErrStorageLost) || len(recovered.Effects) != 1 || slices.ContainsFunc(recovered.Effects, func(effect *agentv1.Effect) bool { return effect.Active || effect.OutcomeUnknown }) {
					t.Fatalf("known refusal did not settle: %+v %v", recovered, err)
				}
			}
			after, err := api.ContainerInspect(t.Context(), containerName, client.ContainerInspectOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if after.Container.State.Running || after.Container.State.StartedAt != before.Container.State.StartedAt || after.Container.HostConfig.RestartPolicy.Name != container.RestartPolicyDisabled {
				t.Fatalf("refusal changed process or restart policy: %+v %+v", after.Container.State, after.Container.HostConfig.RestartPolicy)
			}
		})
	}
}

func TestPostgresStorageInspectionFailureRemainsUnknown(t *testing.T) {
	r, api, clusterID := runtimeFixture(t)
	ensure := postgresStorageFixture(t, r, api, clusterID)
	command := runtimeCommand(t, r, clusterID, &agentv1.CommandInput{ResourceId: ensure.Input.ResourceId, Payload: &agentv1.CommandInput_StartContainer{StartContainer: &agentv1.StartContainer{}}})
	var inspected atomic.Bool
	faulty, err := client.New(client.WithHost(os.Getenv("SYNOD_TEST_DOCKER_HOST")), client.WithResponseHook(func(response *http.Response) {
		if response.Request.Method == http.MethodHead && strings.HasSuffix(response.Request.URL.Path, "/archive") {
			inspected.Store(true)
			response.StatusCode = http.StatusInternalServerError
			response.Status = "500 Internal Server Error"
		}
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer faulty.Close()
	injected, err := New(faulty, r.options)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := injected.Execute(t.Context(), command, nil); !errors.Is(err, runtime.ErrOutcomeUnknown) || errors.Is(err, runtime.ErrStorageLost) {
		t.Fatalf("unavailable stat became a known refusal: %v", err)
	}
	recovered, err := injected.Recover(t.Context(), command, nil)
	if !errors.Is(err, runtime.ErrOutcomeUnknown) || len(recovered.Effects) != 1 || !recovered.Effects[0].OutcomeUnknown || !inspected.Load() {
		t.Fatalf("storage inspection lost uncertainty: %+v %v stat=%v", recovered, err, inspected.Load())
	}
	item, err := api.ContainerInspect(t.Context(), "synod-instance-"+ensure.Input.ResourceId, client.ContainerInspectOptions{})
	if err != nil || item.Container.State.Running || item.Container.HostConfig.RestartPolicy.Name != container.RestartPolicyDisabled {
		t.Fatalf("unavailable stat changed container: %+v %v", item.Container.State, err)
	}
}

func TestPostgresMissingContainerDuringStorageInspectionRemainsUnknown(t *testing.T) {
	r, api, clusterID := runtimeFixture(t)
	ensure := postgresStorageFixture(t, r, api, clusterID)
	command := runtimeCommand(t, r, clusterID, &agentv1.CommandInput{ResourceId: ensure.Input.ResourceId, Payload: &agentv1.CommandInput_StartContainer{StartContainer: &agentv1.StartContainer{}}})
	var statRead, missingContainer atomic.Bool
	faulty, err := client.New(client.WithHost(os.Getenv("SYNOD_TEST_DOCKER_HOST")), client.WithResponseHook(func(response *http.Response) {
		if response.Request.Method == http.MethodHead && strings.HasSuffix(response.Request.URL.Path, "/archive") {
			statRead.Store(true)
			response.StatusCode = http.StatusNotFound
			response.Status = "404 Not Found"
		}
		if statRead.Load() && response.Request.Method == http.MethodGet && strings.Contains(response.Request.URL.Path, "/containers/") && strings.HasSuffix(response.Request.URL.Path, "/json") {
			missingContainer.Store(true)
			response.Body.Close()
			response.Body = io.NopCloser(strings.NewReader(`{"message":"No such container"}`))
			response.StatusCode = http.StatusNotFound
			response.Status = "404 Not Found"
		}
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer faulty.Close()
	injected, err := New(faulty, r.options)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := injected.Execute(t.Context(), command, nil); !errors.Is(err, runtime.ErrOutcomeUnknown) || errors.Is(err, runtime.ErrStorageLost) || !missingContainer.Load() {
		t.Fatalf("missing container misreported as missing data: %v inspected=%v", err, missingContainer.Load())
	}
	statRead.Store(false)
	missingContainer.Store(false)
	recovered, err := injected.Recover(t.Context(), command, nil)
	if !errors.Is(err, runtime.ErrOutcomeUnknown) || len(recovered.Effects) != 1 || !recovered.Effects[0].OutcomeUnknown || !missingContainer.Load() {
		t.Fatalf("missing container settled the storage check: %+v %v inspected=%v", recovered, err, missingContainer.Load())
	}
}

// redisStorageFixture creates one owned Redis instance with its volume and network.
func redisStorageFixture(t *testing.T, r *Runtime, clusterID uuid.UUID) *agentv1.Command {
	t.Helper()
	volumeID, networkID := uuid.NewString(), uuid.NewString()
	for _, input := range []*agentv1.CommandInput{
		{ResourceId: volumeID, Payload: &agentv1.CommandInput_EnsureVolume{EnsureVolume: &agentv1.EnsureVolume{VolumeId: volumeID}}},
		{ResourceId: networkID, Payload: &agentv1.CommandInput_EnsureNetwork{EnsureNetwork: &agentv1.EnsureNetwork{NetworkId: networkID}}},
	} {
		if _, err := r.Execute(t.Context(), runtimeCommand(t, r, clusterID, input), nil); err != nil {
			t.Fatal(err)
		}
	}
	command := runtimeCommand(t, r, clusterID, &agentv1.CommandInput{
		ResourceId: uuid.NewString(),
		Secrets:    []*agentv1.SecretReference{{CredentialId: uuid.NewString(), Version: 1, Purpose: "superuser"}, {CredentialId: uuid.NewString(), Version: 1, Purpose: "application"}, {CredentialId: uuid.NewString(), Version: 1, Purpose: "replication"}},
		Payload: &agentv1.CommandInput_EnsureContainer{EnsureContainer: &agentv1.EnsureContainer{
			Engine: "redis", ImageDigest: redisFixture, VolumeId: volumeID, NetworkId: networkID,
			NetworkMode: agentv1.NetworkMode_NETWORK_MODE_BRIDGE, MemoryBytes: 64 << 20, CpuMillis: 250,
			Redis:     &agentv1.RedisContainerConfig{Port: 16379, Persistence: "rdb", MaxmemoryBytes: 48 << 20, EvictionPolicy: "noeviction"},
			SecretEnv: []*agentv1.SecretEnv{{Name: "REDIS_ADMIN_PASSWORD", Purpose: "superuser"}, {Name: "REDIS_APP_PASSWORD", Purpose: "application"}, {Name: "REDIS_REPLICATION_PASSWORD", Purpose: "replication"}},
		}},
	})
	secrets := runtime.Secrets{"superuser": []byte(uuid.NewString()), "application": []byte(uuid.NewString()), "replication": []byte(uuid.NewString())}
	if _, err := r.Execute(t.Context(), command, secrets); err != nil {
		t.Fatalf("create configured Redis: %v", err)
	}
	return command
}

// Redis writes no file of its own that is always present, so Synod marks the directory
// it prepared. Without the marker an emptied volume would read as a new one, and a
// replica would then copy that empty dataset over its own.
func TestRedisFirstStartMarksItsVolume(t *testing.T) {
	r, api, clusterID := runtimeFixture(t)
	ensure := redisStorageFixture(t, r, clusterID)
	id, name := ensure.Input.ResourceId, "synod-instance-"+ensure.Input.ResourceId
	later := runtimeCommand(t, r, clusterID, &agentv1.CommandInput{ResourceId: id, Payload: &agentv1.CommandInput_StartContainer{StartContainer: &agentv1.StartContainer{}}})
	if _, err := r.Execute(t.Context(), later, nil); !errors.Is(err, runtime.ErrStorageLost) {
		t.Fatalf("an unmarked Redis volume was accepted: %v", err)
	}
	initial := runtimeCommand(t, r, clusterID, &agentv1.CommandInput{ResourceId: id, Payload: &agentv1.CommandInput_StartContainer{StartContainer: &agentv1.StartContainer{AllowInitialization: true}}})
	if _, err := r.Execute(t.Context(), initial, nil); err != nil {
		t.Fatalf("first Redis start: %v", err)
	}
	if got := storageExec(t, api, name, "sh", "-c", "until [ -s /data/.synod-redis-volume ]; do sleep 0.1; done; cat /data/.synod-redis-volume"); got != "synod" {
		t.Fatalf("marker contents %q", got)
	}
	// An ordinary restart finds the marker and starts.
	stop := runtimeCommand(t, r, clusterID, &agentv1.CommandInput{ResourceId: id, Payload: &agentv1.CommandInput_StopContainer{StopContainer: &agentv1.StopContainer{GracePeriod: durationpb.New(10 * time.Second)}}})
	if _, err := r.Execute(t.Context(), stop, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Execute(t.Context(), later, nil); err != nil {
		t.Fatalf("a marked Redis volume was refused on restart: %v", err)
	}
}

// A Redis volume that lost its marker refuses to start, exactly as PostgreSQL does.
func TestRedisLostStorageRefusesStart(t *testing.T) {
	for name, damage := range map[string]string{
		"missing marker":   "rm /data/.synod-redis-volume",
		"empty marker":     ": > /data/.synod-redis-volume",
		"directory marker": "rm /data/.synod-redis-volume; mkdir /data/.synod-redis-volume",
		"symlink marker":   "rm /data/.synod-redis-volume; ln -s /etc/passwd /data/.synod-redis-volume",
		"empty directory":  "find /data -mindepth 1 -delete",
	} {
		t.Run(name, func(t *testing.T) {
			r, api, clusterID := runtimeFixture(t)
			ensure := redisStorageFixture(t, r, clusterID)
			id, containerName := ensure.Input.ResourceId, "synod-instance-"+ensure.Input.ResourceId
			initial := runtimeCommand(t, r, clusterID, &agentv1.CommandInput{ResourceId: id, Payload: &agentv1.CommandInput_StartContainer{StartContainer: &agentv1.StartContainer{AllowInitialization: true}}})
			if _, err := r.Execute(t.Context(), initial, nil); err != nil {
				t.Fatal(err)
			}
			storageExec(t, api, containerName, "sh", "-c", "until [ -s /data/.synod-redis-volume ]; do sleep 0.1; done")
			stop := runtimeCommand(t, r, clusterID, &agentv1.CommandInput{ResourceId: id, Payload: &agentv1.CommandInput_StopContainer{StopContainer: &agentv1.StopContainer{GracePeriod: durationpb.New(10 * time.Second)}}})
			if _, err := r.Execute(t.Context(), stop, nil); err != nil {
				t.Fatal(err)
			}
			before, err := api.ContainerInspect(t.Context(), containerName, client.ContainerInspectOptions{})
			if err != nil {
				t.Fatal(err)
			}
			damageVolume(t, api, r, ensure.Input.GetEnsureContainer().VolumeId, redisFixture, damage)
			restart := runtimeCommand(t, r, clusterID, &agentv1.CommandInput{ResourceId: id, Payload: &agentv1.CommandInput_StartContainer{StartContainer: &agentv1.StartContainer{}}})
			// The initialization flag must not reopen the window once the container ran.
			for _, command := range []*agentv1.Command{restart, initial} {
				if _, err := r.Execute(t.Context(), command, nil); !errors.Is(err, runtime.ErrStorageLost) {
					t.Fatalf("lost Redis storage accepted: %v", err)
				}
				recovered, err := r.Recover(t.Context(), command, nil)
				if !errors.Is(err, runtime.ErrStorageLost) || len(recovered.Effects) != 1 || slices.ContainsFunc(recovered.Effects, func(effect *agentv1.Effect) bool { return effect.Active || effect.OutcomeUnknown }) {
					t.Fatalf("known refusal did not settle: %+v %v", recovered, err)
				}
			}
			after, err := api.ContainerInspect(t.Context(), containerName, client.ContainerInspectOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if after.Container.State.Running || after.Container.State.StartedAt != before.Container.State.StartedAt {
				t.Fatalf("refusal changed the process: %+v", after.Container.State)
			}
		})
	}
}

// damageVolume runs one shell command against a stopped, owned volume.
func damageVolume(t *testing.T, api *client.Client, r *Runtime, volumeID, image, damage string) {
	t.Helper()
	tool, err := api.ContainerCreate(t.Context(), client.ContainerCreateOptions{
		Config:     &container.Config{Image: image, Entrypoint: []string{"sh", "-c"}, Cmd: []string{damage}, Labels: map[string]string{labelInstallation: r.options.InstallationID.String()}},
		HostConfig: &container.HostConfig{NetworkMode: "none", Mounts: []mount.Mount{{Type: mount.TypeVolume, Source: "synod-volume-" + volumeID, Target: "/data"}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := api.ContainerStart(t.Context(), tool.ID, client.ContainerStartOptions{}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	wait := api.ContainerWait(ctx, tool.ID, client.ContainerWaitOptions{})
	select {
	case result := <-wait.Result:
		if result.StatusCode != 0 || result.Error != nil {
			t.Fatalf("storage fault failed: %+v", result)
		}
	case err := <-wait.Error:
		t.Fatal(err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if _, err := api.ContainerRemove(t.Context(), tool.ID, client.ContainerRemoveOptions{RemoveVolumes: true}); err != nil {
		t.Fatal(err)
	}
}
