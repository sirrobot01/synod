package docker

import (
	"archive/tar"
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"testing/iotest"
	"time"

	"github.com/google/uuid"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/client"
	"github.com/sirrobot01/synod/internal/agent/journal"
	"github.com/sirrobot01/synod/internal/operation"
	"github.com/sirrobot01/synod/internal/runtime"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

const redisFixture = "redis@sha256:d30960f73a599496d8b2802c97758bab6b1cd421fd06337f837779c47a57e1f3"

func runtimeFixture(t *testing.T) (*Runtime, *client.Client, uuid.UUID) {
	t.Helper()
	host := os.Getenv("SYNOD_TEST_DOCKER_HOST")
	if host == "" {
		t.Skip("set SYNOD_TEST_DOCKER_HOST for the isolated Docker fixture")
	}
	api, err := client.New(client.WithHost(host))
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := New(api, Options{InstallationID: uuid.New(), NodeID: uuid.New(), AllowBridgeFixture: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		defer api.Close()
		filters := client.Filters{}.Add("label", labelInstallation+"="+runtime.options.InstallationID.String())
		containers, err := api.ContainerList(ctx, client.ContainerListOptions{All: true, Filters: filters})
		if err != nil {
			t.Error(err)
			return
		}
		for _, item := range containers.Items {
			if item.Labels[labelInstallation] != runtime.options.InstallationID.String() {
				t.Fatal("cleanup ownership mismatch")
			}
			if _, err := api.ContainerRemove(ctx, item.ID, client.ContainerRemoveOptions{Force: true, RemoveVolumes: false}); err != nil {
				t.Error(err)
			}
		}
		networks, err := api.NetworkList(ctx, client.NetworkListOptions{Filters: filters})
		if err != nil {
			t.Error(err)
			return
		}
		for _, item := range networks.Items {
			if item.Labels[labelInstallation] != runtime.options.InstallationID.String() {
				t.Fatal("cleanup ownership mismatch")
			}
			if _, err := api.NetworkRemove(ctx, item.ID, client.NetworkRemoveOptions{}); err != nil {
				t.Error(err)
			}
		}
		volumes, err := api.VolumeList(ctx, client.VolumeListOptions{Filters: filters})
		if err != nil {
			t.Error(err)
			return
		}
		for _, item := range volumes.Items {
			if item.Labels[labelInstallation] != runtime.options.InstallationID.String() {
				t.Fatal("cleanup ownership mismatch")
			}
			if _, err := api.VolumeRemove(ctx, item.Name, client.VolumeRemoveOptions{}); err != nil {
				t.Error(err)
			}
		}
	})
	return runtime, api, uuid.New()
}

func runtimeCommand(t *testing.T, r *Runtime, clusterID uuid.UUID, input *agentv1.CommandInput) *agentv1.Command {
	t.Helper()
	input.PayloadVersion = 1
	input.Timeout = durationpb.New(30 * time.Second)
	command := &agentv1.Command{Epoch: &agentv1.Epoch{InstallationId: r.options.InstallationID.String(), NodeId: r.options.NodeID.String(), ClusterId: clusterID.String(), RecoveryGeneration: uuid.NewString(), ExecutionEpoch: 1}, OperationId: uuid.NewString(), StepId: uuid.NewString(), CommandId: uuid.NewString(), DesiredRevision: 1, PlanVersion: 1, Input: input}
	hash, err := operation.CommandHash(command)
	if err != nil {
		t.Fatal(err)
	}
	command.InputHash = hash[:]
	return command
}

func createFixtureContainer(t *testing.T, r *Runtime, clusterID uuid.UUID, mode agentv1.NetworkMode) *agentv1.Command {
	t.Helper()
	volumeID := uuid.NewString()
	networkID := ""
	volume := runtimeCommand(t, r, clusterID, &agentv1.CommandInput{ResourceId: volumeID, Payload: &agentv1.CommandInput_EnsureVolume{EnsureVolume: &agentv1.EnsureVolume{VolumeId: volumeID}}})
	if _, err := r.Execute(t.Context(), volume); err != nil {
		t.Fatalf("volume: %v / %v", err, errors.Unwrap(err))
	}
	if mode == agentv1.NetworkMode_NETWORK_MODE_BRIDGE {
		networkID = uuid.NewString()
		network := runtimeCommand(t, r, clusterID, &agentv1.CommandInput{ResourceId: networkID, Payload: &agentv1.CommandInput_EnsureNetwork{EnsureNetwork: &agentv1.EnsureNetwork{NetworkId: networkID}}})
		if _, err := r.Execute(t.Context(), network); err != nil {
			t.Fatal(err)
		}
	}
	command := runtimeCommand(t, r, clusterID, &agentv1.CommandInput{ResourceId: uuid.NewString(), Payload: &agentv1.CommandInput_EnsureContainer{EnsureContainer: &agentv1.EnsureContainer{ImageDigest: redisFixture, Engine: "redis", VolumeId: volumeID, NetworkId: networkID, NetworkMode: mode, MemoryBytes: 64 << 20, CpuMillis: 250}}})
	if _, err := r.Execute(t.Context(), command); err != nil {
		t.Fatalf("container: %v / %v", err, errors.Unwrap(err))
	}
	return command
}

func TestOwnedContainerLifecycleAndRetainedData(t *testing.T) {
	r, api, clusterID := runtimeFixture(t)
	ensure := createFixtureContainer(t, r, clusterID, agentv1.NetworkMode_NETWORK_MODE_BRIDGE)
	id := ensure.Input.ResourceId
	name := "synod-instance-" + id
	first, err := api.ContainerInspect(t.Context(), name, client.ContainerInspectOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.Execute(t.Context(), ensure); err != nil {
		t.Fatal(err)
	}
	inventory, err := r.Inventory(t.Context())
	if err != nil || len(inventory) != 3 {
		t.Fatalf("inventory %+v: %v", inventory, err)
	}
	var content bytes.Buffer
	archive := tar.NewWriter(&content)
	data := []byte("retained-task-038-data")
	if err := archive.WriteHeader(&tar.Header{Name: "marker", Mode: 0600, Size: int64(len(data))}); err != nil {
		t.Fatal(err)
	}
	if _, err := archive.Write(data); err != nil {
		t.Fatal(err)
	}
	if err := archive.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := api.CopyToContainer(t.Context(), first.Container.ID, client.CopyToContainerOptions{DestinationPath: "/data", Content: &content}); err != nil {
		t.Fatal(err)
	}
	start := runtimeCommand(t, r, clusterID, &agentv1.CommandInput{ResourceId: id, Payload: &agentv1.CommandInput_StartContainer{StartContainer: &agentv1.StartContainer{}}})
	if _, err := r.Execute(t.Context(), start); err != nil {
		t.Fatalf("start: %v / %v", err, errors.Unwrap(err))
	}
	if _, err := r.Execute(t.Context(), start); err != nil {
		t.Fatal(err)
	}
	logs, err := r.Logs(t.Context(), start, 128)
	if err != nil || len(logs) > 128 {
		t.Fatalf("logs length %d: %v", len(logs), err)
	}
	remove := runtimeCommand(t, r, clusterID, &agentv1.CommandInput{ResourceId: id, Payload: &agentv1.CommandInput_RemoveContainer{RemoveContainer: &agentv1.RemoveContainer{}}})
	if _, err := r.Execute(t.Context(), remove); !errors.Is(err, runtime.ErrBusy) {
		t.Fatalf("removed running container: %v", err)
	}
	stop := runtimeCommand(t, r, clusterID, &agentv1.CommandInput{ResourceId: id, Payload: &agentv1.CommandInput_StopContainer{StopContainer: &agentv1.StopContainer{GracePeriod: durationpb.New(5 * time.Second)}}})
	if _, err := r.Execute(t.Context(), stop); err != nil {
		t.Fatal(err)
	}
	stopped, err := api.ContainerInspect(t.Context(), name, client.ContainerInspectOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if stopped.Container.State.Running || stopped.Container.HostConfig.RestartPolicy.Name != container.RestartPolicyDisabled {
		t.Fatal("stopped intent was not preserved")
	}
	if _, err := r.Execute(t.Context(), remove); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Execute(t.Context(), remove); err != nil {
		t.Fatal(err)
	}
	volumeName := "synod-volume-" + ensure.Input.GetEnsureContainer().VolumeId
	if _, err := api.VolumeInspect(t.Context(), volumeName, client.VolumeInspectOptions{}); err != nil {
		t.Fatal("removed retained volume", err)
	}
	if _, err := r.Execute(t.Context(), ensure); err != nil {
		t.Fatal(err)
	}
	replacement, err := api.ContainerInspect(t.Context(), name, client.ContainerInspectOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if replacement.Container.ID == first.Container.ID {
		t.Fatal("container was not replaced")
	}
	copied, err := api.CopyFromContainer(t.Context(), replacement.Container.ID, client.CopyFromContainerOptions{SourcePath: "/data/marker"})
	if err != nil {
		t.Fatal(err)
	}
	defer copied.Content.Close()
	reader := tar.NewReader(copied.Content)
	if _, err := reader.Next(); err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(reader)
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("retained data %q: %v", got, err)
	}
}

func TestRuntimeOwnershipAndConfigurationConflicts(t *testing.T) {
	r, api, clusterID := runtimeFixture(t)
	id := uuid.NewString()
	command := runtimeCommand(t, r, clusterID, &agentv1.CommandInput{ResourceId: id, Payload: &agentv1.CommandInput_EnsureVolume{EnsureVolume: &agentv1.EnsureVolume{VolumeId: id}}})
	labels := r.labels(command, "volume", id)
	labels[labelNode] = uuid.NewString()
	if _, err := api.VolumeCreate(t.Context(), client.VolumeCreateOptions{Name: "synod-volume-" + id, Driver: "local", Labels: labels}); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Execute(t.Context(), command); !errors.Is(err, runtime.ErrOwnershipConflict) {
		t.Fatalf("accepted wrong node volume: %v", err)
	}
	if _, err := api.VolumeInspect(t.Context(), "synod-volume-"+id, client.VolumeInspectOptions{}); err != nil {
		t.Fatal("deleted conflicting volume", err)
	}
	ensure := createFixtureContainer(t, r, clusterID, agentv1.NetworkMode_NETWORK_MODE_BRIDGE)
	changed := proto.Clone(ensure).(*agentv1.Command)
	changed.Input.GetEnsureContainer().MemoryBytes = 128 << 20
	hash, err := operation.CommandHash(changed)
	if err != nil {
		t.Fatal(err)
	}
	changed.InputHash = hash[:]
	if _, err := r.Execute(t.Context(), changed); !errors.Is(err, runtime.ErrConfigurationConflict) {
		t.Fatalf("silently replaced configuration: %v", err)
	}
	item, err := api.ContainerInspect(t.Context(), "synod-instance-"+ensure.Input.ResourceId, client.ContainerInspectOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if item.Container.HostConfig.Memory != 64<<20 {
		t.Fatal("changed resource limits")
	}
}

func TestExplicitHostNetworkAndBridgeGate(t *testing.T) {
	r, api, clusterID := runtimeFixture(t)
	r.options.AllowBridgeFixture = false
	ensure := createFixtureContainer(t, r, clusterID, agentv1.NetworkMode_NETWORK_MODE_HOST)
	item, err := api.ContainerInspect(t.Context(), "synod-instance-"+ensure.Input.ResourceId, client.ContainerInspectOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if item.Container.HostConfig.NetworkMode != "host" || len(item.Container.HostConfig.PortBindings) != 0 {
		t.Fatal("host networking used published ports")
	}
	if item.Container.State.Running {
		t.Fatal("host test unexpectedly started a listener")
	}
	id := uuid.NewString()
	network := runtimeCommand(t, r, clusterID, &agentv1.CommandInput{ResourceId: id, Payload: &agentv1.CommandInput_EnsureNetwork{EnsureNetwork: &agentv1.EnsureNetwork{NetworkId: id}}})
	if _, err := r.Execute(t.Context(), network); !errors.Is(err, runtime.ErrUnsupportedConfiguration) {
		t.Fatalf("bridge fixture not gated: %v", err)
	}
}

func TestRealDockerLostCreateReplyAndRecovery(t *testing.T) {
	for _, failedRecovery := range []bool{false, true} {
		t.Run(map[bool]string{false: "inspect-success", true: "inspect-unavailable"}[failedRecovery], func(t *testing.T) {
			r, _, clusterID := runtimeFixture(t)
			host := os.Getenv("SYNOD_TEST_DOCKER_HOST")
			var dropped atomic.Bool
			faulty, err := client.New(client.WithHost(host), client.WithResponseHook(func(response *http.Response) {
				isCreate := strings.HasSuffix(response.Request.URL.Path, "/volumes/create") && response.StatusCode < 300
				if (isCreate && dropped.CompareAndSwap(false, true)) || (failedRecovery && dropped.Load() && response.Request.Method == http.MethodGet && strings.Contains(response.Request.URL.Path, "/volumes/")) {
					response.Body.Close()
					response.Body = io.NopCloser(iotest.ErrReader(io.ErrUnexpectedEOF))
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
			id := uuid.NewString()
			command := runtimeCommand(t, r, clusterID, &agentv1.CommandInput{ResourceId: id, Payload: &agentv1.CommandInput_EnsureVolume{EnsureVolume: &agentv1.EnsureVolume{VolumeId: id}}})
			j, options, path := runtimeJournal(t, command)
			result, err := injected.Execute(t.Context(), command)
			if failedRecovery {
				if !errors.Is(err, runtime.ErrOutcomeUnknown) || len(result.Effects) != 1 || !result.Effects[0].OutcomeUnknown {
					t.Fatalf("unknown result %+v: %v", result, err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			if !dropped.Load() {
				t.Fatal("fault never reached real Docker create")
			}
			if err := j.Observe(command.CommandId, result.Effects); err != nil {
				t.Fatal(err)
			}
			if err := j.Close(); err != nil {
				t.Fatal(err)
			}
			options.AllowCreate = false
			reopened, err := journal.Open(path, options)
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			retained, err := reopened.Get(command.CommandId)
			if err != nil {
				t.Fatal(err)
			}
			recovered, err := r.Recover(t.Context(), command, retained.Effects)
			if err != nil {
				t.Fatal(err)
			}
			if recovered.Effects[0].NativeOperationId != result.Effects[0].NativeOperationId {
				t.Fatal("recovery changed durable effect identity")
			}
			if err := reopened.Complete(&agentv1.TerminalResult{Epoch: command.Epoch, CommandId: command.CommandId, InputHash: command.InputHash, Sequence: 1, State: "succeeded", Effects: recovered.Effects}); err != nil {
				t.Fatalf("complete after retained observation: %v", err)
			}
			if _, err := r.Execute(t.Context(), command); err != nil {
				t.Fatal(err)
			}
			inventory, err := r.Inventory(t.Context())
			if err != nil || len(inventory) != 1 {
				t.Fatalf("duplicate resource inventory %+v: %v", inventory, err)
			}
		})
	}
}

// Usage must measure a volume this node actually owns, on a real daemon.
func TestUsageMeasuresOwnedResources(t *testing.T) {
	r, _, clusterID := runtimeFixture(t)
	id := uuid.NewString()
	command := runtimeCommand(t, r, clusterID, &agentv1.CommandInput{ResourceId: id, Payload: &agentv1.CommandInput_EnsureVolume{EnsureVolume: &agentv1.EnsureVolume{VolumeId: id}}})
	if _, err := r.Execute(t.Context(), command); err != nil {
		t.Fatal(err)
	}
	resource := uuid.MustParse(id)
	usage, err := r.Usage(t.Context(), []uuid.UUID{resource})
	if err != nil {
		t.Fatal(err)
	}
	if len(usage) != 1 || usage[0].ResourceID != resource {
		t.Fatalf("usage=%+v, want the volume this node owns", usage)
	}
	// A fresh volume can measure zero bytes, but never a negative amount.
	if usage[0].Used.DiskBytes < 0 || usage[0].Used.MemoryBytes < 0 || usage[0].Used.CPUMillis < 0 {
		t.Fatalf("used=%+v", usage[0].Used)
	}
}

// A resource this node does not own is absent, not reported as zero.
func TestUsageOmitsAResourceThisNodeDoesNotOwn(t *testing.T) {
	r, _, _ := runtimeFixture(t)
	usage, err := r.Usage(t.Context(), []uuid.UUID{uuid.New()})
	if err != nil {
		t.Fatal(err)
	}
	if len(usage) != 0 {
		t.Fatalf("usage=%+v, want nothing measured", usage)
	}
}

func TestUsageRefusesAnUnboundedRequest(t *testing.T) {
	r, _, _ := runtimeFixture(t)
	if _, err := r.Usage(t.Context(), make([]uuid.UUID, maxUsageResources+1)); err == nil {
		t.Fatal("an unbounded request was accepted")
	}
	if usage, err := r.Usage(t.Context(), nil); err != nil || usage != nil {
		t.Fatalf("an empty request must answer nothing: %+v %v", usage, err)
	}
}
