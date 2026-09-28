package docker

import (
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
	"github.com/moby/moby/client"
	"github.com/sirrobot01/synod/internal/agent/journal"
	"github.com/sirrobot01/synod/internal/operation"
	"github.com/sirrobot01/synod/internal/runtime"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

const redisFixture = "redis@sha256:d30960f73a599496d8b2802c97758bab6b1cd421fd06337f837779c47a57e1f3"

var redisFixtureSecrets = runtime.Secrets{"superuser": []byte("runtime-test-admin"), "application": []byte("runtime-test-app"), "replication": []byte("runtime-test-repl")}

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
	runtime, err := New(api, Options{InstallationID: uuid.New(), NodeID: uuid.New()})
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

func runtimeCommand(t *testing.T, r *Runtime, appID uuid.UUID, input *agentv1.CommandInput) *agentv1.Command {
	t.Helper()
	input.PayloadVersion = 1
	input.Timeout = durationpb.New(30 * time.Second)
	command := &agentv1.Command{Epoch: &agentv1.Epoch{InstallationId: r.options.InstallationID.String(), NodeId: r.options.NodeID.String(), AppId: appID.String(), RecoveryGeneration: uuid.NewString(), ExecutionEpoch: 1}, OperationId: uuid.NewString(), StepId: uuid.NewString(), CommandId: uuid.NewString(), DesiredRevision: 1, PlanVersion: 1, Input: input}
	hash, err := operation.CommandHash(command)
	if err != nil {
		t.Fatal(err)
	}
	command.InputHash = hash[:]
	return command
}

// fixtureContainer is one tier 3 container with a scratch data mount.
func fixtureContainer() *agentv1.EnsureContainer {
	return &agentv1.EnsureContainer{
		ImageDigest: redisFixture, NetworkMode: agentv1.NetworkMode_NETWORK_MODE_BRIDGE, MemoryBytes: 64 << 20, CpuMillis: 250,
		Generic: &agentv1.GenericContainerConfig{Image: redisFixture, AppName: "fixture", Mounts: []*agentv1.Mount{{Type: "tmpfs", Target: "/data"}}},
	}
}

// createFixtureContainer creates one tier 3 container.
func createFixtureContainer(t *testing.T, r *Runtime, appID uuid.UUID) *agentv1.Command {
	t.Helper()
	command := runtimeCommand(t, r, appID, &agentv1.CommandInput{ResourceId: uuid.NewString(), Payload: &agentv1.CommandInput_EnsureContainer{EnsureContainer: fixtureContainer()}})
	if _, err := r.Execute(t.Context(), command, nil); err != nil {
		t.Fatalf("container: %v / %v", err, errors.Unwrap(err))
	}
	return command
}

func TestRuntimeOwnershipAndConfigurationConflicts(t *testing.T) {
	r, api, appID := runtimeFixture(t)
	ensure := createFixtureContainer(t, r, appID)
	changed := proto.Clone(ensure).(*agentv1.Command)
	changed.Input.GetEnsureContainer().MemoryBytes = 128 << 20
	hash, err := operation.CommandHash(changed)
	if err != nil {
		t.Fatal(err)
	}
	changed.InputHash = hash[:]
	if _, err := r.Execute(t.Context(), changed, redisFixtureSecrets); !errors.Is(err, runtime.ErrConfigurationConflict) {
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

func TestRealDockerLostCreateReplyAndRecovery(t *testing.T) {
	for _, failedRecovery := range []bool{false, true} {
		t.Run(map[bool]string{false: "inspect-success", true: "inspect-unavailable"}[failedRecovery], func(t *testing.T) {
			r, _, appID := runtimeFixture(t)
			host := os.Getenv("SYNOD_TEST_DOCKER_HOST")
			var dropped atomic.Bool
			faulty, err := client.New(client.WithHost(host), client.WithResponseHook(func(response *http.Response) {
				isCreate := strings.HasSuffix(response.Request.URL.Path, "/containers/create") && response.StatusCode < 300
				if (isCreate && dropped.CompareAndSwap(false, true)) || (failedRecovery && dropped.Load() && response.Request.Method == http.MethodGet && strings.Contains(response.Request.URL.Path, "/containers/synod-instance-")) {
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
			command := runtimeCommand(t, r, appID, &agentv1.CommandInput{ResourceId: uuid.NewString(), Payload: &agentv1.CommandInput_EnsureContainer{EnsureContainer: fixtureContainer()}})
			j, options, path := runtimeJournal(t, command)
			result, err := injected.Execute(t.Context(), command, nil)
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
			if _, err := r.Execute(t.Context(), command, nil); err != nil {
				t.Fatal(err)
			}
			inventory, err := r.Inventory(t.Context())
			if err != nil || len(inventory) != 1 {
				t.Fatalf("duplicate resource inventory %+v: %v", inventory, err)
			}
		})
	}
}

// Usage must measure a container this node actually owns, on a real daemon.
func TestUsageMeasuresOwnedResources(t *testing.T) {
	r, _, appID := runtimeFixture(t)
	ensure := createFixtureContainer(t, r, appID)
	start := runtimeCommand(t, r, appID, &agentv1.CommandInput{ResourceId: ensure.Input.ResourceId, Payload: &agentv1.CommandInput_StartContainer{StartContainer: &agentv1.StartContainer{}}})
	if _, err := r.Execute(t.Context(), start, nil); err != nil {
		t.Fatal(err)
	}
	resource := uuid.MustParse(ensure.Input.ResourceId)
	usage, err := r.Usage(t.Context(), []uuid.UUID{resource})
	if err != nil {
		t.Fatal(err)
	}
	if len(usage) != 1 || usage[0].ResourceID != resource {
		t.Fatalf("usage=%+v, want the container this node owns", usage)
	}
	// A fresh container can measure zero bytes, but never a negative amount.
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
