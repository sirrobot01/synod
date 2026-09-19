package executor

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"testing/iotest"
	"time"

	"github.com/containerd/errdefs"
	"github.com/google/uuid"
	"github.com/moby/moby/client"
	"github.com/sirrobot01/synod/internal/agent/capacity"
	"github.com/sirrobot01/synod/internal/agent/journal"
	"github.com/sirrobot01/synod/internal/operation"
	"github.com/sirrobot01/synod/internal/runtime/docker"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

type requestTransport func(*http.Request) (*http.Response, error)

func (f requestTransport) RoundTrip(request *http.Request) (*http.Response, error) { return f(request) }

type fixture struct {
	e       *Executor
	api     *client.Client
	path    string
	options journal.Options
	epoch   *agentv1.Epoch
}

// fixtureHost declares a measurable pool on a real directory. The proc tree is a
// fixture so the numbers do not depend on the machine running the test.
func fixtureHost(t *testing.T) capacity.Host {
	t.Helper()
	proc := t.TempDir()
	if err := os.WriteFile(filepath.Join(proc, "meminfo"), []byte("MemTotal: 16000000 kB\nMemAvailable: 8000000 kB\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(proc, "stat"), []byte("cpu  1 0 1 1 1\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	return capacity.Host{ProcRoot: proc, StoragePath: t.TempDir()}
}

// receive commits one agent receipt
func newFixture(t *testing.T, hook func(*http.Request, http.RoundTripper) (*http.Response, error)) *fixture {
	t.Helper()
	host := os.Getenv("SYNOD_TEST_DOCKER_HOST")
	if host == "" {
		t.Skip("set SYNOD_TEST_DOCKER_HOST for actual Docker tests")
	}
	identity := journal.Identity{InstallationID: uuid.NewString(), NodeID: uuid.NewString(), RecoveryGeneration: uuid.NewString()}
	options := journal.Options{Identity: identity, AllowCreate: true, EmptyInventory: true}
	path := filepath.Join(t.TempDir(), "journal")
	j, err := journal.Open(path, options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { j.Close() })
	api, err := client.New(client.WithHost(host))
	if err != nil {
		t.Fatal(err)
	}
	mutationAPI := api
	if hook != nil {
		base := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			var dialer net.Dialer
			return dialer.DialContext(ctx, "unix", strings.TrimPrefix(host, "unix://"))
		}}
		t.Cleanup(base.CloseIdleConnections)
		mutationAPI, err = client.New(client.WithHost(host), client.WithHTTPClient(&http.Client{Transport: requestTransport(func(request *http.Request) (*http.Response, error) { return hook(request, base) })}))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { mutationAPI.Close() })
	}
	r, err := docker.New(mutationAPI, docker.Options{InstallationID: uuid.MustParse(identity.InstallationID), NodeID: uuid.MustParse(identity.NodeID), AllowBridgeFixture: true})
	if err != nil {
		t.Fatal(err)
	}
	// A real agent always declares a local pool, and the precheck measures it
	// before every effect. A fixture without one would exercise a path that no
	// deployed agent has.
	e, err := New(j, r, Options{
		Workers: 2, QueueSize: 4, PollInterval: 20 * time.Millisecond,
		Host: fixtureHost(t), StorageID: "pool-test", RuntimeVersion: "test", BootID: uuid.NewString(),
	})
	if err != nil {
		t.Fatal(err)
	}
	f := &fixture{e: e, api: api, path: path, options: options, epoch: &agentv1.Epoch{InstallationId: identity.InstallationID, NodeId: identity.NodeID, ClusterId: uuid.NewString(), RecoveryGeneration: identity.RecoveryGeneration, ExecutionEpoch: 1}}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		defer api.Close()
		filters := client.Filters{}.Add("label", "io.synod.installation="+identity.InstallationID)
		containers, err := api.ContainerList(ctx, client.ContainerListOptions{All: true, Filters: filters})
		if err != nil {
			t.Error(err)
			return
		}
		for _, item := range containers.Items {
			if item.Labels["io.synod.installation"] != identity.InstallationID {
				t.Fatal("cleanup identity mismatch")
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
			if item.Labels["io.synod.installation"] != identity.InstallationID {
				t.Fatal("cleanup identity mismatch")
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
			if item.Labels["io.synod.installation"] != identity.InstallationID {
				t.Fatal("cleanup identity mismatch")
			}
			if _, err := api.VolumeRemove(ctx, item.Name, client.VolumeRemoveOptions{}); err != nil {
				t.Error(err)
			}
		}
	})
	return f
}

func volumeCommand(t *testing.T, epoch *agentv1.Epoch) *agentv1.Command {
	t.Helper()
	id := uuid.NewString()
	command := &agentv1.Command{Epoch: proto.Clone(epoch).(*agentv1.Epoch), OperationId: uuid.NewString(), StepId: uuid.NewString(), CommandId: uuid.NewString(), DesiredRevision: 1, PlanVersion: 1, Input: &agentv1.CommandInput{ResourceId: id, PayloadVersion: 1, Timeout: durationpb.New(10 * time.Second), Payload: &agentv1.CommandInput_EnsureVolume{EnsureVolume: &agentv1.EnsureVolume{VolumeId: id}}}}
	hash, err := operation.CommandHash(command)
	if err != nil {
		t.Fatal(err)
	}
	command.InputHash = hash[:]
	return command
}

func localSession(t *testing.T, e *Executor, capabilities ...string) (context.Context, context.CancelFunc) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	if _, err := e.Handle(ctx, &agentv1.ControllerMessage{Message: &agentv1.ControllerMessage_Session{Session: &agentv1.Session{SessionId: uuid.NewString(), Capabilities: capabilities}}}); err != nil {
		t.Fatal(err)
	}
	return ctx, cancel
}

func activateLocal(t *testing.T, e *Executor, ctx context.Context, epoch *agentv1.Epoch) {
	t.Helper()
	if _, err := e.Handle(ctx, &agentv1.ControllerMessage{Message: &agentv1.ControllerMessage_PrepareEpoch{PrepareEpoch: &agentv1.PrepareEpoch{Epoch: epoch}}}); err != nil {
		t.Fatal(err)
	}
	e.mu.Lock()
	task := job{cluster: epoch.ClusterId, barrier: true, session: e.session, gate: e.session.gates[epoch.ClusterId]}
	e.mu.Unlock()
	e.inspectBarrier(t.Context(), task)
	if _, err := e.Handle(ctx, &agentv1.ControllerMessage{Message: &agentv1.ControllerMessage_ActivateEpoch{ActivateEpoch: &agentv1.ActivateEpoch{Epoch: epoch}}}); err != nil {
		t.Fatal(err)
	}
}

func TestSessionLossRequiresFreshActivationBeforeStart(t *testing.T) {
	var f *fixture
	var command *agentv1.Command
	var creates atomic.Int32
	f = newFixture(t, func(request *http.Request, base http.RoundTripper) (*http.Response, error) {
		if strings.HasSuffix(request.URL.Path, "/volumes/create") {
			creates.Add(1)
			record, err := f.e.journal.Get(command.CommandId)
			if err != nil || record.State != journal.Running {
				t.Error("native request preceded durable Start", err)
			}
		}
		return base.RoundTrip(request)
	})
	old, cancel := localSession(t, f.e)
	activateLocal(t, f.e, old, f.epoch)
	command = volumeCommand(t, f.epoch)
	ack, err := f.e.Handle(old, &agentv1.ControllerMessage{Message: &agentv1.ControllerMessage_Command{Command: command}})
	if err != nil || ack.GetAdmissionAck().State != "queued" {
		t.Fatal(ack, err)
	}
	if record, err := f.e.journal.Get(command.CommandId); err != nil || record.State != journal.Queued {
		t.Fatal("ACK preceded admission", err)
	}
	stale := job{cluster: f.epoch.ClusterId, command: command.CommandId, session: f.e.session, gate: f.e.session.gates[f.epoch.ClusterId]}
	cancel()
	fresh, _ := localSession(t, f.e)
	if _, err := f.e.Handle(fresh, &agentv1.ControllerMessage{Message: &agentv1.ControllerMessage_ActivateEpoch{ActivateEpoch: &agentv1.ActivateEpoch{Epoch: f.epoch}}}); !errors.Is(err, operation.ErrBarrierPending) {
		t.Fatal("activated persisted authority on new session", err)
	}
	f.e.execute(t.Context(), stale)
	if _, err := f.api.VolumeInspect(t.Context(), "synod-volume-"+command.Input.ResourceId, client.VolumeInspectOptions{}); !errdefs.IsNotFound(err) {
		t.Fatal("stale session started mutation", err)
	}
	activateLocal(t, f.e, fresh, f.epoch)
	current := job{cluster: f.epoch.ClusterId, command: command.CommandId, session: f.e.session, gate: f.e.session.gates[f.epoch.ClusterId]}
	f.e.execute(t.Context(), current)
	if record, err := f.e.journal.Get(command.CommandId); err != nil || record.State != journal.Terminal || record.Result.State != "succeeded" {
		t.Fatal("command did not complete", err)
	}
	if creates.Load() != 1 {
		t.Fatal("duplicate native creation", creates.Load())
	}
}

func TestUnknownNativeReplyRecoversAfterJournalReopen(t *testing.T) {
	var dropped atomic.Bool
	f := newFixture(t, func(request *http.Request, base http.RoundTripper) (*http.Response, error) {
		response, err := base.RoundTrip(request)
		if err != nil {
			return nil, err
		}
		create := strings.HasSuffix(request.URL.Path, "/volumes/create") && response.StatusCode < 300
		if (create && dropped.CompareAndSwap(false, true)) || (dropped.Load() && request.Method == http.MethodGet && strings.Contains(request.URL.Path, "/volumes/")) {
			response.Body.Close()
			response.Body = io.NopCloser(iotest.ErrReader(io.ErrUnexpectedEOF))
		}
		return response, nil
	})
	ctx, _ := localSession(t, f.e)
	activateLocal(t, f.e, ctx, f.epoch)
	command := volumeCommand(t, f.epoch)
	if _, err := f.e.Handle(ctx, &agentv1.ControllerMessage{Message: &agentv1.ControllerMessage_Command{Command: command}}); err != nil {
		t.Fatal(err)
	}
	f.e.execute(t.Context(), job{cluster: f.epoch.ClusterId, command: command.CommandId, session: f.e.session, gate: f.e.session.gates[f.epoch.ClusterId]})
	record, err := f.e.journal.Get(command.CommandId)
	if err != nil || record.State != journal.Running || len(record.Effects) != 1 || !record.Effects[0].OutcomeUnknown {
		t.Fatal("uncertainty was not retained", record, err)
	}
	if err := f.e.journal.Close(); err != nil {
		t.Fatal(err)
	}
	f.options.AllowCreate = false
	reopened, err := journal.Open(f.path, f.options)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	r, err := docker.New(f.api, docker.Options{InstallationID: uuid.MustParse(f.epoch.InstallationId), NodeID: uuid.MustParse(f.epoch.NodeId)})
	if err != nil {
		t.Fatal(err)
	}
	recovered, err := New(reopened, r, Options{})
	if err != nil {
		t.Fatal(err)
	}
	recovered.execute(t.Context(), job{cluster: f.epoch.ClusterId, command: command.CommandId})
	terminal, err := reopened.Get(command.CommandId)
	if err != nil || terminal.State != journal.Terminal || terminal.Result.State != "succeeded" || terminal.Effects[0].NativeOperationId != record.Effects[0].NativeOperationId {
		t.Fatal("recovery failed", terminal, err)
	}
	if !dropped.Load() {
		t.Fatal("native create fault was not reached")
	}
}
