package docker

import (
	"context"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"testing/iotest"
	"time"

	"github.com/moby/moby/client"
	"github.com/sirrobot01/synod/internal/agent/journal"
	"github.com/sirrobot01/synod/internal/operation"
	"github.com/sirrobot01/synod/internal/runtime"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
)

func runtimeJournal(t *testing.T, command *agentv1.Command) (*journal.Journal, journal.Options, string) {
	t.Helper()
	options := journal.Options{Identity: journal.Identity{InstallationID: command.Epoch.InstallationId, NodeID: command.Epoch.NodeId, RecoveryGeneration: command.Epoch.RecoveryGeneration}, AllowCreate: true, EmptyInventory: true}
	path := filepath.Join(t.TempDir(), "journal")
	j, err := journal.Open(path, options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { j.Close() })
	if err := j.Prepare(command.Epoch); err != nil {
		t.Fatal(err)
	}
	if err := j.RecordBarrier(&agentv1.BarrierResult{Epoch: command.Epoch, AdmissionsClosed: true, Drained: true}); err != nil {
		t.Fatal(err)
	}
	if err := j.Activate(command.Epoch, true); err != nil {
		t.Fatal(err)
	}
	if _, err := j.Admit(command); err != nil {
		t.Fatal(err)
	}
	if _, err := j.Start(command.CommandId); err != nil {
		t.Fatal(err)
	}
	return j, options, path
}

func TestRecoveryResolvesRetainedNativeExec(t *testing.T) {
	r, api, clusterID := runtimeFixture(t)
	ensure := createFixtureContainer(t, r, clusterID, agentv1.NetworkMode_NETWORK_MODE_BRIDGE)
	resourceID := ensure.Input.ResourceId
	start := runtimeCommand(t, r, clusterID, &agentv1.CommandInput{ResourceId: resourceID, Payload: &agentv1.CommandInput_StartContainer{StartContainer: &agentv1.StartContainer{AllowInitialization: true}}})
	if _, err := r.Execute(t.Context(), start, nil); err != nil {
		t.Fatal(err)
	}
	command := runtimeCommand(t, r, clusterID, &agentv1.CommandInput{ResourceId: resourceID, Payload: &agentv1.CommandInput_InspectContainer{InspectContainer: &agentv1.InspectContainer{}}})
	j, options, path := runtimeJournal(t, command)
	// This test creates a native task directly. The product adapter has no exec API.
	native, err := api.ExecCreate(t.Context(), "synod-instance-"+resourceID, client.ExecCreateOptions{Cmd: []string{"sleep", "2"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := api.ExecStart(t.Context(), native.ID, client.ExecStartOptions{Detach: true}); err != nil {
		t.Fatal(err)
	}
	observed, err := r.Execute(t.Context(), command, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(observed.Effects) != 2 || !observed.Effects[1].Active {
		t.Fatalf("native exec not observed: %+v", observed)
	}
	if err := j.Observe(command.CommandId, observed.Effects); err != nil {
		t.Fatal(err)
	}
	terminal := &agentv1.TerminalResult{Epoch: command.Epoch, CommandId: command.CommandId, InputHash: command.InputHash, Sequence: 1, State: "succeeded", Effects: observed.Effects[:1]}
	if err := j.Complete(terminal); !errors.Is(err, operation.ErrBarrierPending) {
		t.Fatalf("journal accepted omitted active exec: %v", err)
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
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	tick := time.NewTicker(20 * time.Millisecond)
	defer tick.Stop()
	var recovered runtime.Result
	for {
		recovered, err = r.Recover(ctx, command, retained.Effects)
		if err != nil {
			t.Fatal(err)
		}
		if !slices.ContainsFunc(recovered.Effects, func(effect *agentv1.Effect) bool { return effect.Active || effect.OutcomeUnknown }) {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("native task did not resolve")
		case <-tick.C:
		}
	}
	if len(recovered.Effects) != 2 || recovered.Effects[1].NativeOperationId != observed.Effects[1].NativeOperationId || recovered.Effects[1].State != "native_exec_exited" {
		t.Fatalf("missing explicit native completion: %+v", recovered)
	}
	// The task has exited. A lost exec-inspection reply must retain its old identity.
	faulty, err := client.New(client.WithHost(os.Getenv("SYNOD_TEST_DOCKER_HOST")), client.WithResponseHook(func(response *http.Response) {
		if strings.Contains(response.Request.URL.Path, "/exec/") && strings.HasSuffix(response.Request.URL.Path, "/json") {
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
	unknown, err := injected.Recover(t.Context(), command, retained.Effects)
	if !errors.Is(err, runtime.ErrOutcomeUnknown) || len(unknown.Effects) != 2 || !unknown.Effects[1].OutcomeUnknown || !unknown.Effects[1].Active || unknown.Effects[1].NativeOperationId != retained.Effects[1].NativeOperationId {
		t.Fatalf("failed exec inspection lost unresolved identity: %+v %v", unknown, err)
	}
	if err := reopened.Observe(command.CommandId, unknown.Effects); err != nil {
		t.Fatal(err)
	}
	recovered, err = r.Recover(t.Context(), command, unknown.Effects)
	if err != nil {
		t.Fatal(err)
	}
	if err := reopened.Observe(command.CommandId, recovered.Effects); err != nil {
		t.Fatal(err)
	}
	terminal.Effects = recovered.Effects
	if err := reopened.Complete(terminal); err != nil {
		t.Fatal(err)
	}
}
