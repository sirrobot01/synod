package executor

import (
	"context"
	"errors"
	"net/http"
	"slices"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/containerd/errdefs"
	"github.com/google/uuid"
	"github.com/moby/moby/client"
	"github.com/sirrobot01/synod/internal/agent/journal"
	"github.com/sirrobot01/synod/internal/controlstream"
	"github.com/sirrobot01/synod/internal/operation"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/proto"
)

func TestInspectionRequiresNegotiationAndDrainedAuthority(t *testing.T) {
	var calls atomic.Int32
	f := newFixture(t, func(request *http.Request, base http.RoundTripper) (*http.Response, error) {
		calls.Add(1)
		return base.RoundTrip(request)
	})
	request := &agentv1.ControllerMessage{Message: &agentv1.ControllerMessage_CommandInspection{CommandInspection: &agentv1.CommandInspectionRequest{RequestId: uuid.NewString(), Command: volumeCommand(t, f.epoch)}}}
	old, _ := localSession(t, f.e)
	if _, err := f.e.Handle(old, request); !errors.Is(err, operation.ErrIncompatibleProtocol) {
		t.Fatal("inspection was not negotiated", err)
	}
	current, _ := localSession(t, f.e, controlstream.CommandInspectionCapability)
	if _, err := f.e.Handle(current, request); !errors.Is(err, operation.ErrBarrierPending) {
		t.Fatal("inspection had no prepared authority", err)
	}
	if _, err := f.e.Handle(current, &agentv1.ControllerMessage{Message: &agentv1.ControllerMessage_PrepareEpoch{PrepareEpoch: &agentv1.PrepareEpoch{Epoch: f.epoch}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := f.e.Handle(current, request); !errors.Is(err, operation.ErrBarrierPending) {
		t.Fatal("inspection preceded native drain", err)
	}
	if calls.Load() != 0 {
		t.Fatal("invalid request inspected runtime")
	}
}

func TestInspectionReadsFreshStateWithoutAdmission(t *testing.T) {
	var creates atomic.Int32
	f := newFixture(t, func(request *http.Request, base http.RoundTripper) (*http.Response, error) {
		if request.Method == http.MethodPost && strings.HasSuffix(request.URL.Path, "/volumes/create") {
			creates.Add(1)
		}
		return base.RoundTrip(request)
	})
	controller := startController(t, f, nil)
	controller.activate(t, f.epoch)
	command := volumeCommand(t, f.epoch)
	controller.send(t, &agentv1.ControllerMessage{DeliveryAttemptId: uuid.NewString(), Message: &agentv1.ControllerMessage_Command{Command: command}})
	terminal := controller.wait(t, func(message *agentv1.AgentMessage) bool { return message.GetTerminalResult() != nil }).GetTerminalResult()
	if terminal.State != "succeeded" {
		t.Fatal(terminal)
	}
	before, err := f.e.journal.Get(command.CommandId)
	if err != nil {
		t.Fatal(err)
	}
	// Fresh preparation permits inspection before activation of the next epoch.
	next := proto.Clone(f.epoch).(*agentv1.Epoch)
	next.ExecutionEpoch++
	controller.send(t, &agentv1.ControllerMessage{Message: &agentv1.ControllerMessage_PrepareEpoch{PrepareEpoch: &agentv1.PrepareEpoch{Epoch: next}}})
	controller.wait(t, func(message *agentv1.AgentMessage) bool {
		return message.GetBarrierResult() != nil && message.GetBarrierResult().Drained
	})
	inspectCommand := proto.Clone(command).(*agentv1.Command)
	inspectCommand.Epoch = next
	request := &agentv1.CommandInspectionRequest{RequestId: uuid.NewString(), Command: inspectCommand, PriorEffects: terminal.Effects}
	controller.send(t, &agentv1.ControllerMessage{Message: &agentv1.ControllerMessage_CommandInspection{CommandInspection: request}})
	present := controller.wait(t, func(message *agentv1.AgentMessage) bool { return message.GetCommandInspection() != nil }).GetCommandInspection()
	if present.RequestId != request.RequestId || !proto.Equal(present.Epoch, next) || present.FailureCode != "" || len(present.Effects) != 1 || present.Effects[0].State != "present" || present.Effects[0].Active || present.Effects[0].OutcomeUnknown {
		t.Fatal("inspection did not prove current volume", present)
	}
	if _, err := f.api.VolumeRemove(t.Context(), "synod-volume-"+command.Input.ResourceId, client.VolumeRemoveOptions{}); err != nil {
		t.Fatal(err)
	}
	request.RequestId = uuid.NewString()
	controller.send(t, &agentv1.ControllerMessage{Message: &agentv1.ControllerMessage_CommandInspection{CommandInspection: request}})
	missing := controller.wait(t, func(message *agentv1.AgentMessage) bool { return message.GetCommandInspection() != nil }).GetCommandInspection()
	if missing.RequestId != request.RequestId || missing.FailureCode == "" || len(missing.Effects) != 1 || !missing.Effects[0].OutcomeUnknown {
		t.Fatal("old success replaced fresh missing state", missing)
	}
	after, err := f.e.journal.Get(command.CommandId)
	if err != nil || after.State != journal.Terminal || !proto.Equal(before.Result, after.Result) || !proto.Equal(after.Epoch, command.Epoch) {
		t.Fatal("inspection changed historical execution", err)
	}
	request = &agentv1.CommandInspectionRequest{RequestId: uuid.NewString(), Command: volumeCommand(t, next)}
	controller.send(t, &agentv1.ControllerMessage{Message: &agentv1.ControllerMessage_CommandInspection{CommandInspection: request}})
	controller.wait(t, func(message *agentv1.AgentMessage) bool { return message.GetCommandInspection() != nil })
	if _, err := f.e.journal.Get(request.Command.CommandId); !errors.Is(err, journal.ErrNotFound) {
		t.Fatal("inspection admitted a command", err)
	}
	if creates.Load() != 1 {
		t.Fatal("inspection repeated volume creation", creates.Load())
	}
	if _, err := f.api.VolumeInspect(t.Context(), "synod-volume-"+request.Command.Input.ResourceId, client.VolumeInspectOptions{}); !errdefs.IsNotFound(err) {
		t.Fatal("inspection created missing resource", err)
	}
}

func TestInspectionBoundsAndRejectsLateAuthority(t *testing.T) {
	for _, replacement := range []string{"session", "epoch"} {
		t.Run(replacement, func(t *testing.T) {
			entered, release := make(chan struct{}), make(chan struct{})
			var once atomic.Bool
			f := newFixture(t, func(request *http.Request, base http.RoundTripper) (*http.Response, error) {
				response, err := base.RoundTrip(request)
				if request.Method == http.MethodGet && strings.Contains(request.URL.Path, "/volumes/synod-volume-") && once.CompareAndSwap(false, true) {
					close(entered)
					<-release
				}
				return response, err
			})
			ctx, _ := localSession(t, f.e, controlstream.CommandInspectionCapability)
			activateLocal(t, f.e, ctx, f.epoch)
			request := &agentv1.ControllerMessage{Message: &agentv1.ControllerMessage_CommandInspection{CommandInspection: &agentv1.CommandInspectionRequest{RequestId: uuid.NewString(), Command: volumeCommand(t, f.epoch)}}}
			done := make(chan error, 1)
			go func() {
				response, err := f.e.Handle(ctx, request)
				if response != nil {
					t.Error("stale inspection returned evidence")
				}
				done <- err
			}()
			<-entered
			if _, err := f.e.Handle(ctx, request); !errors.Is(err, journal.ErrBusy) {
				t.Error("concurrent inspection exceeded bound", err)
			}
			if replacement == "session" {
				localSession(t, f.e, controlstream.CommandInspectionCapability)
			} else {
				next := proto.Clone(f.epoch).(*agentv1.Epoch)
				next.ExecutionEpoch++
				if _, err := f.e.Handle(ctx, &agentv1.ControllerMessage{Message: &agentv1.ControllerMessage_PrepareEpoch{PrepareEpoch: &agentv1.PrepareEpoch{Epoch: next}}}); err != nil {
					t.Error(err)
				}
			}
			close(release)
			if err := <-done; !errors.Is(err, operation.ErrStaleOwner) {
				t.Fatal("old inspection survived authority replacement", err)
			}
		})
	}
}

func TestInspectionRetainsUnknownPriorIdentity(t *testing.T) {
	f := newFixture(t, nil)
	ctx, _ := localSession(t, f.e, controlstream.CommandInspectionCapability)
	activateLocal(t, f.e, ctx, f.epoch)
	command := volumeCommand(t, f.epoch)
	prior := &agentv1.Effect{CommandId: command.CommandId, ResourceId: command.Input.ResourceId, NativeOperationId: "unknown-native-task", Active: true, OutcomeUnknown: true, State: "unknown"}
	request := &agentv1.CommandInspectionRequest{RequestId: uuid.NewString(), Command: command, PriorEffects: []*agentv1.Effect{prior}}
	response, err := f.e.Handle(ctx, &agentv1.ControllerMessage{Message: &agentv1.ControllerMessage_CommandInspection{CommandInspection: request}})
	if err != nil {
		t.Fatal(err)
	}
	result := response.GetCommandInspection()
	if len(result.Effects) != 2 || result.Effects[1].NativeOperationId != prior.NativeOperationId || !result.Effects[1].Active || !result.Effects[1].OutcomeUnknown {
		t.Fatal("inspection omitted unresolved prior identity", result)
	}
}

func TestInspectionCapabilitiesStayDetached(t *testing.T) {
	f := newFixture(t, nil)
	var callbacks atomic.Int32
	controller := startController(t, f, func(_ context.Context, peer controlstream.Peer, _ *agentv1.AgentMessage) error {
		if !slices.Equal(peer.Capabilities, []string{controlstream.CommandInspectionCapability}) {
			t.Error("callback changed negotiated capabilities", peer.Capabilities)
			return controlstream.ErrMessage
		}
		peer.Capabilities[0] = "callback_mutation"
		callbacks.Add(1)
		return nil
	})
	controller.activate(t, f.epoch)
	peer, ok := controller.server.Session(controller.node)
	if !ok || !slices.Equal(peer.Capabilities, []string{controlstream.CommandInspectionCapability}) {
		t.Fatal("callback corrupted live session capabilities", peer)
	}
	peer.Capabilities[0] = "caller_mutation"
	again, ok := controller.server.Session(controller.node)
	if !ok || !slices.Equal(again.Capabilities, []string{controlstream.CommandInspectionCapability}) || callbacks.Load() < 2 {
		t.Fatal("session returned mutable capabilities", again)
	}
}
