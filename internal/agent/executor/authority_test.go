package executor

import (
	"context"
	"errors"
	"net/http"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/containerd/errdefs"
	"github.com/google/uuid"
	"github.com/moby/moby/client"
	"github.com/sirrobot01/synod/internal/agent/journal"
	"github.com/sirrobot01/synod/internal/controlstream"
	"github.com/sirrobot01/synod/internal/operation"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/proto"
)

func TestLateBarrierCannotActivateReplacementSession(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	var once atomic.Bool
	f := newFixture(t, func(request *http.Request, base http.RoundTripper) (*http.Response, error) {
		if strings.HasSuffix(request.URL.Path, "/containers/json") && once.CompareAndSwap(false, true) {
			close(entered)
			select {
			case <-release:
			case <-request.Context().Done():
				return nil, request.Context().Err()
			}
		}
		return base.RoundTrip(request)
	})
	old, cancel := localSession(t, f.e)
	if _, err := f.e.Handle(old, &agentv1.ControllerMessage{Message: &agentv1.ControllerMessage_PrepareEpoch{PrepareEpoch: &agentv1.PrepareEpoch{Epoch: f.epoch}}}); err != nil {
		t.Fatal(err)
	}
	task := job{cluster: f.epoch.ClusterId, barrier: true, session: f.e.session, gate: f.e.session.gates[f.epoch.ClusterId]}
	done := make(chan struct{})
	go func() { f.e.inspectBarrier(t.Context(), task); close(done) }()
	<-entered
	cancel()
	fresh, _ := localSession(t, f.e)
	if _, err := f.e.Handle(fresh, &agentv1.ControllerMessage{Message: &agentv1.ControllerMessage_PrepareEpoch{PrepareEpoch: &agentv1.PrepareEpoch{Epoch: f.epoch}}}); err != nil {
		t.Fatal(err)
	}
	close(release)
	<-done
	if _, err := f.e.Handle(fresh, &agentv1.ControllerMessage{Message: &agentv1.ControllerMessage_ActivateEpoch{ActivateEpoch: &agentv1.ActivateEpoch{Epoch: f.epoch}}}); !errors.Is(err, operation.ErrBarrierPending) {
		t.Fatal("old inventory opened replacement gate", err)
	}
	activateLocal(t, f.e, fresh, f.epoch)
}

func TestCancellationBeforeAndAfterStart(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	var once atomic.Bool
	f := newFixture(t, func(request *http.Request, base http.RoundTripper) (*http.Response, error) {
		if strings.HasSuffix(request.URL.Path, "/volumes/create") && once.CompareAndSwap(false, true) {
			close(entered)
			select {
			case <-release:
			case <-request.Context().Done():
				return nil, request.Context().Err()
			}
		}
		return base.RoundTrip(request)
	})
	ctx, _ := localSession(t, f.e)
	activateLocal(t, f.e, ctx, f.epoch)
	queued := volumeCommand(t, f.epoch)
	if _, err := f.e.Handle(ctx, &agentv1.ControllerMessage{DeliveryAttemptId: uuid.NewString(), Message: &agentv1.ControllerMessage_Command{Command: queued}}); err != nil {
		t.Fatal(err)
	}
	cancelled, err := f.e.Handle(ctx, &agentv1.ControllerMessage{Message: &agentv1.ControllerMessage_Cancel{Cancel: &agentv1.CancelCommand{Epoch: f.epoch, CommandId: queued.CommandId}}})
	if err != nil || cancelled.GetCancellationResult().State != "cancelled" {
		t.Fatal(cancelled, err)
	}
	if record, err := f.e.journal.Get(queued.CommandId); err != nil || record.State != journal.Terminal || record.Result.State != "cancelled" {
		t.Fatal("cancellation was not durable", err)
	}
	if _, err := f.api.VolumeInspect(t.Context(), "synod-volume-"+queued.Input.ResourceId, client.VolumeInspectOptions{}); !errdefs.IsNotFound(err) {
		t.Fatal("queued cancellation had native effect", err)
	}
	command := volumeCommand(t, f.epoch)
	if _, err := f.e.Handle(ctx, &agentv1.ControllerMessage{DeliveryAttemptId: uuid.NewString(), Message: &agentv1.ControllerMessage_Command{Command: command}}); err != nil {
		t.Fatal(err)
	}
	task := job{cluster: f.epoch.ClusterId, command: command.CommandId, session: f.e.session, gate: f.e.session.gates[f.epoch.ClusterId]}
	done := make(chan struct{})
	go func() { f.e.execute(t.Context(), task); close(done) }()
	<-entered
	response, err := f.e.Handle(ctx, &agentv1.ControllerMessage{Message: &agentv1.ControllerMessage_Cancel{Cancel: &agentv1.CancelCommand{Epoch: f.epoch, CommandId: command.CommandId}}})
	if err != nil || response.GetCancellationResult().State != "not_cancellable" {
		t.Fatal(response, err)
	}
	close(release)
	<-done
	if record, err := f.e.journal.Get(command.CommandId); err != nil || record.Result == nil || record.Result.State != "succeeded" {
		t.Fatal("in-flight action was incorrectly cancelled", err)
	}
}

func TestWorkerAndClusterBounds(t *testing.T) {
	entered := make(chan struct{}, 8)
	release := make(chan struct{})
	var active, peak atomic.Int32
	var lock sync.Mutex
	seen := make(map[string]int)
	f := newFixture(t, func(request *http.Request, base http.RoundTripper) (*http.Response, error) {
		if strings.HasSuffix(request.URL.Path, "/volumes/create") {
			n := active.Add(1)
			for old := peak.Load(); n > old && !peak.CompareAndSwap(old, n); old = peak.Load() {
			}
			defer active.Add(-1)
			entered <- struct{}{}
			select {
			case <-release:
			case <-request.Context().Done():
				return nil, request.Context().Err()
			}
		}
		return base.RoundTrip(request)
	})
	controller := startController(t, f, func(_ context.Context, _ controlstream.Peer, message *agentv1.AgentMessage) error {
		if result := message.GetTerminalResult(); result != nil {
			lock.Lock()
			seen[result.CommandId]++
			lock.Unlock()
		}
		return nil
	})
	commands := make([]*agentv1.Command, 0, 4)
	for range 4 {
		epoch := proto.Clone(f.epoch).(*agentv1.Epoch)
		epoch.ClusterId = uuid.NewString()
		controller.activate(t, epoch)
		commands = append(commands, volumeCommand(t, epoch))
	}
	for _, command := range commands {
		controller.send(t, &agentv1.ControllerMessage{DeliveryAttemptId: uuid.NewString(), Message: &agentv1.ControllerMessage_Command{Command: command}})
		ack := controller.wait(t, func(message *agentv1.AgentMessage) bool { return message.GetAdmissionAck() != nil })
		if ack.GetAdmissionAck().State != "queued" {
			t.Fatal(ack)
		}
	}
	<-entered
	<-entered
	if peak.Load() != 2 || cap(f.e.jobs) != 4 {
		t.Fatal("worker or queue bound changed", peak.Load(), cap(f.e.jobs))
	}
	duplicate := volumeCommand(t, commands[0].Epoch)
	controller.send(t, &agentv1.ControllerMessage{DeliveryAttemptId: uuid.NewString(), Message: &agentv1.ControllerMessage_Command{Command: duplicate}})
	ack := controller.wait(t, func(message *agentv1.AgentMessage) bool { return message.GetAdmissionAck() != nil })
	if ack.GetAdmissionAck().ErrorCode != "agent_busy" {
		t.Fatal("overlapping cluster command admitted", ack)
	}
	close(release)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for {
		lock.Lock()
		count := len(seen)
		lock.Unlock()
		if count == 4 {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("workers did not finish")
		case <-tick.C:
		}
	}
	if peak.Load() > 2 {
		t.Fatal("worker limit exceeded", peak.Load())
	}
}

func TestCancelledQueuedJobDoesNotInspectRuntime(t *testing.T) {
	var requests atomic.Int32
	f := newFixture(t, func(request *http.Request, base http.RoundTripper) (*http.Response, error) {
		requests.Add(1)
		return base.RoundTrip(request)
	})
	ctx, _ := localSession(t, f.e)
	activateLocal(t, f.e, ctx, f.epoch)
	command := volumeCommand(t, f.epoch)
	if _, err := f.e.Handle(ctx, &agentv1.ControllerMessage{DeliveryAttemptId: uuid.NewString(), Message: &agentv1.ControllerMessage_Command{Command: command}}); err != nil {
		t.Fatal(err)
	}
	queued := job{cluster: f.epoch.ClusterId, command: command.CommandId, session: f.e.session, gate: f.e.session.gates[f.epoch.ClusterId]}
	if _, err := f.e.Handle(ctx, &agentv1.ControllerMessage{Message: &agentv1.ControllerMessage_Cancel{Cancel: &agentv1.CancelCommand{CommandId: command.CommandId, Epoch: f.epoch}}}); err != nil {
		t.Fatal(err)
	}
	before := requests.Load()
	f.e.execute(t.Context(), queued)
	record, err := f.e.journal.Get(command.CommandId)
	if err != nil || record.State != journal.Terminal || len(record.Effects) != 0 || record.Result.State != "cancelled" {
		t.Fatal("stale queued job changed cancellation", record, err)
	}
	if requests.Load() != before || f.e.failure != nil {
		t.Fatal("cancelled queued job touched runtime or failed executor", f.e.failure)
	}
	if err := f.e.journal.Acknowledge(&agentv1.ResultAck{CommandId: command.CommandId, Epoch: f.epoch, Sequence: 1}); err != nil {
		t.Fatal(err)
	}
	if count, err := f.e.journal.Compact(time.Now().Add(48 * time.Hour)); err != nil || count != 1 {
		t.Fatal("compaction", count, err)
	}
	f.e.execute(t.Context(), queued)
	if requests.Load() != before || f.e.failure != nil {
		t.Fatal("compacted queued job touched runtime or failed executor", f.e.failure)
	}
}

func TestHigherEpochWaitsForStartedWorker(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	var once atomic.Bool
	f := newFixture(t, func(request *http.Request, base http.RoundTripper) (*http.Response, error) {
		if strings.HasSuffix(request.URL.Path, "/volumes/create") && once.CompareAndSwap(false, true) {
			close(entered)
			select {
			case <-release:
			case <-request.Context().Done():
				return nil, request.Context().Err()
			}
		}
		return base.RoundTrip(request)
	})
	controller := startController(t, f, nil)
	controller.activate(t, f.epoch)
	command := volumeCommand(t, f.epoch)
	controller.send(t, &agentv1.ControllerMessage{DeliveryAttemptId: uuid.NewString(), Message: &agentv1.ControllerMessage_Command{Command: command}})
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("native request did not start")
	}
	next := proto.Clone(f.epoch).(*agentv1.Epoch)
	next.ExecutionEpoch++
	controller.send(t, &agentv1.ControllerMessage{Message: &agentv1.ControllerMessage_PrepareEpoch{PrepareEpoch: &agentv1.PrepareEpoch{Epoch: next}}})
	barrier := controller.wait(t, func(message *agentv1.AgentMessage) bool {
		return message.GetBarrierResult() != nil && proto.Equal(message.GetBarrierResult().Epoch, next)
	}).GetBarrierResult()
	if barrier.Drained || !slices.ContainsFunc(barrier.Effects, func(effect *agentv1.Effect) bool { return effect.CommandId == command.CommandId && effect.Active }) {
		t.Fatal("higher epoch omitted the started worker")
	}
	close(release)
	drained := controller.wait(t, func(message *agentv1.AgentMessage) bool {
		return message.GetBarrierResult() != nil && message.GetBarrierResult().Drained && proto.Equal(message.GetBarrierResult().Epoch, next)
	}).GetBarrierResult()
	if !slices.ContainsFunc(drained.Effects, func(effect *agentv1.Effect) bool {
		return effect.CommandId == command.CommandId && effect.NativeOperationId == "" && !effect.Active && !effect.OutcomeUnknown
	}) {
		t.Fatal("barrier lost explicit Running placeholder resolution")
	}
	controller.send(t, &agentv1.ControllerMessage{Message: &agentv1.ControllerMessage_ActivateEpoch{ActivateEpoch: &agentv1.ActivateEpoch{Epoch: next}}})
	controller.send(t, &agentv1.ControllerMessage{DeliveryAttemptId: uuid.NewString(), Message: &agentv1.ControllerMessage_Command{Command: command}})
	rejected := controller.wait(t, func(message *agentv1.AgentMessage) bool {
		return message.GetAdmissionAck() != nil && message.GetAdmissionAck().CommandId == command.CommandId
	}).GetAdmissionAck()
	if rejected.ErrorCode != "stale_owner" {
		t.Fatal("old epoch admitted after transfer", rejected)
	}
	fresh := volumeCommand(t, next)
	controller.send(t, &agentv1.ControllerMessage{DeliveryAttemptId: uuid.NewString(), Message: &agentv1.ControllerMessage_Command{Command: fresh}})
	result := controller.wait(t, func(message *agentv1.AgentMessage) bool {
		return message.GetTerminalResult() != nil && message.GetTerminalResult().CommandId == fresh.CommandId
	}).GetTerminalResult()
	if result.State != "succeeded" {
		t.Fatal("new epoch failed after native drain", result)
	}
}
