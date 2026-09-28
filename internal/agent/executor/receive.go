package executor

import (
	"bytes"
	"context"
	"errors"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/agent/capacity"
	"github.com/sirrobot01/synod/internal/agent/journal"
	"github.com/sirrobot01/synod/internal/agent/replica"
	"github.com/sirrobot01/synod/internal/agent/replication"
	"github.com/sirrobot01/synod/internal/controlstream"
	"github.com/sirrobot01/synod/internal/operation"
	"github.com/sirrobot01/synod/internal/router"
	"github.com/sirrobot01/synod/internal/runtime"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Handle accepts only transport-authenticated callbacks. Their context identifies a session.
func (e *Executor) Handle(ctx context.Context, message *agentv1.ControllerMessage) (*agentv1.AgentMessage, error) {
	if message == nil || ctx.Err() != nil {
		return nil, controlstream.ErrDisconnected
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if ctx.Err() != nil {
		return nil, controlstream.ErrDisconnected
	}
	if e.failure != nil {
		return nil, e.failure
	}
	if value := message.GetSession(); value != nil {
		if value.SessionId == "" || ctx.Done() == nil {
			return nil, controlstream.ErrMessage
		}
		e.session = &session{ctx: ctx, id: value.SessionId, gates: make(map[string]*epochGate), sent: make(map[string]bool), inspection: slices.Contains(value.Capabilities, controlstream.CommandInspectionCapability)}
		e.session.router = slices.Contains(value.Capabilities, controlstream.RouterCapability)
		e.session.replicating = slices.Contains(value.Capabilities, replication.Capability)
		select {
		case e.wake <- struct{}{}:
		default:
		}
		return nil, nil
	}
	current := e.session
	if current == nil || current.ctx.Err() != nil || ctx.Done() != current.ctx.Done() {
		return nil, operation.ErrStaleOwner
	}
	defer func() {
		select {
		case e.wake <- struct{}{}:
		default:
		}
	}()
	switch value := message.Message.(type) {
	case *agentv1.ControllerMessage_WriterLease:
		lease := value.WriterLease
		if !current.replicating || lease == nil || lease.SessionId != current.id || controlstream.CheckControllerMessage(message, "", "") != nil || e.options.Replication == nil {
			return nil, controlstream.ErrMessage
		}
		assignment, ok := e.options.Replication.Lookup(lease.AppId)
		if !ok || assignment.Role != replication.RoleActive || assignment.Epoch != lease.Epoch {
			return nil, operation.ErrStaleOwner
		}
		if e.options.BootTime == nil {
			return nil, controlstream.ErrMessage
		}
		now, err := e.options.BootTime()
		if err != nil {
			return nil, err
		}
		if e.leases == nil {
			e.leases = make(map[string]writerLease)
		}
		expires := time.Duration(lease.ExpiresAtBootNanos)
		if expires <= now {
			return nil, nil
		}
		if expires-now > time.Duration(lease.Seconds)*time.Second {
			return nil, controlstream.ErrMessage
		}
		previous := e.leases[lease.AppId]
		if previous.epoch != lease.Epoch || expires > previous.expires {
			e.leases[lease.AppId] = writerLease{epoch: lease.Epoch, expires: expires}
		}
		return nil, nil
	case *agentv1.ControllerMessage_Routes:
		table := value.Routes
		if !current.router || table == nil || table.SessionId != current.id || controlstream.CheckControllerMessage(message, "", "") != nil {
			return nil, controlstream.ErrMessage
		}
		ack := &agentv1.RouteAck{SessionId: current.id, Revision: table.Revision}
		switch {
		case e.options.Router == nil:
			ack.FailureCode = "router_unavailable"
		case current.routes != nil && table.Revision < current.routes.Revision:
			ack.FailureCode = "stale_route_revision"
		case current.routes != nil && table.Revision == current.routes.Revision:
			if !proto.Equal(table, current.routes) {
				ack.FailureCode = "route_revision_conflict"
			}
		default:
			if err := e.options.Router.Apply(ctx, table.Routes); err != nil {
				switch {
				case errors.Is(err, router.ErrPortConflict):
					ack.FailureCode = "route_port_conflict"
				case errors.Is(err, router.ErrConfiguration):
					ack.FailureCode = "invalid_route_configuration"
				default:
					ack.FailureCode = "router_closed"
				}
			} else {
				current.routes = proto.Clone(table).(*agentv1.RouteTable)
			}
		}
		return &agentv1.AgentMessage{Message: &agentv1.AgentMessage_RouteAck{RouteAck: ack}}, nil
	case *agentv1.ControllerMessage_Replication:
		table := value.Replication
		if !current.replicating || table == nil || table.SessionId != current.id || controlstream.CheckControllerMessage(message, "", "") != nil {
			return nil, controlstream.ErrMessage
		}
		ack := &agentv1.ReplicationAck{SessionId: current.id, Revision: table.Revision}
		switch {
		case e.options.Replication == nil:
			ack.FailureCode = "replication_unavailable"
		case current.replication != nil && table.Revision < current.replication.Revision:
			ack.FailureCode = "stale_replication_revision"
		case current.replication != nil && table.Revision == current.replication.Revision:
			if !proto.Equal(table, current.replication) {
				ack.FailureCode = "replication_revision_conflict"
			}
		default:
			if err := e.recordReplicationEpochs(table.Apps); err != nil {
				if errors.Is(err, operation.ErrStaleOwner) {
					ack.FailureCode = "stale_replication_epoch"
				} else {
					ack.FailureCode = "replication_unavailable"
				}
				break
			}
			for appID, lease := range e.leases {
				index := slices.IndexFunc(table.Apps, func(a *agentv1.AppReplication) bool {
					return a.AppId == appID && a.Role == replication.RoleActive && a.Epoch == lease.epoch
				})
				if index < 0 {
					delete(e.leases, appID)
				}
			}
			e.options.Replication.Set(table.Apps)
			current.replication = proto.Clone(table).(*agentv1.ReplicationTable)
		}
		return &agentv1.AgentMessage{Message: &agentv1.AgentMessage_ReplicationAck{ReplicationAck: ack}}, nil
	case *agentv1.ControllerMessage_PrepareEpoch:
		if value.PrepareEpoch == nil || value.PrepareEpoch.Epoch == nil {
			return nil, controlstream.ErrMessage
		}
		epoch := value.PrepareEpoch.Epoch
		old := current.gates[epoch.AppId]
		if old != nil && proto.Equal(old.epoch, epoch) && !old.active {
			return &agentv1.AgentMessage{Message: &agentv1.AgentMessage_BarrierResult{BarrierResult: proto.Clone(old.report).(*agentv1.BarrierResult)}}, nil
		}
		pending := 0
		for _, gate := range current.gates {
			if !gate.active && !gate.ready {
				pending++
			}
		}
		if pending >= e.options.QueueSize {
			return nil, journal.ErrBusy
		}
		if err := e.journal.Prepare(epoch); err != nil {
			return nil, err
		}
		if err := e.journal.CloseAdmission(epoch); err != nil {
			return nil, err
		}
		report, err := e.journal.Barrier(epoch)
		if err != nil {
			return nil, err
		}
		report.Drained, report.FailureCode = false, "inspection_pending"
		if err := e.journal.RecordBarrier(report); err != nil {
			return nil, err
		}
		current.gates[epoch.AppId] = &epochGate{epoch: proto.Clone(epoch).(*agentv1.Epoch), report: report}
		return &agentv1.AgentMessage{Message: &agentv1.AgentMessage_BarrierResult{BarrierResult: proto.Clone(report).(*agentv1.BarrierResult)}}, nil
	case *agentv1.ControllerMessage_ActivateEpoch:
		if value.ActivateEpoch == nil || value.ActivateEpoch.Epoch == nil {
			return nil, controlstream.ErrMessage
		}
		epoch := value.ActivateEpoch.Epoch
		gate := current.gates[epoch.AppId]
		if gate == nil || !gate.ready || !proto.Equal(gate.epoch, epoch) || e.active[epoch.AppId] {
			return nil, operation.ErrBarrierPending
		}
		if err := e.journal.Activate(epoch, true); err != nil {
			return nil, err
		}
		gate.active = true
		return nil, nil
	case *agentv1.ControllerMessage_Command:
		command := value.Command
		if command == nil || command.Epoch == nil {
			return nil, controlstream.ErrMessage
		}
		attemptID, err := uuid.Parse(message.DeliveryAttemptId)
		if err != nil || attemptID == uuid.Nil || attemptID.String() != message.DeliveryAttemptId {
			return nil, controlstream.ErrMessage
		}
		ack := &agentv1.AdmissionAck{DeliveryAttemptId: message.DeliveryAttemptId, Epoch: proto.Clone(command.Epoch).(*agentv1.Epoch), CommandId: command.CommandId, InputHash: bytes.Clone(command.InputHash), State: "rejected"}
		gate := current.gates[command.Epoch.AppId]
		if gate == nil || !gate.active || !proto.Equal(gate.epoch, command.Epoch) {
			ack.ErrorCode = "stale_owner"
		} else {
			record, err := e.journal.Admit(command)
			if err != nil {
				ack.ErrorCode = failureCode(err)
			} else {
				ack.State = string(record.State)
			}
		}
		return &agentv1.AgentMessage{Message: &agentv1.AgentMessage_AdmissionAck{AdmissionAck: ack}}, nil
	case *agentv1.ControllerMessage_ResultAck:
		if err := e.journal.Acknowledge(value.ResultAck); err != nil {
			return nil, err
		}
		delete(current.sent, value.ResultAck.CommandId)
		return nil, nil
	case *agentv1.ControllerMessage_Cancel:
		if value.Cancel == nil || value.Cancel.Epoch == nil {
			return nil, controlstream.ErrMessage
		}
		request := value.Cancel
		record, err := e.journal.Get(request.CommandId)
		if err != nil {
			return nil, err
		}
		gate := current.gates[request.Epoch.AppId]
		if gate == nil || !proto.Equal(gate.epoch, request.Epoch) || !proto.Equal(record.Epoch, request.Epoch) {
			return nil, operation.ErrStaleOwner
		}
		state := "not_cancellable"
		if record.State == journal.Queued {
			result := &agentv1.TerminalResult{Epoch: record.Epoch, CommandId: record.CommandID, InputHash: record.InputHash, Sequence: 1, State: "cancelled", Effects: record.Effects}
			if err := e.journal.Complete(result); err != nil {
				return nil, err
			}
			state = "cancelled"
		} else if record.Result != nil && record.Result.State == "cancelled" {
			state = "cancelled"
		}
		return &agentv1.AgentMessage{Message: &agentv1.AgentMessage_CancellationResult{CancellationResult: &agentv1.CancellationResult{Epoch: record.Epoch, CommandId: record.CommandID, State: state, Effects: record.Effects}}}, nil
	case *agentv1.ControllerMessage_Secret:
		// Held in session memory and never written down. There is no reply: the
		// command that needs it is what proves it arrived.
		if err := e.acceptSecret(current, value.Secret); err != nil {
			return nil, controlstream.ErrMessage
		}
		return nil, nil
	case *agentv1.ControllerMessage_CapacityRequest:
		return e.capacity(ctx, current, value.CapacityRequest)
	case *agentv1.ControllerMessage_CommandInspection:
		return e.inspectCommand(ctx, current, value.CommandInspection)
	case *agentv1.ControllerMessage_Inventory:
		if value.Inventory == nil || value.Inventory.PageSize == 0 || value.Inventory.PageSize > 128 {
			return nil, controlstream.ErrMessage
		}
		return e.inventory(ctx, current, value.Inventory)
	default:
		return nil, controlstream.ErrMessage
	}
}

// inventory runs one bounded read outside the gate lock and binds its page to that session.
func (e *Executor) inventory(ctx context.Context, current *session, request *agentv1.InventoryRequest) (*agentv1.AgentMessage, error) {
	if e.inventoryBusy {
		return nil, journal.ErrBusy
	}
	offset := 0
	if request.Cursor != "" {
		id, index, ok := strings.Cut(request.Cursor, ":")
		parsed, err := strconv.Atoi(index)
		if !ok || id != current.inventoryID || err != nil || parsed < 0 || parsed > len(current.inventory) {
			return nil, controlstream.ErrMessage
		}
		offset = parsed
	} else {
		e.inventoryBusy = true
		e.mu.Unlock()
		read, cancel := context.WithTimeout(ctx, e.options.InspectionTimeout)
		resources, err := e.runtime.Inventory(read)
		var found []runtime.Drift
		if err == nil {
			// The drift scan is a separate read because Inventory refuses a
			// malformed owned identity outright, and a report of what is wrong on
			// the host is exactly what an operator needs when it does.
			found, err = e.runtime.Drift(read)
		}
		cancel()
		e.mu.Lock()
		e.inventoryBusy = false
		if err != nil {
			return nil, err
		}
		if e.session != current || current.ctx.Err() != nil {
			return nil, operation.ErrStaleOwner
		}
		current.inventoryID, current.inventory = uuid.NewString(), resources
		current.truncated = len(found) > maxDriftReported
		if current.truncated {
			found = found[:maxDriftReported]
		}
		current.drift = found
	}
	end := min(offset+int(request.PageSize), len(current.inventory))
	page := &agentv1.InventoryPage{}
	for _, resource := range current.inventory[offset:end] {
		page.Resources = append(page.Resources, &agentv1.Observation{ResourceId: resource.ResourceID.String(), Lifecycle: resource.State, ObservedAt: timestamppb.Now(), Health: resource.Health, Ports: resource.Ports})
	}
	if offset == 0 {
		page.DriftTruncated = current.truncated
		for _, item := range current.drift {
			page.Drift = append(page.Drift, driftItem(item))
		}
	}
	if end < len(current.inventory) {
		page.NextCursor = current.inventoryID + ":" + strconv.Itoa(end)
	}
	return &agentv1.AgentMessage{Message: &agentv1.AgentMessage_Inventory{Inventory: page}}, nil
}

func failureCode(err error) string {
	switch {
	case errors.Is(err, errWriterLease):
		return "writer_lease_expired"
	case errors.Is(err, replica.ErrNoReplica):
		return "no_replica"
	case errors.Is(err, runtime.ErrStorageLost):
		return "storage_lost"
	case errors.Is(err, journal.ErrBusy), errors.Is(err, controlstream.ErrBusy):
		return "agent_busy"
	case errors.Is(err, operation.ErrStaleOwner):
		return "stale_owner"
	case errors.Is(err, operation.ErrCommandConflict):
		return "command_conflict"
	case errors.Is(err, ErrSecretUnavailable):
		return "secret_unavailable"
	case errors.Is(err, journal.ErrRecoveryRequired):
		return "recovery_required"
	case errors.Is(err, capacity.ErrCapacityUnavailable):
		return "capacity_unavailable"
	case errors.Is(err, capacity.ErrPortOccupied):
		return "host_port_occupied"
	default:
		return "runtime_failed"
	}
}

// maxDriftReported bounds what one scan reports. A host with more drift than this
// needs an operator on it, not a longer message.
const maxDriftReported = 256

// driftItem carries one report to the controller. Identities travel as text and an
// absent one stays absent, because a nil identity is not an observation.
func driftItem(item runtime.Drift) *agentv1.DriftItem {
	reported := &agentv1.DriftItem{
		Kind: item.Kind, NativeId: item.NativeID, Name: item.Name,
		State: item.State, Class: string(item.Class), Detail: item.Detail,
	}
	if item.AppID != uuid.Nil {
		reported.AppId = item.AppID.String()
	}
	if item.ResourceID != uuid.Nil {
		reported.ResourceId = item.ResourceID.String()
	}
	return reported
}
