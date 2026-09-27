package executor

import (
	"bytes"
	"context"

	"github.com/sirrobot01/synod/internal/agent/journal"
	"github.com/sirrobot01/synod/internal/controlstream"
	"github.com/sirrobot01/synod/internal/operation"
	"github.com/sirrobot01/synod/internal/runtime"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/proto"
)

// inspectCommand holds the cluster slot during a bounded read, without admitting work.
// Handle owns the gate lock on entry and exit.
func (e *Executor) inspectCommand(ctx context.Context, current *session, request *agentv1.CommandInspectionRequest) (*agentv1.AgentMessage, error) {
	if !current.inspection {
		return nil, operation.ErrIncompatibleProtocol
	}
	identity := e.journal.Identity()
	if err := controlstream.CheckCommandInspectionRequest(request, identity.InstallationID, identity.NodeID); err != nil {
		return nil, err
	}
	command := request.Command
	cluster := command.Epoch.ClusterId
	gate := current.gates[cluster]
	if gate == nil || !gate.ready || !proto.Equal(gate.epoch, command.Epoch) {
		return nil, operation.ErrBarrierPending
	}
	if e.inspectionBusy || e.active[cluster] {
		return nil, journal.ErrBusy
	}
	e.inspectionBusy, e.active[cluster] = true, true
	read, cancel := context.WithTimeout(ctx, e.options.InspectionTimeout)
	e.mu.Unlock()
	var result runtime.Result
	var err error
	if command.Input.GetFinalAppSnapshot() != nil || command.Input.GetRemoveAppData() != nil {
		result, err = e.recoverAppData(read, command)
	} else {
		result, err = e.runtime.Recover(read, command, request.PriorEffects)
	}
	cancel()
	e.mu.Lock()
	e.inspectionBusy = false
	delete(e.active, cluster)
	if ctx.Err() != nil || e.session != current || current.gates[cluster] != gate || e.failure != nil {
		return nil, operation.ErrStaleOwner
	}
	response := &agentv1.CommandInspectionResult{RequestId: request.RequestId, Epoch: proto.Clone(command.Epoch).(*agentv1.Epoch), CommandId: command.CommandId, InputHash: bytes.Clone(command.InputHash), Effects: result.Effects}
	if err != nil {
		response.FailureCode = failureCode(err)
	}
	if err := controlstream.CheckCommandInspectionResult(response, identity.InstallationID, identity.NodeID); err != nil {
		return nil, err
	}
	return &agentv1.AgentMessage{Message: &agentv1.AgentMessage_CommandInspection{CommandInspection: response}}, nil
}
