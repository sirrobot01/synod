// Package agentstream connects authenticated transport to durable metadata receipt.
package agentstream

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/controlstream"
	"github.com/sirrobot01/synod/internal/operation"
	"github.com/sirrobot01/synod/internal/placement"
	"github.com/sirrobot01/synod/internal/store/postgres"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
)

// ErrUnsupportedMessage reports an agent message that this receiver cannot commit.
var ErrUnsupportedMessage = errors.New("agent message requires a controller worker")

// Receiver commits agent evidence to the metadata store.
type Receiver struct{ Store *postgres.Store }

// Handle commits security-bound evidence. It never grants authority or reconciles a plan.
func (r Receiver) Handle(ctx context.Context, peer controlstream.Peer, message *agentv1.AgentMessage) error {
	if r.Store == nil || message == nil {
		return fmt.Errorf("%w: the receiver has no store or the message is absent", ErrUnsupportedMessage)
	}
	sessionID, err := uuid.Parse(peer.SessionID)
	if err != nil {
		return fmt.Errorf("%w: parse the session of node %s: %w", ErrUnsupportedMessage, peer.NodeID, err)
	}
	if sessionID == uuid.Nil {
		return fmt.Errorf("%w: node %s carries a nil session", ErrUnsupportedMessage, peer.NodeID)
	}
	switch value := message.Message.(type) {
	case *agentv1.AgentMessage_Hello:
		if value.Hello == nil || value.Hello.BootId != peer.BootID || value.Hello.JournalGeneration != peer.JournalGeneration {
			return fmt.Errorf("%w: the Hello of node %s does not match the boot and journal generation of its session", ErrUnsupportedMessage, peer.NodeID)
		}
		return r.Store.RecordAgentSession(ctx, peer.InstallationID, peer.NodeID, sessionID, value.Hello)
	case *agentv1.AgentMessage_ReplicationAck:
		return r.Store.RecordAppReplicationAck(ctx, peer.InstallationID, peer.NodeID, sessionID, value.ReplicationAck)
	case *agentv1.AgentMessage_RouteAck:
		return r.Store.RecordAppRouteAck(ctx, peer.InstallationID, peer.NodeID, sessionID, value.RouteAck)
	case *agentv1.AgentMessage_Heartbeat:
		return r.Store.RecordAgentHeartbeat(ctx, peer.InstallationID, peer.NodeID, sessionID, value.Heartbeat)
	case *agentv1.AgentMessage_TerminalResult:
		return r.Store.RecordAgentResult(ctx, peer.InstallationID, peer.NodeID, value.TerminalResult)
	case *agentv1.AgentMessage_CapacityReport:
		report, err := capacityReport(peer, sessionID, value.CapacityReport)
		if err != nil {
			return err
		}
		return r.Store.RecordNodeCapacity(ctx, peer.InstallationID, peer.NodeID, report)
	case *agentv1.AgentMessage_Inventory:
		return r.recordDrift(ctx, peer, sessionID, value.Inventory)
	default:
		return fmt.Errorf("%w: node %s sent %T", ErrUnsupportedMessage, peer.NodeID, message.Message)
	}
}

// capacityReport reads a measured report into the placement evidence type.
//
// The installation, node, and session come from the authenticated peer and never
// from the frame. A claimed identity must not outrank the certificate that proved
// one. The boot and journal generation on the wire are the replay defence: they
// say what the agent measured against, and a value that disagrees with the peer
// means the host rebooted or recovered since the challenge was issued.
func capacityReport(peer controlstream.Peer, sessionID uuid.UUID, report *agentv1.CapacityReport) (placement.CapacityReport, error) {
	var found placement.CapacityReport
	if err := controlstream.CheckCapacityReport(report); err != nil {
		return found, err
	}
	if report.SessionId != peer.SessionID || report.BootId != peer.BootID || report.JournalGeneration != peer.JournalGeneration {
		return found, operation.ErrStaleOwner
	}
	probeID, err := uuid.Parse(report.ProbeId)
	if err != nil {
		return found, fmt.Errorf("%w: parse the probe of the capacity report from node %s: %w", ErrUnsupportedMessage, peer.NodeID, err)
	}
	bootID, err := uuid.Parse(report.BootId)
	if err != nil {
		return found, fmt.Errorf("%w: parse the boot of the capacity report from node %s: %w", ErrUnsupportedMessage, peer.NodeID, err)
	}
	generation, err := uuid.Parse(report.JournalGeneration)
	if err != nil {
		return found, fmt.Errorf("%w: parse the journal generation of the capacity report from node %s: %w", ErrUnsupportedMessage, peer.NodeID, err)
	}
	found = placement.CapacityReport{
		ProbeID: probeID, ProbeRevision: report.ProbeRevision,
		InstallationID: peer.InstallationID, NodeID: peer.NodeID, SessionID: sessionID,
		BootID: bootID, JournalGeneration: generation,
		Architecture: report.Architecture, RuntimeVersion: report.RuntimeVersion, StorageID: report.StorageId,
		Capacity: vector(report.Capacity), Used: vector(report.Used),
		ExistingPaths: report.ExistingPaths,
	}
	for _, item := range report.Reservations {
		reservationID, err := uuid.Parse(item.ReservationId)
		if err != nil {
			return placement.CapacityReport{}, fmt.Errorf("%w: parse a reservation of the capacity report from node %s: %w", ErrUnsupportedMessage, peer.NodeID, err)
		}
		resourceID, err := uuid.Parse(item.ResourceId)
		if err != nil {
			return placement.CapacityReport{}, fmt.Errorf("%w: parse the resource of reservation %s from node %s: %w", ErrUnsupportedMessage, reservationID, peer.NodeID, err)
		}
		found.Reservations = append(found.Reservations, placement.Usage{ReservationID: reservationID, ResourceID: resourceID, Used: vector(item.Used)})
	}
	return found, nil
}

func vector(v *agentv1.ResourceVector) placement.Vector {
	return placement.Vector{CPUMillis: v.GetCpuMillicores(), MemoryBytes: v.GetMemoryBytes(), DiskBytes: v.GetDiskBytes()}
}
