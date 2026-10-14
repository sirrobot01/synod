// Package agentstream records what authenticated agents report.
package agentstream

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/backup"
	"github.com/sirrobot01/synod/internal/controlstream"
	"github.com/sirrobot01/synod/internal/pki"
	"github.com/sirrobot01/synod/internal/proof"
	"github.com/sirrobot01/synod/internal/restore"
	"github.com/sirrobot01/synod/internal/store"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/proto"
)

// ErrUnsupportedMessage reports an agent message that this receiver cannot record.
var ErrUnsupportedMessage = errors.New("unsupported agent message")

// Receiver records agent sessions and heartbeats in the metadata store.
type Receiver struct {
	Store *store.Store
	Send  func(uuid.UUID, string, *agentv1.ControllerMessage) error
	// Authority issues renewed agent certificates. Without it, renewal requests fail.
	Authority *pki.CA
	// Logger reports terminal events that the store refused. It can be nil.
	Logger *slog.Logger
}

// Handle records one message. It returns nil only after the record commits.
func (r Receiver) Handle(ctx context.Context, peer controlstream.Peer, message *agentv1.AgentMessage) error {
	if r.Store == nil || message == nil {
		return fmt.Errorf("%w: the receiver has no store or the message is absent", ErrUnsupportedMessage)
	}
	sessionID, err := uuid.Parse(peer.SessionID)
	if err != nil || sessionID == uuid.Nil {
		return fmt.Errorf("%w: node %s carries an invalid session %q", ErrUnsupportedMessage, peer.NodeID, peer.SessionID)
	}
	switch value := message.Message.(type) {
	case *agentv1.AgentMessage_DestinationHealth:
		return r.Store.RecordDestinationHealth(ctx, peer.NodeID, sessionID, value.DestinationHealth)
	case *agentv1.AgentMessage_ProofEvent:
		if value.ProofEvent == nil || len(value.ProofEvent.EventJson) > 64<<10 {
			return ErrUnsupportedMessage
		}
		var event proof.Event
		if json.Unmarshal(value.ProofEvent.EventJson, &event) != nil {
			return ErrUnsupportedMessage
		}
		if err := r.Store.RecordProofEvent(ctx, peer.NodeID, sessionID, event); err != nil && !r.keepFirst(peer, event.ID, event.Status, err) {
			return err
		}
		if (event.Status == "succeeded" || event.Status == "failed") && r.Send != nil {
			return r.Send(peer.NodeID, peer.SessionID, &agentv1.ControllerMessage{Message: &agentv1.ControllerMessage_OperationAck{OperationAck: &agentv1.OperationAck{OperationId: event.ID.String()}}})
		}
		return nil
	case *agentv1.AgentMessage_RestoreEvent:
		if value.RestoreEvent == nil || len(value.RestoreEvent.EventJson) > 64<<10 {
			return ErrUnsupportedMessage
		}
		var event restore.Event
		if json.Unmarshal(value.RestoreEvent.EventJson, &event) != nil {
			return ErrUnsupportedMessage
		}
		if err := r.Store.RecordRestoreEvent(ctx, peer.NodeID, sessionID, event); err != nil && !r.keepFirst(peer, event.ID, event.Status, err) {
			return err
		}
		if (event.Status == "succeeded" || event.Status == "failed") && r.Send != nil {
			return r.Send(peer.NodeID, peer.SessionID, &agentv1.ControllerMessage{Message: &agentv1.ControllerMessage_OperationAck{OperationAck: &agentv1.OperationAck{OperationId: event.ID.String()}}})
		}
		return nil
	case *agentv1.AgentMessage_Hello:
		hello := value.Hello
		if hello != nil && hello.DestinationAddress != "" {
			hello = proto.Clone(hello).(*agentv1.Hello)
			host, port, err := net.SplitHostPort(hello.DestinationAddress)
			if err != nil {
				return ErrUnsupportedMessage
			}
			if host == "" || (net.ParseIP(host) != nil && net.ParseIP(host).IsUnspecified()) {
				host, _, err = net.SplitHostPort(peer.RemoteAddress)
				if err != nil {
					return ErrUnsupportedMessage
				}
			}
			hello.DestinationAddress = net.JoinHostPort(host, port)
		}
		return r.Store.RecordAgentSession(ctx, peer.NodeID, sessionID, hello)
	case *agentv1.AgentMessage_BackupEvent:
		if value.BackupEvent == nil || len(value.BackupEvent.EventJson) > 64<<10 {
			return ErrUnsupportedMessage
		}
		var event backup.Event
		if json.Unmarshal(value.BackupEvent.EventJson, &event) != nil {
			return ErrUnsupportedMessage
		}
		if err := r.Store.RecordBackupEvent(ctx, peer.NodeID, sessionID, event); err != nil && !r.keepFirst(peer, event.ID, event.Status, err) {
			return err
		}
		if (event.Status == "succeeded" || event.Status == "failed") && r.Send != nil {
			return r.Send(peer.NodeID, peer.SessionID, &agentv1.ControllerMessage{Message: &agentv1.ControllerMessage_OperationAck{OperationAck: &agentv1.OperationAck{OperationId: event.ID.String()}}})
		}
		return nil
	case *agentv1.AgentMessage_DestinationAck:
		if value.DestinationAck == nil {
			return ErrUnsupportedMessage
		}
		return r.Store.AcknowledgeDestinations(ctx, peer.NodeID, sessionID, value.DestinationAck.Revision)
	case *agentv1.AgentMessage_Inventory:
		return r.Store.RecordInventory(ctx, peer.NodeID, sessionID, value.Inventory)
	case *agentv1.AgentMessage_Heartbeat:
		return r.Store.RecordAgentHeartbeat(ctx, peer.NodeID, sessionID, value.Heartbeat)
	case *agentv1.AgentMessage_CertificateRequest:
		if value.CertificateRequest == nil || r.Send == nil {
			return ErrUnsupportedMessage
		}
		issued, err := r.Store.RenewAgentCertificate(ctx, peer.NodeID, sessionID, value.CertificateRequest.CsrDer, r.Authority)
		if err != nil {
			return err
		}
		return r.Send(peer.NodeID, peer.SessionID, &agentv1.ControllerMessage{Message: &agentv1.ControllerMessage_CertificateIssued{CertificateIssued: &agentv1.CertificateIssued{CertificateId: issued.CertificateID.String(), CertificatePem: issued.CertificatePEM, CaCertificatePem: issued.CACertificatePEM}}})
	default:
		return fmt.Errorf("%w: node %s sent %T", ErrUnsupportedMessage, peer.NodeID, message.Message)
	}
}

// keepFirst reports whether a refused event is a second terminal result for an operation
// that already has one. The store keeps the first result. The caller acknowledges the event,
// so the agent drops it. Otherwise the agent sends it again on each connection, and its later
// messages wait behind it. An agent that lost its journal can run a scheduled job again.
func (r Receiver) keepFirst(peer controlstream.Peer, id uuid.UUID, status string, err error) bool {
	var refused *store.Error
	if !errors.As(err, &refused) || refused.Code != store.OperationConflict || (status != "succeeded" && status != "failed") {
		return false
	}
	if r.Logger != nil {
		r.Logger.Warn("agent result conflicts with the saved result; the saved result stays", "node_id", peer.NodeID, "operation_id", id, "status", status)
	}
	return true
}
