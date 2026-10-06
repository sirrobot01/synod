// Package agentstream records what authenticated agents report.
package agentstream

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/backup"
	"github.com/sirrobot01/synod/internal/controlstream"
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
		if err := r.Store.RecordBackupEvent(ctx, peer.NodeID, sessionID, event); err != nil {
			return err
		}
		if (event.Status == "succeeded" || event.Status == "failed") && r.Send != nil {
			return r.Send(peer.NodeID, peer.SessionID, &agentv1.ControllerMessage{Message: &agentv1.ControllerMessage_BackupAck{BackupAck: &agentv1.BackupAck{OperationId: event.ID.String()}}})
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
	default:
		return fmt.Errorf("%w: node %s sent %T", ErrUnsupportedMessage, peer.NodeID, message.Message)
	}
}
