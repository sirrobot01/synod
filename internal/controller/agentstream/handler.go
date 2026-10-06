// Package agentstream records what authenticated agents report.
package agentstream

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/controlstream"
	"github.com/sirrobot01/synod/internal/store"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
)

// ErrUnsupportedMessage reports an agent message that this receiver cannot record.
var ErrUnsupportedMessage = errors.New("unsupported agent message")

// Receiver records agent sessions and heartbeats in the metadata store.
type Receiver struct{ Store *store.Store }

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
		return r.Store.RecordAgentSession(ctx, peer.NodeID, sessionID, value.Hello)
	case *agentv1.AgentMessage_Heartbeat:
		return r.Store.RecordAgentHeartbeat(ctx, peer.NodeID, sessionID, value.Heartbeat)
	default:
		return fmt.Errorf("%w: node %s sent %T", ErrUnsupportedMessage, peer.NodeID, message.Message)
	}
}
