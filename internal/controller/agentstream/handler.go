// Package agentstream records what authenticated agents report.
package agentstream

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"time"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/backup"
	"github.com/sirrobot01/synod/internal/controlstream"
	"github.com/sirrobot01/synod/internal/operation"
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
	// Logger reports each operation result once, and terminal events that the store refused. It can be nil.
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
		done := r.finished(ctx, event.ID)
		if err := r.Store.RecordProofEvent(ctx, peer.NodeID, sessionID, event); err != nil {
			if !r.keepFirst(peer, event.ID, event.Status, err) {
				return err
			}
		} else if !done {
			r.logResult(ctx, peer, event.ID)
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
		done := r.finished(ctx, event.ID)
		if err := r.Store.RecordRestoreEvent(ctx, peer.NodeID, sessionID, event); err != nil {
			if !r.keepFirst(peer, event.ID, event.Status, err) {
				return err
			}
		} else if !done {
			r.logResult(ctx, peer, event.ID)
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
		done := r.finished(ctx, event.ID)
		if err := r.Store.RecordBackupEvent(ctx, peer.NodeID, sessionID, event); err != nil {
			if !r.keepFirst(peer, event.ID, event.Status, err) {
				return err
			}
		} else if !done {
			r.logResult(ctx, peer, event.ID)
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
		r.Logger.Warn("agent sent a second result for a finished operation; the first result stays", "device", peer.NodeID, "operation", id, "status", status)
	}
	return true
}

// finished reports whether the operation already has its result, so a resent result is logged once.
func (r Receiver) finished(ctx context.Context, id uuid.UUID) bool {
	op, err := r.Store.GetOperation(ctx, id)
	return err == nil && op.Status.Terminal()
}

var resultName = map[string]string{"backup": "backup", "restore": "restore", "restore_drill": "restore test", "integrity_check": "repository check", "repository_maintenance": "repository maintenance"}

// logResult writes one line when an operation gets its result: what ran, on which device, and how it ended.
// The agent's own log holds the cause of a failure.
func (r Receiver) logResult(ctx context.Context, peer controlstream.Peer, id uuid.UUID) {
	if r.Logger == nil {
		return
	}
	op, err := r.Store.GetOperation(ctx, id)
	if err != nil || !op.Status.Terminal() {
		return
	}
	device := peer.NodeID.String()
	if node, err := r.Store.GetNode(ctx, peer.NodeID); err == nil && node.Name != "" {
		device = node.Name
	}
	attrs := []any{"device", device, "operation", id}
	if op.AppID != uuid.Nil {
		app := op.AppID.String()
		if catalog, err := r.Store.ListApps(ctx); err == nil {
			for _, item := range catalog.Apps {
				if item.ID == op.AppID && item.Definition != nil {
					app = item.Definition.Name
				}
			}
		}
		attrs = append([]any{"app", app}, attrs...)
	}
	if op.FinishedAt != nil {
		attrs = append(attrs, "duration", op.FinishedAt.Sub(op.CreatedAt).Round(time.Second))
	}
	name := cmp.Or(resultName[op.Kind], op.Kind)
	if op.Status == operation.Succeeded {
		r.Logger.Info(name+" succeeded", attrs...)
		return
	}
	r.Logger.Warn(name+" failed; the device log has the cause", append(attrs, "code", op.FailureCode)...)
}
