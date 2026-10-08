package controlstream

import (
	"encoding/json"
	"errors"
	"slices"
	"strings"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/backup"
	"github.com/sirrobot01/synod/internal/destination"
	"github.com/sirrobot01/synod/internal/inventory"
	"github.com/sirrobot01/synod/internal/proof"
	"github.com/sirrobot01/synod/internal/restore"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
)

// ProtocolVersion is the one controller-agent protocol version this build speaks.
const ProtocolVersion uint32 = 1

// ErrIncompatibleProtocol reports a Hello that this controller cannot serve.
var ErrIncompatibleProtocol = errors.New("incompatible_protocol")

func canonicalID(id string) bool {
	parsed, err := uuid.Parse(id)
	return err == nil && parsed != uuid.Nil && parsed.String() == id
}

// visible reports text of 1 to limit bytes with no control or space characters.
func visible(value string, limit int) bool {
	if len(value) < 1 || len(value) > limit {
		return false
	}
	return !strings.ContainsFunc(value, func(r rune) bool { return r < '!' || r > '~' })
}

// NegotiateHello checks the agent identity and protocol and returns the shared capabilities.
func NegotiateHello(hello *agentv1.Hello, supported []string) ([]string, error) {
	if hello == nil || !canonicalID(hello.InstallationId) || !canonicalID(hello.NodeId) || hello.Protocol == nil {
		return nil, ErrIncompatibleProtocol
	}
	if hello.Protocol.Minimum > ProtocolVersion || hello.Protocol.Maximum < ProtocolVersion || hello.Protocol.Minimum > hello.Protocol.Maximum {
		return nil, ErrIncompatibleProtocol
	}
	if hello.DestinationAddress != "" && !destination.ValidAddress(hello.DestinationAddress, true) {
		return nil, ErrIncompatibleProtocol
	}
	if len(hello.Capabilities) > 64 || len(hello.RuntimeVersion) > 256 || len(hello.Hostname) > 253 || len(hello.AgentVersion) > 128 {
		return nil, ErrIncompatibleProtocol
	}
	switch hello.Architecture {
	case "amd64", "arm64":
	default:
		return nil, ErrIncompatibleProtocol
	}
	capabilities := make([]string, 0, len(hello.Capabilities))
	for _, capability := range hello.Capabilities {
		if !visible(capability, 128) {
			return nil, ErrIncompatibleProtocol
		}
		if slices.Contains(supported, capability) && !slices.Contains(capabilities, capability) {
			capabilities = append(capabilities, capability)
		}
	}
	slices.Sort(capabilities)
	return capabilities, nil
}

// CheckAgentMessage validates the wire shape of one agent message after Hello.
func CheckAgentMessage(message *agentv1.AgentMessage) error {
	switch value := message.GetMessage().(type) {
	case *agentv1.AgentMessage_DestinationHealth:
		if destination.ValidateHealth(value.DestinationHealth) != nil {
			return ErrMessage
		}
		return nil
	case *agentv1.AgentMessage_ProofEvent:
		if value.ProofEvent == nil || len(value.ProofEvent.EventJson) > 64<<10 {
			return ErrMessage
		}
		var event proof.Event
		if json.Unmarshal(value.ProofEvent.EventJson, &event) != nil || event.ID == uuid.Nil || !slices.Contains([]string{"queued", "running", "waiting", "succeeded", "failed"}, event.Status) {
			return ErrMessage
		}
		return nil
	case *agentv1.AgentMessage_RestoreEvent:
		if value.RestoreEvent == nil || len(value.RestoreEvent.EventJson) > 64<<10 {
			return ErrMessage
		}
		var event restore.Event
		if json.Unmarshal(value.RestoreEvent.EventJson, &event) != nil || event.ID == uuid.Nil || !slices.Contains([]string{"queued", "running", "waiting", "succeeded", "failed"}, event.Status) {
			return ErrMessage
		}
		return nil
	case *agentv1.AgentMessage_BackupEvent:
		if value.BackupEvent == nil || len(value.BackupEvent.EventJson) > 64<<10 {
			return ErrMessage
		}
		var event backup.Event
		if json.Unmarshal(value.BackupEvent.EventJson, &event) != nil || event.ID == uuid.Nil || !slices.Contains([]string{"queued", "running", "waiting", "succeeded", "failed"}, event.Status) {
			return ErrMessage
		}
		return nil
	case *agentv1.AgentMessage_DestinationAck:
		if value.DestinationAck == nil || value.DestinationAck.Revision < 1 {
			return ErrMessage
		}
		return nil
	case *agentv1.AgentMessage_Inventory:
		if inventory.Validate(value.Inventory) != nil {
			return ErrMessage
		}
		return nil
	case *agentv1.AgentMessage_Heartbeat:
		if value.Heartbeat == nil || value.Heartbeat.Sequence == 0 || value.Heartbeat.ObservedAt.CheckValid() != nil {
			return ErrMessage
		}
		return nil
	case *agentv1.AgentMessage_CertificateRequest:
		if value.CertificateRequest == nil || len(value.CertificateRequest.CsrDer) == 0 || len(value.CertificateRequest.CsrDer) > 64<<10 {
			return ErrMessage
		}
		return nil
	default:
		return ErrMessage
	}
}

// CheckControllerMessage validates the wire shape of one controller message.
func CheckControllerMessage(message *agentv1.ControllerMessage) error {
	switch value := message.GetMessage().(type) {
	case *agentv1.ControllerMessage_FileRequest:
		if value.FileRequest == nil || len(value.FileRequest.RequestJson) > 8192 {
			return ErrMessage
		}
		var request restore.FileRequest
		if json.Unmarshal(value.FileRequest.RequestJson, &request) != nil || request.Validate() != nil {
			return ErrMessage
		}
		return nil
	case *agentv1.ControllerMessage_ProofJob:
		if value.ProofJob == nil || len(value.ProofJob.RequestJson) > 8192 {
			return ErrMessage
		}
		var request proof.Request
		if json.Unmarshal(value.ProofJob.RequestJson, &request) != nil || request.Validate() != nil {
			return ErrMessage
		}
		return nil
	case *agentv1.ControllerMessage_RestoreJob:
		if value.RestoreJob == nil || len(value.RestoreJob.RequestJson) > 8192 {
			return ErrMessage
		}
		var request restore.Request
		if json.Unmarshal(value.RestoreJob.RequestJson, &request) != nil || request.Validate() != nil {
			return ErrMessage
		}
		return nil
	case *agentv1.ControllerMessage_BackupJob:
		if value.BackupJob == nil || len(value.BackupJob.RequestJson) > 8192 {
			return ErrMessage
		}
		var request backup.Request
		if json.Unmarshal(value.BackupJob.RequestJson, &request) != nil || request.Validate() != nil {
			return ErrMessage
		}
		return nil
	case *agentv1.ControllerMessage_OperationAck:
		if value.OperationAck == nil || !canonicalID(value.OperationAck.OperationId) {
			return ErrMessage
		}
		return nil
	case *agentv1.ControllerMessage_DestinationPlan:
		if value.DestinationPlan == nil || len(value.DestinationPlan.SettingsJson) == 0 || len(value.DestinationPlan.SettingsJson) > destination.MaxPlanBytes {
			return ErrMessage
		}
		return nil
	case *agentv1.ControllerMessage_Session:
		if value.Session == nil || !canonicalID(value.Session.SessionId) {
			return ErrMessage
		}
		return nil
	case *agentv1.ControllerMessage_Error:
		if value.Error == nil || !visible(value.Error.Code, 64) || len(value.Error.Message) > 1024 {
			return ErrMessage
		}
		return nil
	case *agentv1.ControllerMessage_CertificateIssued:
		if value.CertificateIssued == nil || !canonicalID(value.CertificateIssued.CertificateId) || len(value.CertificateIssued.CertificatePem) == 0 || len(value.CertificateIssued.CertificatePem) > 64<<10 || len(value.CertificateIssued.CaCertificatePem) == 0 || len(value.CertificateIssued.CaCertificatePem) > 64<<10 {
			return ErrMessage
		}
		return nil
	default:
		return ErrMessage
	}
}
