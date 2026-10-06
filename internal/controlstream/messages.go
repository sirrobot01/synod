package controlstream

import (
	"errors"
	"slices"
	"strings"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/destination"
	"github.com/sirrobot01/synod/internal/inventory"
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
	default:
		return ErrMessage
	}
}

// CheckControllerMessage validates the wire shape of one controller message.
func CheckControllerMessage(message *agentv1.ControllerMessage) error {
	switch value := message.GetMessage().(type) {
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
	default:
		return ErrMessage
	}
}
