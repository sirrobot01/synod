package controlstream

import (
	"testing"

	"github.com/google/uuid"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
)

func TestRouteMessagesRequireBoundedShape(t *testing.T) {
	id := uuid.NewString()
	table := &agentv1.RouteTable{SessionId: id, Revision: 1, Routes: []*agentv1.AppRoute{{AppId: uuid.NewString(), Protocol: "tcp", IngressPort: 8080, Backends: []string{"127.0.0.1:32000"}}}}
	message := &agentv1.ControllerMessage{Message: &agentv1.ControllerMessage_Routes{Routes: table}}
	if err := CheckControllerMessage(message, "", ""); err != nil {
		t.Fatal(err)
	}
	table.Routes[0].Backends = []string{"malformed"}
	if err := CheckControllerMessage(message, "", ""); err == nil {
		t.Fatal("invalid backend crossed wire validation")
	}
	ack := &agentv1.RouteAck{SessionId: id, Revision: 1, FailureCode: "route_port_conflict"}
	response := &agentv1.AgentMessage{Message: &agentv1.AgentMessage_RouteAck{RouteAck: ack}}
	if err := CheckAgentMessage(response, "", ""); err != nil {
		t.Fatal(err)
	}
	ack.FailureCode = "arbitrary error detail"
	if err := CheckAgentMessage(response, "", ""); err == nil {
		t.Fatal("unbounded error detail accepted")
	}
	ack.FailureCode = ""
	ack.Revision = 0
	if err := CheckAgentMessage(response, "", ""); err == nil {
		t.Fatal("zero revision accepted")
	}
}
