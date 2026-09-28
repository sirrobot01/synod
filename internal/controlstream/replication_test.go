package controlstream

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/agent/replication"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
)

func TestReplicationMessagesRequireBoundedShape(t *testing.T) {
	id := uuid.NewString()
	table := &agentv1.ReplicationTable{SessionId: id, Revision: 1, Apps: []*agentv1.AppReplication{{AppId: uuid.NewString(), Epoch: 1, Role: replication.RoleStandby, Peers: []*agentv1.ReplicaPeer{{NodeId: uuid.NewString(), Address: "192.168.0.21:8445"}}}}}
	message := &agentv1.ControllerMessage{Message: &agentv1.ControllerMessage_Replication{Replication: table}}
	if err := CheckControllerMessage(message, "", ""); err != nil {
		t.Fatal(err)
	}
	table.Apps[0].Role = "writer"
	if err := CheckControllerMessage(message, "", ""); err == nil {
		t.Fatal("invalid role crossed wire validation")
	}
	table.Apps[0].Role, table.SessionId = replication.RoleStandby, "session"
	if err := CheckControllerMessage(message, "", ""); err == nil {
		t.Fatal("invalid session accepted")
	}
	ack := &agentv1.ReplicationAck{SessionId: id, Revision: 1, FailureCode: "stale_replication_revision"}
	response := &agentv1.AgentMessage{Message: &agentv1.AgentMessage_ReplicationAck{ReplicationAck: ack}}
	if err := CheckAgentMessage(response, "", ""); err != nil {
		t.Fatal(err)
	}
	ack.FailureCode = "arbitrary error detail"
	if err := CheckAgentMessage(response, "", ""); err == nil {
		t.Fatal("unbounded ack detail accepted")
	}
	status := &agentv1.ReplicaStatus{AppId: uuid.NewString(), Role: replication.RoleActive, FailureCode: replication.FailureFreeze}
	heartbeat := &agentv1.AgentMessage{Message: &agentv1.AgentMessage_Heartbeat{Heartbeat: &agentv1.Heartbeat{Replicas: []*agentv1.ReplicaStatus{status}}}}
	if err := CheckAgentMessage(heartbeat, "", ""); err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(){func() { status.FailureCode = "disk on fire" }, func() { status.FailureCode, status.Role = "", "leader" }, func() { status.Role, status.AppId = replication.RoleActive, "app" }} {
		mutate()
		if err := CheckAgentMessage(heartbeat, "", ""); err == nil {
			t.Fatalf("invalid replica status accepted: %v", status)
		}
	}
}

func TestWriterLeaseRequiresBoundedShape(t *testing.T) {
	lease := &agentv1.WriterLease{SessionId: uuid.NewString(), AppId: uuid.NewString(), Epoch: 1, Seconds: 30, ExpiresAtBootNanos: uint64(30 * time.Second)}
	message := &agentv1.ControllerMessage{Message: &agentv1.ControllerMessage_WriterLease{WriterLease: lease}}
	if err := CheckControllerMessage(message, "", ""); err != nil {
		t.Fatal(err)
	}
	for _, seconds := range []uint32{0, 31, 600} {
		lease.Seconds = seconds
		if err := CheckControllerMessage(message, "", ""); err == nil {
			t.Fatalf("accepted %d seconds", seconds)
		}
	}
	lease.Seconds = 30
	lease.Epoch = 0
	if err := CheckControllerMessage(message, "", ""); err == nil {
		t.Fatal("accepted zero epoch")
	}
}

func TestDatabaseReportsPreserveTheControlSession(t *testing.T) {
	for _, code := range []string{"", "copying", "drifted", "disconnected", "failed", "writer_lease_expired", "primary_setup_failed", "fence_failed"} {
		t.Run(code, func(t *testing.T) {
			status := &agentv1.ReplicaStatus{AppId: uuid.NewString(), Role: replication.RoleStandby, Epoch: 1, State: "failed", FailureCode: code, LagSeconds: -1}
			message := &agentv1.AgentMessage{Message: &agentv1.AgentMessage_Heartbeat{Heartbeat: &agentv1.Heartbeat{Replicas: []*agentv1.ReplicaStatus{status}}}}
			if err := CheckAgentMessage(message, "", ""); err != nil {
				t.Fatalf("database report ends the session: %v", err)
			}
			status.FailureCode = "raw database error text"
			if err := CheckAgentMessage(message, "", ""); err == nil {
				t.Fatal("unbounded error accepted")
			}
		})
	}
}

func TestDatabaseSwitchoverHoldBinding(t *testing.T) {
	id := uuid.NewString()
	assignment := &agentv1.AppReplication{AppId: uuid.NewString(), Epoch: 1, Role: replication.RoleStandby, Database: &agentv1.DatabaseAssignment{Engine: "redis", PrimaryHost: "192.0.2.1", PrimaryPort: 6379, SwitchoverOperationId: id}}
	if err := replication.Validate([]*agentv1.AppReplication{assignment}); err != nil {
		t.Fatal(err)
	}
	assignment.Role, assignment.Database.PrimaryHost, assignment.Database.PrimaryPort = replication.RoleActive, "", 0
	if err := replication.Validate([]*agentv1.AppReplication{assignment}); err == nil {
		t.Fatal("primary accepted a standby hold")
	}
	status := &agentv1.ReplicaStatus{AppId: assignment.AppId, Role: replication.RoleStandby, Epoch: 1, State: "streaming", DatabaseHoldId: id}
	message := &agentv1.AgentMessage{Message: &agentv1.AgentMessage_Heartbeat{Heartbeat: &agentv1.Heartbeat{Replicas: []*agentv1.ReplicaStatus{status}}}}
	if err := CheckAgentMessage(message, "", ""); err != nil {
		t.Fatal(err)
	}
	status.DatabaseHoldId = "invalid"
	if err := CheckAgentMessage(message, "", ""); err == nil {
		t.Fatal("invalid hold accepted")
	}
	status.DatabaseHoldId, status.Role = id, replication.RoleActive
	if err := CheckAgentMessage(message, "", ""); err == nil {
		t.Fatal("primary reported a standby hold")
	}
}
