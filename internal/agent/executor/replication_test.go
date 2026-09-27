package executor

import (
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/agent/replication"
	"github.com/sirrobot01/synod/internal/controlstream"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
)

func TestReplicationTableBindsSessionAndRevision(t *testing.T) {
	table := &replication.Table{}
	e := &Executor{options: Options{Replication: table}}
	app := uuid.NewString()
	standby := &agentv1.AppReplication{AppId: app, Epoch: 1, Role: replication.RoleStandby, Peers: []*agentv1.ReplicaPeer{{NodeId: uuid.NewString(), Address: "192.168.0.21:8445"}}}
	withoutCapability, _ := localSession(t, e)
	message := &agentv1.ControllerMessage{Message: &agentv1.ControllerMessage_Replication{Replication: &agentv1.ReplicationTable{SessionId: e.session.id, Revision: 1, Apps: []*agentv1.AppReplication{standby}}}}
	if _, err := e.Handle(withoutCapability, message); !errors.Is(err, controlstream.ErrMessage) {
		t.Fatalf("table accepted without the capability: %v", err)
	}
	ctx, _ := localSession(t, e, replication.Capability)
	sent := &agentv1.ReplicationTable{SessionId: e.session.id, Revision: 2, Apps: []*agentv1.AppReplication{standby}}
	message = &agentv1.ControllerMessage{Message: &agentv1.ControllerMessage_Replication{Replication: sent}}
	for range 2 {
		ack, err := e.Handle(ctx, message)
		if err != nil || ack.GetReplicationAck().FailureCode != "" {
			t.Fatalf("apply/replay: %v %v", ack, err)
		}
	}
	if got, ok := table.Lookup(app); !ok || got.Role != replication.RoleStandby {
		t.Fatalf("assignment: %v %v", got, ok)
	}
	sent.Revision = 1
	if ack, err := e.Handle(ctx, message); err != nil || ack.GetReplicationAck().FailureCode != "stale_replication_revision" {
		t.Fatalf("stale revision: %v %v", ack, err)
	}
	sent.Revision, sent.Apps = 2, nil
	if ack, err := e.Handle(ctx, message); err != nil || ack.GetReplicationAck().FailureCode != "replication_revision_conflict" {
		t.Fatalf("changed replay: %v %v", ack, err)
	}
	if _, ok := table.Lookup(app); !ok {
		t.Fatal("a refused table changed the assignment")
	}
	sent.Revision, sent.Apps = 3, []*agentv1.AppReplication{{AppId: app, Epoch: 0, Role: replication.RoleStandby}}
	if _, err := e.Handle(ctx, message); !errors.Is(err, controlstream.ErrMessage) {
		t.Fatalf("invalid table accepted: %v", err)
	}
	unavailable := &Executor{}
	ctx, _ = localSession(t, unavailable, replication.Capability)
	sent.SessionId, sent.Apps = unavailable.session.id, nil
	if ack, err := unavailable.Handle(ctx, message); err != nil || ack.GetReplicationAck().FailureCode != "replication_unavailable" {
		t.Fatalf("no replication table: %v %v", ack, err)
	}
}
