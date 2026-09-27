package postgres

import (
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/agent/replication"
	"github.com/sirrobot01/synod/internal/app"
	"github.com/sirrobot01/synod/internal/operation"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestAppReplicaEvidenceRequiresAcknowledgedAssignment(t *testing.T) {
	s, installation, node, session := capacityFixture(t)
	item, err := s.CreateApp(t.Context(), app.App{InstallationID: installation, Name: "writer", Tier: app.TierSingleWriter, Spec: app.AppSpec{SchemaVersion: 1, Name: "writer", Tier: app.TierSingleWriter, Copies: 1, Replication: new(app.DefaultReplication()), Service: app.ServiceSpec{Image: "example", Mounts: []app.MountSpec{{Type: "replicated", Source: "config", Target: "/config"}}}, Router: app.RouterSpec{Protocol: "tcp", Port: 8080}}})
	if err != nil {
		t.Fatal(err)
	}
	active := uuid.New()
	if _, err := s.pool.Exec(t.Context(), `INSERT INTO nodes (id,installation_id,public_key_hash,enrolled_at) VALUES ($1,$2,$3,clock_timestamp())`, active, installation, make([]byte, 32)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.pool.Exec(t.Context(), `UPDATE apps SET writer_epoch=1,writer_node_id=$2 WHERE id=$1`, item.ID, active); err != nil {
		t.Fatal(err)
	}
	if _, err := s.pool.Exec(t.Context(), `INSERT INTO app_copies(id,installation_id,app_id,node_id,copy_index,host_port,role,observed_state,created_at,updated_at) VALUES($1,$2,$3,$4,1,0,'standby','pending',clock_timestamp(),clock_timestamp())`, uuid.New(), installation, item.ID, node); err != nil {
		t.Fatal(err)
	}
	desired := &agentv1.ReplicationTable{SessionId: session.String(), Apps: []*agentv1.AppReplication{{AppId: item.ID.String(), Epoch: 1, Role: replication.RoleStandby, Peers: []*agentv1.ReplicaPeer{{NodeId: active.String(), Address: "192.168.0.21:8445"}}}}}
	table, acknowledged, err := s.PrepareAppReplication(t.Context(), installation, node, desired)
	if err != nil || acknowledged || table.Revision != 1 {
		t.Fatalf("prepare: %v %v %v", table, acknowledged, err)
	}
	completed := time.Now().Add(-time.Minute).UTC().Truncate(time.Microsecond)
	report := &agentv1.ReplicaStatus{AppId: item.ID.String(), Role: replication.RoleStandby, Epoch: 1, Sequence: 5, CompletedAt: timestamppb.New(completed)}
	heartbeat := &agentv1.Heartbeat{Sequence: 1, ObservedAt: timestamppb.Now(), Replicas: []*agentv1.ReplicaStatus{report}}
	if err := s.RecordAgentHeartbeat(t.Context(), installation, node, session, heartbeat); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := s.pool.QueryRow(t.Context(), `SELECT count(*) FROM app_replicas`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("unacknowledged evidence: %d %v", count, err)
	}
	ack := &agentv1.ReplicationAck{SessionId: session.String(), Revision: table.Revision}
	if err := s.RecordAppReplicationAck(t.Context(), installation, node, session, ack); err != nil {
		t.Fatal(err)
	}
	same, acknowledged, err := s.PrepareAppReplication(t.Context(), installation, node, desired)
	if err != nil || !acknowledged || !proto.Equal(same, table) {
		t.Fatalf("replay: %v %v %v", same, acknowledged, err)
	}
	heartbeat.Sequence++
	if err := s.RecordAgentHeartbeat(t.Context(), installation, node, session, heartbeat); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*agentv1.ReplicaStatus){
		"unassigned app": func(r *agentv1.ReplicaStatus) { r.AppId = uuid.NewString() },
		"wrong role":     func(r *agentv1.ReplicaStatus) { r.Role = replication.RoleActive },
		"wrong epoch":    func(r *agentv1.ReplicaStatus) { r.Epoch = 2 },
		"future time": func(r *agentv1.ReplicaStatus) {
			r.Sequence = 6
			r.CompletedAt = timestamppb.New(time.Now().Add(time.Hour))
		},
		"old sequence": func(r *agentv1.ReplicaStatus) { r.Sequence = 4; r.FailureCode = replication.FailureStale },
		"changed time": func(r *agentv1.ReplicaStatus) { r.CompletedAt = timestamppb.Now() },
	} {
		t.Run(name, func(t *testing.T) {
			heartbeat.Sequence++
			changed := proto.Clone(report).(*agentv1.ReplicaStatus)
			mutate(changed)
			heartbeat.Replicas = []*agentv1.ReplicaStatus{changed}
			if err := s.RecordAgentHeartbeat(t.Context(), installation, node, session, heartbeat); err != nil {
				t.Fatal(err)
			}
			var sequence int64
			var at time.Time
			var failure string
			if err := s.pool.QueryRow(t.Context(), `SELECT sequence,completed_at,failure_code FROM app_replicas WHERE app_id=$1`, item.ID).Scan(&sequence, &at, &failure); err != nil {
				t.Fatal(err)
			}
			if sequence != 5 || !at.Equal(completed) || failure != "" {
				t.Fatalf("changed proof: %d %v %s", sequence, at, failure)
			}
		})
	}
	heartbeat.Sequence++
	heartbeat.Replicas = []*agentv1.ReplicaStatus{{AppId: item.ID.String(), Role: replication.RoleStandby, FailureCode: replication.FailureApply}}
	if err := s.RecordAgentHeartbeat(t.Context(), installation, node, session, heartbeat); err != nil {
		t.Fatal(err)
	}
	var sequence int64
	var failure string
	if err := s.pool.QueryRow(t.Context(), `SELECT sequence,failure_code FROM app_replicas WHERE app_id=$1`, item.ID).Scan(&sequence, &failure); err != nil || sequence != 5 || failure != replication.FailureApply {
		t.Fatalf("failure discarded proof: %d %q %v", sequence, failure, err)
	}
	presence, err := s.GetAgentPresence(t.Context(), installation, node)
	if err != nil {
		t.Fatal(err)
	}
	next := uuid.New()
	if err := s.RecordAgentSession(t.Context(), installation, node, next, presence.Hello); err != nil {
		t.Fatal(err)
	}
	if err := s.RecordAppReplicationAck(t.Context(), installation, node, session, ack); !errors.Is(err, ErrStaleSession) {
		t.Fatalf("stale ack: %v", err)
	}
	heartbeat.Sequence++
	if err := s.RecordAgentHeartbeat(t.Context(), installation, node, session, heartbeat); !errors.Is(err, ErrStaleSession) {
		t.Fatalf("stale report: %v", err)
	}
	desired.SessionId = next.String()
	table, acknowledged, err = s.PrepareAppReplication(t.Context(), installation, node, desired)
	if err != nil || acknowledged || table.Revision != 2 {
		t.Fatalf("new session: %v %v %v", table, acknowledged, err)
	}
	ack.SessionId = next.String()
	if err := s.RecordAppReplicationAck(t.Context(), installation, node, next, ack); err != nil {
		t.Fatal(err)
	}
	_, acknowledged, err = s.PrepareAppReplication(t.Context(), installation, node, desired)
	if err != nil || acknowledged {
		t.Fatalf("old revision acknowledged new table: %v %v", acknowledged, err)
	}
	desired.Apps[0].Epoch = 2
	if _, err := s.pool.Exec(t.Context(), `UPDATE apps SET writer_epoch=2 WHERE id=$1`, item.ID); err != nil {
		t.Fatal(err)
	}
	table, _, err = s.PrepareAppReplication(t.Context(), installation, node, desired)
	if err != nil {
		t.Fatal(err)
	}
	ack.Revision = table.Revision
	if err := s.RecordAppReplicationAck(t.Context(), installation, node, next, ack); err != nil {
		t.Fatal(err)
	}
	heartbeat.Sequence = 1
	if err := s.RecordAgentHeartbeat(t.Context(), installation, node, next, heartbeat); err != nil {
		t.Fatal(err)
	}
	var epoch int64
	var at *time.Time
	if err := s.pool.QueryRow(t.Context(), `SELECT writer_epoch,sequence,completed_at FROM app_replicas WHERE app_id=$1`, item.ID).Scan(&epoch, &sequence, &at); err != nil || epoch != 2 || sequence != 0 || at != nil {
		t.Fatalf("new epoch kept old proof: %d %d %v %v", epoch, sequence, at, err)
	}
	desired.Apps[0].Epoch = 1
	if _, _, err := s.PrepareAppReplication(t.Context(), installation, node, desired); !errors.Is(err, operation.ErrCommandConflict) {
		t.Fatalf("stale assignment: %v", err)
	}
	if _, err := s.pool.Exec(t.Context(), `UPDATE apps SET desired_lifecycle='stopped' WHERE id=$1`, item.ID); err != nil {
		t.Fatal(err)
	}
	desired.Apps[0].Epoch = 2
	if _, _, err := s.PrepareAppReplication(t.Context(), installation, node, desired); !errors.Is(err, operation.ErrCommandConflict) {
		t.Fatalf("stopped assignment: %v", err)
	}
	desired.Apps = nil
	if _, _, err := s.PrepareAppReplication(t.Context(), installation, node, desired); err != nil {
		t.Fatalf("withdraw: %v", err)
	}
}
