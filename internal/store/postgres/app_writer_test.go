package postgres

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/agent/replication"
	"github.com/sirrobot01/synod/internal/app"
	"github.com/sirrobot01/synod/internal/operation"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestWriterGrantRequiresCurrentAcknowledgedAssignment(t *testing.T) {
	s, installation, node, session := capacityFixture(t)
	presence, err := s.GetAgentPresence(t.Context(), installation, node)
	if err != nil {
		t.Fatal(err)
	}
	presence.Hello.Capabilities = []string{replication.Capability}
	session = uuid.New()
	if err := s.RecordAgentSession(t.Context(), installation, node, session, presence.Hello); err != nil {
		t.Fatal(err)
	}
	item, err := s.CreateApp(t.Context(), app.App{InstallationID: installation, Name: "leased", Tier: app.TierSingleWriter, Spec: app.AppSpec{SchemaVersion: 1, Name: "leased", Tier: app.TierSingleWriter, Copies: 1, Replication: new(app.DefaultReplication()), Service: app.ServiceSpec{Image: "example", Mounts: []app.MountSpec{{Type: "replicated", Source: "config", Target: "/config"}}}, Router: app.RouterSpec{Protocol: "tcp", Port: 8080}}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.pool.Exec(t.Context(), `UPDATE apps SET writer_epoch=1,writer_node_id=$2 WHERE id=$1`, item.ID, node); err != nil {
		t.Fatal(err)
	}
	if _, err := s.pool.Exec(t.Context(), `INSERT INTO app_copies(id,installation_id,app_id,node_id,copy_index,host_port,role,observed_state,created_at,updated_at) VALUES($1,$2,$3,$4,0,8081,'active','pending',clock_timestamp(),clock_timestamp())`, uuid.New(), installation, item.ID, node); err != nil {
		t.Fatal(err)
	}
	heartbeat := &agentv1.Heartbeat{Sequence: 1, ObservedAt: timestamppb.Now(), WriterClockNanos: uint64(time.Hour)}
	if err := s.RecordAgentHeartbeat(t.Context(), installation, node, session, heartbeat); err != nil {
		t.Fatal(err)
	}
	grant, err := s.PrepareAppWriterLease(t.Context(), installation, item.ID, node, session, 1, false)
	if err != nil || grant != nil {
		t.Fatalf("grant without assignment: %v %v", grant, err)
	}
	desired := &agentv1.ReplicationTable{SessionId: session.String(), Apps: []*agentv1.AppReplication{{AppId: item.ID.String(), Role: replication.RoleActive, Epoch: 1, IntervalSeconds: 60, Peers: []*agentv1.ReplicaPeer{{NodeId: uuid.NewString(), Address: "192.168.0.21:8445"}}}}}
	table, _, err := s.PrepareAppReplication(t.Context(), installation, node, desired)
	if err != nil {
		t.Fatal(err)
	}
	grant, err = s.PrepareAppWriterLease(t.Context(), installation, item.ID, node, session, 1, false)
	if err != nil || grant != nil {
		t.Fatalf("grant before acknowledgement: %v %v", grant, err)
	}
	if err := s.RecordAppReplicationAck(t.Context(), installation, node, session, &agentv1.ReplicationAck{SessionId: session.String(), Revision: table.Revision}); err != nil {
		t.Fatal(err)
	}
	// Concurrent dispatch loops must reserve only one renewal window.
	grants := make(chan *agentv1.WriterLease, 2)
	errs := make(chan error, 2)
	var workers sync.WaitGroup
	for range 2 {
		workers.Go(func() {
			g, err := s.PrepareAppWriterLease(t.Context(), installation, item.ID, node, session, 1, false)
			grants <- g
			errs <- err
		})
	}
	workers.Wait()
	count := 0
	for range 2 {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
		if g := <-grants; g != nil {
			count++
			if g.ExpiresAtBootNanos != uint64(time.Hour+30*time.Second) || g.SessionId != session.String() || g.Epoch != 1 {
				t.Fatalf("grant clock or identity: %v", g)
			}
		}
	}
	if count != 1 {
		t.Fatalf("got %d concurrent grants", count)
	}
	var boundary time.Time
	if err := s.pool.QueryRow(t.Context(), `SELECT writer_last_grant_at FROM apps WHERE id=$1`, item.ID).Scan(&boundary); err != nil || time.Since(boundary) > time.Minute {
		t.Fatalf("missing durable boundary: %v %v", boundary, err)
	}
	for _, test := range []struct {
		name string
		sql  string
	}{
		{"unhealthy", `UPDATE app_copies SET health='unhealthy',observed_session_id=$2 WHERE app_id=$1`},
		{"stopped", `UPDATE app_copies SET observed_state='failed',health='unknown',observed_session_id=$2 WHERE app_id=$1`},
		{"stale heartbeat", `UPDATE agent_presence SET last_seen_at=clock_timestamp()-interval '1 minute' WHERE node_id=$1`},
		{"recovery", `UPDATE agent_presence SET recovery_required=true WHERE node_id=$1`},
		{"revoked", `UPDATE nodes SET revoked_at=clock_timestamp() WHERE id=$1`},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := s.pool.Exec(t.Context(), `UPDATE apps SET writer_last_grant_at=clock_timestamp()-interval '11 seconds' WHERE id=$1`, item.ID); err != nil {
				t.Fatal(err)
			}
			args := []any{node}
			if test.name == "unhealthy" || test.name == "stopped" {
				args = []any{item.ID, session}
			}
			if _, err := s.pool.Exec(t.Context(), test.sql, args...); err != nil {
				t.Fatal(err)
			}
			g, err := s.PrepareAppWriterLease(t.Context(), installation, item.ID, node, session, 1, false)
			if err != nil || g != nil {
				t.Fatalf("unsafe grant: %v %v", g, err)
			}
			if _, err := s.pool.Exec(t.Context(), `UPDATE app_copies SET health='unknown',observed_state='pending' WHERE app_id=$1`, item.ID); err != nil {
				t.Fatal(err)
			}
			if _, err := s.pool.Exec(t.Context(), `UPDATE agent_presence SET last_seen_at=clock_timestamp(),recovery_required=false WHERE node_id=$1`, node); err != nil {
				t.Fatal(err)
			}
		})
	}
	for name, invoke := range map[string]func() error{
		"wrong epoch": func() error {
			_, err := s.PrepareAppWriterLease(t.Context(), installation, item.ID, node, session, 2, false)
			return err
		},
		"wrong node": func() error {
			_, err := s.PrepareAppWriterLease(t.Context(), installation, item.ID, uuid.New(), session, 1, false)
			return err
		},
	} {
		t.Run(name, func(t *testing.T) {
			if err := invoke(); !errors.Is(err, operation.ErrCommandConflict) {
				t.Fatalf("mismatched writer: %v", err)
			}
		})
	}
	if _, err := s.PrepareAppWriterLease(t.Context(), installation, item.ID, node, uuid.New(), 1, false); !errors.Is(err, ErrStaleSession) {
		t.Fatalf("stale session: %v", err)
	}
}
