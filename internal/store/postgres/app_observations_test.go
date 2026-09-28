package postgres

import (
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/app"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestAppObservationRequiresCurrentNodeSessionPortAndTime(t *testing.T) {
	for _, name := range []string{"routed", "worker"} {
		t.Run(name, func(t *testing.T) {
			worker := name == "worker"
			s, installation, node, session := capacityFixture(t)
			spec := app.App{InstallationID: installation, Name: "web", Tier: app.TierParallel, Spec: app.AppSpec{SchemaVersion: 1, Name: "web", Tier: app.TierParallel, Copies: 1, Service: app.ServiceSpec{Image: "nginx", Ports: []app.PortSpec{{ContainerPort: 80}}}, Router: app.RouterSpec{Port: 8080, Protocol: "http"}}}
			if worker {
				spec.Spec.Router = app.RouterSpec{}
				spec.Spec.Service.Ports = nil
			}
			item, err := s.CreateApp(t.Context(), spec)
			if err != nil {
				t.Fatal(err)
			}
			copy := app.Copy{ID: uuid.New(), AppID: item.ID, NodeID: node, CopyIndex: 0, HostPort: 23000, ObservedState: "pending"}
			if worker {
				copy.HostPort = 0
			}
			if err := s.SaveAppCopies(t.Context(), installation, item.ID, []app.Copy{copy}); err != nil {
				t.Fatal(err)
			}
			heartbeat := &agentv1.Heartbeat{Sequence: 1, ObservedAt: timestamppb.Now(), Observations: []*agentv1.Observation{{ResourceId: copy.ID.String(), Lifecycle: "running", Health: "healthy", ObservedAt: timestamppb.Now(), Ports: []*agentv1.PortBinding{{HostAddress: "0.0.0.0", HostPort: 23000, ContainerPort: 80, Transport: "tcp"}}}}}
			if err := s.RecordAgentHeartbeat(t.Context(), installation, node, uuid.New(), heartbeat); !errors.Is(err, ErrStaleSession) {
				t.Fatalf("foreign session: %v", err)
			}
			for _, test := range []struct {
				name     string
				at       time.Time
				port     uint32
				revision int64
			}{
				{"old", time.Now().Add(-time.Minute), 23000, 1},
				{"future", time.Now().Add(time.Minute), 23000, 1},
				{"wrong revision", time.Now(), 23000, 2},
				{"wrong port", time.Now(), 23001, 1},
			} {
				t.Run(test.name, func(t *testing.T) {
					heartbeat.Sequence++
					heartbeat.Observations[0].ObservedAt = timestamppb.New(test.at)
					heartbeat.Observations[0].Ports = []*agentv1.PortBinding{{HostAddress: "0.0.0.0", HostPort: test.port, ContainerPort: 80, Transport: "tcp"}}
					if worker && test.name != "wrong port" {
						heartbeat.Observations[0].Ports = nil
					}
					heartbeat.Observations[0].ObservedRevision = test.revision
					if err := s.RecordAgentHeartbeat(t.Context(), installation, node, session, heartbeat); err != nil {
						t.Fatal(err)
					}
					copies, err := s.GetAppCopies(t.Context(), installation, item.ID)
					if err != nil || copies[0].Health == "healthy" {
						t.Fatalf("invalid evidence made healthy: %+v %v", copies, err)
					}
				})
			}
			heartbeat.Sequence++
			heartbeat.Observations[0].ObservedAt = timestamppb.Now()
			heartbeat.Observations[0].Ports = []*agentv1.PortBinding{{HostAddress: "0.0.0.0", HostPort: 23000, ContainerPort: 80, Transport: "tcp"}}
			if worker {
				heartbeat.Observations[0].Ports = nil
			}
			heartbeat.Observations[0].ObservedRevision = 0
			if err := s.RecordAgentHeartbeat(t.Context(), installation, node, session, heartbeat); err != nil {
				t.Fatal(err)
			}
			copies, err := s.GetAppCopies(t.Context(), installation, item.ID)
			if err != nil || copies[0].Health != "healthy" {
				t.Fatalf("fresh evidence: %+v %v", copies, err)
			}
			presence, err := s.GetAgentPresence(t.Context(), installation, node)
			if err != nil {
				t.Fatal(err)
			}
			next := uuid.New()
			if err := s.RecordAgentSession(t.Context(), installation, node, next, presence.Hello); err != nil {
				t.Fatal(err)
			}
			copies, err = s.GetAppCopies(t.Context(), installation, item.ID)
			if err != nil || copies[0].Health != "unknown" {
				t.Fatalf("old session health survived: %+v %v", copies, err)
			}
			// A new heartbeat cannot refresh an expired observation timestamp.
			if _, err := s.pool.Exec(t.Context(), `UPDATE app_copies SET observed_session_id=$1,observed_at=clock_timestamp()-interval '1 minute' WHERE id=$2`, next, copy.ID); err != nil {
				t.Fatal(err)
			}
			heartbeat.Sequence = 1
			heartbeat.Observations[0].ObservedAt = timestamppb.New(time.Now().Add(-time.Minute))
			if err := s.RecordAgentHeartbeat(t.Context(), installation, node, next, heartbeat); err != nil {
				t.Fatal(err)
			}
			copies, err = s.GetAppCopies(t.Context(), installation, item.ID)
			if err != nil || copies[0].Health != "unknown" {
				t.Fatalf("stale sample refreshed: %+v %v", copies, err)
			}
			if err := s.RecordAppRouteAck(t.Context(), installation, node, session, &agentv1.RouteAck{SessionId: session.String(), Revision: 1}); !errors.Is(err, ErrStaleSession) {
				t.Fatalf("old route ack: %v", err)
			}
		})
	}
}
