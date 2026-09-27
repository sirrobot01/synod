package executor

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/controlstream"
	"github.com/sirrobot01/synod/internal/operation"
	"github.com/sirrobot01/synod/internal/router"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/proto"
)

func TestRoutesBindSessionAndRevision(t *testing.T) {
	routes, err := router.New(t.Context(), router.Options{ListenAddress: "127.0.0.1"})
	if err != nil {
		t.Fatal(err)
	}
	defer routes.Close()
	e := &Executor{options: Options{Router: routes}}
	ctx, cancel := localSession(t, e, controlstream.RouterCapability)
	table := &agentv1.RouteTable{SessionId: e.session.id, Revision: 2}
	message := &agentv1.ControllerMessage{Message: &agentv1.ControllerMessage_Routes{Routes: table}}
	for range 2 {
		ack, err := e.Handle(ctx, message)
		if err != nil || ack.GetRouteAck().FailureCode != "" {
			t.Fatalf("apply/replay: %v %v", ack, err)
		}
	}
	table.Revision = 1
	ack, err := e.Handle(ctx, message)
	if err != nil || ack.GetRouteAck().FailureCode != "stale_route_revision" {
		t.Fatalf("stale revision: %v %v", ack, err)
	}
	table.Revision = 2
	table.Routes = []*agentv1.AppRoute{{AppId: uuid.NewString(), IngressPort: 32100, Protocol: "tcp"}}
	ack, err = e.Handle(ctx, message)
	if err != nil || ack.GetRouteAck().FailureCode != "route_revision_conflict" {
		t.Fatalf("changed replay: %v %v", ack, err)
	}
	fresh, _ := localSession(t, e, controlstream.RouterCapability)
	if _, err := e.Handle(ctx, message); !errors.Is(err, operation.ErrStaleOwner) {
		t.Fatalf("replaced session changed routes: %v", err)
	}
	cancel()
	if _, err := e.Handle(fresh, message); !errors.Is(err, controlstream.ErrMessage) {
		t.Fatalf("wrong session ID accepted: %v", err)
	}
	table.SessionId = e.session.id
	table.Revision = 1
	table.Routes = nil
	ack, err = e.Handle(fresh, message)
	if err != nil || ack.GetRouteAck().FailureCode != "" {
		t.Fatalf("new session cannot apply its first snapshot: %v %v", ack, err)
	}
}

func TestRouteSnapshotCrossesAuthenticatedStream(t *testing.T) {
	f := newFixture(t, nil)
	routes, err := router.New(t.Context(), router.Options{ListenAddress: "127.0.0.1"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { routes.Close() })
	f.e.options.Router = routes
	controller := startController(t, f, nil)
	deadline := time.Now().Add(5 * time.Second)
	var sessionID string
	for {
		if peer, ok := controller.server.Session(controller.node); ok {
			sessionID = peer.SessionID
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("agent did not connect")
		}
		time.Sleep(10 * time.Millisecond)
	}
	occupied, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer occupied.Close()
	address := occupied.Addr().String()
	table := &agentv1.RouteTable{SessionId: sessionID, Revision: 1, Routes: []*agentv1.AppRoute{{AppId: uuid.NewString(), Protocol: "tcp", IngressPort: uint32(occupied.Addr().(*net.TCPAddr).Port)}}}
	controller.send(t, &agentv1.ControllerMessage{Message: &agentv1.ControllerMessage_Routes{Routes: table}})
	ack := controller.wait(t, func(m *agentv1.AgentMessage) bool { return m.GetRouteAck() != nil }).GetRouteAck()
	if ack.FailureCode != "route_port_conflict" {
		t.Fatalf("occupied ingress acknowledgement: %v", ack)
	}
	occupied.Close()
	controller.send(t, &agentv1.ControllerMessage{Message: &agentv1.ControllerMessage_Routes{Routes: proto.Clone(table).(*agentv1.RouteTable)}})
	ack = controller.wait(t, func(m *agentv1.AgentMessage) bool { return m.GetRouteAck() != nil }).GetRouteAck()
	if ack.FailureCode != "" || ack.Revision != 1 {
		t.Fatalf("route retry acknowledgement: %v", ack)
	}
	if listener, err := net.Listen("tcp", address); err == nil {
		listener.Close()
		t.Fatal("acknowledged ingress was not reserved")
	}
	// A disconnected controller does not remove ingress or stop its independent probes.
	controller.cancel()
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", address)
	if err != nil {
		t.Fatalf("disconnect removed ingress: %v", err)
	}
	conn.Close()
}
