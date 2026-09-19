package controlstream

import (
	"context"
	"testing"
	"testing/synctest"

	"github.com/google/uuid"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestSessionPublicationOrdersRegistrationPerNode(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		node := uuid.New()
		firstCtx, firstCancel := context.WithCancel(t.Context())
		defer firstCancel()
		secondCtx, secondCancel := context.WithCancel(t.Context())
		defer secondCancel()
		otherCtx, otherCancel := context.WithCancel(t.Context())
		defer otherCancel()
		first := &session{peer: Peer{NodeID: node, SessionID: "first"}, ctx: firstCtx, cancel: firstCancel, outgoing: make(chan *agentv1.ControllerMessage, 1)}
		second := &session{peer: Peer{NodeID: node, SessionID: "second"}, ctx: secondCtx, cancel: secondCancel, outgoing: make(chan *agentv1.ControllerMessage, 1)}
		other := &session{peer: Peer{NodeID: uuid.New(), SessionID: "other"}, ctx: otherCtx, cancel: otherCancel, outgoing: make(chan *agentv1.ControllerMessage, 1)}
		committed, release := make(chan struct{}), make(chan struct{})
		durable := make(map[uuid.UUID]string)
		server := &Server{config: Config{MaxSessions: 4, Handle: func(ctx context.Context, peer Peer, _ *agentv1.AgentMessage) error {
			durable[peer.NodeID] = peer.SessionID
			if peer.SessionID == "first" {
				close(committed)
				<-release
			}
			return nil
		}}, sessions: make(map[uuid.UUID]*session), hellos: make(map[uuid.UUID]*helloGuard)}
		firstDone, secondDone := make(chan error, 1), make(chan error, 1)
		go func() { firstDone <- server.publishSession(firstCtx, first, nil, nil) }()
		<-committed
		go func() { secondDone <- server.publishSession(secondCtx, second, nil, nil) }()
		synctest.Wait()
		if durable[node] != "first" || len(secondDone) != 0 {
			t.Fatal("second registration passed first publication")
		}
		if err := server.publishSession(otherCtx, other, nil, nil); err != nil {
			t.Fatal(err)
		}
		if durable[other.peer.NodeID] != "other" {
			t.Fatal("different node could not register")
		}
		close(release)
		if err := <-firstDone; err != nil {
			t.Fatal(err)
		}
		if err := <-secondDone; err != nil {
			t.Fatal(err)
		}
		current, ok := server.Session(node)
		if !ok || current.SessionID != durable[node] || current.SessionID != "second" {
			t.Fatal("durable and live sessions differ")
		}
		if firstCtx.Err() == nil || secondCtx.Err() != nil {
			t.Fatal("wrong session cancelled")
		}
		if len(server.hellos) != 0 {
			t.Fatal("registration guards retained")
		}
	})
}

func TestSessionPublicationCancelsWaitingHello(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		node := uuid.New()
		ctx, cancel := context.WithCancel(t.Context())
		first := &session{peer: Peer{NodeID: node, SessionID: "first"}, ctx: t.Context(), cancel: func() {}, outgoing: make(chan *agentv1.ControllerMessage, 1)}
		second := &session{peer: Peer{NodeID: node, SessionID: "second"}, ctx: ctx, cancel: cancel, outgoing: make(chan *agentv1.ControllerMessage, 1)}
		committed, release := make(chan struct{}), make(chan struct{})
		calls := 0
		server := &Server{config: Config{MaxSessions: 1, Handle: func(context.Context, Peer, *agentv1.AgentMessage) error {
			calls++
			close(committed)
			<-release
			return nil
		}}, sessions: make(map[uuid.UUID]*session), hellos: make(map[uuid.UUID]*helloGuard)}
		firstDone, secondDone := make(chan error, 1), make(chan error, 1)
		go func() { firstDone <- server.publishSession(t.Context(), first, nil, nil) }()
		<-committed
		go func() { secondDone <- server.publishSession(ctx, second, nil, nil) }()
		synctest.Wait()
		cancel()
		if err := <-secondDone; status.Code(err) != codes.Canceled {
			t.Fatalf("cancelled wait: %v", err)
		}
		if calls != 1 || server.hellos[node].users != 1 {
			t.Fatal("cancelled waiter registered or retained its guard")
		}
		close(release)
		if err := <-firstDone; err != nil {
			t.Fatal(err)
		}
		if len(server.hellos) != 0 {
			t.Fatal("registration guard retained")
		}
	})
}
