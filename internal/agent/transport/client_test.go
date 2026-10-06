package transport

import (
	"context"
	"errors"
	"testing"

	"github.com/sirrobot01/synod/internal/controlstream"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestCallbackResponseCannotEnterReplacement(t *testing.T) {
	old := &connection{ctx: context.Background(), outgoing: make(chan *agentv1.AgentMessage, 1), maxBytes: 1024}
	replacement := &connection{ctx: t.Context(), outgoing: make(chan *agentv1.AgentMessage, 1), maxBytes: 1024}
	client := &Client{config: Config{Hello: &agentv1.Hello{}}, active: replacement}
	response := &agentv1.AgentMessage{Message: &agentv1.AgentMessage_Heartbeat{Heartbeat: &agentv1.Heartbeat{Sequence: 1, ObservedAt: timestamppb.Now()}}}
	// The old handler has returned. Replacement wins the lock before its response.
	// Even an uncancelled old context cannot authorize delivery to the new stream.
	if err := client.send(old, response); !errors.Is(err, controlstream.ErrDisconnected) {
		t.Fatalf("stale callback response accepted: %v", err)
	}
	if len(replacement.outgoing) != 0 {
		t.Fatal("stale response entered replacement queue")
	}
	if err := client.send(replacement, response); err != nil {
		t.Fatal(err)
	}
	if len(replacement.outgoing) != 1 {
		t.Fatal("current callback response was lost")
	}
}
