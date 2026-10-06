package api

import (
	"encoding/json"
	"testing"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/controller/agentstream"
	"github.com/sirrobot01/synod/internal/controlstream"
	"github.com/sirrobot01/synod/internal/inventory"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestAppsFromAuthenticatedAgentToAPI(t *testing.T) {
	server, metadata, _ := identityServer(t)
	if response := call(t, server, "GET", "/v1/apps", ""); response.Code != 401 {
		t.Fatalf("anonymous app list: %d", response.Code)
	}
	node := enrollTestNode(t, metadata)
	peer := controlstream.Peer{NodeID: node, SessionID: uuid.NewString()}
	receiver := agentstream.Receiver{Store: metadata}
	if err := receiver.Handle(t.Context(), peer, &agentv1.AgentMessage{Message: &agentv1.AgentMessage_Hello{Hello: &agentv1.Hello{NodeId: node.String(), Architecture: "arm64", Hostname: "odin"}}}); err != nil {
		t.Fatal(err)
	}
	message := &agentv1.AgentMessage{Message: &agentv1.AgentMessage_Inventory{Inventory: &agentv1.AppInventory{ObservedAt: timestamppb.Now(), Apps: []*agentv1.DiscoveredApp{{Key: "container:abc", Name: "notes", Kind: "container", Containers: []*agentv1.DiscoveredContainer{{Id: "abc", Name: "notes", Image: "nginx:stable"}}}}}}}
	if err := controlstream.CheckAgentMessage(message); err != nil {
		t.Fatal(err)
	}
	if err := receiver.Handle(t.Context(), peer, message); err != nil {
		t.Fatal(err)
	}
	response := call(t, server, "GET", "/v1/apps", "", withBootstrap)
	var catalog inventory.Catalog
	if response.Code != 200 || json.Unmarshal(response.Body.Bytes(), &catalog) != nil || len(catalog.Apps) != 1 || catalog.Apps[0].Definition.Name != "notes" || catalog.Apps[0].NodeID != node || catalog.Apps[0].NodeName != "odin" {
		t.Fatalf("app list: %d %s", response.Code, response.Body.String())
	}
}

func TestAgentDestinationAddressUsesObservedPeer(t *testing.T) {
	_, metadata, _ := identityServer(t)
	node := enrollTestNode(t, metadata)
	receiver := agentstream.Receiver{Store: metadata}
	peer := controlstream.Peer{NodeID: node, SessionID: uuid.NewString(), RemoteAddress: "192.0.2.15:54000"}
	hello := &agentv1.Hello{NodeId: node.String(), Architecture: "arm64", DestinationAddress: "[::]:8445"}
	if err := receiver.Handle(t.Context(), peer, &agentv1.AgentMessage{Message: &agentv1.AgentMessage_Hello{Hello: hello}}); err != nil {
		t.Fatal(err)
	}
	saved, err := metadata.GetNode(t.Context(), node)
	if err != nil || saved.DestinationAddress != "192.0.2.15:8445" {
		t.Fatalf("observed destination address: %v", err)
	}
	if hello.DestinationAddress != "[::]:8445" {
		t.Fatal("receiver changed caller's Hello")
	}
	hello.DestinationAddress = "backup.example:18445"
	peer.SessionID = uuid.NewString()
	if err := receiver.Handle(t.Context(), peer, &agentv1.AgentMessage{Message: &agentv1.AgentMessage_Hello{Hello: hello}}); err != nil {
		t.Fatal(err)
	}
	saved, err = metadata.GetNode(t.Context(), node)
	if err != nil || saved.DestinationAddress != "backup.example:18445" {
		t.Fatalf("explicit destination address: %v", err)
	}
}
