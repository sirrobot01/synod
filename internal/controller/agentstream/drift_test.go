package agentstream

import (
	"testing"

	"github.com/google/uuid"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
)

// The report takes its identity from the certificate that proved one, and lands in
// the drift tables and nowhere else.
func TestDriftPageIsRecordedAgainstThePeer(t *testing.T) {
	store, _, peer, sessionID, _ := challenged(t)
	resource := uuid.New()
	page := &agentv1.InventoryPage{
		// An owned resource observed as stopped travels on the same page. Nothing
		// about it may move desired state, and nothing here reads it.
		Resources: []*agentv1.Observation{{ResourceId: resource.String(), Lifecycle: "exited"}},
		Drift: []*agentv1.DriftItem{
			{Kind: "container", NativeId: "aaaa", Name: "someone-elses-postgres", State: "running", Class: "unowned"},
			{Kind: "volume", NativeId: "synod-volume-x", Name: "synod-volume-x", State: "present",
				Class: "mislabelled", ClusterId: uuid.NewString(), ResourceId: resource.String(), Detail: "resource_label_unreadable"},
		},
	}
	message := &agentv1.AgentMessage{Message: &agentv1.AgentMessage_Inventory{Inventory: page}}
	if err := (Receiver{Store: store}).Handle(t.Context(), peer, message); err != nil {
		t.Fatal(err)
	}
	stored, err := store.ReadNodeDrift(t.Context(), peer.InstallationID, peer.NodeID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.NodeID != peer.NodeID || stored.InstallationID != peer.InstallationID || stored.SessionID != sessionID {
		t.Fatalf("report=%+v, want the peer's own identity", stored)
	}
	if len(stored.Items) != 2 {
		t.Fatalf("items=%d, want both objects", len(stored.Items))
	}
	for _, item := range stored.Items {
		if item.Class == "mislabelled" && item.ResourceID != resource {
			t.Fatalf("item=%+v, want the recovered identity kept", item)
		}
	}
}

// Drift travels on the first page of a scan. A later page carrying none would erase
// what the first page said, so an empty page records nothing.
func TestALaterPageDoesNotEraseTheDriftReport(t *testing.T) {
	store, _, peer, _, _ := challenged(t)
	receiver := Receiver{Store: store}
	first := &agentv1.AgentMessage{Message: &agentv1.AgentMessage_Inventory{Inventory: &agentv1.InventoryPage{
		Drift: []*agentv1.DriftItem{{Kind: "volume", NativeId: "v1", Name: "v1", State: "present", Class: "unowned"}},
	}}}
	if err := receiver.Handle(t.Context(), peer, first); err != nil {
		t.Fatal(err)
	}
	later := &agentv1.AgentMessage{Message: &agentv1.AgentMessage_Inventory{Inventory: &agentv1.InventoryPage{
		Resources: []*agentv1.Observation{{ResourceId: uuid.NewString(), Lifecycle: "running"}},
	}}}
	if err := receiver.Handle(t.Context(), peer, later); err != nil {
		t.Fatal(err)
	}
	stored, err := store.ReadNodeDrift(t.Context(), peer.InstallationID, peer.NodeID)
	if err != nil || len(stored.Items) != 1 {
		t.Fatalf("report=%+v err=%v, want the first page's report kept", stored, err)
	}
}

// An identity that does not parse is refused rather than dropped. A report that
// quietly loses what it saw is not a report.
func TestDriftPageRefusesAnUnreadableIdentity(t *testing.T) {
	store, _, peer, _, _ := challenged(t)
	page := &agentv1.InventoryPage{Drift: []*agentv1.DriftItem{
		{Kind: "volume", NativeId: "v1", Name: "v1", State: "present", Class: "mislabelled", ResourceId: "not-a-uuid"},
	}}
	message := &agentv1.AgentMessage{Message: &agentv1.AgentMessage_Inventory{Inventory: page}}
	if err := (Receiver{Store: store}).Handle(t.Context(), peer, message); err == nil {
		t.Fatal("an unreadable identity was accepted")
	}
}
