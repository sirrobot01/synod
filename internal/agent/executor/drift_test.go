package executor

import (
	"context"
	"testing"

	"github.com/google/uuid"
	noderuntime "github.com/sirrobot01/synod/internal/runtime"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
)

// scan asks for one inventory page of the given size.
func scan(t *testing.T, e *Executor, ctx context.Context, cursor string, size uint32) *agentv1.InventoryPage {
	t.Helper()
	message, err := e.Handle(ctx, &agentv1.ControllerMessage{
		Message: &agentv1.ControllerMessage_Inventory{Inventory: &agentv1.InventoryRequest{Cursor: cursor, PageSize: size}},
	})
	if err != nil {
		t.Fatal(err)
	}
	return message.GetInventory()
}

// Drift is a property of the host, not of a page, so it travels once. A later page
// carrying it again would say the host holds twice what it holds.
func TestInventoryCarriesDriftOnTheFirstPageOnly(t *testing.T) {
	clusterID, resource := uuid.New(), uuid.New()
	native := &measuredRuntime{
		resources: []noderuntime.Resource{
			{ClusterID: clusterID, ResourceID: uuid.New(), Kind: "volume", State: "present"},
			{ClusterID: clusterID, ResourceID: uuid.New(), Kind: "container", State: "running"},
		},
		drift: []noderuntime.Drift{
			{Kind: "container", NativeID: "aaaa", Name: "someone-elses", State: "running", Class: noderuntime.DriftUnowned},
			{Kind: "volume", NativeID: "synod-volume-x", Name: "synod-volume-x", State: "present",
				Class: noderuntime.DriftMislabelled, ClusterID: clusterID, ResourceID: resource, Detail: "resource_label_unreadable"},
		},
	}
	e, ctx, _ := challenged(t, native)
	first := scan(t, e, ctx, "", 1)
	if len(first.Resources) != 1 || first.NextCursor == "" {
		t.Fatalf("page=%+v, want one resource and more to come", first)
	}
	if len(first.Drift) != 2 || first.DriftTruncated {
		t.Fatalf("drift=%+v truncated=%v, want both objects once", first.Drift, first.DriftTruncated)
	}
	// An identity that survived is carried; one that never existed stays absent.
	var unowned, mislabelled *agentv1.DriftItem
	for _, item := range first.Drift {
		switch item.Class {
		case "unowned":
			unowned = item
		case "mislabelled":
			mislabelled = item
		}
	}
	if unowned == nil || unowned.ClusterId != "" || unowned.ResourceId != "" {
		t.Fatalf("item=%+v, want no identity claimed for an object that was never ours", unowned)
	}
	if mislabelled == nil || mislabelled.ResourceId != resource.String() || mislabelled.ClusterId != clusterID.String() {
		t.Fatalf("item=%+v, want the recovered identity carried", mislabelled)
	}
	second := scan(t, e, ctx, first.NextCursor, 1)
	if len(second.Resources) != 1 || len(second.Drift) != 0 || second.DriftTruncated {
		t.Fatalf("page=%+v, want the rest of the inventory and no repeated drift", second)
	}
}

// A host with more drift than one report carries says so rather than growing the
// message. More than this needs an operator on the host.
func TestInventoryBoundsTheDriftItReports(t *testing.T) {
	native := &measuredRuntime{}
	for range maxDriftReported + 10 {
		native.drift = append(native.drift, noderuntime.Drift{
			Kind: "volume", NativeID: "v" + uuid.NewString(), Name: "v", State: "present",
			Class: noderuntime.DriftUnowned,
		})
	}
	e, ctx, _ := challenged(t, native)
	page := scan(t, e, ctx, "", 16)
	if len(page.Drift) != maxDriftReported || !page.DriftTruncated {
		t.Fatalf("drift=%d truncated=%v, want a bounded report that says it is bounded", len(page.Drift), page.DriftTruncated)
	}
}
