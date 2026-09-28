package postgres

import (
	"testing"

	"github.com/google/uuid"
)

// reported builds one sound report for the fixture node.
func reported(host *placementHost, items ...DriftItem) DriftReport {
	return DriftReport{InstallationID: host.id, NodeID: host.node, SessionID: uuid.New(), Items: items}
}

// A drift record must keep what separates "this was ours and we lost track of it"
// from "this was never ours".
func TestDriftRecordKeepsWhatWasOurs(t *testing.T) {
	host := placementFixture(t)
	claim := host.claimApp(t, "db")
	resource := uuid.New()
	items := []DriftItem{
		{Kind: "container", NativeID: "aaaa", Name: "someone-elses-postgres", State: "running", Class: DriftUnowned},
		{Kind: "volume", NativeID: "synod-volume-x", Name: "synod-volume-x", State: "present", Class: DriftForeign},
		{Kind: "volume", NativeID: "synod-volume-y", Name: "synod-volume-y", State: "present",
			Class: DriftMislabelled, AppID: claim.AppID, ResourceID: resource, Detail: "resource_label_unreadable"},
		{Kind: "container", NativeID: "bbbb", Name: "synod-instance-z", State: "running",
			Class: DriftChanged, AppID: claim.AppID, ResourceID: resource, Detail: "restart_policy_changed"},
	}
	if err := host.store.RecordNodeDrift(t.Context(), reported(host, items...)); err != nil {
		t.Fatal(err)
	}
	stored, err := host.store.ReadNodeDrift(t.Context(), host.id, host.node)
	if err != nil {
		t.Fatal(err)
	}
	if stored.Truncated || stored.ObservedAt.IsZero() || len(stored.Items) != 4 {
		t.Fatalf("report=%+v, want all four objects", stored)
	}
	for _, item := range stored.Items {
		switch item.Class {
		case DriftUnowned, DriftForeign:
			if item.AppID != uuid.Nil || item.ResourceID != uuid.Nil {
				t.Fatalf("item=%+v, want no identity claimed for an object that is not ours", item)
			}
		case DriftMislabelled, DriftChanged:
			if item.AppID != claim.AppID || item.ResourceID != resource || item.Detail == "" {
				t.Fatalf("item=%+v, want the recovered identity and what disagrees", item)
			}
		}
	}
}

// A report is a snapshot, not a log. An object an operator has removed must stop
// being reported, so a later report replaces the whole set.
func TestDriftRecordIsASnapshot(t *testing.T) {
	host := placementFixture(t)
	first := DriftItem{Kind: "volume", NativeID: "gone", Name: "gone", State: "present", Class: DriftUnowned}
	if err := host.store.RecordNodeDrift(t.Context(), reported(host, first)); err != nil {
		t.Fatal(err)
	}
	second := DriftItem{Kind: "volume", NativeID: "still-here", Name: "still-here", State: "present", Class: DriftUnowned}
	later := reported(host, second)
	later.Truncated = true
	if err := host.store.RecordNodeDrift(t.Context(), later); err != nil {
		t.Fatal(err)
	}
	stored, err := host.store.ReadNodeDrift(t.Context(), host.id, host.node)
	if err != nil {
		t.Fatal(err)
	}
	if !stored.Truncated || len(stored.Items) != 1 || stored.Items[0].NativeID != "still-here" {
		t.Fatalf("report=%+v, want only what the later scan found", stored)
	}
	if stored.SessionID != later.SessionID {
		t.Fatalf("session=%s, want the session that took the later report", stored.SessionID)
	}
	// A host with nothing wrong on it reports nothing, and that is not an error.
	if err := host.store.RecordNodeDrift(t.Context(), reported(host)); err != nil {
		t.Fatal(err)
	}
	empty, err := host.store.ReadNodeDrift(t.Context(), host.id, host.node)
	if err != nil || len(empty.Items) != 0 {
		t.Fatalf("report=%+v err=%v, want an empty report", empty, err)
	}
}

// Drift is an observation. Specification 33 forbids a manual restart or stop moving
// desired state on its own, so recording what a host looks like moves nothing.
func TestDriftRecordNeverMovesDesiredState(t *testing.T) {
	host := placementFixture(t)
	claim := host.claimApp(t, "db")
	before, err := host.store.GetApp(t.Context(), host.id, claim.AppID)
	if err != nil {
		t.Fatal(err)
	}
	// A manually stopped container of ours, and its volume relabelled by hand.
	items := []DriftItem{
		{Kind: "container", NativeID: "cccc", Name: "synod-instance-q", State: "exited",
			Class: DriftChanged, AppID: claim.AppID, ResourceID: uuid.New(), Detail: "restart_policy_changed"},
		{Kind: "volume", NativeID: "synod-volume-q", Name: "synod-volume-q", State: "present",
			Class: DriftMislabelled, AppID: claim.AppID, Detail: "identity_unreadable"},
	}
	if err := host.store.RecordNodeDrift(t.Context(), reported(host, items...)); err != nil {
		t.Fatal(err)
	}
	after, err := host.store.GetApp(t.Context(), host.id, claim.AppID)
	if err != nil {
		t.Fatal(err)
	}
	if after.DesiredLifecycle != before.DesiredLifecycle || after.DesiredRevision != before.DesiredRevision {
		t.Fatalf("desired state moved from %s/%d to %s/%d", before.DesiredLifecycle, before.DesiredRevision, after.DesiredLifecycle, after.DesiredRevision)
	}
	if after.ObservedLifecycle != before.ObservedLifecycle || after.ObservedRevision != before.ObservedRevision {
		t.Fatalf("an observation of the host rewrote the app's observed state: %+v", after)
	}
	// No operation was admitted to correct anything either.
	if operations := host.count(t, `SELECT count(*) FROM operations WHERE app_id=$1`, claim.AppID); operations != 1 {
		t.Fatalf("operations=%d, want only the admission that created the app", operations)
	}
}

func TestDriftRecordRefusesAnUnsoundReport(t *testing.T) {
	host := placementFixture(t)
	sound := DriftItem{Kind: "volume", NativeID: "v1", Name: "v1", State: "present", Class: DriftUnowned}
	for name, spoil := range map[string]func(*DriftReport){
		"unknown kind":  func(r *DriftReport) { r.Items[0].Kind = "secret" },
		"unknown class": func(r *DriftReport) { r.Items[0].Class = DriftClass("suspicious") },
		"no native id":  func(r *DriftReport) { r.Items[0].NativeID = "" },
		"no session":    func(r *DriftReport) { r.SessionID = uuid.Nil },
		"no node":       func(r *DriftReport) { r.NodeID = uuid.Nil },
		// An object that was never ours has no Synod identity to report, and
		// inventing one would let a foreign object claim a app's resource.
		"unowned claiming an identity": func(r *DriftReport) { r.Items[0].ResourceID = uuid.New() },
		"a repeated object": func(r *DriftReport) {
			r.Items = append(r.Items, r.Items[0])
		},
	} {
		t.Run(name, func(t *testing.T) {
			bad := reported(host, sound)
			spoil(&bad)
			if err := host.store.RecordNodeDrift(t.Context(), bad); err == nil {
				t.Fatalf("an unsound report was accepted for %s", name)
			}
		})
	}
	if total := host.count(t, `SELECT count(*) FROM node_drift_items WHERE installation_id=$1`, host.id); total != 0 {
		t.Fatalf("items=%d, want none", total)
	}
}
