package store

import (
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestInventorySurvivesFailuresAndRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "metadata.db")
	s, err := Open(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	ca := authority(t)
	node, _ := join(t, s, ca)
	other, _ := join(t, s, ca)
	session := uuid.New()
	if err := s.RecordAgentSession(t.Context(), node, session, &agentv1.Hello{NodeId: node.String(), Architecture: "amd64", Hostname: "server"}); err != nil {
		t.Fatal(err)
	}
	report := &agentv1.AppInventory{ObservedAt: timestamppb.New(time.Now().Add(-time.Minute)), Apps: []*agentv1.DiscoveredApp{{Key: "compose:notes", Name: "notes", Kind: "compose", Containers: []*agentv1.DiscoveredContainer{{Id: "abc", Name: "web", Image: "nginx:stable"}}}}}
	if err := s.RecordInventory(t.Context(), node, session, report); err != nil {
		t.Fatal(err)
	}
	catalog, err := s.ListApps(t.Context())
	if err != nil || len(catalog.Apps) != 1 || len(catalog.Devices) != 2 || !catalog.Apps[0].Present {
		t.Fatalf("first report: %+v %v", catalog, err)
	}
	id := catalog.Apps[0].ID
	if id != uuid.NewSHA1(node, []byte("compose:notes")) {
		t.Fatal("app ID does not bind its source node")
	}
	if err := s.RecordInventory(t.Context(), other, session, report); !errors.Is(err, ErrStaleSession) {
		t.Fatalf("other node session: %v", err)
	}
	bad := proto.Clone(report).(*agentv1.AppInventory)
	bad.Apps = append(bad.Apps, bad.Apps[0])
	if err := s.RecordInventory(t.Context(), node, session, bad); code(err) != InvalidRequest {
		t.Fatalf("duplicate app key: %v", err)
	}
	failed := &agentv1.AppInventory{ObservedAt: timestamppb.Now(), FailureCode: "docker_scan_failed"}
	if err := s.RecordInventory(t.Context(), node, session, failed); err != nil {
		t.Fatal(err)
	}
	catalog, err = s.ListApps(t.Context())
	if err != nil || len(catalog.Apps) != 1 || !catalog.Apps[0].Present || catalog.Apps[0].ID != id {
		t.Fatalf("failed scan erased saved apps: %+v %v", catalog, err)
	}
	foundFailure := false
	for _, d := range catalog.Devices {
		if d.NodeID == node && d.FailureCode == "docker_scan_failed" {
			foundFailure = true
		}
	}
	if !foundFailure {
		t.Fatal("scan failure was not recorded")
	}
	if err := s.RecordInventory(t.Context(), node, session, report); err != nil {
		t.Fatal(err)
	}
	catalog, err = s.ListApps(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	for _, d := range catalog.Devices {
		if d.NodeID == node && d.FailureCode == "" {
			t.Fatal("old report overwrote newer failure")
		}
	}
	newSession := uuid.New()
	if err := s.RecordAgentSession(t.Context(), node, newSession, &agentv1.Hello{NodeId: node.String(), Architecture: "amd64", Hostname: "server"}); err != nil {
		t.Fatal(err)
	}
	empty := &agentv1.AppInventory{ObservedAt: timestamppb.Now()}
	if err := s.RecordInventory(t.Context(), node, session, empty); !errors.Is(err, ErrStaleSession) {
		t.Fatalf("stale session changed presence: %v", err)
	}
	if err := s.RecordInventory(t.Context(), node, newSession, empty); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	catalog, err = s.ListApps(t.Context())
	if err != nil || len(catalog.Apps) != 1 || catalog.Apps[0].Present || catalog.Apps[0].ID != id {
		t.Fatalf("removed app after restart: %+v %v", catalog, err)
	}
	report.ObservedAt = timestamppb.New(time.Now().Add(time.Second))
	report.Apps[0].Containers[0].Id = "replacement"
	if err := s.RecordInventory(t.Context(), node, newSession, report); err != nil {
		t.Fatal(err)
	}
	catalog, err = s.ListApps(t.Context())
	if err != nil || catalog.Apps[0].ID != id || !catalog.Apps[0].Present || catalog.Apps[0].Definition.Containers[0].Id != "replacement" {
		t.Fatalf("Compose recreation lost identity: %+v %v", catalog, err)
	}
}
