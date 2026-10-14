package controller

import (
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/controlstream"
	"github.com/sirrobot01/synod/internal/destination"
	"github.com/sirrobot01/synod/internal/inventory"
	"github.com/sirrobot01/synod/internal/repository"
	"github.com/sirrobot01/synod/internal/restore"
	"github.com/sirrobot01/synod/internal/secret"
	"github.com/sirrobot01/synod/internal/store"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
)

// A first backup changes the plan, so every agent is briefly behind. Reads must still find an agent.
func TestFileReaderPrefersCurrentAgentsAndFallsBackToOnlineAgents(t *testing.T) {
	approved := time.Now()
	source, peer, offline, unapproved := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	nodes := []store.Node{{ID: unapproved}, {ID: offline, ApprovedAt: &approved}, {ID: source, ApprovedAt: &approved}, {ID: peer, ApprovedAt: &approved}}
	remote := destination.Destination{ID: uuid.New(), Location: repository.Location{Kind: "device"}}
	local := destination.Destination{ID: uuid.New(), NodeID: peer, Location: repository.Location{Kind: "local"}}
	session := func(id uuid.UUID) (controlstream.Peer, bool) {
		if id == offline {
			return controlstream.Peer{}, false
		}
		return controlstream.Peer{NodeID: id, SessionID: id.String(), Capabilities: []string{"files.read"}}, true
	}
	listing := func(current ...uuid.UUID) destination.Listing {
		result := destination.Listing{Destinations: []destination.Destination{remote, local}}
		for _, id := range []uuid.UUID{source, peer, offline, unapproved} {
			result.Delivery = append(result.Delivery, destination.Delivery{NodeID: id, SessionID: id.String(), Current: slices.Contains(current, id)})
		}
		return result
	}
	for _, test := range []struct {
		name    string
		current []uuid.UUID
		request restore.FileRequest
		want    uuid.UUID
	}{
		{"current agent first", []uuid.UUID{peer}, restore.FileRequest{DestinationID: remote.ID}, peer},
		{"first agent when all are behind", nil, restore.FileRequest{DestinationID: remote.ID}, source},
		{"local destination owner only", []uuid.UUID{source}, restore.FileRequest{DestinationID: local.ID}, peer},
		{"selected agent only", []uuid.UUID{source}, restore.FileRequest{DestinationID: remote.ID, NodeID: peer}, peer},
		{"no offline agent", nil, restore.FileRequest{DestinationID: remote.ID, NodeID: offline}, uuid.Nil},
		{"no unapproved agent", nil, restore.FileRequest{DestinationID: remote.ID, NodeID: unapproved}, uuid.Nil},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, ok := fileReader(nodes, listing(test.current...), test.request, session)
			if got != test.want || ok != (test.want != uuid.Nil) {
				t.Fatalf("fileReader() = %s, %t; want %s", got, ok, test.want)
			}
		})
	}
}

// Before the first destination, an app has no snapshots. That is not an agent fault.
func TestReadSnapshotFileWithoutDestinationIsEmpty(t *testing.T) {
	metadata, err := store.Open(t.Context(), filepath.Join(t.TempDir(), "synod.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer metadata.Close()
	key := uuid.New()
	ring, err := secret.NewKeyring(key, map[uuid.UUID][]byte{key: make([]byte, 32)})
	if err != nil {
		t.Fatal(err)
	}
	node := uuid.New()
	app := uuid.NewSHA1(node, []byte("compose:web"))
	definition := &agentv1.DiscoveredApp{Key: "compose:web", Name: "web", Kind: "compose", Containers: []*agentv1.DiscoveredContainer{{Id: "web", Name: "web", Image: "nginx"}}}
	catalog := repository.Catalog{Version: 1, InstallationID: uuid.New(), Generation: 1, WrittenAt: time.Now(), Devices: []repository.Device{{ID: node, Name: "server", Architecture: "amd64"}}, Apps: []inventory.App{{ID: app, NodeID: node, NodeName: "server", ObservedAt: time.Now(), Definition: definition}}}
	if err := metadata.ImportCatalog(t.Context(), catalog, ring); err != nil {
		t.Fatal(err)
	}
	for action, failure := range map[string]string{restore.FileSnapshots: "", restore.FileList: "snapshot_not_found", restore.FileRead: "snapshot_not_found"} {
		header, body, err := readSnapshotFile(t.Context(), metadata, nil, restore.FileRequest{AppID: app, Action: action, Path: "/srv"})
		if err != nil || header.FailureCode != failure || len(header.Snapshots) != 0 {
			t.Fatalf("%s without a destination: %+v %v", action, header, err)
		}
		body.Close()
	}
}
