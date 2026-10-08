package controller

import (
	"slices"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/controlstream"
	"github.com/sirrobot01/synod/internal/destination"
	"github.com/sirrobot01/synod/internal/repository"
	"github.com/sirrobot01/synod/internal/restore"
	"github.com/sirrobot01/synod/internal/store"
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
