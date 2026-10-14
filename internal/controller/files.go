package controller

import (
	"context"
	"io"
	"slices"
	"strings"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/controlstream"
	"github.com/sirrobot01/synod/internal/destination"
	"github.com/sirrobot01/synod/internal/inventory"
	"github.com/sirrobot01/synod/internal/restore"
	"github.com/sirrobot01/synod/internal/store"
)

// readSnapshotFile sends the request to an approved agent that can read the destination.
func readSnapshotFile(ctx context.Context, metadata *store.Store, stream *controlstream.Server, request restore.FileRequest) (restore.FileHeader, io.ReadCloser, error) {
	catalog, err := metadata.ListApps(ctx)
	if err != nil {
		return restore.FileHeader{}, nil, err
	}
	index := slices.IndexFunc(catalog.Apps, func(app inventory.App) bool { return app.ID == request.AppID })
	if index < 0 {
		return restore.FileHeader{}, nil, &store.Error{Code: store.NotFound}
	}
	nodes, err := metadata.ListNodes(ctx)
	if err != nil {
		return restore.FileHeader{}, nil, err
	}
	listing, err := metadata.ListDestinations(ctx)
	if err != nil {
		return restore.FileHeader{}, nil, err
	}
	// Without a matching destination there is no snapshot to read. This is the normal
	// state before the first destination, so it is an empty answer, not an agent fault.
	if !slices.ContainsFunc(listing.Destinations, func(d destination.Destination) bool {
		return request.DestinationID == uuid.Nil || d.ID == request.DestinationID
	}) {
		header := restore.FileHeader{}
		if request.Action != restore.FileSnapshots {
			header.FailureCode = "snapshot_not_found"
		}
		return header, io.NopCloser(strings.NewReader("")), nil
	}
	// Prefer the source. It can also read its local destinations.
	source := catalog.Apps[index].NodeID
	slices.SortStableFunc(nodes, func(a, b store.Node) int {
		if a.ID == b.ID {
			return 0
		}
		if a.ID == source {
			return -1
		}
		if b.ID == source {
			return 1
		}
		return 0
	})
	reader, ok := fileReader(nodes, listing, request, stream.Session)
	if !ok {
		return restore.FileHeader{}, nil, controlstream.ErrDisconnected
	}
	request.NodeID = reader
	return stream.ReadFile(ctx, request)
}

// fileReader returns the first online agent that can read the destination. It prefers
// agents with current settings. A plan change, such as a first backup that initializes
// a destination, leaves every agent briefly behind. An agent with older settings still
// reads a destination that it has saved, and refuses one that it has not.
func fileReader(nodes []store.Node, listing destination.Listing, request restore.FileRequest, session func(uuid.UUID) (controlstream.Peer, bool)) (uuid.UUID, bool) {
	behind := uuid.Nil
	for _, node := range nodes {
		if node.ApprovedAt == nil || (request.NodeID != uuid.Nil && node.ID != request.NodeID) {
			continue
		}
		peer, online := session(node.ID)
		if !online || !slices.Contains(peer.Capabilities, "files.read") {
			continue
		}
		accessible := slices.ContainsFunc(listing.Destinations, func(d destination.Destination) bool {
			return (request.DestinationID == uuid.Nil || d.ID == request.DestinationID) && (d.Location.Kind != "local" || d.NodeID == node.ID)
		})
		if !accessible {
			continue
		}
		if slices.ContainsFunc(listing.Delivery, func(d destination.Delivery) bool {
			return d.NodeID == node.ID && d.Current && d.SessionID == peer.SessionID
		}) {
			return node.ID, true
		}
		if behind == uuid.Nil {
			behind = node.ID
		}
	}
	return behind, behind != uuid.Nil
}
