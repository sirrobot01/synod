package controller

import (
	"context"
	"io"
	"slices"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/controlstream"
	"github.com/sirrobot01/synod/internal/destination"
	"github.com/sirrobot01/synod/internal/inventory"
	"github.com/sirrobot01/synod/internal/restore"
	"github.com/sirrobot01/synod/internal/store"
)

// readSnapshotFile selects an approved agent with current destination settings.
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
	for _, node := range nodes {
		if node.ApprovedAt == nil || (request.NodeID != uuid.Nil && node.ID != request.NodeID) {
			continue
		}
		peer, online := stream.Session(node.ID)
		if !online || !slices.Contains(peer.Capabilities, "files.read") {
			continue
		}
		current := slices.ContainsFunc(listing.Delivery, func(d destination.Delivery) bool {
			return d.NodeID == node.ID && d.Current && d.SessionID == peer.SessionID
		})
		accessible := slices.ContainsFunc(listing.Destinations, func(d destination.Destination) bool {
			return (request.DestinationID == uuid.Nil || d.ID == request.DestinationID) && (d.Location.Kind != "local" || d.NodeID == node.ID)
		})
		if !current || !accessible {
			continue
		}
		request.NodeID = node.ID
		return stream.ReadFile(ctx, request)
	}
	return restore.FileHeader{}, nil, controlstream.ErrDisconnected
}
