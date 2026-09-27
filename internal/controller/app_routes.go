package controller

import (
	"context"
	"log/slog"
	"slices"
	"time"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/controlstream"
	"github.com/sirrobot01/synod/internal/store/postgres"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
)

func runAppRoutes(ctx context.Context, store *postgres.Store, transport *controlstream.Server, installation uuid.UUID, logger *slog.Logger) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		work, cancel := context.WithTimeout(ctx, 10*time.Second)
		targets, err := store.AppRouteTargets(work, installation)
		if err != nil && ctx.Err() == nil {
			logger.Warn("read app routes", "error", err)
		}
		for node, target := range targets {
			peer, ok := transport.Session(node)
			if !ok || peer.SessionID != target.Table.SessionId || !slices.Contains(peer.Capabilities, controlstream.RouterCapability) {
				continue
			}
			table, acknowledged, err := store.PrepareAppRoutes(work, installation, node, target.Table)
			if err != nil {
				if ctx.Err() == nil {
					logger.Warn("prepare app routes", "node_id", node, "error", err)
				}
				continue
			}
			if acknowledged {
				continue
			}
			if err := transport.Send(node, peer.SessionID, &agentv1.ControllerMessage{Message: &agentv1.ControllerMessage_Routes{Routes: table}}); err != nil && ctx.Err() == nil {
				logger.Warn("send app routes", "node_id", node, "error", err)
			}
		}
		cancel()
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
