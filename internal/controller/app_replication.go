package controller

import (
	"context"
	"log/slog"
	"slices"
	"time"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/agent/replication"
	"github.com/sirrobot01/synod/internal/app"
	"github.com/sirrobot01/synod/internal/controlstream"
	"github.com/sirrobot01/synod/internal/store/postgres"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
)

func runAppReplication(ctx context.Context, store *postgres.Store, transport *controlstream.Server, installation uuid.UUID, logger *slog.Logger) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		work, cancel := context.WithTimeout(ctx, 10*time.Second)
		apps, err := store.ListApps(work, installation)
		if err != nil && ctx.Err() == nil {
			logger.Warn("read app failover candidates", "error", err)
		}
		for _, item := range apps {
			if item.Tier != app.TierSingleWriter || item.DesiredLifecycle != "running" {
				continue
			}
			if err := store.ReconcileAppMove(work, installation, item.ID); err != nil && ctx.Err() == nil {
				logger.Warn("reconcile app move", "app_id", item.ID, "error", err)
			}
			if _, err := store.AdmitAppFailover(work, installation, item.ID); err != nil && ctx.Err() == nil {
				logger.Warn("admit app failover", "app_id", item.ID, "error", err)
			}
		}
		targets, err := store.AppReplicationTargets(work, installation)
		if err != nil && ctx.Err() == nil {
			logger.Warn("read app replication", "error", err)
		}
		for node, desired := range targets {
			peer, ok := transport.Session(node)
			if !ok || peer.SessionID != desired.SessionId || !slices.Contains(peer.Capabilities, replication.Capability) {
				continue
			}
			table, acknowledged, err := store.PrepareAppReplication(work, installation, node, desired)
			if err != nil {
				if ctx.Err() == nil {
					logger.Warn("prepare app replication", "node_id", node, "error", err)
				}
				continue
			}
			if !acknowledged {
				if err := transport.Send(node, peer.SessionID, &agentv1.ControllerMessage{Message: &agentv1.ControllerMessage_Replication{Replication: table}}); err != nil && ctx.Err() == nil {
					logger.Warn("send app replication", "node_id", node, "error", err)
				}
				continue
			}
			for _, assignment := range table.Apps {
				if assignment.Role != replication.RoleActive {
					continue
				}
				grant, err := store.PrepareAppWriterLease(work, installation, uuid.MustParse(assignment.AppId), node, uuid.MustParse(peer.SessionID), assignment.Epoch, false)
				if err != nil {
					if ctx.Err() == nil {
						logger.Warn("prepare app writer lease", "app_id", assignment.AppId, "error", err)
					}
					continue
				}
				if grant == nil {
					continue
				}
				if err := transport.Send(node, peer.SessionID, &agentv1.ControllerMessage{Message: &agentv1.ControllerMessage_WriterLease{WriterLease: grant}}); err != nil && ctx.Err() == nil {
					logger.Warn("send app writer lease", "app_id", assignment.AppId, "error", err)
				}
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
