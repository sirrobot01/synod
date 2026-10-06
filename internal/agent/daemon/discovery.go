package daemon

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/moby/moby/client"
	"github.com/sirrobot01/synod/internal/agent/discovery"
	"github.com/sirrobot01/synod/internal/agent/transport"
	"github.com/sirrobot01/synod/internal/controlstream"
	"github.com/sirrobot01/synod/internal/inventory"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// discover refreshes metadata each minute and resends it after a connection loss.
func discover(ctx context.Context, api *client.Client, stream *transport.Client, logger *slog.Logger) {
	tick := time.NewTicker(5 * time.Second)
	defer tick.Stop()
	var report *agentv1.AppInventory
	var nextScan time.Time
	for {
		if ctx.Err() != nil {
			return
		}
		if report == nil || time.Now().After(nextScan) {
			scan, cancel := context.WithTimeout(ctx, time.Minute)
			var err error
			report, err = discovery.Scan(scan, api, nil)
			cancel()
			if ctx.Err() != nil {
				return
			}
			if err != nil {
				// Docker errors can contain container configuration. Log only a fixed code.
				logger.Warn("app_discovery_failed", "code", "docker_scan_failed")
				report = &agentv1.AppInventory{ObservedAt: timestamppb.Now(), FailureCode: "docker_scan_failed"}
			} else if inventory.Validate(report) != nil {
				logger.Warn("app_discovery_failed", "code", "inventory_limit")
				report = &agentv1.AppInventory{ObservedAt: timestamppb.Now(), FailureCode: "inventory_limit"}
			}
			nextScan = time.Now().Add(time.Minute)
		}
		err := stream.Send(&agentv1.AgentMessage{Message: &agentv1.AgentMessage_Inventory{Inventory: report}})
		if err != nil && !errors.Is(err, controlstream.ErrDisconnected) && !errors.Is(err, controlstream.ErrBusy) {
			logger.Warn("app_inventory_send_failed", "error", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}
