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
				// Scan errors name containers, volumes, and Docker's reason, never configuration values.
				logger.Warn("app discovery failed; the controller keeps the last app list", "code", "docker_scan_failed", "error", err)
				report = &agentv1.AppInventory{ObservedAt: timestamppb.Now(), FailureCode: "docker_scan_failed"}
			} else if inventory.Validate(report) != nil {
				logger.Warn("app discovery found more than one report can hold (512 apps, 900 KiB); the controller keeps the last app list", "code", "inventory_limit")
				report = &agentv1.AppInventory{ObservedAt: timestamppb.Now(), FailureCode: "inventory_limit"}
			}
			nextScan = time.Now().Add(time.Minute)
		}
		err := stream.Send(&agentv1.AgentMessage{Message: &agentv1.AgentMessage_Inventory{Inventory: report}})
		if err != nil && !errors.Is(err, controlstream.ErrDisconnected) && !errors.Is(err, controlstream.ErrBusy) {
			logger.Warn("app list not sent to the controller", "error", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}
