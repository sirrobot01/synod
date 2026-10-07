package daemon

import (
	"context"
	"log/slog"
	"math"
	"os"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/agent/journal"
	"github.com/sirrobot01/synod/internal/agent/transport"
	"github.com/sirrobot01/synod/internal/destination"
	"github.com/sirrobot01/synod/internal/secret"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"golang.org/x/sys/unix"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// destinationHealth reads free space from directories owned by this agent.
// A missing directory or an unreadable filesystem has no known capacity.
func destinationHealth(plan destination.Plan, nodeID uuid.UUID) *agentv1.DestinationHealth {
	report := &agentv1.DestinationHealth{ObservedAt: timestamppb.Now()}
	for _, entry := range plan.Destinations {
		d := entry.Destination
		if d.NodeID != nodeID || (d.Location.Kind != "local" && d.Location.Kind != "device") {
			continue
		}
		file, err := os.OpenFile(d.Location.Path, os.O_RDONLY|syscall.O_NONBLOCK, 0)
		if err != nil {
			continue
		}
		info, err := file.Stat()
		if err != nil || !info.IsDir() {
			file.Close()
			continue
		}
		var stat unix.Statfs_t
		err = unix.Fstatfs(int(file.Fd()), &stat)
		file.Close()
		if err != nil || stat.Bsize <= 0 || stat.Blocks == 0 || uint64(stat.Blocks) > math.MaxInt64/uint64(stat.Bsize) || uint64(stat.Bavail) > uint64(stat.Blocks) {
			continue
		}
		report.Destinations = append(report.Destinations, &agentv1.DestinationSpace{DestinationId: d.ID.String(), TotalBytes: int64(stat.Blocks) * int64(stat.Bsize), AvailableBytes: int64(stat.Bavail) * int64(stat.Bsize)})
	}
	return report
}

func reportDestinationHealth(ctx context.Context, cache *journal.Journal, ring *secret.Keyring, nodeID uuid.UUID, stream *transport.Client, logger *slog.Logger) {
	tick := time.NewTicker(time.Minute)
	defer tick.Stop()
	for {
		plan, err := cache.DestinationPlan(ctx, ring)
		if err == nil {
			report := destinationHealth(plan, nodeID)
			if len(report.Destinations) > 0 {
				_ = stream.Send(&agentv1.AgentMessage{Message: &agentv1.AgentMessage_DestinationHealth{DestinationHealth: report}})
			}
		} else if ctx.Err() == nil {
			logger.Warn("destination_health_failed", "code", "destination_cache_unavailable")
		}
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}
