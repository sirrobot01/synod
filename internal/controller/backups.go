package controller

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"slices"
	"time"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/controlstream"
	"github.com/sirrobot01/synod/internal/destination"
	"github.com/sirrobot01/synod/internal/store"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
)

func deliverBackups(ctx context.Context, metadata *store.Store, stream *controlstream.Server, logger *slog.Logger) {
	tick := time.NewTicker(5 * time.Second)
	defer tick.Stop()
	for {
		if ctx.Err() != nil {
			return
		}
		if err := sendBackupJobs(ctx, metadata, stream); err != nil && ctx.Err() == nil {
			logger.Warn("backup_delivery_failed", "code", "backup_delivery_unavailable")
		}
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}

// sendBackupJobs waits for current credentials at the source and at a device destination.
func sendBackupJobs(ctx context.Context, metadata *store.Store, stream *controlstream.Server) error {
	listing, err := metadata.ListDestinations(ctx)
	if err != nil {
		return err
	}
	current := make(map[uuid.UUID]string)
	for _, delivery := range listing.Delivery {
		if delivery.Current {
			current[delivery.NodeID] = delivery.SessionID
		}
	}
	destinations := make(map[uuid.UUID]destination.Destination)
	for _, destination := range listing.Destinations {
		destinations[destination.ID] = destination
	}
	var failures []error
	initializers, err := metadata.PendingInitializations(ctx)
	if err != nil {
		return err
	}
	seen := make(map[uuid.UUID]bool)
	for _, candidate := range initializers {
		target, exists := destinations[candidate.DestinationID]
		peer, online := stream.Session(candidate.Schedule.NodeID)
		if !exists || !online || seen[candidate.DestinationID] || current[candidate.Schedule.NodeID] != peer.SessionID || !slices.Contains(peer.Capabilities, "backup.exec") || (target.Location.Kind == "device" && current[target.NodeID] == "") {
			continue
		}
		seen[candidate.DestinationID] = true
		slot := candidate.Schedule.Due(time.Now())
		if slot.IsZero() {
			continue
		}
		if _, err := metadata.CreateInitializationBackup(ctx, candidate.Schedule.ID, candidate.DestinationID, slot); err != nil {
			if failure, ok := errors.AsType[*store.Error](err); ok && slices.Contains([]store.ErrorCode{store.BackupInitializing, store.NotFound, store.OperationConflict}, failure.Code) {
				continue
			}
			failures = append(failures, err)
		}
	}
	jobs, err := metadata.BackupRequests(ctx)
	if err != nil {
		return err
	}
	for _, job := range jobs {
		target, exists := destinations[job.DestinationID]
		if !exists || current[job.NodeID] == "" || (target.Location.Kind == "device" && current[target.NodeID] == "") {
			continue
		}
		peer, ok := stream.Session(job.NodeID)
		if !ok || peer.SessionID != current[job.NodeID] || !slices.Contains(peer.Capabilities, "backup.exec") {
			continue
		}
		raw, err := json.Marshal(job)
		if err != nil {
			return err
		}
		err = stream.Send(job.NodeID, peer.SessionID, &agentv1.ControllerMessage{Message: &agentv1.ControllerMessage_BackupJob{BackupJob: &agentv1.BackupJob{RequestJson: raw}}})
		if err != nil && !errors.Is(err, controlstream.ErrDisconnected) && !errors.Is(err, controlstream.ErrBusy) {
			failures = append(failures, err)
		}
	}
	return errors.Join(failures...)
}
