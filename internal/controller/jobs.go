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

func deliverJobs(ctx context.Context, metadata *store.Store, stream *controlstream.Server, logger *slog.Logger) {
	tick := time.NewTicker(5 * time.Second)
	defer tick.Stop()
	// A failure that repeats is logged once.
	var last string
	for {
		if ctx.Err() != nil {
			return
		}
		err := sendJobs(ctx, metadata, stream)
		if err != nil && ctx.Err() == nil && err.Error() != last {
			logger.Warn("jobs not sent to agents; retrying", "error", err)
		}
		last = ""
		if err != nil {
			last = err.Error()
		}
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		case <-metadata.JobsAdded():
		}
	}
}

// sendJobs waits for current credentials at the worker and at an explicit device destination.
func sendJobs(ctx context.Context, metadata *store.Store, stream *controlstream.Server) error {
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
	restores, err := metadata.RestoreRequests(ctx)
	if err != nil {
		return errors.Join(append(failures, err)...)
	}
	for _, job := range restores {
		peer, online := stream.Session(job.NodeID)
		if !online || current[job.NodeID] != peer.SessionID || !slices.Contains(peer.Capabilities, "restore.exec") {
			continue
		}
		if job.DestinationID != uuid.Nil {
			target, exists := destinations[job.DestinationID]
			if !exists || (target.Location.Kind == "device" && current[target.NodeID] == "") {
				continue
			}
		}
		raw, err := json.Marshal(job)
		if err != nil {
			return err
		}
		err = stream.Send(job.NodeID, peer.SessionID, &agentv1.ControllerMessage{Message: &agentv1.ControllerMessage_RestoreJob{RestoreJob: &agentv1.RestoreJob{RequestJson: raw}}})
		if err != nil && !errors.Is(err, controlstream.ErrDisconnected) && !errors.Is(err, controlstream.ErrBusy) {
			failures = append(failures, err)
		}
	}
	proofs, err := metadata.ProofRequests(ctx)
	if err != nil {
		return errors.Join(append(failures, err)...)
	}
	for _, job := range proofs {
		peer, online := stream.Session(job.NodeID)
		if !online || current[job.NodeID] != peer.SessionID || !slices.Contains(peer.Capabilities, "proof.exec") {
			continue
		}
		if job.DestinationID != uuid.Nil {
			target, exists := destinations[job.DestinationID]
			if !exists || (target.Location.Kind == "device" && current[target.NodeID] == "") {
				continue
			}
		}
		raw, err := json.Marshal(job)
		if err != nil {
			return err
		}
		err = stream.Send(job.NodeID, peer.SessionID, &agentv1.ControllerMessage{Message: &agentv1.ControllerMessage_ProofJob{ProofJob: &agentv1.ProofJob{RequestJson: raw}}})
		if err != nil && !errors.Is(err, controlstream.ErrDisconnected) && !errors.Is(err, controlstream.ErrBusy) {
			failures = append(failures, err)
		}
	}
	return errors.Join(failures...)
}
