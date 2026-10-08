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
	"github.com/sirrobot01/synod/internal/secret"
	"github.com/sirrobot01/synod/internal/store"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
)

// deliverDestinations resends until the current agent session confirms a durable save.
func deliverDestinations(ctx context.Context, installationID uuid.UUID, metadata *store.Store, ring *secret.Keyring, stream *controlstream.Server, logger *slog.Logger) {
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	for {
		if ctx.Err() != nil {
			return
		}
		if err := sendDestinationPlans(ctx, installationID, metadata, ring, stream); err != nil && ctx.Err() == nil {
			logger.Warn("destination_delivery_failed", "code", "destination_delivery_unavailable")
		}
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}

func sendDestinationPlans(ctx context.Context, installationID uuid.UUID, metadata *store.Store, ring *secret.Keyring, stream *controlstream.Server) error {
	listing, err := metadata.ListDestinations(ctx)
	if err != nil {
		return err
	}
	var failures []error
	for _, delivery := range listing.Delivery {
		if delivery.Current {
			continue
		}
		peer, ok := stream.Session(delivery.NodeID)
		if !ok || !slices.Contains(peer.Capabilities, "destinations") {
			continue
		}
		plan, err := metadata.DestinationPlan(ctx, installationID, delivery.NodeID, uuid.MustParse(peer.SessionID), ring)
		if err != nil {
			failures = append(failures, err)
			continue
		}
		encoded, err := json.Marshal(plan)
		if err != nil {
			failures = append(failures, err)
			continue
		}
		err = stream.Send(delivery.NodeID, peer.SessionID, &agentv1.ControllerMessage{Message: &agentv1.ControllerMessage_DestinationPlan{DestinationPlan: &agentv1.DestinationPlan{SettingsJson: encoded}}})
		clear(encoded)
		if err != nil && !errors.Is(err, controlstream.ErrDisconnected) && !errors.Is(err, controlstream.ErrBusy) {
			failures = append(failures, err)
		}
	}
	return errors.Join(failures...)
}
