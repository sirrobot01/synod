package controller

import (
	"context"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/notification"
	"github.com/sirrobot01/synod/internal/secret"
	"github.com/sirrobot01/synod/internal/store"
)

func deliverNotifications(ctx context.Context, installationID uuid.UUID, metadata *store.Store, ring *secret.Keyring, logger *slog.Logger) {
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	var refresh time.Time
	var claimFailure string
	for {
		if ctx.Err() != nil {
			return
		}
		if time.Now().After(refresh) {
			if err := metadata.RefreshNotifications(ctx, installationID, ring); err != nil && ctx.Err() == nil {
				logger.Warn("alert scan failed; retrying in 15 seconds", "error", err)
			}
			if err := metadata.ReassignMaintenance(ctx); err != nil && ctx.Err() == nil {
				logger.Warn("repository maintenance assignment failed; retrying in 15 seconds", "error", err)
			}
			refresh = time.Now().Add(15 * time.Second)
		}
		delivery, err := metadata.ClaimNotification(ctx, installationID, ring)
		// The loop runs every second. A store failure that repeats is logged once.
		if err != nil && ctx.Err() == nil && err.Error() != claimFailure {
			logger.Warn("alert queue unreadable; alerts wait", "error", err)
		}
		claimFailure = ""
		if err != nil {
			claimFailure = err.Error()
		}
		if delivery != nil && err == nil {
			sendErr := notification.Send(ctx, delivery.Settings, delivery.Message)
			// A cancelled send stays leased. The next controller retries it after expiry.
			if ctx.Err() != nil {
				return
			}
			if err := metadata.CompleteNotification(ctx, delivery.ID, delivery.Attempt, sendErr); err != nil {
				logger.Warn("alert result not saved; the alert can be sent again", "alert", delivery.ID, "error", err)
			}
			if sendErr != nil {
				// The provider error can contain the private notification URL, so only notification status shows its code.
				logger.Warn("alert not delivered; synod notification status shows the reason", "alert", delivery.ID, "attempt", delivery.Attempt)
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}
