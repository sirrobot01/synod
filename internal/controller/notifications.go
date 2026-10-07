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
	for {
		if ctx.Err() != nil {
			return
		}
		if time.Now().After(refresh) {
			if err := metadata.RefreshNotifications(ctx, installationID, ring); err != nil && ctx.Err() == nil {
				logger.Warn("notification_scan_failed", "code", "notification_store_unavailable")
			}
			refresh = time.Now().Add(15 * time.Second)
		}
		delivery, err := metadata.ClaimNotification(ctx, installationID, ring)
		if err != nil && ctx.Err() == nil {
			logger.Warn("notification_claim_failed", "code", "notification_store_unavailable")
		}
		if delivery != nil && err == nil {
			sendErr := notification.Send(ctx, delivery.Settings, delivery.Message)
			// A cancelled send stays leased. The next controller retries it after expiry.
			if ctx.Err() != nil {
				return
			}
			if err := metadata.CompleteNotification(ctx, delivery.ID, delivery.Attempt, sendErr); err != nil {
				logger.Warn("notification_record_failed", "alert_id", delivery.ID, "code", "notification_store_unavailable")
			}
			if sendErr != nil {
				logger.Warn("notification_send_failed", "alert_id", delivery.ID, "attempt", delivery.Attempt, "code", "delivery_failed")
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}
