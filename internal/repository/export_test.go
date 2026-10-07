package repository

import (
	"context"
	"time"

	"github.com/google/uuid"
)

// ClaimMaintenanceAt claims maintenance with a moved clock, so a test can pass the handover wait.
func ClaimMaintenanceAt(ctx context.Context, location Location, credentials Credentials, installationID, nodeID uuid.UUID, revision int64, offset time.Duration) (MaintenanceState, error) {
	st, err := storage(ctx, location, credentials, false)
	if err != nil {
		return "", err
	}
	defer st.Close(context.WithoutCancel(ctx))
	r, err := open(ctx, st, credentials, nodeID.String(), func() time.Time { return time.Now().Add(offset) })
	if err != nil {
		return "", err
	}
	defer r.Close(context.WithoutCancel(ctx))
	return r.maintain(ctx, installationID, nodeID, revision)
}
