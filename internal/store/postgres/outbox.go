package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// outboxChannel carries one installation ID for each committed outbox row.
const outboxChannel = "synod_outbox"

// OutboxListener reports committed outbox events on a dedicated connection.
// A notification is a hint. The caller must still scan pending events on a timer.
type OutboxListener struct {
	conn           *pgxpool.Conn
	installationID uuid.UUID
}

// ListenOutbox starts listening. Call it before the first scan of pending events.
func (s *Store) ListenOutbox(ctx context.Context, installationID uuid.UUID) (*OutboxListener, error) {
	if installationID == uuid.Nil {
		return nil, &AdmissionError{Code: InvalidRequest}
	}
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return nil, admissionError(err)
	}
	if _, err := conn.Exec(ctx, "LISTEN "+outboxChannel); err != nil {
		conn.Release()
		return nil, admissionError(err)
	}
	return &OutboxListener{conn: conn, installationID: installationID}, nil
}

// Wait blocks until an event for the installation commits or ctx ends.
// An ended ctx returns false without an error and leaves the listener usable.
func (l *OutboxListener) Wait(ctx context.Context) (bool, error) {
	for {
		notification, err := l.conn.Conn().WaitForNotification(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return false, nil
			}
			return false, admissionError(err)
		}
		if notification.Payload == l.installationID.String() {
			return true, nil
		}
	}
}

// Close returns the connection to the pool.
// A connection that will not stop listening is taken out of the pool instead,
// because returning it would hand its subscriptions to the next borrower.
func (l *OutboxListener) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := l.conn.Exec(ctx, "UNLISTEN *"); err != nil {
		return errors.Join(fmt.Errorf("stop listening for installation %s: %w", l.installationID, err), l.conn.Hijack().Close(ctx))
	}
	l.conn.Release()
	return nil
}
