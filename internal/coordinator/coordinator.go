// Package coordinator manages PostgreSQL cluster ownership leases. A lease does not fence native effects.
package coordinator

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ErrNotLeader reports that this process no longer holds elected ownership.
var ErrNotLeader = errors.New("not_leader")

// Coordinator elects one cluster owner in metadata PostgreSQL.
type Coordinator struct {
	pool *pgxpool.Pool
}

// New builds a Coordinator on a metadata database pool.
func New(pool *pgxpool.Pool) (*Coordinator, error) {
	if pool == nil {
		return nil, fmt.Errorf("coordinator requires a database pool")
	}
	return &Coordinator{pool: pool}, nil
}

// Leadership is one elected ownership term. Done closes when the term ends.
type Leadership struct {
	OwnerID        string
	LeaseID        int64
	Revision       int64
	Done           <-chan struct{}
	done           chan struct{}
	closeOnce      sync.Once
	pool           *pgxpool.Pool
	installationID uuid.UUID
	clusterID      uuid.UUID
	ttlSeconds     int
	cancelRenew    context.CancelFunc
}

const acquireQuery = `
INSERT INTO cluster_leases (installation_id, cluster_id, owner_id, lease_id, revision, expires_at)
VALUES ($1, $2, $3, nextval('cluster_lease_id_seq'), 1, clock_timestamp() + $4 * interval '1 second')
ON CONFLICT (installation_id, cluster_id) DO UPDATE
SET owner_id = EXCLUDED.owner_id,
    lease_id = EXCLUDED.lease_id,
    revision = cluster_leases.revision + 1,
    expires_at = EXCLUDED.expires_at
WHERE cluster_leases.expires_at <= clock_timestamp()
RETURNING lease_id, revision;
`

// Campaign waits for elected ownership. Its context controls the wait lifetime.
func (c *Coordinator) Campaign(ctx context.Context, installationID, clusterID uuid.UUID, ownerID string, ttlSeconds int) (*Leadership, error) {
	if installationID == uuid.Nil || clusterID == uuid.Nil || ownerID == "" || len(ownerID) > 256 || ttlSeconds < 3 || ttlSeconds > 300 {
		return nil, fmt.Errorf("invalid election identity or lease TTL")
	}

	var leaseID, revision int64
	err := c.pool.QueryRow(ctx, acquireQuery, installationID, clusterID, ownerID, ttlSeconds).Scan(&leaseID, &revision)
	if err == nil {
		return c.newLeadership(installationID, clusterID, ownerID, leaseID, revision, ttlSeconds), nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, fmt.Errorf("campaign for cluster %s: %w", clusterID, err)
	}

	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-ticker.C:
			err := c.pool.QueryRow(ctx, acquireQuery, installationID, clusterID, ownerID, ttlSeconds).Scan(&leaseID, &revision)
			if err == nil {
				return c.newLeadership(installationID, clusterID, ownerID, leaseID, revision, ttlSeconds), nil
			}
			if !errors.Is(err, pgx.ErrNoRows) {
				if ctx.Err() != nil {
					return nil, ctx.Err()
				}
				return nil, fmt.Errorf("campaign for cluster %s: %w", clusterID, err)
			}
		}
	}
}

func (c *Coordinator) newLeadership(installationID, clusterID uuid.UUID, ownerID string, leaseID, revision int64, ttlSeconds int) *Leadership {
	done := make(chan struct{})
	renewCtx, cancelRenew := context.WithCancel(context.Background())
	l := &Leadership{
		OwnerID:        ownerID,
		LeaseID:        leaseID,
		Revision:       revision,
		Done:           done,
		done:           done,
		pool:           c.pool,
		installationID: installationID,
		clusterID:      clusterID,
		ttlSeconds:     ttlSeconds,
		cancelRenew:    cancelRenew,
	}
	go l.renewLoop(renewCtx)
	return l
}

func (l *Leadership) renewLoop(ctx context.Context) {
	interval := time.Duration(l.ttlSeconds) * time.Second / 3
	if interval < 250*time.Millisecond {
		interval = 250 * time.Millisecond
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			renewCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
			err := l.renew(renewCtx)
			cancel()
			if err != nil {
				l.closeDone()
				return
			}
		}
	}
}

const renewQuery = `
UPDATE cluster_leases
SET expires_at = clock_timestamp() + $1 * interval '1 second'
WHERE installation_id = $2
  AND cluster_id = $3
  AND owner_id = $4
  AND lease_id = $5
  AND revision = $6
  AND expires_at > clock_timestamp()
RETURNING expires_at;
`

func (l *Leadership) renew(ctx context.Context) error {
	var expires time.Time
	return l.pool.QueryRow(ctx, renewQuery, l.ttlSeconds, l.installationID, l.clusterID, l.OwnerID, l.LeaseID, l.Revision).Scan(&expires)
}

func (l *Leadership) closeDone() {
	l.closeOnce.Do(func() {
		close(l.done)
	})
}

const verifyQuery = `
SELECT EXISTS(
	SELECT 1 FROM cluster_leases
	WHERE installation_id = $1
	  AND cluster_id = $2
	  AND owner_id = $3
	  AND lease_id = $4
	  AND revision = $5
	  AND expires_at > clock_timestamp()
);
`

// Verify performs an authority check before a durable claim or dispatch.
// An error requires the caller to stop new work, even if Done has not closed yet.
func (l *Leadership) Verify(ctx context.Context) error {
	select {
	case <-l.Done:
		return ErrNotLeader
	default:
	}
	var exists bool
	if err := l.pool.QueryRow(ctx, verifyQuery, l.installationID, l.clusterID, l.OwnerID, l.LeaseID, l.Revision).Scan(&exists); err != nil {
		return fmt.Errorf("verify election authority: %w", err)
	}
	if !exists {
		l.closeDone()
		return ErrNotLeader
	}
	return nil
}

const releaseQuery = `
UPDATE cluster_leases
SET expires_at = clock_timestamp() - interval '1 second'
WHERE installation_id = $1
  AND cluster_id = $2
  AND owner_id = $3
  AND lease_id = $4
  AND revision = $5;
`

// Close stops renewal immediately and revokes the lease.
func (l *Leadership) Close(ctx context.Context) error {
	l.cancelRenew()
	l.closeDone()
	if _, err := l.pool.Exec(ctx, releaseQuery, l.installationID, l.clusterID, l.OwnerID, l.LeaseID, l.Revision); err != nil {
		return fmt.Errorf("revoke election lease: %w", err)
	}
	return nil
}
