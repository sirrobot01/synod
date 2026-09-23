// Package coordinator manages etcd ownership. A lease does not fence native effects.
package coordinator

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
)

// ErrNotLeader reports that this process no longer holds elected ownership.
var ErrNotLeader = errors.New("not_leader")

// Coordinator elects one cluster owner under an etcd key prefix.
type Coordinator struct {
	client *clientv3.Client
	prefix string
}

// New builds a Coordinator on an etcd client and an absolute key prefix.
func New(client *clientv3.Client, prefix string) (*Coordinator, error) {
	if client == nil || !strings.HasPrefix(prefix, "/") || strings.HasSuffix(prefix, "/") || strings.Contains(prefix, "//") {
		return nil, fmt.Errorf("coordinator requires a client and an absolute key prefix without a trailing slash")
	}
	return &Coordinator{client: client, prefix: prefix}, nil
}

// Leadership is one elected ownership term. Done closes when the term ends.
type Leadership struct {
	OwnerID  string
	Key      string
	LeaseID  int64
	Revision int64
	Done     <-chan struct{}
	client   *clientv3.Client
	session  *concurrency.Session
}

// Campaign waits for elected ownership. Its context controls the lease lifetime.
func (c *Coordinator) Campaign(ctx context.Context, installationID, clusterID uuid.UUID, ownerID string, ttlSeconds int) (*Leadership, error) {
	if installationID == uuid.Nil || clusterID == uuid.Nil || ownerID == "" || len(ownerID) > 256 || ttlSeconds < 3 || ttlSeconds > 300 {
		return nil, fmt.Errorf("invalid election identity or lease TTL")
	}
	session, err := concurrency.NewSession(c.client, concurrency.WithTTL(ttlSeconds), concurrency.WithContext(ctx))
	if err != nil {
		return nil, fmt.Errorf("start election session: %w", err)
	}
	prefix := c.prefix + "/" + installationID.String() + "/clusters/" + clusterID.String()
	election := concurrency.NewElection(session, prefix)
	if err := election.Campaign(ctx, ownerID); err != nil {
		// A session that will not close leaves the lease alive, so that failure has
		// to reach the caller beside the one that caused it.
		return nil, errors.Join(fmt.Errorf("campaign for cluster %s: %w", clusterID, err), session.Close())
	}
	return &Leadership{OwnerID: ownerID, Key: election.Key(), LeaseID: int64(session.Lease()), Revision: election.Rev(), Done: session.Done(), client: c.client, session: session}, nil
}

// Verify performs a linearizable authority check before a durable claim or dispatch.
// An error requires the caller to stop new work, even if Done has not closed yet.
func (l *Leadership) Verify(ctx context.Context) error {
	select {
	case <-l.Done:
		return ErrNotLeader
	default:
	}
	result, err := l.client.Txn(ctx).If(
		clientv3.Compare(clientv3.CreateRevision(l.Key), "=", l.Revision),
		clientv3.Compare(clientv3.LeaseValue(l.Key), "=", l.LeaseID),
		clientv3.Compare(clientv3.Value(l.Key), "=", l.OwnerID),
	).Then(clientv3.OpGet(l.Key)).Commit()
	if err != nil {
		return fmt.Errorf("verify election authority: %w", err)
	}
	if !result.Succeeded {
		return ErrNotLeader
	}
	return nil
}

// Close stops renewal immediately and revokes the lease with the caller's deadline.
func (l *Leadership) Close(ctx context.Context) error {
	l.session.Orphan()
	_, err := l.client.Revoke(ctx, clientv3.LeaseID(l.LeaseID))
	if err != nil {
		return fmt.Errorf("revoke election lease: %w", err)
	}
	return nil
}
