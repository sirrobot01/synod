package coordinator

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

func testClient(t *testing.T) *clientv3.Client {
	t.Helper()
	endpoint := os.Getenv("SYNOD_TEST_ETCD_ENDPOINT")
	if endpoint == "" {
		t.Skip("set SYNOD_TEST_ETCD_ENDPOINT or run the task-011 fixture")
	}
	client, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: time.Second, Logger: zap.NewNop()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { client.Close() })
	return client
}

func TestLeadershipRenewalAndRevocation(t *testing.T) {
	client := testClient(t)
	coord, err := New(client, "/task-011/"+uuid.NewString())
	if err != nil {
		t.Fatal(err)
	}
	leader, err := coord.Campaign(t.Context(), uuid.New(), uuid.New(), "owner", 3)
	if err != nil {
		t.Fatal(err)
	}
	if leader.Key == "" || leader.LeaseID == 0 || leader.Revision < 1 {
		t.Fatalf("leadership evidence: %+v", leader)
	}
	if err := leader.Verify(t.Context()); err != nil {
		t.Fatal(err)
	}
	select {
	case <-time.After(4 * time.Second):
	case <-leader.Done:
		t.Fatal("lease expired while renewal was active")
	case <-t.Context().Done():
		t.Fatal(t.Context().Err())
	}
	if err := leader.Verify(t.Context()); err != nil {
		t.Fatal("renewal did not preserve lease", err)
	}
	if err := leader.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	select {
	case <-leader.Done:
	case <-time.After(time.Second):
		t.Fatal("lease loss was not signalled")
	}
	if err := leader.Verify(t.Context()); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("revoked owner verified: %v", err)
	}
}

func TestDisconnectedOwnerAndContender(t *testing.T) {
	firstClient := testClient(t)
	secondClient := testClient(t)
	prefix := "/task-011/" + uuid.NewString()
	firstCoordinator, err := New(firstClient, prefix)
	if err != nil {
		t.Fatal(err)
	}
	secondCoordinator, err := New(secondClient, prefix)
	if err != nil {
		t.Fatal(err)
	}
	installation, clusterID := uuid.New(), uuid.New()
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	first, err := firstCoordinator.Campaign(ctx, installation, clusterID, "first", 3)
	if err != nil {
		t.Fatal(err)
	}
	type outcome struct {
		leader *Leadership
		err    error
	}
	result := make(chan outcome, 1)
	go func() {
		leader, err := secondCoordinator.Campaign(ctx, installation, clusterID, "second", 3)
		result <- outcome{leader, err}
	}()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		keys, err := secondClient.Get(ctx, prefix, clientv3.WithPrefix())
		if err != nil {
			t.Fatal(err)
		}
		if len(keys.Kvs) == 2 {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("contender did not join election")
		case <-ticker.C:
		}
	}
	select {
	case result := <-result:
		t.Fatalf("contender bypassed live leader: %v", result.err)
	default:
	}
	if err := firstClient.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-first.Done:
	case <-ctx.Done():
		t.Fatal("disconnected lease did not signal loss")
	}
	if err := first.Verify(ctx); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("disconnected owner verified: %v", err)
	}
	select {
	case next := <-result:
		if next.err != nil {
			t.Fatal(next.err)
		}
		defer next.leader.Close(t.Context())
		if next.leader.Revision <= first.Revision || next.leader.LeaseID == first.LeaseID {
			t.Fatal("takeover reused lease evidence")
		}
		if err := next.leader.Verify(ctx); err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("lease expiry did not admit contender")
	}
}

func TestAuthorityValueAndUnavailableClient(t *testing.T) {
	client := testClient(t)
	coord, err := New(client, "/task-011/"+uuid.NewString())
	if err != nil {
		t.Fatal(err)
	}
	leader, err := coord.Campaign(t.Context(), uuid.New(), uuid.New(), "owner", 3)
	if err != nil {
		t.Fatal(err)
	}
	defer leader.Close(t.Context())
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := leader.Verify(ctx); err == nil {
		t.Fatal("unavailable verification succeeded")
	}
	if _, err := client.Put(t.Context(), leader.Key, "different-owner", clientv3.WithLease(clientv3.LeaseID(leader.LeaseID))); err != nil {
		t.Fatal(err)
	}
	if err := leader.Verify(t.Context()); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("changed election value verified: %v", err)
	}
}
