package executor

import (
	"context"
	"errors"
	"os"
	"slices"
	"testing"
	"time"

	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

// Each membership action succeeds at once when its result already holds, so a repeated plan is safe.
func TestEtcdMembershipActionsAreIdempotent(t *testing.T) {
	endpoint := os.Getenv("SYNOD_TEST_ETCD_ENDPOINT")
	if endpoint == "" {
		t.Skip("run scripts/test-etcd.sh")
	}
	client, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: 5 * time.Second, Logger: zap.NewNop()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { client.Close() })
	peer := "https://192.0.2.50:2380"
	learner := &agentv1.EtcdMembership{Action: "add_learner", Name: "etcd-new", PeerUrl: peer}
	for range 2 {
		if err := changeEtcdMembership(t.Context(), client, learner); err != nil {
			t.Fatal(err)
		}
	}
	members, err := client.MemberList(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	var added uint64
	count := 0
	for _, member := range members.Members {
		if slices.Contains(member.PeerURLs, peer) {
			added, count = member.ID, count+1
			if !member.IsLearner {
				t.Fatal("the new member votes before it was promoted")
			}
		}
	}
	t.Cleanup(func() {
		if _, err := client.MemberRemove(context.Background(), added); err != nil {
			t.Error(err)
		}
	})
	if count != 1 {
		t.Fatalf("learner count=%d, want one", count)
	}
	// A learner that never started never catches up, so promotion waits until the timeout.
	short, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	if err := changeEtcdMembership(short, client, &agentv1.EtcdMembership{Action: "promote", Name: "synod-test"}); err != nil {
		t.Fatalf("promoting a voting member: %v", err)
	}
	if err := changeEtcdMembership(short, client, &agentv1.EtcdMembership{Action: "promote", Name: "etcd-absent"}); err == nil {
		t.Fatal("an absent member was promoted")
	}
	if err := changeEtcdMembership(t.Context(), client, &agentv1.EtcdMembership{Action: "remove", Name: "etcd-absent"}); err != nil {
		t.Fatalf("removing an absent member: %v", err)
	}
	if err := changeEtcdMembership(t.Context(), client, &agentv1.EtcdMembership{Action: "grow", Name: "etcd-new"}); err == nil || errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("unknown action: %v", err)
	}
}
