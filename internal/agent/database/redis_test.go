package database

import (
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/agent/database/enginefixture"
)

func TestRedisReplicatesAndPromotes(t *testing.T) {
	if os.Getenv("SYNOD_TEST_DOCKER_HOST") == "" {
		t.Skip("set SYNOD_TEST_DOCKER_HOST for the isolated Docker fixture")
	}
	network := "synod-redis-" + uuid.NewString()[:8]
	if _, err := enginefixture.Docker(t, "", "network", "create", network); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = enginefixture.Docker(t, "", "network", "rm", network) })
	tls := enginefixture.TLS(t)
	config := Redis{AdminPassword: "admin-" + uuid.NewString(), ReplicationPassword: "replica-" + uuid.NewString()}
	primaryName, replicaName := network+"-a", network+"-b"
	primary := enginefixture.StartRedis(t, network, primaryName, tls, config.AdminPassword, config.ReplicationPassword, RedisSocket)
	replica := enginefixture.StartRedis(t, network, replicaName, tls, config.AdminPassword, config.ReplicationPassword, RedisSocket)
	ctx := t.Context()
	if err := config.EnsurePrimary(ctx, primary); err != nil {
		t.Fatal(err)
	}
	if _, err := config.cli(ctx, primary, "", "SET", "k1", "v1"); err != nil {
		t.Fatal(err)
	}
	endpoint := Endpoint{Host: primaryName, Port: 6379}
	for range 2 {
		if err := config.EnsureReplica(ctx, replica, endpoint); err != nil {
			t.Fatal(err)
		}
	}
	enginefixture.Wait(t, 60*time.Second, func() error {
		status, err := config.Status(ctx, replica)
		if err != nil || status.State != StateStreaming {
			return fmt.Errorf("replica: %+v %v", status, err)
		}
		return nil
	})
	if out, err := config.cli(ctx, replica, "", "GET", "k1"); err != nil || !strings.Contains(string(out), "v1") {
		t.Fatalf("replicated key: %q %v", out, err)
	}
	if _, err := config.cli(ctx, replica, "", "SET", "k2", "x"); err == nil {
		t.Fatal("the replica accepted a write")
	}
	if out, err := replica(ctx, nil, "", "ps", "-eo", "args"); err == nil && strings.Contains(string(out), config.ReplicationPassword) {
		t.Fatal("a password reached the process list")
	}
	if _, err := enginefixture.Docker(t, "", "stop", "-t", "3", primaryName); err != nil {
		t.Fatal(err)
	}
	enginefixture.Wait(t, 10*time.Second, func() error {
		status, err := config.Status(ctx, replica)
		if err != nil || status.State != "disconnected" {
			return fmt.Errorf("complete standby after upstream loss: %+v %v", status, err)
		}
		return nil
	})
	if err := config.EnsurePrimary(ctx, replica); err != nil {
		t.Fatal(err)
	}
	if _, err := config.cli(ctx, replica, "", "SET", "k3", "after"); err != nil {
		t.Fatalf("write after promotion: %v", err)
	}
	status, err := config.Status(ctx, replica)
	if err != nil || status.State != StateFailed {
		t.Fatalf("a primary reports a replica state: %+v %v", status, err)
	}
}
