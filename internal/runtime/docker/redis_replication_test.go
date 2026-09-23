package docker

import (
	"context"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/moby/moby/client"
	goredis "github.com/redis/go-redis/v9"
	"github.com/sirrobot01/synod/internal/engine"
	redisengine "github.com/sirrobot01/synod/internal/engine/redis"
	"github.com/sirrobot01/synod/internal/runtime"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestRedisNativeReplication(t *testing.T) {
	r, api, clusterID := runtimeFixture(t)
	ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
	defer cancel()
	networkID := uuid.NewString()
	if _, err := r.Execute(ctx, runtimeCommand(t, r, clusterID, &agentv1.CommandInput{ResourceId: networkID, Payload: &agentv1.CommandInput_EnsureNetwork{EnsureNetwork: &agentv1.EnsureNetwork{NetworkId: networkID}}}), nil); err != nil {
		t.Fatal(err)
	}
	adminPassword, appPassword := uuid.NewString(), uuid.NewString()
	replicationPassword := `replication space " quote \ slash $ dollar # hash`
	secrets := runtime.Secrets{"superuser": []byte(adminPassword), "application": []byte(appPassword), "replication": []byte(replicationPassword)}
	create := func(upstream *agentv1.RedisUpstream, material runtime.Secrets) (string, engine.Endpoint) {
		t.Helper()
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		port := listener.Addr().(*net.TCPAddr).Port
		listener.Close()
		volumeID, instanceID := uuid.NewString(), uuid.NewString()
		if _, err := r.Execute(ctx, runtimeCommand(t, r, clusterID, &agentv1.CommandInput{ResourceId: volumeID, Payload: &agentv1.CommandInput_EnsureVolume{EnsureVolume: &agentv1.EnsureVolume{VolumeId: volumeID}}}), nil); err != nil {
			t.Fatal(err)
		}
		ensure := runtimeCommand(t, r, clusterID, &agentv1.CommandInput{ResourceId: instanceID,
			Secrets: []*agentv1.SecretReference{{CredentialId: uuid.NewString(), Version: 1, Purpose: "superuser"}, {CredentialId: uuid.NewString(), Version: 1, Purpose: "application"}, {CredentialId: uuid.NewString(), Version: 1, Purpose: "replication"}},
			Payload: &agentv1.CommandInput_EnsureContainer{EnsureContainer: &agentv1.EnsureContainer{
				Engine: "redis", ImageDigest: redisFixture, VolumeId: volumeID, NetworkId: networkID, NetworkMode: agentv1.NetworkMode_NETWORK_MODE_BRIDGE, MemoryBytes: 64 << 20, CpuMillis: 250,
				Redis:     &agentv1.RedisContainerConfig{Port: 16379, Persistence: "rdb+aof", MaxmemoryBytes: 48 << 20, EvictionPolicy: "noeviction", Upstream: upstream},
				Ports:     []*agentv1.PortBinding{{HostAddress: "127.0.0.1", HostPort: uint32(port), ContainerPort: 16379, Transport: "tcp"}},
				SecretEnv: []*agentv1.SecretEnv{{Name: "REDIS_ADMIN_PASSWORD", Purpose: "superuser"}, {Name: "REDIS_APP_PASSWORD", Purpose: "application"}, {Name: "REDIS_REPLICATION_PASSWORD", Purpose: "replication"}},
			}},
		})
		if _, err := r.Execute(ctx, ensure, material); err != nil {
			t.Fatalf("create Redis member: %v", err)
		}
		if _, err := r.Execute(ctx, runtimeCommand(t, r, clusterID, &agentv1.CommandInput{ResourceId: instanceID, Payload: &agentv1.CommandInput_StartContainer{StartContainer: &agentv1.StartContainer{AllowInitialization: true}}}), nil); err != nil {
			t.Fatalf("start Redis member: %v", err)
		}
		return instanceID, engine.Endpoint{Address: "127.0.0.1", Port: uint16(port), Database: "0", Role: "synod_admin"}
	}
	connect := func(endpoint engine.Endpoint, role, password string) *goredis.Client {
		t.Helper()
		c := goredis.NewClient(&goredis.Options{Addr: net.JoinHostPort(endpoint.Address, strconv.Itoa(int(endpoint.Port))), Username: role, Password: password, Protocol: 2, DisableIdentity: true, MaxRetries: -1, DialerRetries: 1, ContextTimeoutEnabled: true, DialTimeout: time.Second, ReadTimeout: time.Second, WriteTimeout: time.Second})
		t.Cleanup(func() { c.Close() })
		return c
	}
	await := func(endpoint engine.Endpoint) {
		t.Helper()
		deadline, done := context.WithTimeout(ctx, 25*time.Second)
		defer done()
		for {
			observed := redisengine.New().Probe(deadline, endpoint, []byte(adminPassword), 1)
			if observed.Ready.State == engine.ProbePassed {
				return
			}
			select {
			case <-deadline.Done():
				t.Fatalf("Redis member readiness failed: %s", observed.Ready.Reason)
			case <-time.After(50 * time.Millisecond):
			}
		}
	}
	stop := func(id string) {
		t.Helper()
		if _, err := r.Execute(ctx, runtimeCommand(t, r, clusterID, &agentv1.CommandInput{ResourceId: id, Payload: &agentv1.CommandInput_StopContainer{StopContainer: &agentv1.StopContainer{GracePeriod: durationpb.New(15 * time.Second)}}}), nil); err != nil {
			t.Fatalf("stop member: %v", err)
		}
	}
	start := func(id string) {
		t.Helper()
		if _, err := r.Execute(ctx, runtimeCommand(t, r, clusterID, &agentv1.CommandInput{ResourceId: id, Payload: &agentv1.CommandInput_StartContainer{StartContainer: &agentv1.StartContainer{}}}), nil); err != nil {
			t.Fatalf("restart member: %v", err)
		}
	}
	primaryID, primary := create(nil, secrets)
	primary.RedisReplication = &engine.RedisReplicationCheck{ExpectedRole: "primary"}
	await(primary)
	inspected, err := api.ContainerInspect(ctx, "synod-instance-"+primaryID, client.ContainerInspectOptions{})
	if err != nil {
		t.Fatal(err)
	}
	address := ""
	for _, network := range inspected.Container.NetworkSettings.Networks {
		if network.IPAddress.IsValid() {
			address = network.IPAddress.String()
		}
	}
	if address == "" {
		t.Fatal("primary has no fixture network address")
	}
	upstream := &agentv1.RedisUpstream{Address: address, Port: 16379}
	replicaID, replica := create(upstream, secrets)
	replica.RedisReplication = &engine.RedisReplicationCheck{ExpectedRole: "replica", Upstream: &engine.RedisUpstream{Address: address, Port: 16379}}
	app := connect(primary, "synod_app", appPassword)
	reader := connect(replica, "synod_app", appPassword)
	replication := connect(primary, "synod_repl", replicationPassword)
	for cycle := range 2 {
		await(primary)
		await(replica)
		drift, err := r.Drift(ctx)
		if err != nil {
			t.Fatal("inspect Redis member configuration failed")
		}
		for _, item := range drift {
			if item.Name == "synod-instance-"+primaryID || item.Name == "synod-instance-"+replicaID {
				t.Fatalf("Redis member configuration changed: %s", item.Detail)
			}
		}
		if cycle == 1 {
			for _, member := range []*goredis.Client{app, reader} {
				if value, err := member.Get(ctx, "fixture").Result(); err != nil || value != "replicated-value" {
					t.Fatal("member lost the fixture value after restart")
				}
			}
		}
		if err := replication.Ping(ctx).Err(); err != nil {
			t.Fatal("replication credential cannot authenticate")
		}
		for _, err := range []error{replication.Get(ctx, "fixture").Err(), replication.Set(ctx, "forbidden", "value", 0).Err(), replication.Publish(ctx, "forbidden", "value").Err()} {
			if !goredis.HasErrorPrefix(err, "NOPERM") {
				t.Fatal("replication credential exceeded its restricted ACL")
			}
		}
		connection := app.Conn()
		if err := connection.Set(ctx, "fixture", "replicated-value", 0).Err(); err != nil {
			connection.Close()
			t.Fatal("primary application write failed")
		}
		acknowledged, err := connection.Wait(ctx, 1, 5*time.Second).Result()
		connection.Close()
		if err != nil || acknowledged != 1 {
			t.Fatalf("replication acknowledgement count=%d", acknowledged)
		}
		if value, err := reader.Get(ctx, "fixture").Result(); err != nil || value != "replicated-value" {
			t.Fatal("replica did not copy the primary value")
		}
		if err := reader.Set(ctx, "forbidden", "value", 0).Err(); !goredis.HasErrorPrefix(err, "READONLY") {
			t.Fatal("replica accepted an application write")
		}
		if err := reader.ACLList(ctx).Err(); !goredis.HasErrorPrefix(err, "NOPERM") {
			t.Fatal("replica application can inspect ACLs")
		}
		if err := reader.ConfigSet(ctx, "maxmemory-policy", "noeviction").Err(); !goredis.HasErrorPrefix(err, "NOPERM") {
			t.Fatal("replica application can change native configuration")
		}
		if cycle == 0 {
			stop(primaryID)
			stop(replicaID)
			start(primaryID)
			await(primary)
			start(replicaID)
		}
	}
	wrongUpstream := replica
	wrongUpstream.RedisReplication = &engine.RedisReplicationCheck{ExpectedRole: "replica", Upstream: &engine.RedisUpstream{Address: address, Port: 16380}}
	if observed := redisengine.New().Probe(ctx, wrongUpstream, []byte(adminPassword), 1); observed.Ready.State != engine.ProbeFailed {
		t.Fatal("wrong upstream passed native readiness")
	}
	wrongSecrets := runtime.Secrets{"superuser": []byte(adminPassword), "application": []byte(appPassword), "replication": []byte("wrong-replication-password")}
	_, wrongReplica := create(upstream, wrongSecrets)
	wrongAdmin := connect(wrongReplica, "synod_admin", adminPassword)
	wrongDeadline, wrongDone := context.WithTimeout(ctx, 10*time.Second)
	defer wrongDone()
	for {
		if err := wrongAdmin.Do(wrongDeadline, "ROLE").Err(); err == nil {
			break
		}
		select {
		case <-wrongDeadline.Done():
			t.Fatal("wrong-credential fixture did not start its native listener")
		case <-time.After(50 * time.Millisecond):
		}
	}
	wrongReplica.RedisReplication = replica.RedisReplication
	if observed := redisengine.New().Probe(ctx, wrongReplica, []byte(adminPassword), 1); observed.Ready.State != engine.ProbeFailed {
		t.Fatal("wrong replication credential passed native readiness")
	}
	stop(primaryID)
	deadline, done := context.WithTimeout(ctx, 10*time.Second)
	defer done()
	for {
		observed := redisengine.New().Probe(deadline, replica, []byte(adminPassword), 1)
		if observed.Ready.State == engine.ProbeFailed {
			break
		}
		select {
		case <-deadline.Done():
			t.Fatal("disconnected replica remained ready")
		case <-time.After(50 * time.Millisecond):
		}
	}
	if _, err := reader.Get(ctx, "fixture").Result(); !goredis.HasErrorPrefix(err, "MASTERDOWN") {
		t.Fatal("disconnected replica served stale application data")
	}
}
