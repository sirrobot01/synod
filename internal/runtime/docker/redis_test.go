package docker

import (
	"context"
	"errors"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/moby/moby/client"
	goredis "github.com/redis/go-redis/v9"
	"github.com/sirrobot01/synod/internal/runtime"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestRedisConfiguredAuthenticationAndPersistence(t *testing.T) {
	for _, persistence := range []string{"none", "rdb", "aof", "rdb+aof"} {
		t.Run(persistence, func(t *testing.T) {
			r, api, clusterID := runtimeFixture(t)
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			hostPort := listener.Addr().(*net.TCPAddr).Port
			listener.Close()
			volumeID, networkID, instanceID := uuid.NewString(), uuid.NewString(), uuid.NewString()
			for _, input := range []*agentv1.CommandInput{
				{ResourceId: volumeID, Payload: &agentv1.CommandInput_EnsureVolume{EnsureVolume: &agentv1.EnsureVolume{VolumeId: volumeID}}},
				{ResourceId: networkID, Payload: &agentv1.CommandInput_EnsureNetwork{EnsureNetwork: &agentv1.EnsureNetwork{NetworkId: networkID}}},
			} {
				if _, err := r.Execute(t.Context(), runtimeCommand(t, r, clusterID, input), nil); err != nil {
					t.Fatal(err)
				}
			}
			policy := "noeviction"
			if persistence == "rdb+aof" {
				policy = "allkeys-lru"
			}
			ensure := runtimeCommand(t, r, clusterID, &agentv1.CommandInput{
				ResourceId: instanceID,
				Secrets:    []*agentv1.SecretReference{{CredentialId: uuid.NewString(), Version: 1, Purpose: "superuser"}, {CredentialId: uuid.NewString(), Version: 1, Purpose: "application"}, {CredentialId: uuid.NewString(), Version: 1, Purpose: "replication"}},
				Payload: &agentv1.CommandInput_EnsureContainer{EnsureContainer: &agentv1.EnsureContainer{
					Engine: "redis", ImageDigest: redisFixture, VolumeId: volumeID, NetworkId: networkID,
					NetworkMode: agentv1.NetworkMode_NETWORK_MODE_BRIDGE, MemoryBytes: 64 << 20, CpuMillis: 250,
					Redis:     &agentv1.RedisContainerConfig{Port: 16379, Persistence: persistence, MaxmemoryBytes: 48 << 20, EvictionPolicy: policy},
					Ports:     []*agentv1.PortBinding{{HostAddress: "127.0.0.1", HostPort: uint32(hostPort), ContainerPort: 16379, Transport: "tcp"}},
					SecretEnv: []*agentv1.SecretEnv{{Name: "REDIS_ADMIN_PASSWORD", Purpose: "superuser"}, {Name: "REDIS_APP_PASSWORD", Purpose: "application"}, {Name: "REDIS_REPLICATION_PASSWORD", Purpose: "replication"}},
				}},
			})
			adminPassword, appPassword := uuid.NewString(), uuid.NewString()
			if _, err := r.Execute(t.Context(), ensure, runtime.Secrets{"superuser": []byte(adminPassword), "application": []byte(appPassword), "replication": []byte("runtime-test-repl")}); err != nil {
				item, inspectErr := api.ContainerInspect(t.Context(), "synod-instance-"+instanceID, client.ContainerInspectOptions{})
				if inspectErr == nil {
					t.Logf("mounts=%+v tmpfs=%+v", item.Container.Mounts, item.Container.HostConfig.Tmpfs)
				}
				t.Fatalf("create configured Redis: %v", err)
			}
			address := net.JoinHostPort("127.0.0.1", strconv.Itoa(hostPort))
			clients := make(map[string]*goredis.Client)
			for role, password := range map[string]string{"synod_admin": adminPassword, "synod_app": appPassword, "default": adminPassword, "": ""} {
				client := goredis.NewClient(&goredis.Options{Addr: address, Username: role, Password: password, Protocol: 2, DisableIdentity: true, MaxRetries: -1, DialerRetries: 1, ContextTimeoutEnabled: true, DialTimeout: time.Second, ReadTimeout: time.Second, WriteTimeout: time.Second})
				clients[role] = client
				t.Cleanup(func() { client.Close() })
			}
			admin, app := clients["synod_admin"], clients["synod_app"]
			crossed := goredis.NewClient(&goredis.Options{Addr: address, Username: "synod_app", Password: adminPassword, Protocol: 2, DisableIdentity: true, MaxRetries: -1, DialerRetries: 1, ContextTimeoutEnabled: true, DialTimeout: time.Second, ReadTimeout: time.Second, WriteTimeout: time.Second})
			t.Cleanup(func() { crossed.Close() })
			for started := range 2 {
				start := runtimeCommand(t, r, clusterID, &agentv1.CommandInput{ResourceId: instanceID, Payload: &agentv1.CommandInput_StartContainer{StartContainer: &agentv1.StartContainer{AllowInitialization: started == 0}}})
				if _, err := r.Execute(t.Context(), start, nil); err != nil {
					t.Fatalf("start %d: %v", started, err)
				}
				ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
				defer cancel()
				for {
					if err := app.Ping(ctx).Err(); err == nil {
						break
					}
					select {
					case <-ctx.Done():
						logs, _ := r.Logs(t.Context(), start, 4096)
						t.Fatalf("configured Redis did not become ready: %s", logs)
					case <-time.After(25 * time.Millisecond):
					}
				}
				drift, err := r.Drift(ctx)
				if err != nil {
					t.Fatal(err)
				}
				for _, item := range drift {
					if item.Name == "synod-instance-"+instanceID {
						t.Fatalf("configured Redis reported drift: %+v", item)
					}
				}
				wantSave, wantAOF := "", "no"
				if persistence == "rdb" || persistence == "rdb+aof" {
					wantSave = "60 1"
				}
				if persistence == "aof" || persistence == "rdb+aof" {
					wantAOF = "yes"
				}
				for key, want := range map[string]string{"port": "16379", "maxmemory": strconv.Itoa(48 << 20), "maxmemory-policy": policy, "save": wantSave, "appendonly": wantAOF, "appendfsync": "everysec"} {
					config, err := admin.ConfigGet(ctx, key).Result()
					if err != nil {
						t.Fatal(err)
					}
					if config[key] != want {
						t.Fatalf("%s=%q, want %q", key, config[key], want)
					}
				}
				if err := app.ConfigGet(ctx, "port").Err(); !goredis.HasErrorPrefix(err, "NOPERM") {
					t.Fatalf("application CONFIG access was not denied: %v", err)
				}
				if err := app.ACLList(ctx).Err(); !goredis.HasErrorPrefix(err, "NOPERM") {
					t.Fatalf("application ACL access was not denied: %v", err)
				}
				if err := clients["default"].Ping(ctx).Err(); !goredis.HasErrorPrefix(err, "WRONGPASS") {
					t.Fatalf("default user accepted administrator credential: %v", err)
				}
				if err := clients[""].Ping(ctx).Err(); !goredis.HasErrorPrefix(err, "NOAUTH") {
					t.Fatalf("unauthenticated access was not denied: %v", err)
				}
				if err := crossed.Ping(ctx).Err(); !goredis.HasErrorPrefix(err, "WRONGPASS") {
					t.Fatalf("application role accepted administrator credential: %v", err)
				}
				if started == 1 {
					value, err := app.Get(ctx, "retained-key").Result()
					if persistence == "none" {
						if !errors.Is(err, goredis.Nil) {
							t.Fatalf("disabled persistence retained data: %q %v", value, err)
						}
					} else if err != nil || value != "retained-value" {
						t.Fatalf("%s lost data on stop/start: %q %v", persistence, value, err)
					}
					break
				}
				if err := app.Set(ctx, "retained-key", "retained-value", 0).Err(); err != nil {
					t.Fatal(err)
				}
				stop := runtimeCommand(t, r, clusterID, &agentv1.CommandInput{ResourceId: instanceID, Payload: &agentv1.CommandInput_StopContainer{StopContainer: &agentv1.StopContainer{GracePeriod: durationpb.New(10 * time.Second)}}})
				if _, err := r.Execute(t.Context(), stop, nil); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}
