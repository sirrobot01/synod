package integration_test

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/moby/moby/client"
	goredis "github.com/redis/go-redis/v9"
	"github.com/sirrobot01/synod/internal/cluster"
	"github.com/sirrobot01/synod/internal/operation"
	"github.com/sirrobot01/synod/internal/secret"
	"github.com/sirrobot01/synod/internal/store/postgres"
)

func TestRedisStartProvesReplication(t *testing.T) {
	for _, scenario := range []struct{ name, persistence, fault string }{
		{name: "rdb", persistence: "rdb"},
		{name: "aof", persistence: "aof"},
		{name: "combined", persistence: "rdb+aof"},
		{name: "wrong_role", persistence: "aof", fault: "redis_role_mismatch"},
		{name: "disconnected", persistence: "aof", fault: "redis_replication_link_down"},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			h := start(t, 30*time.Second)
			h.awaitAgent(t, 30*time.Second)
			created, err := h.store.CreateCluster(t.Context(), postgres.CreateClusterRequest{
				Identity:  postgres.RequestIdentity{InstallationID: h.installationID, ActorID: "integration", APIOperation: postgres.CreateClusterAPIOperation, IdempotencyKey: uuid.NewString()},
				RequestID: uuid.NewString(), Spec: cluster.CreateSpec{SchemaVersion: 1, Name: "lifecycle-cache", Engine: cluster.Redis,
					Redis: &cluster.RedisSpec{Version: "8.2.7", Topology: "standalone", Persistence: scenario.persistence}},
			})
			if err != nil {
				t.Fatal(err)
			}
			if op := h.await(t, created.OperationID, 4*time.Minute); op.Status != operation.Succeeded {
				t.Fatalf("create: %s %s", op.Status, failure(op))
			}
			primary := h.redis(t, created.ClusterID, h.memberPort(t, created.ClusterID, "primary"))
			defer primary.Close()
			if err := primary.Set(t.Context(), "proof", "kept-through-start", 0).Err(); err != nil {
				t.Fatal(err)
			}
			added := h.scale(t, created.ClusterID, 1, nil)
			if op := h.await(t, added.OperationID, 4*time.Minute); op.Status != operation.Succeeded {
				t.Fatalf("scale: %s %s", op.Status, failure(op))
			}
			replica := h.redis(t, created.ClusterID, h.memberPort(t, created.ClusterID, "replica"))
			defer replica.Close()
			if scenario.fault == "" {
				h.actAndAwait(t, created.ClusterID, postgres.StopClusterAPIOperation)
				h.actAndAwait(t, created.ClusterID, postgres.StartClusterAPIOperation)
				for _, member := range []*goredis.Client{primary, replica} {
					if value, err := member.Get(t.Context(), "proof").Result(); err != nil || value != "kept-through-start" {
						t.Fatalf("data after start: %q: %v", value, err)
					}
				}
				if err := replica.Set(t.Context(), "forbidden", "write", 0).Err(); err == nil {
					t.Fatal("replica accepted a write after start")
				}
				h.actAndAwait(t, created.ClusterID, postgres.DeleteClusterAPIOperation)
				volumes, err := h.api.VolumeList(t.Context(), client.VolumeListOptions{Filters: client.Filters{}.Add("label", "io.synod.cluster="+created.ClusterID.String(), "io.synod.kind=volume")})
				if err != nil || len(volumes.Items) != 2 {
					t.Fatalf("delete did not retain both volumes: %v", err)
				}
				return
			}
			credential, err := h.store.ReadClusterCredential(t.Context(), h.installationID, created.ClusterID, postgres.SuperuserCredential)
			if err != nil {
				t.Fatal(err)
			}
			password, err := h.ring.Open(secret.Binding{InstallationID: h.installationID, OwnerType: "cluster", OwnerID: created.ClusterID, CredentialID: credential.CredentialID, Version: credential.Version}, credential.Envelope)
			if err != nil {
				t.Fatal(err)
			}
			options := *replica.Options()
			if scenario.name == "disconnected" {
				options = *primary.Options()
			}
			options.Username, options.Password = credential.RoleName, string(password)
			admin := goredis.NewClient(&options)
			defer admin.Close()
			if scenario.name == "wrong_role" {
				if err := admin.Do(t.Context(), "REPLICAOF", "NO", "ONE").Err(); err != nil {
					t.Fatal(err)
				}
			} else {
				if err := admin.ACLSetUser(t.Context(), "synod_repl", "off").Err(); err != nil {
					t.Fatal(err)
				}
				if err := admin.Do(t.Context(), "CLIENT", "KILL", "TYPE", "replica").Err(); err != nil {
					t.Fatal(err)
				}
			}
			before, err := h.store.GetCluster(t.Context(), h.installationID, created.ClusterID)
			if err != nil {
				t.Fatal(err)
			}
			admitted, err := h.store.AdmitClusterAction(t.Context(), postgres.ClusterActionRequest{
				Identity:  postgres.RequestIdentity{InstallationID: h.installationID, ActorID: "integration", APIOperation: postgres.StartClusterAPIOperation, IdempotencyKey: uuid.NewString()},
				RequestID: uuid.NewString(), ClusterID: created.ClusterID, ExpectedRevision: before.DesiredRevision,
			})
			if err != nil {
				t.Fatal(err)
			}
			op := h.await(t, admitted.OperationID, 4*time.Minute)
			if op.Status != operation.Failed || failure(op) != scenario.fault {
				t.Fatalf("unsafe start: %s %s%s", op.Status, failure(op), h.receipts(t, admitted.OperationID))
			}
			after, err := h.store.GetCluster(t.Context(), h.installationID, created.ClusterID)
			if err != nil || after.ObservedRevision != before.ObservedRevision {
				t.Fatalf("failed start published observed revision: %+v: %v", after, err)
			}
			if value, err := primary.Get(t.Context(), "proof").Result(); err != nil || value != "kept-through-start" {
				t.Fatalf("fault lost primary data: %q: %v", value, err)
			}
		})
	}
}
