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

func TestRedisReplicaRetryRecoversWithoutReplacingData(t *testing.T) {
	for _, lostStorage := range []bool{false, true} {
		t.Run(map[bool]string{false: "recover", true: "lost_storage"}[lostStorage], func(t *testing.T) {
			h := start(t, 30*time.Second)
			h.awaitAgent(t, 30*time.Second)
			created, err := h.store.CreateCluster(t.Context(), postgres.CreateClusterRequest{
				Identity: postgres.RequestIdentity{InstallationID: h.installationID, ActorID: "integration",
					APIOperation: postgres.CreateClusterAPIOperation, IdempotencyKey: uuid.NewString()},
				RequestID: uuid.NewString(), Spec: cluster.CreateSpec{SchemaVersion: 1, Name: "retry-cache", Engine: cluster.Redis,
					Redis: &cluster.RedisSpec{Version: "8.2.7", Topology: "standalone", Persistence: "aof"}},
			})
			if err != nil {
				t.Fatal(err)
			}
			if op := h.await(t, created.OperationID, 4*time.Minute); op.Status != operation.Succeeded {
				t.Fatalf("create: %s %s", op.Status, failure(op))
			}
			primary := h.redis(t, created.ClusterID, h.memberPort(t, created.ClusterID, "primary"))
			defer primary.Close()
			if err := primary.Set(t.Context(), "proof", "kept-through-retry", 0).Err(); err != nil {
				t.Fatal(err)
			}
			credential, err := h.store.ReadClusterCredential(t.Context(), h.installationID, created.ClusterID, postgres.SuperuserCredential)
			if err != nil {
				t.Fatal(err)
			}
			password, err := h.ring.Open(secret.Binding{InstallationID: h.installationID, OwnerType: "cluster", OwnerID: created.ClusterID,
				CredentialID: credential.CredentialID, Version: credential.Version}, credential.Envelope)
			if err != nil {
				t.Fatal(err)
			}
			options := *primary.Options()
			options.Username, options.Password = credential.RoleName, string(password)
			admin := goredis.NewClient(&options)
			defer admin.Close()
			// Refuse the native handshake until the add ends with a known failure.
			if err := admin.ACLSetUser(t.Context(), "synod_repl", "off").Err(); err != nil {
				t.Fatal(err)
			}
			added := h.scale(t, created.ClusterID, 1, nil)
			if op := h.await(t, added.OperationID, 4*time.Minute); op.Status != operation.Failed {
				t.Fatalf("replication fault did not fail the add: %s %s", op.Status, failure(op))
			}
			members, err := h.store.ReadClusterMembers(t.Context(), h.installationID, created.ClusterID)
			if err != nil || len(members) != 2 {
				t.Fatalf("failed membership: %+v: %v", members, err)
			}
			var residual postgres.Member
			for _, member := range members {
				if member.Role == "replica" {
					residual = member
				}
			}
			if residual.State != "residual" {
				t.Fatalf("failed add did not record residual state: %+v", residual)
			}
			name := "synod-instance-" + residual.ResourceID.String()
			before, err := h.api.ContainerInspect(t.Context(), name, client.ContainerInspectOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if lostStorage {
				if _, err := h.api.ContainerRemove(t.Context(), before.Container.ID, client.ContainerRemoveOptions{Force: true}); err != nil {
					t.Fatal(err)
				}
				for _, mount := range before.Container.Mounts {
					if mount.Destination == "/data" {
						if _, err := h.api.VolumeRemove(t.Context(), mount.Name, client.VolumeRemoveOptions{}); err != nil {
							t.Fatal(err)
						}
					}
				}
			}
			if err := admin.ACLSetUser(t.Context(), "synod_repl", "on").Err(); err != nil {
				t.Fatal(err)
			}
			retried := h.scale(t, created.ClusterID, 1, nil)
			op := h.await(t, retried.OperationID, 4*time.Minute)
			if op.RetryOf == nil || *op.RetryOf != added.OperationID {
				t.Fatal("the retry lost its prior operation link")
			}
			if lostStorage {
				if op.Status != operation.Failed || failure(op) != "storage_lost" {
					t.Fatalf("retry replaced lost data: %s %s%s", op.Status, failure(op), h.receipts(t, retried.OperationID))
				}
			} else {
				if op.Status != operation.Succeeded {
					t.Fatalf("retry: %s %s%s", op.Status, failure(op), h.receipts(t, retried.OperationID))
				}
				after, err := h.api.ContainerInspect(t.Context(), name, client.ContainerInspectOptions{})
				if err != nil || after.Container.ID != before.Container.ID {
					t.Fatalf("retry replaced the replica container: %v", err)
				}
				replica := h.redis(t, created.ClusterID, h.memberPort(t, created.ClusterID, "replica"))
				defer replica.Close()
				if value, err := replica.Get(t.Context(), "proof").Result(); err != nil || value != "kept-through-retry" {
					t.Fatalf("replica data after retry: %q: %v", value, err)
				}
			}
			after, err := h.store.ReadClusterMembers(t.Context(), h.installationID, created.ClusterID)
			if err != nil || len(after) != len(members) {
				t.Fatalf("retry changed member count: %+v: %v", after, err)
			}
			for i, member := range after {
				if member.MemberID != members[i].MemberID || member.ResourceID != members[i].ResourceID {
					t.Fatal("retry replaced a member identity")
				}
				if !lostStorage && member.State != "active" {
					t.Fatalf("successful retry left member %s in %s", member.MemberID, member.State)
				}
			}
			if value, err := primary.Get(t.Context(), "proof").Result(); err != nil || value != "kept-through-retry" {
				t.Fatalf("primary data after retry: %q: %v", value, err)
			}
		})
	}
}
