package integration_test

import (
	"fmt"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/moby/moby/client"
	goredis "github.com/redis/go-redis/v9"
	"github.com/sirrobot01/synod/internal/cluster"
	"github.com/sirrobot01/synod/internal/operation"
	"github.com/sirrobot01/synod/internal/secret"
	"github.com/sirrobot01/synod/internal/store/postgres"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/proto"
)

// A replica added through the controller must reach the primary, copy what it holds,
// and serve it. Everything below this test is proved at the plan and Store level only,
// so this is the first evidence that native replication works at all.
func TestRedisReplicaSyncsFromItsPrimary(t *testing.T) {
	for _, promoted := range []bool{false, true} {
		t.Run(map[bool]string{false: "remove", true: "refuse_promoted"}[promoted], func(t *testing.T) {
			h := start(t, 30*time.Second)
			h.awaitAgent(t, 30*time.Second)

			admission, err := h.store.CreateCluster(t.Context(), postgres.CreateClusterRequest{
				Identity: postgres.RequestIdentity{
					InstallationID: h.installationID, ActorID: "integration",
					APIOperation: postgres.CreateClusterAPIOperation, IdempotencyKey: uuid.NewString(),
				},
				RequestID: uuid.NewString(),
				Spec: cluster.CreateSpec{
					SchemaVersion: 1, Name: "cache", Engine: cluster.Redis,
					// A replicated cluster needs persistence. Without it a restart empties both
					// members and replication hands back an empty dataset.
					Redis: &cluster.RedisSpec{Version: "8.2.7", Topology: "standalone", Persistence: "aof"},
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			if op := h.await(t, admission.OperationID, 4*time.Minute); op.Status != operation.Succeeded {
				t.Fatalf("create finished %s: %s%s", op.Status, failure(op), h.receipts(t, admission.OperationID))
			}

			primary := h.redis(t, admission.ClusterID, h.memberPort(t, admission.ClusterID, "primary"))
			defer primary.Close()
			if err := primary.Set(t.Context(), "proof", "written-before-the-replica-existed", 0).Err(); err != nil {
				t.Fatal(err)
			}

			scaled := h.scale(t, admission.ClusterID, 1, nil)
			if op := h.await(t, scaled.OperationID, 4*time.Minute); op.Status != operation.Succeeded {
				t.Fatalf("scale up finished %s: %s", op.Status, failure(op))
			}

			// The member records must show what the operation claims to have built.
			members, err := h.store.ReadClusterMembers(t.Context(), h.installationID, admission.ClusterID)
			if err != nil {
				t.Fatal(err)
			}
			roles := map[string]int{}
			for _, each := range members {
				if each.State != "active" {
					t.Fatalf("member %s is %s, want active after a successful operation", each.MemberID, each.State)
				}
				roles[each.Role]++
			}
			if roles["primary"] != 1 || roles["replica"] != 1 {
				t.Fatalf("members %+v, want one active primary and one active replica", members)
			}

			// The replica answers for itself, and it holds what the primary held before it
			// existed. A readiness check that passed without a real sync would miss this.
			replica := h.redis(t, admission.ClusterID, h.memberPort(t, admission.ClusterID, "replica"))
			defer replica.Close()
			value, err := replica.Get(t.Context(), "proof").Result()
			if err != nil {
				t.Fatalf("read from the replica: %v", err)
			}
			if value != "written-before-the-replica-existed" {
				t.Fatalf("replica holds %q, want the primary's data", value)
			}
			// A replica must refuse a write. Accepting one would split the dataset.
			if err := replica.Set(t.Context(), "proof", "written-to-a-replica", 0).Err(); err == nil {
				t.Fatal("the replica accepted a write")
			}

			// Data written after the replica joined must reach it too, so the link is live
			// rather than a single copy taken at join time.
			if err := primary.Set(t.Context(), "after", "written-while-replicating", 0).Err(); err != nil {
				t.Fatal(err)
			}
			deadline := time.Now().Add(30 * time.Second)
			for {
				got, err := replica.Get(t.Context(), "after").Result()
				if err == nil && got == "written-while-replicating" {
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("the replica did not receive a later write: %q %v", got, err)
				}
				time.Sleep(100 * time.Millisecond)
			}

			// Removing the replica leaves the primary serving.
			plan, err := h.store.PreviewClusterScaleDown(t.Context(), h.installationID, admission.ClusterID, "integration")
			if err != nil {
				t.Fatal(err)
			}
			if promoted {
				credential, err := h.store.ReadClusterCredential(t.Context(), h.installationID, admission.ClusterID, postgres.SuperuserCredential)
				if err != nil {
					t.Fatal(err)
				}
				password, err := h.ring.Open(secret.Binding{InstallationID: h.installationID, OwnerType: "cluster", OwnerID: admission.ClusterID, CredentialID: credential.CredentialID, Version: credential.Version}, credential.Envelope)
				if err != nil {
					t.Fatal(err)
				}
				options := *replica.Options()
				options.Username, options.Password = credential.RoleName, string(password)
				admin := goredis.NewClient(&options)
				defer admin.Close()
				if err := admin.Do(t.Context(), "REPLICAOF", "NO", "ONE").Err(); err != nil {
					t.Fatal(err)
				}
			}
			removed := h.scale(t, admission.ClusterID, 0, &cluster.ActionConfirmation{
				PlanToken: plan.PlanToken, Confirm: true, DataDisposition: "retain",
			})
			if promoted {
				op := h.await(t, removed.OperationID, 4*time.Minute)
				if op.Status != operation.Failed || failure(op) != "redis_role_mismatch" {
					t.Fatalf("promoted member removal: %s %s%s", op.Status, failure(op), h.receipts(t, removed.OperationID))
				}
				var dispatched int
				if err := h.pool.QueryRow(t.Context(), `SELECT count(*) FROM command_dispatches WHERE operation_id=$1`, removed.OperationID).Scan(&dispatched); err != nil || dispatched != 1 {
					t.Fatalf("removal dispatched beyond its role proof: %d: %v", dispatched, err)
				}
				after, err := h.store.ReadClusterMembers(t.Context(), h.installationID, admission.ClusterID)
				if err != nil || len(after) != 2 {
					t.Fatalf("refusal changed members: %+v: %v", after, err)
				}
				for _, member := range []*goredis.Client{primary, replica} {
					if got, err := member.Get(t.Context(), "proof").Result(); err != nil || got != "written-before-the-replica-existed" {
						t.Fatalf("refusal stopped a member or lost data: %q: %v", got, err)
					}
				}
				return
			}
			if op := h.await(t, removed.OperationID, 4*time.Minute); op.Status != operation.Succeeded {
				t.Fatalf("scale down finished %s: %s", op.Status, failure(op))
			}
			after, err := h.store.ReadClusterMembers(t.Context(), h.installationID, admission.ClusterID)
			if err != nil {
				t.Fatal(err)
			}
			if len(after) != 1 || after[0].Role != "primary" {
				t.Fatalf("members %+v, want the primary alone", after)
			}
			if got, err := primary.Get(t.Context(), "proof").Result(); err != nil || got != "written-before-the-replica-existed" {
				t.Fatalf("the primary stopped serving after the removal: %q %v", got, err)
			}

			volumes, err := h.api.VolumeList(t.Context(), client.VolumeListOptions{Filters: client.Filters{}.Add("label", "io.synod.cluster="+admission.ClusterID.String(), "io.synod.kind=volume")})
			if err != nil || len(volumes.Items) != 2 {
				t.Fatalf("removal did not retain the replica volume: %v", err)
			}
		})
	}
}

// scale admits one replica count, with a confirmation when it removes.
func (h *harness) scale(t *testing.T, clusterID uuid.UUID, replicas int, confirmation *cluster.ActionConfirmation) postgres.Admission {
	t.Helper()
	found, err := h.store.GetCluster(t.Context(), h.installationID, clusterID)
	if err != nil {
		t.Fatal(err)
	}
	admission, err := h.store.AdmitClusterAction(t.Context(), postgres.ClusterActionRequest{
		Identity: postgres.RequestIdentity{
			InstallationID: h.installationID, ActorID: "integration",
			APIOperation: postgres.ScaleClusterAPIOperation, IdempotencyKey: uuid.NewString(),
		},
		RequestID: uuid.NewString(), ClusterID: clusterID, ExpectedRevision: found.DesiredRevision,
		Replicas: &replicas, Confirmation: confirmation,
	})
	if err != nil {
		t.Fatal(err)
	}
	return admission
}

// memberPort answers the host port one member's reservation holds.
func (h *harness) memberPort(t *testing.T, clusterID uuid.UUID, role string) uint16 {
	t.Helper()
	members, err := h.store.ReadClusterMembers(t.Context(), h.installationID, clusterID)
	if err != nil {
		t.Fatal(err)
	}
	holds, err := h.store.ReadPlacementHolds(t.Context(), h.installationID)
	if err != nil {
		t.Fatal(err)
	}
	for _, member := range members {
		if member.Role != role {
			continue
		}
		for _, hold := range holds {
			if hold.ResourceID != member.ResourceID {
				continue
			}
			for _, port := range hold.Ports {
				if port.Transport == "tcp" {
					return port.Number
				}
			}
		}
	}
	t.Fatalf("no %s member holds a port", role)
	return 0
}

// redis opens an application connection to one member through its published port.
func (h *harness) redis(t *testing.T, clusterID uuid.UUID, port uint16) *goredis.Client {
	t.Helper()
	credential, err := h.store.ReadClusterCredential(t.Context(), h.installationID, clusterID, postgres.ApplicationCredential)
	if err != nil {
		t.Fatal(err)
	}
	password, err := h.ring.Open(secret.Binding{
		InstallationID: h.installationID, OwnerType: "cluster", OwnerID: clusterID,
		CredentialID: credential.CredentialID, Version: credential.Version,
	}, credential.Envelope)
	if err != nil {
		t.Fatal(err)
	}
	client := goredis.NewClient(&goredis.Options{
		Addr:     net.JoinHostPort("127.0.0.1", strconv.Itoa(int(port))),
		Username: credential.RoleName, Password: string(password),
		Protocol: 2, DisableIdentity: true, MaxRetries: -1,
		ContextTimeoutEnabled: true, DialTimeout: 2 * time.Second,
		ReadTimeout: 2 * time.Second, WriteTimeout: 2 * time.Second,
	})
	deadline := time.Now().Add(30 * time.Second)
	for {
		if err := client.Ping(t.Context()).Err(); err == nil {
			return client
		}
		if time.Now().After(deadline) {
			client.Close()
			t.Fatalf("member on port %d did not answer", port)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// receipts reports what each dispatched command answered, so a mapped failure code can
// be traced to the check that produced it.
func (h *harness) receipts(t *testing.T, operationID uuid.UUID) string {
	t.Helper()
	rows, err := h.pool.Query(t.Context(), `SELECT result_bytes FROM command_results r
 JOIN command_dispatches d USING (command_id,execution_epoch,recovery_generation)
 WHERE d.operation_id=$1 ORDER BY r.command_id`, operationID)
	if err != nil {
		return " (receipts unavailable: " + err.Error() + ")"
	}
	defer rows.Close()
	var report strings.Builder
	for rows.Next() {
		var encoded []byte
		if err := rows.Scan(&encoded); err != nil {
			return " (receipt unreadable: " + err.Error() + ")"
		}
		var result agentv1.TerminalResult
		if proto.Unmarshal(encoded, &result) != nil {
			continue
		}
		fmt.Fprintf(&report, "\n  command=%s state=%s failure=%s", result.CommandId, result.State, result.FailureCode)
		for _, effect := range result.Effects {
			fmt.Fprintf(&report, "\n    effect state=%s native=%s active=%v unknown=%v",
				effect.State, effect.NativeOperationId, effect.Active, effect.OutcomeUnknown)
		}
	}
	return report.String()
}
