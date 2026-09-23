package provision

import (
	"bytes"
	"errors"
	"testing"

	"github.com/sirrobot01/synod/internal/cluster"
	"github.com/sirrobot01/synod/internal/placement"
	"github.com/sirrobot01/synod/internal/secret"
	"github.com/sirrobot01/synod/internal/store/postgres"
	"google.golang.org/protobuf/proto"
)

func TestRedisProvisionConfiguresPersistenceAndDistinctCredentials(t *testing.T) {
	for _, persistence := range []string{"none", "rdb", "aof", "rdb+aof"} {
		t.Run(persistence, func(t *testing.T) {
			spec := cluster.CreateSpec{SchemaVersion: 1, Name: "cache", Engine: cluster.Redis,
				Redis: &cluster.RedisSpec{Version: "8.2.7", Topology: "standalone", Persistence: persistence}}
			f := newFixture(t, spec)
			if err := f.planner.Plan(t.Context(), f.claim); err != nil {
				t.Fatal(err)
			}
			plan := installedPlan(t, f)
			if len(plan.Steps) != 4 || !plan.Steps[2].Command.Input.GetStartContainer().AllowInitialization {
				t.Fatal("Redis create must initialize and check readiness")
			}
			ensure := plan.Steps[1].Command.Input.GetEnsureContainer()
			if ensure.Engine != "redis" || ensure.Redis == nil || ensure.Redis.Port != uint32(f.decider.decision.Assignments[0].Ports[0].Number) || ensure.Redis.Persistence != persistence || ensure.Redis.MaxmemoryBytes != ensure.MemoryBytes/4*3 || ensure.Redis.EvictionPolicy != "noeviction" {
				t.Fatalf("Redis configuration=%v, want reserved port, requested persistence, and safe memory defaults", ensure.Redis)
			}
			if len(ensure.PlainEnv) != 0 || len(ensure.SecretEnv) != 3 || ensure.SecretEnv[0].Name != "REDIS_ADMIN_PASSWORD" || ensure.SecretEnv[1].Name != "REDIS_APP_PASSWORD" || ensure.SecretEnv[2].Name != "REDIS_REPLICATION_PASSWORD" {
				t.Fatal("Redis environment does not bind distinct admin, application, and replication passwords")
			}
			var credentialCount int
			if err := f.pool.QueryRow(t.Context(), `SELECT count(*) FROM cluster_credentials WHERE installation_id=$1 AND cluster_id=$2`, f.claim.InstallationID, f.claim.ClusterID).Scan(&credentialCount); err != nil {
				t.Fatal(err)
			}
			if credentialCount != 3 {
				t.Fatalf("stored credentials=%d, want exactly three", credentialCount)
			}
			databases, err := f.store.ReadClusterDatabases(t.Context(), f.claim.InstallationID, f.claim.ClusterID)
			if err != nil || len(databases) != 0 {
				t.Fatalf("Redis databases=%v, err=%v, want no PostgreSQL database intent", databases, err)
			}
			encoded, err := proto.Marshal(plan)
			if err != nil {
				t.Fatal(err)
			}
			refs := plan.Steps[1].Command.Input.Secrets
			if len(refs) != 3 {
				t.Fatalf("credential references=%d, want three", len(refs))
			}
			if len(ensure.Redis.Credentials) != len(refs) {
				t.Fatal("Redis restart metadata does not retain all credential references")
			}
			var passwords [][]byte
			for i, purpose := range []string{postgres.SuperuserCredential, postgres.ApplicationCredential, postgres.ReplicationCredential} {
				if !proto.Equal(ensure.Redis.Credentials[i], refs[i]) {
					t.Fatal("Redis restart metadata differs from the command credential reference")
				}
				stored, err := f.store.ReadClusterCredential(t.Context(), f.claim.InstallationID, f.claim.ClusterID, purpose)
				if err != nil {
					t.Fatal(err)
				}
				role := []string{"synod_admin", "synod_app", "synod_repl"}[i]
				if stored.RoleName != role || refs[i].CredentialId != stored.CredentialID.String() || refs[i].Version != stored.Version || refs[i].Purpose != purpose || ensure.SecretEnv[i].Purpose != purpose {
					t.Fatalf("credential purpose=%s does not match its stored role and plan reference", purpose)
				}
				password, err := f.ring.Open(secret.Binding{InstallationID: f.claim.InstallationID, OwnerType: "cluster", OwnerID: f.claim.ClusterID, CredentialID: stored.CredentialID, Version: stored.Version}, stored.Envelope)
				if err != nil || len(password) == 0 {
					t.Fatalf("decrypt %s credential: %v", purpose, err)
				}
				if bytes.Contains(encoded, password) || bytes.Contains(stored.Envelope.Sealed, password) {
					t.Fatal("plaintext credential appears in the plan or persisted ciphertext")
				}
				passwords = append(passwords, password)
			}
			for i := range refs {
				for j := range i {
					if refs[i].CredentialId == refs[j].CredentialId || bytes.Equal(passwords[i], passwords[j]) {
						t.Fatal("admin, application, and replication credentials are not distinct")
					}
				}
			}
			ready := plan.Steps[3].Command.Input
			if ready.GetCheckReadiness().Database != "0" || ready.GetCheckReadiness().Role != "synod_app" || len(ready.Secrets) != 1 || !proto.Equal(ready.Secrets[0], refs[1]) {
				t.Fatal("create readiness does not prove application access to Redis database zero")
			}
		})
	}
}

func TestRedisProvisionReusesCredentialsAfterPlacementFailure(t *testing.T) {
	f := newFixture(t, cluster.CreateSpec{SchemaVersion: 1, Name: "cache", Engine: cluster.Redis,
		Redis: &cluster.RedisSpec{Version: "8.2.7", Topology: "standalone", Persistence: "aof", MaxmemoryBytes: 256 << 20, EvictionPolicy: "allkeys-lru"}})
	f.decider.err = placement.ErrUnsatisfied
	if err := f.planner.Plan(t.Context(), f.claim); !errors.Is(err, placement.ErrUnsatisfied) {
		t.Fatalf("first placement: %v", err)
	}
	var prior []postgres.Credential
	for _, purpose := range []string{postgres.SuperuserCredential, postgres.ApplicationCredential, postgres.ReplicationCredential} {
		credential, err := f.store.ReadClusterCredential(t.Context(), f.claim.InstallationID, f.claim.ClusterID, purpose)
		if err != nil {
			t.Fatal(err)
		}
		prior = append(prior, credential)
	}
	f.decider.err = nil
	if err := f.planner.Plan(t.Context(), f.claim); err != nil {
		t.Fatal(err)
	}
	plan := installedPlan(t, f)
	if len(plan.Steps[1].Command.Input.Secrets) != len(prior) {
		t.Fatal("retry did not retain all three Redis credentials")
	}
	for i, reference := range plan.Steps[1].Command.Input.Secrets {
		if reference.CredentialId != prior[i].CredentialID.String() || reference.Version != prior[i].Version {
			t.Fatal("retry replaced a persisted Redis credential")
		}
		stored, err := f.store.ReadClusterCredential(t.Context(), f.claim.InstallationID, f.claim.ClusterID, prior[i].Purpose)
		if err != nil {
			t.Fatal(err)
		}
		if stored.RoleName != prior[i].RoleName || stored.Envelope.KeyID != prior[i].Envelope.KeyID || stored.Envelope.FormatVersion != prior[i].Envelope.FormatVersion || !bytes.Equal(stored.Envelope.Sealed, prior[i].Envelope.Sealed) {
			t.Fatal("retry changed a sealed Redis credential")
		}
	}
	configuration := plan.Steps[1].Command.Input.GetEnsureContainer().Redis
	if configuration.MaxmemoryBytes != 256<<20 || configuration.EvictionPolicy != "allkeys-lru" {
		t.Fatal("Redis plan ignored the explicit memory policy")
	}
	start := f.planFor(t, postgres.StartClusterAPIOperation)
	ready := start.Steps[1].Command.Input
	if start.Steps[0].Command.Input.GetStartContainer().AllowInitialization || ready.GetCheckReadiness().Database != "0" || ready.GetCheckReadiness().Role != "synod_admin" || len(ready.Secrets) != 1 || ready.Secrets[0].CredentialId != prior[0].CredentialID.String() || ready.GetCheckReadiness().RedisReplication.GetExpectedRole() != "primary" {
		t.Fatal("Redis lifecycle start does not reuse its admin credential and prove the primary role")
	}
	stop := f.planFor(t, postgres.StopClusterAPIOperation)
	if len(stop.Steps) != 1 || stop.Steps[0].Command.Input.GetStopContainer() == nil {
		t.Fatal("Redis stop must preserve its storage")
	}
	deleted := f.planFor(t, postgres.DeleteClusterAPIOperation)
	if len(deleted.Steps) != 2 || deleted.Steps[0].Command.Input.GetStopContainer() == nil || deleted.Steps[1].Command.Input.GetRemoveContainer() == nil {
		t.Fatal("Redis delete must stop and remove only its container")
	}
}

func TestRedisProvisionRefusesMemoryWithoutPersistenceHeadroom(t *testing.T) {
	f := newFixture(t, cluster.CreateSpec{SchemaVersion: 1, Name: "cache", Engine: cluster.Redis,
		Redis: &cluster.RedisSpec{Version: "8.2.7", Topology: "standalone", Persistence: "rdb", MaxmemoryBytes: 1 << 30}})
	if err := f.planner.Plan(t.Context(), f.claim); err == nil {
		t.Fatal("Redis accepted maxmemory above three quarters of its container memory")
	}
	var credentials, allocations int
	if err := f.pool.QueryRow(t.Context(), `SELECT (SELECT count(*) FROM cluster_credentials), (SELECT count(*) FROM resource_allocations)`).Scan(&credentials, &allocations); err != nil {
		t.Fatal(err)
	}
	if credentials != 0 || allocations != 0 {
		t.Fatal("invalid Redis memory policy persisted credentials or reservations")
	}
}
