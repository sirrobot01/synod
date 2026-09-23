package operation

import (
	"strings"
	"testing"

	"github.com/google/uuid"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/proto"
)

func TestRedisContainerRequiresBoundedAuthenticatedSettings(t *testing.T) {
	for name, change := range map[string]func(*agentv1.EnsureContainer){
		"missing settings":    func(p *agentv1.EnsureContainer) { p.Redis = nil },
		"wrong engine":        func(p *agentv1.EnsureContainer) { p.Engine = "postgres" },
		"mixed engines":       func(p *agentv1.EnsureContainer) { p.Postgres = &agentv1.PostgresContainerConfig{Port: 5432} },
		"unbounded memory":    func(p *agentv1.EnsureContainer) { p.Redis.MaxmemoryBytes = 0 },
		"no headroom":         func(p *agentv1.EnsureContainer) { p.Redis.MaxmemoryBytes = p.MemoryBytes },
		"invalid port":        func(p *agentv1.EnsureContainer) { p.Redis.Port = 65536 },
		"config injection":    func(p *agentv1.EnsureContainer) { p.Redis.EvictionPolicy = "noeviction\nuser default on nopass" },
		"unknown persistence": func(p *agentv1.EnsureContainer) { p.Redis.Persistence = "snapshot" },
		"missing credential":  func(p *agentv1.EnsureContainer) { p.SecretEnv = p.SecretEnv[:1] },
		"shared credential":   func(p *agentv1.EnsureContainer) { p.SecretEnv[1].Purpose = "superuser" },
	} {
		t.Run(name, func(t *testing.T) {
			command := commandFixture()
			settings := &agentv1.EnsureContainer{
				Engine: "redis", ImageDigest: "redis@sha256:" + strings.Repeat("a", 64), VolumeId: uuid.NewString(),
				NetworkMode: agentv1.NetworkMode_NETWORK_MODE_HOST, MemoryBytes: 64 << 20,
				Redis:     &agentv1.RedisContainerConfig{Port: 6379, Persistence: "rdb+aof", MaxmemoryBytes: 48 << 20, EvictionPolicy: "noeviction"},
				SecretEnv: []*agentv1.SecretEnv{{Name: "REDIS_ADMIN_PASSWORD", Purpose: "superuser"}, {Name: "REDIS_APP_PASSWORD", Purpose: "application"}, {Name: "REDIS_REPLICATION_PASSWORD", Purpose: "replication"}},
			}
			command.Input.Secrets = []*agentv1.SecretReference{{CredentialId: uuid.NewString(), Version: 1, Purpose: "superuser"}, {CredentialId: uuid.NewString(), Version: 1, Purpose: "application"}, {CredentialId: uuid.NewString(), Version: 1, Purpose: "replication"}}
			settings.Redis.Credentials = command.Input.Secrets
			command.Input.Payload = &agentv1.CommandInput_EnsureContainer{EnsureContainer: settings}
			if _, err := CommandHash(command); err != nil {
				t.Fatal(err)
			}
			change(settings)
			if _, err := CommandHash(command); err == nil {
				t.Fatal("accepted invalid Redis settings")
			}
			if err := ValidateRedisContainer(settings, command.Input.Secrets); err == nil {
				t.Fatal("stored configuration accepted invalid Redis settings")
			}
		})
	}
}

func TestRedisReplicaCommandContracts(t *testing.T) {
	command := commandFixture()
	command.Input.Secrets = []*agentv1.SecretReference{
		{CredentialId: uuid.NewString(), Version: 1, Purpose: "superuser"},
		{CredentialId: uuid.NewString(), Version: 1, Purpose: "application"},
		{CredentialId: uuid.NewString(), Version: 1, Purpose: "replication"},
	}
	command.Input.Payload = &agentv1.CommandInput_EnsureContainer{EnsureContainer: &agentv1.EnsureContainer{
		Engine: "redis", ImageDigest: "redis@sha256:" + strings.Repeat("a", 64), VolumeId: uuid.NewString(),
		NetworkMode: agentv1.NetworkMode_NETWORK_MODE_HOST, MemoryBytes: 64 << 20,
		Redis: &agentv1.RedisContainerConfig{Port: 6379, Persistence: "aof", MaxmemoryBytes: 48 << 20, EvictionPolicy: "noeviction",
			Upstream: &agentv1.RedisUpstream{Address: "10.1.0.2", Port: 16379}},
		SecretEnv: []*agentv1.SecretEnv{{Name: "REDIS_ADMIN_PASSWORD", Purpose: "superuser"}, {Name: "REDIS_APP_PASSWORD", Purpose: "application"}, {Name: "REDIS_REPLICATION_PASSWORD", Purpose: "replication"}},
	}}
	for _, reference := range command.Input.Secrets {
		command.Input.GetEnsureContainer().Redis.Credentials = append(command.Input.GetEnsureContainer().Redis.Credentials, proto.Clone(reference).(*agentv1.SecretReference))
	}
	baseline, err := CommandHash(command)
	if err != nil {
		t.Fatal(err)
	}
	for name, change := range map[string]func(*agentv1.Command){
		"ephemeral replica":        func(c *agentv1.Command) { c.Input.GetEnsureContainer().Redis.Persistence = "none" },
		"zero port":                func(c *agentv1.Command) { c.Input.GetEnsureContainer().Redis.Upstream.Port = 0 },
		"overflow port":            func(c *agentv1.Command) { c.Input.GetEnsureContainer().Redis.Upstream.Port = 65536 },
		"missing reference":        func(c *agentv1.Command) { c.Input.Secrets = c.Input.Secrets[:2] },
		"missing stored reference": func(c *agentv1.Command) { c.Input.GetEnsureContainer().Redis.Credentials = nil },
		"nil stored reference":     func(c *agentv1.Command) { c.Input.GetEnsureContainer().Redis.Credentials[2] = nil },
		"wrong stored version":     func(c *agentv1.Command) { c.Input.GetEnsureContainer().Redis.Credentials[2].Version++ },
		"shared identity": func(c *agentv1.Command) {
			c.Input.Secrets[2].CredentialId = c.Input.Secrets[0].CredentialId
			c.Input.GetEnsureContainer().Redis.Credentials[2].CredentialId = c.Input.Secrets[0].CredentialId
		},
		"shared purpose": func(c *agentv1.Command) {
			c.Input.Secrets[2].Purpose = "superuser"
			c.Input.GetEnsureContainer().Redis.Credentials[2].Purpose = "superuser"
		},
		"plain environment": func(c *agentv1.Command) {
			c.Input.GetEnsureContainer().PlainEnv = []*agentv1.PlainEnv{{Name: "REDIS_REPLICATION_PASSWORD", Value: "untracked"}}
		},
	} {
		t.Run(name, func(t *testing.T) {
			changed := proto.Clone(command).(*agentv1.Command)
			change(changed)
			if _, err := CommandHash(changed); err == nil {
				t.Fatal("accepted invalid replication command")
			}
		})
	}
	for _, address := range []string{"", "primary.example", "0.0.0.0", "::", "224.0.0.1", "255.255.255.255", "169.254.1.1", "fe80::1", "fe80::1%en0", "::ffff:10.1.0.2", "2001:DB8::1", "10.1.0.2\nuser default on nopass"} {
		t.Run(address, func(t *testing.T) {
			changed := proto.Clone(command).(*agentv1.Command)
			changed.Input.GetEnsureContainer().Redis.Upstream.Address = address
			if _, err := CommandHash(changed); err == nil {
				t.Fatal("accepted invalid upstream address")
			}
		})
	}
	for _, address := range []string{"10.1.0.3", "127.0.0.1", "::1", "2001:db8::1"} {
		changed := proto.Clone(command).(*agentv1.Command)
		changed.Input.GetEnsureContainer().Redis.Upstream.Address = address
		hash, err := CommandHash(changed)
		if err != nil || hash == baseline {
			t.Fatalf("upstream %s must be valid and change the immutable hash: %v", address, err)
		}
	}
}

func TestRedisReplicationReadinessContract(t *testing.T) {
	command := commandFixture()
	command.Input.Secrets = []*agentv1.SecretReference{{CredentialId: uuid.NewString(), Version: 1, Purpose: "superuser"}}
	command.Input.Payload = &agentv1.CommandInput_CheckReadiness{CheckReadiness: &agentv1.CheckReadiness{
		Address: "127.0.0.1", Port: 6379, Database: "0", Role: "synod_admin", Engine: "redis",
		RedisReplication: &agentv1.RedisReplicationCheck{ExpectedRole: "replica", Upstream: &agentv1.RedisUpstream{Address: "10.1.0.2", Port: 16379}},
	}}
	baseline, err := CommandHash(command)
	if err != nil {
		t.Fatal(err)
	}
	for name, change := range map[string]func(*agentv1.Command){
		"wrong engine":           func(c *agentv1.Command) { c.Input.GetCheckReadiness().Engine = "postgres" },
		"wrong database":         func(c *agentv1.Command) { c.Input.GetCheckReadiness().Database = "1" },
		"application role":       func(c *agentv1.Command) { c.Input.GetCheckReadiness().Role = "synod_app" },
		"application credential": func(c *agentv1.Command) { c.Input.Secrets[0].Purpose = "application" },
		"no credential":          func(c *agentv1.Command) { c.Input.Secrets = nil },
		"extra credential": func(c *agentv1.Command) {
			c.Input.Secrets = append(c.Input.Secrets, proto.Clone(c.Input.Secrets[0]).(*agentv1.SecretReference))
		},
		"missing upstream": func(c *agentv1.Command) { c.Input.GetCheckReadiness().RedisReplication.Upstream = nil },
		"invalid upstream": func(c *agentv1.Command) {
			c.Input.GetCheckReadiness().RedisReplication.Upstream.Address = "master.example"
		},
		"invalid upstream port": func(c *agentv1.Command) { c.Input.GetCheckReadiness().RedisReplication.Upstream.Port = 0 },
		"unknown role":          func(c *agentv1.Command) { c.Input.GetCheckReadiness().RedisReplication.ExpectedRole = "sentinel" },
		"primary with upstream": func(c *agentv1.Command) { c.Input.GetCheckReadiness().RedisReplication.ExpectedRole = "primary" },
	} {
		t.Run(name, func(t *testing.T) {
			changed := proto.Clone(command).(*agentv1.Command)
			change(changed)
			if _, err := CommandHash(changed); err == nil {
				t.Fatal("accepted invalid native readiness check")
			}
		})
	}
	for name, change := range map[string]func(*agentv1.RedisReplicationCheck){
		"upstream address": func(c *agentv1.RedisReplicationCheck) { c.Upstream.Address = "10.1.0.3" },
		"upstream port":    func(c *agentv1.RedisReplicationCheck) { c.Upstream.Port++ },
		"primary":          func(c *agentv1.RedisReplicationCheck) { c.ExpectedRole, c.Upstream = "primary", nil },
	} {
		t.Run(name, func(t *testing.T) {
			changed := proto.Clone(command).(*agentv1.Command)
			change(changed.Input.GetCheckReadiness().RedisReplication)
			hash, err := CommandHash(changed)
			if err != nil || hash == baseline {
				t.Fatalf("changed expectation must be valid and change the hash: %v", err)
			}
		})
	}
}
