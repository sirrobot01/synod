package operation

import (
	"fmt"
	"net/netip"
	"slices"

	"github.com/google/uuid"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/proto"
)

// ValidateRedisContainer checks the fixed Redis configuration and credential bindings.
func ValidateRedisContainer(p *agentv1.EnsureContainer, references []*agentv1.SecretReference) error {
	if p == nil || p.Redis == nil {
		return fmt.Errorf("redis container settings are required")
	}
	config := p.Redis
	if p.Engine != "redis" || p.Postgres != nil || config.Port == 0 || config.Port > 65535 || config.MaxmemoryBytes <= 0 || config.MaxmemoryBytes > p.MemoryBytes/4*3 || len(p.PlainEnv) != 0 {
		return fmt.Errorf("redis container limits are invalid")
	}
	if len(p.SecretEnv) != 3 || !slices.ContainsFunc(p.SecretEnv, func(env *agentv1.SecretEnv) bool {
		return env != nil && env.Name == "REDIS_ADMIN_PASSWORD" && env.Purpose == "superuser"
	}) || !slices.ContainsFunc(p.SecretEnv, func(env *agentv1.SecretEnv) bool {
		return env != nil && env.Name == "REDIS_APP_PASSWORD" && env.Purpose == "application"
	}) || !slices.ContainsFunc(p.SecretEnv, func(env *agentv1.SecretEnv) bool {
		return env != nil && env.Name == "REDIS_REPLICATION_PASSWORD" && env.Purpose == "replication"
	}) {
		return fmt.Errorf("redis credential environment is invalid")
	}
	if len(references) != 3 || len(config.Credentials) != 3 {
		return fmt.Errorf("redis requires separate admin, application, and replication credentials")
	}
	purposes := make(map[string]bool, 3)
	identities := make(map[uuid.UUID]bool, 3)
	for _, reference := range references {
		if reference == nil {
			return fmt.Errorf("redis credential reference is absent")
		}
		id, err := uuid.Parse(reference.CredentialId)
		if err != nil || id == uuid.Nil || reference.Version < 1 || id.String() != reference.CredentialId || identities[id] || purposes[reference.Purpose] ||
			(reference.Purpose != "superuser" && reference.Purpose != "application" && reference.Purpose != "replication") {
			return fmt.Errorf("redis credential references are invalid")
		}
		purposes[reference.Purpose], identities[id] = true, true
		if !slices.ContainsFunc(config.Credentials, func(stored *agentv1.SecretReference) bool {
			return proto.Equal(stored, reference)
		}) {
			return fmt.Errorf("redis stored credential references do not match the command")
		}
	}
	if config.Upstream != nil && (!validRedisUpstream(config.Upstream) || config.Persistence == "none") {
		return fmt.Errorf("redis replica requires durable persistence and a valid upstream")
	}
	switch config.Persistence {
	case "none", "rdb", "aof", "rdb+aof":
	default:
		return fmt.Errorf("redis persistence is invalid")
	}
	switch config.EvictionPolicy {
	case "noeviction", "allkeys-lru", "allkeys-lfu", "allkeys-random", "volatile-lru", "volatile-lfu", "volatile-random", "volatile-ttl":
	default:
		return fmt.Errorf("redis eviction policy is invalid")
	}
	return nil
}

func validRedisUpstream(upstream *agentv1.RedisUpstream) bool {
	if upstream == nil || upstream.Port == 0 || upstream.Port > 65535 {
		return false
	}
	address, err := netip.ParseAddr(upstream.Address)
	return err == nil && address.String() == upstream.Address && address.Zone() == "" && !address.Is4In6() &&
		(address.IsGlobalUnicast() || address.IsLoopback())
}
