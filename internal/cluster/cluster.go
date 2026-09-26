package cluster

import (
	"fmt"
	"net/netip"
	"regexp"
	"time"

	"github.com/google/uuid"
)

// Engine names a supported database engine.
type Engine string

const (
	PostgreSQL Engine = "postgres"
	Redis      Engine = "redis"
)

// DesiredLifecycle is the lifecycle state an operator asked for.
type DesiredLifecycle string

// ObservedLifecycle is the lifecycle state the control plane last observed.
type ObservedLifecycle string

const (
	DesiredRunning       DesiredLifecycle  = "running"
	DesiredStopped       DesiredLifecycle  = "stopped"
	DesiredDeleted       DesiredLifecycle  = "deleted"
	ObservedProvisioning ObservedLifecycle = "provisioning"
	ObservedRunning      ObservedLifecycle = "running"
	ObservedStopped      ObservedLifecycle = "stopped"
	ObservedDeleting     ObservedLifecycle = "deleting"
	ObservedDeleted      ObservedLifecycle = "deleted"
	ObservedUnknown      ObservedLifecycle = "unknown"
)

// CreateSpec is the versioned representation used to hash admission requests.
type CreateSpec struct {
	SchemaVersion int             `json:"schema_version"`
	Name          string          `json:"name"`
	Engine        Engine          `json:"engine"`
	PostgreSQL    *PostgreSQLSpec `json:"postgresql"`
	Redis         *RedisSpec      `json:"redis"`
}

// PostgreSQLSpec holds the PostgreSQL version, topology, and replication policy.
type PostgreSQLSpec struct {
	Version           string `json:"version"`
	Topology          string `json:"topology"`
	ReplicationPolicy string `json:"replication_policy,omitempty"`
	// FailureDomain is the host failure a patroni cluster survives. Only host is supported.
	FailureDomain string `json:"failure_domain,omitempty"`
	// Endpoint is an optional operator load balancer in front of the routers of a patroni cluster.
	Endpoint *PostgresEndpoint `json:"endpoint,omitempty"`
}

// PostgresEndpoint is an operator TCP load balancer. It listens on the writer and reader
// ports and forwards to the router ports that the endpoint output lists.
type PostgresEndpoint struct {
	Address    string `json:"address"`
	WriterPort uint16 `json:"writer_port"`
	ReaderPort uint16 `json:"reader_port"`
}

// RedisSpec holds the Redis version, topology, and persistence.
type RedisSpec struct {
	Version        string `json:"version"`
	Topology       string `json:"topology"`
	Persistence    string `json:"persistence"`
	MaxmemoryBytes int64  `json:"maxmemory_bytes,omitzero"`
	EvictionPolicy string `json:"eviction_policy,omitempty"`
}

var namePattern = regexp.MustCompile(`^[a-z][a-z0-9-]{0,62}$`)
var versionPattern = regexp.MustCompile(`^[0-9]+\.[0-9]+(\.[0-9]+)?$`)

// Validate checks storage input. The API must also check qualified capabilities.
func (s CreateSpec) Validate() error {
	if s.SchemaVersion != 1 {
		return fmt.Errorf("unsupported spec schema version %d", s.SchemaVersion)
	}
	if !namePattern.MatchString(s.Name) {
		return fmt.Errorf("name must match %s", namePattern)
	}
	switch s.Engine {
	case PostgreSQL:
		if s.PostgreSQL == nil || s.Redis != nil {
			return fmt.Errorf("postgres requires only a postgresql spec")
		}
		switch {
		case s.PostgreSQL.Topology == "standalone" && s.PostgreSQL.FailureDomain == "":
		case s.PostgreSQL.Topology == "patroni" && s.PostgreSQL.FailureDomain == "host":
		default:
			return fmt.Errorf("postgres topology must be standalone, or patroni with failure domain host")
		}
		if endpoint := s.PostgreSQL.Endpoint; endpoint != nil {
			address, err := netip.ParseAddr(endpoint.Address)
			if s.PostgreSQL.Topology != "patroni" || err != nil || address.String() != endpoint.Address || !address.IsGlobalUnicast() || address.Zone() != "" ||
				endpoint.WriterPort == 0 || endpoint.ReaderPort == 0 || endpoint.WriterPort == endpoint.ReaderPort {
				return fmt.Errorf("postgres endpoint needs topology patroni, a canonical routable IP, and two different ports")
			}
		}
		switch s.PostgreSQL.ReplicationPolicy {
		case "", "asynchronous":
		case "synchronous":
			// Standalone PostgreSQL has no replica to wait for.
			if s.PostgreSQL.Topology != "patroni" {
				return fmt.Errorf("postgres synchronous replication needs topology patroni")
			}
		default:
			return fmt.Errorf("postgres replication policy must be asynchronous or synchronous")
		}
		if len(s.PostgreSQL.Version) > 64 || !versionPattern.MatchString(s.PostgreSQL.Version) {
			return fmt.Errorf("postgres version must have two or three numeric parts")
		}
	case Redis:
		if s.Redis == nil || s.PostgreSQL != nil {
			return fmt.Errorf("redis requires only a redis spec")
		}
		if s.Redis.Topology != "standalone" {
			return fmt.Errorf("redis topology must be standalone")
		}
		if len(s.Redis.Version) > 64 || !versionPattern.MatchString(s.Redis.Version) {
			return fmt.Errorf("redis version must have two or three numeric parts")
		}
		switch s.Redis.Persistence {
		case "none", "rdb", "aof", "rdb+aof":
		default:
			return fmt.Errorf("redis persistence must be none, rdb, aof, or rdb+aof")
		}
		if s.Redis.MaxmemoryBytes < 0 {
			return fmt.Errorf("redis maxmemory bytes must not be negative")
		}
		switch s.Redis.EvictionPolicy {
		case "", "noeviction", "allkeys-lru", "allkeys-lfu", "allkeys-random", "volatile-lru", "volatile-lfu", "volatile-random", "volatile-ttl":
		default:
			return fmt.Errorf("redis eviction policy is unsupported")
		}
	default:
		return fmt.Errorf("engine must be postgres or redis")
	}
	return nil
}

// Cluster is one stored cluster with its desired and observed state.
type Cluster struct {
	ID                uuid.UUID         `json:"id"`
	InstallationID    uuid.UUID         `json:"installation_id"`
	Spec              CreateSpec        `json:"spec"`
	DesiredRevision   int64             `json:"desired_revision"`
	DesiredLifecycle  DesiredLifecycle  `json:"desired_lifecycle"`
	DesiredReplicas   int               `json:"desired_replicas"`
	ObservedRevision  int64             `json:"observed_revision"`
	ObservedLifecycle ObservedLifecycle `json:"observed_lifecycle"`
	ExecutionEpoch    int64             `json:"execution_epoch"`
	CreatedAt         time.Time         `json:"created_at"`
	UpdatedAt         time.Time         `json:"updated_at"`
	DeletedAt         *time.Time        `json:"deleted_at"`
}
