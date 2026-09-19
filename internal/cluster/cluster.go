package cluster

import (
	"fmt"
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

// CreateSpec is the stable v1 hash representation. Keep its fields and tags fixed.
// Add a versioned representation before changing admission defaults or fields.
type CreateSpec struct {
	SchemaVersion int             `json:"schema_version"`
	Name          string          `json:"name"`
	Engine        Engine          `json:"engine"`
	PostgreSQL    *PostgreSQLSpec `json:"postgresql"`
	Redis         *RedisSpec      `json:"redis"`
}

// PostgreSQLSpec holds the PostgreSQL version and topology.
type PostgreSQLSpec struct {
	Version  string `json:"version"`
	Topology string `json:"topology"`
}

// RedisSpec holds the Redis version, topology, and persistence.
type RedisSpec struct {
	Version     string `json:"version"`
	Topology    string `json:"topology"`
	Persistence string `json:"persistence"`
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
		if s.PostgreSQL.Topology != "standalone" {
			return fmt.Errorf("postgres topology must be standalone")
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
	ObservedRevision  int64             `json:"observed_revision"`
	ObservedLifecycle ObservedLifecycle `json:"observed_lifecycle"`
	ExecutionEpoch    int64             `json:"execution_epoch"`
	CreatedAt         time.Time         `json:"created_at"`
	UpdatedAt         time.Time         `json:"updated_at"`
	DeletedAt         *time.Time        `json:"deleted_at"`
}
