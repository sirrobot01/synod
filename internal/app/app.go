package app

import (
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/google/uuid"
)

// Tier represents the application workload tier defined in the product brief.
type Tier int

const (
	// TierDatabase is Tier 1: first-class databases with native replication.
	TierDatabase Tier = 1
	// TierSingleWriter is Tier 2: single-writer apps with replicated state and failover.
	TierSingleWriter Tier = 2
	// TierParallel is Tier 3: stateless parallel apps and workers.
	TierParallel Tier = 3
)

// CopiesMode specifies how target instance counts are calculated.
type CopiesMode string

const (
	CopiesModeFixed      CopiesMode = "fixed"
	CopiesModeAllCapable CopiesMode = "all_capable"
)

// DevicePreference specifies affinity and constraints across devices.
type DevicePreference struct {
	Preferred []string `json:"preferred,omitempty"`
	Allowed   []string `json:"allowed,omitempty"`
	Never     []string `json:"never,omitempty"`
}

// MountSpec specifies volume and directory mounts for a service.
// A replicated mount names a Synod-owned volume by Source; only tier 2 uses it.
type MountSpec struct {
	Type     string `json:"type"` // "bind", "tmpfs", or "replicated"
	Source   string `json:"source"`
	Target   string `json:"target"`
	ReadOnly bool   `json:"read_only,omitempty"`
}

// PortSpec specifies exposed container ports.
type PortSpec struct {
	ContainerPort uint16 `json:"container_port"`
	Protocol      string `json:"protocol,omitempty"` // "tcp" or "udp"
}

// ResourceSpec specifies container limits and the disk reservation.
type ResourceSpec struct {
	DiskBytes   int64 `json:"disk_bytes,omitempty"`
	MemoryBytes int64 `json:"memory_bytes,omitempty"`
	CPUMillis   int64 `json:"cpu_millis,omitempty"`
}

// HealthCheckSpec specifies application health checking parameters.
type HealthCheckSpec struct {
	Type               string   `json:"type"` // "http", "tcp", "exec"
	Path               string   `json:"path,omitempty"`
	Port               uint16   `json:"port,omitempty"`
	Command            []string `json:"command,omitempty"`
	IntervalSeconds    uint32   `json:"interval_seconds,omitempty"`
	TimeoutSeconds     uint32   `json:"timeout_seconds,omitempty"`
	UnhealthyThreshold uint32   `json:"unhealthy_threshold,omitempty"`
}

// ServiceSpec defines the container configuration to execute.
type ServiceSpec struct {
	Image                  string            `json:"image"`
	Command                []string          `json:"command"`
	Entrypoint             []string          `json:"entrypoint"`
	Environment            map[string]string `json:"environment,omitempty"`
	Ports                  []PortSpec        `json:"ports,omitempty"`
	Mounts                 []MountSpec       `json:"mounts,omitempty"`
	Resources              ResourceSpec      `json:"resources,omitempty"`
	HealthCheck            *HealthCheckSpec  `json:"health_check,omitempty"`
	SupportedArchitectures []string          `json:"supported_architectures,omitempty"`
}

// RouterSpec defines ingress router settings.
// For a tier 1 database, Port is the writer route and ReaderPort is the optional reader route.
type RouterSpec struct {
	Protocol   string `json:"protocol"` // "http" or "tcp"
	Port       uint16 `json:"port"`
	ReaderPort uint16 `json:"reader_port,omitempty"`
}

// Database engines for a tier 1 app.
const (
	EnginePostgres = "postgres"
	EngineRedis    = "redis"
)

// DatabaseSpec names the engine of a tier 1 app and the databases Synod creates in it.
// The engine catalog supplies the image, so a tier 1 app sets no service.
type DatabaseSpec struct {
	Engine    string   `json:"engine"`
	Version   string   `json:"version"`
	Databases []string `json:"databases,omitempty"`
}

var (
	versionPattern  = regexp.MustCompile(`^[0-9]+(\.[0-9]+){0,2}$`)
	databasePattern = regexp.MustCompile(`^[a-z][a-z0-9_]{0,62}$`)
)

func (d *DatabaseSpec) validate() error {
	switch {
	case d == nil:
		return fmt.Errorf("tier 1 apps require database settings")
	case !versionPattern.MatchString(d.Version):
		return fmt.Errorf("database version must be a version number such as 17")
	}
	switch d.Engine {
	case EnginePostgres:
		if len(d.Databases) < 1 || len(d.Databases) > 16 {
			return fmt.Errorf("postgres apps require 1 to 16 databases")
		}
		seen := map[string]bool{}
		for _, name := range d.Databases {
			if !databasePattern.MatchString(name) || seen[name] {
				return fmt.Errorf("database names must be unique and match %s", databasePattern)
			}
			seen[name] = true
		}
	case EngineRedis:
		if len(d.Databases) != 0 {
			return fmt.Errorf("redis apps do not take database names")
		}
	default:
		return fmt.Errorf("database engine must be postgres or redis")
	}
	return nil
}

// Failback modes for a tier 2 app.
const (
	FailbackAuto   = "auto"
	FailbackManual = "manual"
)

// LeaseSeconds is the writer lease of a tier 2 app. Failover waits at least two leases.
const LeaseSeconds = 30

// ReplicationSpec sets how a tier 2 app keeps copies of its data on standby devices.
type ReplicationSpec struct {
	Standbys              int    `json:"standbys"`
	IntervalSeconds       uint32 `json:"interval_seconds"`
	FailoverAfterSeconds  uint32 `json:"failover_after_seconds"`
	Failback              string `json:"failback"`
	FailbackStableSeconds uint32 `json:"failback_stable_seconds"`
}

// DefaultDatabaseReplication returns the product defaults for a tier 1 app.
// A database switchover interrupts clients, so fail-back is manual.
func DefaultDatabaseReplication() ReplicationSpec {
	return ReplicationSpec{Standbys: 1, FailoverAfterSeconds: 90, Failback: FailbackManual, FailbackStableSeconds: 600}
}

// DefaultReplication returns the product defaults for a tier 2 app.
func DefaultReplication() ReplicationSpec {
	return ReplicationSpec{Standbys: 1, IntervalSeconds: 60, FailoverAfterSeconds: 90, Failback: FailbackAuto, FailbackStableSeconds: 600}
}

// validate checks the settings of a tier 1 or tier 2 app. A database replicates
// continuously, so it has no interval and may run without a standby.
func (r *ReplicationSpec) validate(tier Tier) error {
	switch {
	case r == nil:
		return fmt.Errorf("tier %d apps require replication settings", tier)
	case tier == TierDatabase && (r.Standbys < 0 || r.Standbys > 3):
		return fmt.Errorf("replication standbys must be 0 to 3")
	case tier == TierSingleWriter && (r.Standbys < 1 || r.Standbys > 3):
		return fmt.Errorf("replication standbys must be 1 to 3")
	case tier == TierDatabase && r.IntervalSeconds != 0:
		return fmt.Errorf("tier 1 apps replicate continuously and take no interval")
	case tier == TierSingleWriter && (r.IntervalSeconds < 10 || r.IntervalSeconds > 3600):
		return fmt.Errorf("replication interval must be 10 to 3600 seconds")
	case r.FailoverAfterSeconds < 2*LeaseSeconds:
		return fmt.Errorf("failover delay must be at least %d seconds", 2*LeaseSeconds)
	case r.Failback != FailbackAuto && r.Failback != FailbackManual:
		return fmt.Errorf("failback must be auto or manual")
	case r.FailbackStableSeconds < 60 || r.FailbackStableSeconds > 86400:
		return fmt.Errorf("failback stable period must be 60 to 86400 seconds")
	}
	return nil
}

// AppSpec is the versioned admission specification of an application.
type AppSpec struct {
	SchemaVersion    int              `json:"schema_version"`
	Name             string           `json:"name"`
	Tier             Tier             `json:"tier"`
	Copies           int              `json:"copies"`
	CopiesMode       CopiesMode       `json:"copies_mode,omitempty"`
	DevicePreference DevicePreference `json:"device_preference,omitempty"`
	Service          ServiceSpec      `json:"service"`
	Router           RouterSpec       `json:"router"`
	Replication      *ReplicationSpec `json:"replication,omitempty"`
	Database         *DatabaseSpec    `json:"database,omitempty"`
}

var namePattern = regexp.MustCompile(`^[a-z][a-z0-9-]{0,62}$`)

// Validate checks AppSpec validity against product rules.
func (s AppSpec) Validate() error {
	if s.SchemaVersion != 1 {
		return fmt.Errorf("unsupported spec schema version %d", s.SchemaVersion)
	}
	if !namePattern.MatchString(s.Name) {
		return fmt.Errorf("name must match %s", namePattern)
	}
	if s.Tier == TierDatabase {
		return s.validateDatabase()
	}
	if s.Database != nil || s.Router.ReaderPort != 0 {
		return fmt.Errorf("database settings and a reader port apply only to tier 1 apps")
	}
	if strings.TrimSpace(s.Service.Image) == "" {
		return fmt.Errorf("service image is required")
	}
	if s.CopiesMode != "" && s.CopiesMode != CopiesModeFixed && s.CopiesMode != CopiesModeAllCapable {
		return fmt.Errorf("copies_mode must be fixed or all_capable")
	}
	if s.Copies < 0 {
		return fmt.Errorf("copies cannot be negative")
	}
	if s.Service.Resources.MemoryBytes < 0 || s.Service.Resources.CPUMillis < 0 || s.Service.Resources.DiskBytes < 0 {
		return fmt.Errorf("resource limits cannot be negative")
	}
	for _, port := range s.Service.Ports {
		if port.ContainerPort == 0 || (port.Protocol != "" && port.Protocol != "tcp" && port.Protocol != "udp") {
			return fmt.Errorf("service ports require a positive port and tcp or udp protocol")
		}
	}
	for name, value := range s.Service.Environment {
		if name == "" || strings.ContainsAny(name, "=\x00") || strings.ContainsRune(value, '\x00') {
			return fmt.Errorf("environment contains an invalid name or value")
		}
	}
	if h := s.Service.HealthCheck; h != nil {
		switch h.Type {
		case "http":
			if h.Port == 0 || !strings.HasPrefix(h.Path, "/") || len(h.Command) != 0 {
				return fmt.Errorf("http health check requires a port and absolute path, without a command")
			}
		case "tcp":
			if h.Port == 0 || h.Path != "" || len(h.Command) != 0 {
				return fmt.Errorf("tcp health check requires a port, without a path or command")
			}
		case "exec":
			if len(h.Command) == 0 || h.Command[0] == "" || h.Port != 0 || h.Path != "" {
				return fmt.Errorf("exec health check requires a command, without a port or path")
			}
		default:
			return fmt.Errorf("health check type must be http, tcp, or exec")
		}
	}
	switch s.Tier {
	case TierParallel:
		mode := s.CopiesMode
		if mode == "" {
			mode = CopiesModeFixed
		}
		if mode == CopiesModeFixed && s.Copies < 1 {
			return fmt.Errorf("copies must be at least 1")
		}
		if s.Replication != nil {
			return fmt.Errorf("replication settings apply only to tier 2 apps")
		}
	case TierSingleWriter:
		if s.Copies != 1 || s.CopiesMode == CopiesModeAllCapable {
			return fmt.Errorf("tier 2 apps must have exactly 1 active copy")
		}
		if err := s.Replication.validate(s.Tier); err != nil {
			return err
		}
	default:
		return fmt.Errorf("unsupported tier: %d", s.Tier)
	}
	replicated := map[string]bool{}
	for _, m := range s.Service.Mounts {
		switch {
		case m.Type == "bind":
			if m.Source == "" || m.Source[0] != '/' {
				return fmt.Errorf("bind mount source must be an absolute path: %s", m.Source)
			}
		case m.Type == "tmpfs":
		case m.Type == "replicated" && s.Tier == TierSingleWriter:
			if !namePattern.MatchString(m.Source) {
				return fmt.Errorf("replicated volume name must match %s", namePattern)
			}
			if replicated[m.Source] {
				return fmt.Errorf("replicated volume %s is mounted twice", m.Source)
			}
			replicated[m.Source] = true
		case s.Tier == TierParallel:
			return fmt.Errorf("tier 3 apps only support bind and tmpfs mounts")
		default:
			return fmt.Errorf("tier 2 apps support replicated, bind, and tmpfs mounts")
		}
		if m.Target == "" || m.Target[0] != '/' {
			return fmt.Errorf("mount target must be an absolute path: %s", m.Target)
		}
	}
	if s.Tier == TierSingleWriter && len(replicated) == 0 {
		return fmt.Errorf("tier 2 apps require at least one replicated volume")
	}
	if s.Tier == TierParallel && len(s.Service.Ports) == 0 {
		if s.Router != (RouterSpec{}) {
			return fmt.Errorf("workers without service ports require an empty router")
		}
		if s.Service.HealthCheck != nil && s.Service.HealthCheck.Type != "exec" {
			return fmt.Errorf("workers without service ports support exec health checks")
		}
		return nil
	}
	if s.Router.Port == 0 {
		return fmt.Errorf("router port is required")
	}
	if s.Router.Protocol != "http" && s.Router.Protocol != "tcp" {
		return fmt.Errorf("router protocol must be http or tcp")
	}
	return nil
}

// validateDatabase checks a tier 1 app. The catalog supplies the container, so the
// user sets no service, and both routes are TCP.
func (s AppSpec) validateDatabase() error {
	service := s.Service
	if service.Image != "" || len(service.Command) != 0 || len(service.Entrypoint) != 0 || len(service.Environment) != 0 || len(service.Ports) != 0 ||
		len(service.Mounts) != 0 || service.Resources != (ResourceSpec{}) || service.HealthCheck != nil || len(service.SupportedArchitectures) != 0 {
		return fmt.Errorf("tier 1 apps take their container from the engine catalog and set no service")
	}
	if s.Copies != 1 || s.CopiesMode == CopiesModeAllCapable {
		return fmt.Errorf("tier 1 apps have exactly 1 primary")
	}
	if err := s.Database.validate(); err != nil {
		return err
	}
	if err := s.Replication.validate(s.Tier); err != nil {
		return err
	}
	switch {
	case s.Router.Protocol != "tcp":
		return fmt.Errorf("tier 1 routes use tcp")
	case s.Router.Port == 0:
		return fmt.Errorf("the writer port is required")
	case s.Router.ReaderPort == s.Router.Port:
		return fmt.Errorf("the reader port must differ from the writer port")
	}
	return nil
}

// App represents a stored application instance.
type App struct {
	Status            *Status    `json:"status,omitempty"`
	ID                uuid.UUID  `json:"id"`
	InstallationID    uuid.UUID  `json:"installation_id"`
	Name              string     `json:"name"`
	Tier              Tier       `json:"tier"`
	Spec              AppSpec    `json:"spec"`
	DesiredRevision   int64      `json:"desired_revision"`
	DesiredLifecycle  string     `json:"desired_lifecycle"`
	ObservedRevision  int64      `json:"observed_revision"`
	ObservedLifecycle string     `json:"observed_lifecycle"`
	CreatedAt         time.Time  `json:"created_at"`
	UpdatedAt         time.Time  `json:"updated_at"`
	DeletedAt         *time.Time `json:"deleted_at,omitempty"`
}

// Copy represents one deployed copy of an app running on a node.
type Copy struct {
	Role          string     `json:"role"`
	Health        string     `json:"health"`
	ObservedAt    *time.Time `json:"observed_at,omitempty"`
	ID            uuid.UUID  `json:"id"`
	AppID         uuid.UUID  `json:"app_id"`
	NodeID        uuid.UUID  `json:"node_id"`
	CopyIndex     int        `json:"copy_index"`
	HostPort      uint16     `json:"host_port"`
	ObservedState string     `json:"observed_state"`
	CreatedAt     time.Time  `json:"created_at"`
	UpdatedAt     time.Time  `json:"updated_at"`
}

// Status reports current copy and route evidence.
type Status struct {
	LastPauseSeconds  float64         `json:"last_pause_seconds,omitempty"`
	MaxPauseSeconds   float64         `json:"max_pause_seconds,omitempty"`
	SkippedSnapshots  uint64          `json:"skipped_snapshots,omitempty"`
	ActiveNodeID      uuid.UUID       `json:"active_node_id,omitzero"`
	WriterEpoch       uint64          `json:"writer_epoch,omitempty"`
	Placement         string          `json:"placement,omitempty"`
	Standbys          []StandbyStatus `json:"standbys,omitempty"`
	LossWindowSeconds *float64        `json:"loss_window_seconds,omitempty"`
	State             string          `json:"state"`
	OperationID       uuid.UUID       `json:"operation_id"`
	OperationState    string          `json:"operation_state"`
	FailureCode       string          `json:"failure_code,omitempty"`
	Copies            []Copy          `json:"copies"`
	Endpoints         []Endpoint      `json:"endpoints"`
}

type Endpoint struct {
	NodeID      uuid.UUID `json:"node_id"`
	Address     string    `json:"address"`
	Ready       bool      `json:"ready"`
	FailureCode string    `json:"failure_code,omitempty"`
}

// StandbyStatus separates stored snapshot evidence from current failover eligibility.
type StandbyStatus struct {
	NodeID      uuid.UUID  `json:"node_id"`
	Epoch       uint64     `json:"epoch"`
	Sequence    uint64     `json:"sequence"`
	CompletedAt *time.Time `json:"completed_at,omitempty"`
	LagSeconds  *float64   `json:"lag_seconds,omitempty"`
	Usable      bool       `json:"usable"`
	FailureCode string     `json:"failure_code,omitempty"`
	// State is the engine replication state of a tier 1 standby.
	State string `json:"state,omitempty"`
}

// Credential is the client login of a tier 1 app. Only an audited reveal returns it.
type Credential struct {
	AppID        uuid.UUID `json:"app_id"`
	CredentialID uuid.UUID `json:"credential_id"`
	Version      int64     `json:"version"`
	Username     string    `json:"username"`
	Password     string    `json:"password"`
}
