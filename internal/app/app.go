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
	// TierParallel is Tier 3: stateless parallel apps behind a generic router.
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
type RouterSpec struct {
	Protocol string `json:"protocol"` // "http" or "tcp"
	Port     uint16 `json:"port"`
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

// DefaultReplication returns the product defaults for a tier 2 app.
func DefaultReplication() ReplicationSpec {
	return ReplicationSpec{Standbys: 1, IntervalSeconds: 60, FailoverAfterSeconds: 90, Failback: FailbackAuto, FailbackStableSeconds: 600}
}

func (r *ReplicationSpec) validate() error {
	switch {
	case r == nil:
		return fmt.Errorf("tier 2 apps require replication settings")
	case r.Standbys < 1 || r.Standbys > 3:
		return fmt.Errorf("replication standbys must be 1 to 3")
	case r.IntervalSeconds < 10 || r.IntervalSeconds > 3600:
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
		if err := s.Replication.validate(); err != nil {
			return err
		}
	case TierDatabase:
		return fmt.Errorf("tier 1 databases use dedicated database creation")
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
	if s.Router.Port == 0 {
		return fmt.Errorf("router port is required")
	}
	if s.Router.Protocol != "http" && s.Router.Protocol != "tcp" {
		return fmt.Errorf("router protocol must be http or tcp")
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
}
