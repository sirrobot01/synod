// Package placement selects bounded, deterministic placements without host effects.
package placement

import (
	"errors"
	"time"

	"github.com/google/uuid"
)

// Placement failure reasons.
var (
	ErrInvalid     = errors.New("invalid placement input")
	ErrUnsatisfied = errors.New("placement requirements cannot be satisfied")
	ErrSearchLimit = errors.New("placement search limit reached")
	ErrChanged     = errors.New("placement inputs changed")
	ErrConflict    = errors.New("placement identity conflicts")
)

// Bounds and freshness limits for one placement.
const (
	MaxResources   = 64
	MaxNodes       = 256
	MaxAllocations = 8192
	MaxSearch      = 100000
	PresenceAge    = 20 * time.Second
	ProbeWindow    = 10 * time.Second
	CapacityAge    = 30 * time.Second
)

// Vector is a resource amount in CPU millis, memory bytes, and disk bytes.
type Vector struct {
	CPUMillis   int64 `json:"cpu_millis"`
	MemoryBytes int64 `json:"memory_bytes"`
	DiskBytes   int64 `json:"disk_bytes"`
}

// Budgets holds the workload, supporting, and backup amounts for one resource.
type Budgets struct {
	Workload   *Vector `json:"workload"`
	Supporting *Vector `json:"supporting"`
	Backup     *Vector `json:"backup"`
}

// EngineVersion names one engine and version that a node supports.
type EngineVersion struct {
	Engine  string `json:"engine"`
	Version string `json:"version"`
}

// NodePolicy holds the operator policy and fixed capability of one node.
// Headroom is the capacity that placement must leave unused.
type NodePolicy struct {
	InstallationID uuid.UUID         `json:"installation_id"`
	NodeID         uuid.UUID         `json:"node_id"`
	Revision       int64             `json:"revision"`
	Architecture   string            `json:"architecture"`
	RuntimeVersion string            `json:"runtime_version"`
	StorageID      string            `json:"storage_id"`
	ClientAddress  string            `json:"client_address,omitempty"`
	Engines        []EngineVersion   `json:"engines"`
	Labels         map[string]string `json:"labels"`
	FailureDomains map[string]string `json:"failure_domains"`
	Headroom       Vector            `json:"headroom"`
	Cordoned       bool              `json:"cordoned"`
	Maintenance    bool              `json:"maintenance"`
}

// CapacityProbe requests a fresh capacity report from one node.
type CapacityProbe struct {
	ID                uuid.UUID `json:"id"`
	InstallationID    uuid.UUID `json:"installation_id"`
	NodeID            uuid.UUID `json:"node_id"`
	SessionID         uuid.UUID `json:"session_id"`
	BootID            uuid.UUID `json:"boot_id"`
	JournalGeneration uuid.UUID `json:"journal_generation"`
	Revision          int64     `json:"revision"`
	PolicyRevision    int64     `json:"policy_revision"`
	IssuedAt          time.Time `json:"issued_at"`
	ExpiresAt         time.Time `json:"expires_at"`
	ValidUntil        time.Time `json:"valid_until"`
}

// Usage is the amount that one reservation uses on a node.
type Usage struct {
	ReservationID uuid.UUID `json:"reservation_id"`
	ResourceID    uuid.UUID `json:"resource_id"`
	Used          Vector    `json:"used"`
}

// CapacityReport is an agent report of node capacity and current use.
type CapacityReport struct {
	ProbeID           uuid.UUID `json:"probe_id"`
	ProbeRevision     int64     `json:"probe_revision"`
	InstallationID    uuid.UUID `json:"installation_id"`
	NodeID            uuid.UUID `json:"node_id"`
	SessionID         uuid.UUID `json:"session_id"`
	BootID            uuid.UUID `json:"boot_id"`
	JournalGeneration uuid.UUID `json:"journal_generation"`
	Architecture      string    `json:"architecture"`
	RuntimeVersion    string    `json:"runtime_version"`
	StorageID         string    `json:"storage_id"`
	Capacity          Vector    `json:"capacity"`
	Used              Vector    `json:"used"`
	Reservations      []Usage   `json:"reservations"`
}

// Node is one candidate host with its presence, policy, probe, and report.
type Node struct {
	ID                uuid.UUID       `json:"id"`
	InstallationID    uuid.UUID       `json:"installation_id"`
	Revoked           bool            `json:"revoked"`
	SessionID         uuid.UUID       `json:"session_id"`
	BootID            uuid.UUID       `json:"boot_id"`
	JournalGeneration uuid.UUID       `json:"journal_generation"`
	LastSeenAt        time.Time       `json:"last_seen_at"`
	RecoveryRequired  bool            `json:"recovery_required"`
	Policy            *NodePolicy     `json:"policy"`
	Probe             *CapacityProbe  `json:"probe"`
	Report            *CapacityReport `json:"report"`
}

// PortRange is the span of host ports an installation lets placement choose from.
// A resource asks for one by requesting port number zero.
type PortRange struct {
	First uint16 `json:"first"`
	Last  uint16 `json:"last"`
}

// Port is one host port binding. A number of zero in a request asks placement to
// choose one from the installation's range.
type Port struct {
	Address   string `json:"address"`
	Transport string `json:"transport"`
	Number    uint16 `json:"number"`
	// Name tells apart two ports of one resource, such as database and rest.
	Name string `json:"name,omitempty"`
}

// Resource is one unit to place with the requirements a node must meet.
type Resource struct {
	ID               uuid.UUID         `json:"id"`
	ReservationID    uuid.UUID         `json:"reservation_id"`
	OwnedResourceIDs []uuid.UUID       `json:"owned_resource_ids"`
	Kind             string            `json:"kind"`
	Role             string            `json:"role"`
	Engine           string            `json:"engine"`
	Version          string            `json:"version"`
	Architecture     string            `json:"architecture"`
	RuntimeVersion   string            `json:"runtime_version"`
	StorageID        string            `json:"storage_id"`
	NodeAffinity     uuid.UUID         `json:"node_affinity"`
	Labels           map[string]string `json:"labels"`
	Budgets          Budgets           `json:"budgets"`
	Ports            []Port            `json:"ports"`
}

// Group names resources that must spread across a failure domain.
type Group struct {
	ID        string      `json:"id"`
	DomainKey string      `json:"domain_key"`
	Members   []uuid.UUID `json:"members"`
}

// Request is one immutable placement request for one operation revision.
type Request struct {
	Version         int        `json:"version"`
	InstallationID  uuid.UUID  `json:"installation_id"`
	ClusterID       uuid.UUID  `json:"cluster_id"`
	OperationID     uuid.UUID  `json:"operation_id"`
	DesiredRevision int64      `json:"desired_revision"`
	Profile         string     `json:"profile"`
	PortRange       PortRange  `json:"port_range"`
	Resources       []Resource `json:"resources"`
	Groups          []Group    `json:"groups"`
}

// Allocation is a committed reservation of node capacity and ports.
type Allocation struct {
	ID                 uuid.UUID         `json:"id"`
	InstallationID     uuid.UUID         `json:"installation_id"`
	NodeID             uuid.UUID         `json:"node_id"`
	ClusterID          uuid.UUID         `json:"cluster_id"`
	ResourceID         uuid.UUID         `json:"resource_id"`
	CreatedByOperation uuid.UUID         `json:"created_by_operation"`
	CreatedRevision    int64             `json:"created_revision"`
	OwnedResourceIDs   []uuid.UUID       `json:"owned_resource_ids"`
	Budget             Vector            `json:"budget"`
	Ports              []Port            `json:"ports"`
	Groups             map[string]string `json:"groups"`
	Released           bool              `json:"released"`
}

// Snapshot is the observed installation state that a decision reads.
type Snapshot struct {
	InstallationID uuid.UUID    `json:"installation_id"`
	ObservedAt     time.Time    `json:"observed_at"`
	Nodes          []Node       `json:"nodes"`
	Allocations    []Allocation `json:"allocations"`
}

// Assignment places one resource on one node. Ports are the concrete bindings the
// decision chose, in the order the resource requested them, because a request may
// name a range rather than a number.
type Assignment struct {
	ResourceID     uuid.UUID `json:"resource_id"`
	ReservationID  uuid.UUID `json:"reservation_id"`
	NodeID         uuid.UUID `json:"node_id"`
	PolicyRevision int64     `json:"policy_revision"`
	ProbeRevision  int64     `json:"probe_revision"`
	Ports          []Port    `json:"ports"`
}

// Decision is the result of one placement request.
type Decision struct {
	Assignments []Assignment `json:"assignments"`
}
