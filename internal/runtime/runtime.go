// Package runtime defines the node execution contract that agents run commands through.
package runtime

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
)

var ErrOwnershipConflict = errors.New("runtime_ownership_conflict")
var ErrConfigurationConflict = errors.New("runtime_configuration_conflict")
var ErrStorageLost = fmt.Errorf("storage_lost: %w", ErrConfigurationConflict)
var ErrUnsupportedConfiguration = errors.New("runtime_unsupported_configuration")
var ErrOutcomeUnknown = errors.New("runtime_outcome_unknown")
var ErrBusy = errors.New("runtime_busy")

// ErrNotApplied reports a command that provably changed nothing, such as a failed image pull.
var ErrNotApplied = errors.New("runtime_not_applied")

// Error reports a runtime failure with a stable kind and its cause.
type Error struct {
	Kind  error
	Cause error
}

// Error omits driver detail, which can name a socket or a host path.
func (e *Error) Error() string { return "runtime: " + e.Kind.Error() }

func (e *Error) Unwrap() error { return e.Cause }

// Is matches the stable kind, so a caller tests the kind and not the cause.
func (e *Error) Is(target error) bool { return errors.Is(e.Kind, target) }

// Result is the effect list that one command produced.
type Result struct{ Effects []*agentv1.Effect }

// Secrets are the resolved credentials for one command, keyed by purpose.
//
// They are a function argument and never part of a command. That is structural, not a
// convention: a driver serializes commands into labels, journals, and inventory reads,
// and a value that is never inside a command cannot be captured by any of them.
type Secrets map[string][]byte

// Resource is one owned native object and its observed state.
type Resource struct {
	ClusterID  uuid.UUID
	ResourceID uuid.UUID
	Kind       string
	Name       string
	NativeID   string
	State      string
	Health     string
	Ports      []*agentv1.PortBinding
}

// DriftClass says how an object on the host relates to this installation. The
// driver decides it from labels alone: only the controller knows what it has a
// record of, so the driver never guesses at ownership it cannot see.
type DriftClass string

const (
	// DriftUnowned is an object with no Synod labels. It was never ours.
	DriftUnowned DriftClass = "unowned"
	// DriftForeign is an object labelled for another installation or another node.
	DriftForeign DriftClass = "foreign"
	// DriftMislabelled is an object labelled for this node whose identity does not
	// hold together. It was ours, and something has changed it since.
	DriftMislabelled DriftClass = "mislabelled"
	// DriftChanged is an object this node owns whose configuration is no longer the
	// configuration this node created it with. It is reported, never corrected.
	DriftChanged DriftClass = "changed"
)

// Drift is one object on the host that this node did not expect to find as it is.
//
// It is a report. Nothing acts on it: specification 33 requires reporting unowned
// containers and unexpected changes without deleting them, and keeps adoption of
// arbitrary external databases outside v0.1.
type Drift struct {
	Kind     string
	NativeID string
	Name     string
	State    string
	Class    DriftClass
	// ClusterID and ResourceID are set only when the object still carries a
	// readable Synod identity.
	ClusterID  uuid.UUID
	ResourceID uuid.UUID
	// Detail is a stable code naming what disagrees. It is never prose, and it
	// never carries a host path or a label value.
	Detail string
}

// ClusterInspection is the owned inventory and effects of one cluster on a node.
type ClusterInspection struct {
	Resources []Resource
	Effects   []*agentv1.Effect
}

// Amount is a measured triple for one resource. It mirrors the shape that
// placement reserves against, which this package does not import: a driver must
// not depend on the controller.
type Amount struct {
	CPUMillis   int64
	MemoryBytes int64
	DiskBytes   int64
}

// ResourceUsage is what one owned resource was observed to use.
type ResourceUsage struct {
	ResourceID uuid.UUID
	Used       Amount
}

// Runtime executes typed commands on exactly owned node resources.
// An implementation requires durable journal admission before Execute.
type Runtime interface {
	// Execute runs one command. Secrets carries the credentials the command's
	// references name; it is empty for a command that needs none.
	Execute(ctx context.Context, command *agentv1.Command, secrets Secrets) (Result, error)
	Recover(ctx context.Context, command *agentv1.Command, prior []*agentv1.Effect) (Result, error)
	Inventory(ctx context.Context) ([]Resource, error)
	InspectCluster(ctx context.Context, clusterID uuid.UUID, prior []*agentv1.Effect) (ClusterInspection, error)
	// Usage measures what the named owned resources consume. A resource the
	// driver does not own is absent from the answer rather than reported as
	// zero, because zero is a measurement and absence is not.
	Usage(ctx context.Context, resources []uuid.UUID) ([]ResourceUsage, error)
	// Drift reports objects on the host that this node did not expect to find as
	// they are. It reads only, and it removes nothing.
	Drift(ctx context.Context) ([]Drift, error)
}

// AppContainer identifies an owned tier 2 container for fencing and snapshots.
type AppContainer struct {
	AppID      string
	ResourceID string
	PID        int
	Running    bool
	Mounts     []*agentv1.Mount
}
