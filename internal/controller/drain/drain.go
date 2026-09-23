// Package drain decides what a node's work needs before the node can be emptied. It
// moves nothing, stops nothing, and deletes nothing.
//
// Specification 33 states that drain does not imply local volumes can move, and that a
// node must not be reported drained while required data or an active role is
// unhandled. The failure it prevents is a drain that reports success while a primary
// still serves from the node.
package drain

import (
	"bytes"
	"slices"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/cluster"
	"github.com/sirrobot01/synod/internal/store/postgres"
)

// Action is what an operator must do to a cluster before this node is empty.
type Action string

const (
	ActionNone Action = "none"
	// ActionRecreate is only offered when the cluster holds no durable data here,
	// because a local volume does not move.
	ActionRecreate Action = "recreate"
	// ActionSwitchover hands the active role to another member.
	ActionSwitchover Action = "switchover"
	// ActionRestore accepts downtime and rebuilds elsewhere from a backup.
	ActionRestore Action = "restore"
)

// Reasons a cluster or a node still needs a decision. They are stable codes: an
// operator who reads "cannot drain" learns nothing about what to do next.
const (
	ReasonLocalData         = "local_data_does_not_move"
	ReasonActiveRole        = "active_role_on_this_node"
	ReasonNoStandby         = "no_standby_member"
	ReasonNoRestoreSource   = "no_restore_source"
	ReasonWorkloadOnNode    = "workload_on_node"
	ReasonOperationInFlight = "operation_in_flight"
	ReasonNotCordoned       = "node_not_cordoned"
)

// Volume is one piece of storage this node holds for a cluster.
type Volume struct {
	ResourceID           uuid.UUID
	Retention            postgres.Retention
	CapacityRequestBytes int64
}

// Workload is everything one cluster holds on the node being drained.
type Workload struct {
	ClusterID        uuid.UUID
	Spec             cluster.CreateSpec
	DesiredLifecycle cluster.DesiredLifecycle
	// Resources are the reservations this cluster still holds on the node.
	Resources []uuid.UUID
	Volumes   []Volume
	// InFlight reports an operation that has not finished for this cluster.
	InFlight bool
}

// State is what the node holds right now.
type State struct {
	InstallationID uuid.UUID
	NodeID         uuid.UUID
	Cordoned       bool
	Workloads      []Workload
}

// Disposition is the decision one cluster needs before the node can be emptied.
type Disposition struct {
	ClusterID uuid.UUID
	Name      string
	Engine    cluster.Engine
	Topology  string
	Action    Action
	// Downtime reports that carrying out Action stops the cluster serving.
	Downtime  bool
	Resources []uuid.UUID
	Volumes   []Volume
	// Unhandled names every reason this cluster still holds the node.
	Unhandled []string
}

// Report answers what stands between this node and being empty.
type Report struct {
	NodeID   uuid.UUID
	Cordoned bool
	Clusters []Disposition
	// Drained is never true while data or an active role is unhandled.
	Drained bool
	// Unhandled names node-level reasons, separate from any cluster's own.
	Unhandled []string
}

// Decide answers what the node's work needs. It is pure: same state, same report.
func Decide(state State) Report {
	report := Report{NodeID: state.NodeID, Cordoned: state.Cordoned}
	if !state.Cordoned {
		// Draining a node the planner can still fill is a race the operator loses.
		report.Unhandled = append(report.Unhandled, ReasonNotCordoned)
	}
	for _, workload := range state.Workloads {
		report.Clusters = append(report.Clusters, decide(workload))
	}
	slices.SortFunc(report.Clusters, func(a, b Disposition) int {
		return bytes.Compare(a.ClusterID[:], b.ClusterID[:])
	})
	report.Drained = len(report.Unhandled) == 0
	for _, disposition := range report.Clusters {
		if len(disposition.Unhandled) != 0 {
			report.Drained = false
		}
	}
	return report
}

// decide answers what one cluster needs before this node is free of it.
func decide(workload Workload) Disposition {
	spec := workload.Spec
	disposition := Disposition{
		ClusterID: workload.ClusterID, Name: spec.Name, Engine: spec.Engine,
		Topology: topology(spec), Resources: workload.Resources, Volumes: workload.Volumes,
		Action: ActionNone,
	}
	// Durable data is the thing that cannot move.
	data := durable(spec) && holdsData(workload)
	standalone := disposition.Topology == "standalone"
	switch {
	case len(workload.Resources) == 0:
		// Storage only. The data needs a disposition, not a move.
	case data && !standalone:
		// The role can move to another member. The data stays and still needs a
		// disposition of its own.
		disposition.Action = ActionSwitchover
	case data:
		disposition.Action, disposition.Downtime = ActionRestore, true
		// This installation keeps no backup artifacts yet, and specification 33
		// requires showing that rather than moving the cluster and finding out.
		disposition.Unhandled = append(disposition.Unhandled, ReasonNoRestoreSource)
	default:
		disposition.Action, disposition.Downtime = ActionRecreate, true
	}
	if data {
		disposition.Unhandled = append(disposition.Unhandled, ReasonLocalData)
	}
	if len(workload.Resources) != 0 {
		disposition.Unhandled = append(disposition.Unhandled, ReasonWorkloadOnNode)
	}
	// A standalone cluster has one member, so the node running it holds the role
	// outright and a switchover has nowhere to go.
	if workload.DesiredLifecycle == cluster.DesiredRunning && len(workload.Resources) != 0 {
		disposition.Unhandled = append(disposition.Unhandled, ReasonActiveRole)
		if standalone {
			disposition.Unhandled = append(disposition.Unhandled, ReasonNoStandby)
		}
	}
	if workload.InFlight {
		disposition.Unhandled = append(disposition.Unhandled, ReasonOperationInFlight)
	}
	return disposition
}

// durable reports a workload whose data an operator expects to survive. Only Redis
// with persistence explicitly turned off is not durable.
func durable(spec cluster.CreateSpec) bool {
	return spec.Engine != cluster.Redis || spec.Redis == nil || spec.Redis.Persistence != "none"
}

// holdsData reports storage on this node that still holds the cluster's data.
func holdsData(workload Workload) bool {
	for _, volume := range workload.Volumes {
		if volume.Retention != postgres.Purged {
			return true
		}
	}
	return false
}

func topology(spec cluster.CreateSpec) string {
	switch {
	case spec.PostgreSQL != nil:
		return spec.PostgreSQL.Topology
	case spec.Redis != nil:
		return spec.Redis.Topology
	}
	return ""
}
