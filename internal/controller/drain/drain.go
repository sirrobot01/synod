// Package drain decides what a node's work needs before the node can be emptied.
//
// It decides and reports. It moves nothing, stops nothing, and deletes nothing.
// Specification 33 states that drain does not imply local volumes can move, that a
// standalone workload requiring downtime or backup recovery must be shown rather than
// silently moved, and that a node must not be reported drained while required data or
// an active role is unhandled. Every one of those is a reason to hand an operator a
// decision instead of taking it for them.
//
// The failure this package exists to prevent is a drain that reports success while a
// primary still serves from the node.
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
	// ActionNone means nothing this cluster holds on the node needs a decision.
	ActionNone Action = "none"
	// ActionRecreate places the workload elsewhere and starts it there. It is only
	// offered when the cluster holds no durable data on this node, because a local
	// volume does not move.
	ActionRecreate Action = "recreate"
	// ActionSwitchover hands the active role to another member. It needs a member
	// that can take it.
	ActionSwitchover Action = "switchover"
	// ActionRestore accepts downtime and rebuilds the cluster elsewhere from a
	// backup. It is what durable local data leaves as the only honest option.
	ActionRestore Action = "restore"
)

// Reasons a cluster or a node still needs a decision. These are stable codes: an
// operator who reads "cannot drain" learns nothing about what to do next.
const (
	// ReasonLocalData reports durable data on this node. It does not move.
	ReasonLocalData = "local_data_does_not_move"
	// ReasonActiveRole reports that this node runs the cluster's only active member.
	ReasonActiveRole = "active_role_on_this_node"
	// ReasonNoStandby reports a switchover with nowhere to switch to.
	ReasonNoStandby = "no_standby_member"
	// ReasonNoRestoreSource reports a restore with nothing to restore from.
	ReasonNoRestoreSource = "no_restore_source"
	// ReasonWorkloadOnNode reports a reservation this cluster still holds here. A
	// node holding one is not empty, whatever the cluster is doing.
	ReasonWorkloadOnNode = "workload_on_node"
	// ReasonOperationInFlight reports a cluster already being changed. Acting on it
	// now races the operation that is already running.
	ReasonOperationInFlight = "operation_in_flight"
	// ReasonNotCordoned reports a node that still takes new placement.
	ReasonNotCordoned = "node_not_cordoned"
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
	// Downtime reports that carrying out Action stops the cluster serving. An
	// operator has to be told this rather than discover it.
	Downtime  bool
	Resources []uuid.UUID
	Volumes   []Volume
	// Unhandled names every reason this cluster still holds the node. An empty list
	// means the node is free of this cluster.
	Unhandled []string
}

// Report answers what stands between this node and being empty.
type Report struct {
	NodeID   uuid.UUID
	Cordoned bool
	Clusters []Disposition
	// Drained is true only when nothing on this node still needs a decision. It is
	// never true while data or an active role is unhandled.
	Drained bool
	// Unhandled names node-level reasons, separate from any cluster's own.
	Unhandled []string
}

// Decide answers what the node's work needs. It is pure: same state, same report.
func Decide(state State) Report {
	report := Report{NodeID: state.NodeID, Cordoned: state.Cordoned}
	if !state.Cordoned {
		// A drain of a node the planner can still fill is a race the operator loses.
		// Cordon comes first, and until it does the rest of the report is advice.
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
	// Durable data is the thing that cannot move. A Redis instance an operator asked
	// not to persist is the one case where this node holds nothing worth carrying,
	// so its workload can be placed elsewhere and started empty.
	data := durable(spec) && holdsData(workload)
	standalone := disposition.Topology == "standalone"
	switch {
	case len(workload.Resources) == 0:
		// Storage only. Nothing runs here, so there is nothing to move; the data
		// needs a disposition, which is the operator's to give.
	case data && !standalone:
		// The active role can move to another member. The data cannot: it stays on
		// this node and still needs a disposition of its own afterwards.
		disposition.Action = ActionSwitchover
	case data:
		// One member, and its data is here. Moving this cluster means downtime and
		// rebuilding it somewhere else from a backup.
		disposition.Action, disposition.Downtime = ActionRestore, true
		// A restore needs something to restore from, and this installation keeps no
		// backup artifacts yet. Saying so is the point: specification 33 requires
		// showing a standalone workload that needs downtime or backup recovery
		// rather than moving it and finding out.
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
	// An active role is held by whatever is meant to be serving. A standalone
	// cluster has one member, so the node running it holds the role outright and a
	// switchover has nowhere to go.
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
