package drain

import (
	"slices"
	"testing"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/cluster"
	"github.com/sirrobot01/synod/internal/store/postgres"
)

// standalone builds one admissible standalone cluster of the given engine.
func standalone(name string, engine cluster.Engine, persistence string) cluster.CreateSpec {
	spec := cluster.CreateSpec{SchemaVersion: 1, Name: name, Engine: engine}
	if engine == cluster.PostgreSQL {
		spec.PostgreSQL = &cluster.PostgreSQLSpec{Version: "17.11", Topology: "standalone"}
	} else {
		spec.Redis = &cluster.RedisSpec{Version: "8.2.7", Topology: "standalone", Persistence: persistence}
	}
	if err := spec.Validate(); err != nil {
		panic(err)
	}
	return spec
}

// running puts one cluster on the node with a retained volume and a live reservation.
func running(spec cluster.CreateSpec) Workload {
	return Workload{
		ClusterID: uuid.New(), Spec: spec, DesiredLifecycle: cluster.DesiredRunning,
		Resources: []uuid.UUID{uuid.New()},
		Volumes:   []Volume{{ResourceID: uuid.New(), Retention: postgres.Retained, CapacityRequestBytes: 100 << 30}},
	}
}

func only(t *testing.T, report Report) Disposition {
	t.Helper()
	if len(report.Clusters) != 1 {
		t.Fatalf("clusters=%d, want one", len(report.Clusters))
	}
	return report.Clusters[0]
}

// Cordon is what stops the planner refilling a node behind a drain, so a report for
// an uncordoned node says so and never claims the node is drained.
func TestDrainRefusesAnUncordonedNode(t *testing.T) {
	report := Decide(State{InstallationID: uuid.New(), NodeID: uuid.New()})
	if report.Drained {
		t.Fatal("an uncordoned node with nothing on it was reported drained")
	}
	if !slices.Contains(report.Unhandled, ReasonNotCordoned) {
		t.Fatalf("unhandled=%v, want the node named as uncordoned", report.Unhandled)
	}
	// Cordoned and empty is the one state that is actually drained.
	empty := Decide(State{InstallationID: uuid.New(), NodeID: uuid.New(), Cordoned: true})
	if !empty.Drained || len(empty.Unhandled) != 0 {
		t.Fatalf("report=%+v, want a cordoned empty node drained", empty)
	}
}

// The failure this package exists to prevent: a drain that reports success while a
// standalone database still serves from the node.
func TestDrainIsBlockedByAStandaloneWorkload(t *testing.T) {
	workload := running(standalone("db", cluster.PostgreSQL, ""))
	report := Decide(State{NodeID: uuid.New(), Cordoned: true, Workloads: []Workload{workload}})
	if report.Drained {
		t.Fatal("a node running a standalone database was reported drained")
	}
	found := only(t, report)
	// The data is here and it does not move, so the only honest option is downtime
	// and a rebuild somewhere else.
	if found.Action != ActionRestore || !found.Downtime {
		t.Fatalf("action=%q downtime=%v, want a restore with downtime", found.Action, found.Downtime)
	}
	for _, reason := range []string{ReasonLocalData, ReasonNoRestoreSource, ReasonWorkloadOnNode} {
		if !slices.Contains(found.Unhandled, reason) {
			t.Fatalf("unhandled=%v, want %s", found.Unhandled, reason)
		}
	}
	// The volume is named, so an operator can see which storage is holding the node.
	if len(found.Volumes) != 1 || found.Volumes[0] != workload.Volumes[0] {
		t.Fatalf("volumes=%+v, want the storage that holds the node", found.Volumes)
	}
}

// A drain must be blocked by an active role, and must not pretend a switchover is
// available when there is no second member to take it.
func TestDrainIsBlockedByAnActiveRole(t *testing.T) {
	workload := running(standalone("cache", cluster.Redis, "none"))
	report := Decide(State{NodeID: uuid.New(), Cordoned: true, Workloads: []Workload{workload}})
	if report.Drained {
		t.Fatal("a node holding an active role was reported drained")
	}
	found := only(t, report)
	// Nothing durable is here, so the workload can be placed elsewhere. That is
	// still a restart, and the role is still held here until it happens.
	if found.Action != ActionRecreate || !found.Downtime {
		t.Fatalf("action=%q downtime=%v, want a recreate with downtime", found.Action, found.Downtime)
	}
	if slices.Contains(found.Unhandled, ReasonLocalData) {
		t.Fatalf("unhandled=%v, want no data claim for an unpersisted Redis", found.Unhandled)
	}
	for _, reason := range []string{ReasonActiveRole, ReasonNoStandby} {
		if !slices.Contains(found.Unhandled, reason) {
			t.Fatalf("unhandled=%v, want %s", found.Unhandled, reason)
		}
	}
	// Stopping the cluster hands back the role. The reservation still holds the node.
	workload.DesiredLifecycle = cluster.DesiredStopped
	stopped := only(t, Decide(State{NodeID: uuid.New(), Cordoned: true, Workloads: []Workload{workload}}))
	if slices.Contains(stopped.Unhandled, ReasonActiveRole) {
		t.Fatalf("unhandled=%v, want a stopped cluster to hold no role", stopped.Unhandled)
	}
	if !slices.Contains(stopped.Unhandled, ReasonWorkloadOnNode) {
		t.Fatalf("unhandled=%v, want the reservation still counted", stopped.Unhandled)
	}
}

// A local volume never moves. A cluster whose workload has already gone still holds
// the node through its data, and that data needs a disposition, not a migration.
func TestRetainedDataHoldsTheNodeWithNothingToMove(t *testing.T) {
	workload := running(standalone("db", cluster.PostgreSQL, ""))
	workload.Resources = nil
	report := Decide(State{NodeID: uuid.New(), Cordoned: true, Workloads: []Workload{workload}})
	found := only(t, report)
	if found.Action != ActionNone || found.Downtime {
		t.Fatalf("action=%q downtime=%v, want nothing to move", found.Action, found.Downtime)
	}
	if !slices.Contains(found.Unhandled, ReasonLocalData) || report.Drained {
		t.Fatalf("report=%+v, want retained data to hold the node", report)
	}
	// Once the data is actually gone, nothing holds the node any more.
	workload.Volumes[0].Retention = postgres.Purged
	purged := Decide(State{NodeID: uuid.New(), Cordoned: true, Workloads: []Workload{workload}})
	if !purged.Drained || len(only(t, purged).Unhandled) != 0 {
		t.Fatalf("report=%+v, want a node with purged storage drained", purged)
	}
}

// Data does not move for an HA member either. A switchover moves the role and leaves
// the storage exactly where it was.
func TestSwitchoverMovesTheRoleAndNotTheData(t *testing.T) {
	spec := standalone("db", cluster.PostgreSQL, "")
	spec.PostgreSQL.Topology = "ha"
	workload := running(spec)
	found := only(t, Decide(State{NodeID: uuid.New(), Cordoned: true, Workloads: []Workload{workload}}))
	if found.Action != ActionSwitchover || found.Downtime {
		t.Fatalf("action=%q downtime=%v, want a switchover without downtime", found.Action, found.Downtime)
	}
	if !slices.Contains(found.Unhandled, ReasonLocalData) {
		t.Fatalf("unhandled=%v, want the data still held here", found.Unhandled)
	}
	// There is a member to switch to, so no standby is missing and no restore is
	// needed.
	for _, reason := range []string{ReasonNoStandby, ReasonNoRestoreSource} {
		if slices.Contains(found.Unhandled, reason) {
			t.Fatalf("unhandled=%v, want no %s", found.Unhandled, reason)
		}
	}
}

// Acting on a cluster while an operation is running races it.
func TestDrainWaitsForAnOperationInFlight(t *testing.T) {
	workload := running(standalone("db", cluster.PostgreSQL, ""))
	workload.InFlight = true
	found := only(t, Decide(State{NodeID: uuid.New(), Cordoned: true, Workloads: []Workload{workload}}))
	if !slices.Contains(found.Unhandled, ReasonOperationInFlight) {
		t.Fatalf("unhandled=%v, want the running operation named", found.Unhandled)
	}
}

// The same state must always give the same report, so two operators reading a drain
// see the same thing and an automated caller can compare two reads.
func TestDecideIsDeterministic(t *testing.T) {
	state := State{NodeID: uuid.New(), Cordoned: true, Workloads: []Workload{
		running(standalone("db", cluster.PostgreSQL, "")),
		running(standalone("cache", cluster.Redis, "rdb")),
		running(standalone("queue", cluster.Redis, "none")),
	}}
	first := Decide(state)
	slices.Reverse(state.Workloads)
	second := Decide(state)
	if len(first.Clusters) != 3 || len(second.Clusters) != 3 {
		t.Fatalf("clusters=%d and %d, want three each", len(first.Clusters), len(second.Clusters))
	}
	for i := range first.Clusters {
		if first.Clusters[i].ClusterID != second.Clusters[i].ClusterID {
			t.Fatalf("order changed with the input: %+v vs %+v", first.Clusters, second.Clusters)
		}
	}
}
