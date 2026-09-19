package placement

import (
	"encoding/json"
	"errors"
	"github.com/google/uuid"
	"math"
	"slices"
	"testing"
	"time"
)

func fixture() (Request, Snapshot) {
	now := time.Now().UTC()
	installation := uuid.New()
	resource := Resource{ID: uuid.New(), ReservationID: uuid.New(), Kind: "instance", Engine: "postgres", Version: "17.11", Architecture: "arm64", RuntimeVersion: "docker-29.1.3", StorageID: "local-data", Budgets: Budgets{Workload: new(Vector{1000, 1000, 1000}), Supporting: new(Vector{100, 100, 100}), Backup: new(Vector{0, 0, 100})}}
	resource.OwnedResourceIDs = []uuid.UUID{resource.ID}
	request := Request{Version: 1, InstallationID: installation, ClusterID: uuid.New(), OperationID: uuid.New(), DesiredRevision: 1, Profile: "compact", Resources: []Resource{resource}}
	snapshot := Snapshot{InstallationID: installation, ObservedAt: now}
	for range 3 {
		id, session, boot, journal := uuid.New(), uuid.New(), uuid.New(), uuid.New()
		probe := &CapacityProbe{ID: uuid.New(), InstallationID: installation, NodeID: id, SessionID: session, BootID: boot, JournalGeneration: journal, Revision: 1, PolicyRevision: 1, IssuedAt: now.Add(-time.Second), ExpiresAt: now.Add(9 * time.Second), ValidUntil: now.Add(29 * time.Second)}
		report := &CapacityReport{ProbeID: probe.ID, ProbeRevision: probe.Revision, InstallationID: installation, NodeID: id, SessionID: session, BootID: boot, JournalGeneration: journal, Architecture: "arm64", RuntimeVersion: "docker-29.1.3", StorageID: "local-data", Capacity: Vector{10000, 10000, 10000}, Used: Vector{1000, 1000, 1000}}
		policy := &NodePolicy{InstallationID: installation, NodeID: id, Revision: 1, Architecture: "arm64", RuntimeVersion: report.RuntimeVersion, StorageID: report.StorageID, Engines: []EngineVersion{{"postgres", "17.11"}}, Headroom: Vector{1000, 1000, 1000}, FailureDomains: map[string]string{"rack": id.String()}}
		snapshot.Nodes = append(snapshot.Nodes, Node{ID: id, InstallationID: installation, SessionID: session, BootID: boot, JournalGeneration: journal, LastSeenAt: now, Policy: policy, Probe: probe, Report: report})
	}
	return request, snapshot
}

func TestDeterministicPlacementAndHardFilters(t *testing.T) {
	request, snapshot := fixture()
	want := slices.MinFunc(snapshot.Nodes, func(a, b Node) int { return slices.Compare(a.ID[:], b.ID[:]) }).ID
	decision, err := Plan(request, snapshot)
	if err != nil || decision.Assignments[0].NodeID != want {
		t.Fatalf("tie break: %v %v", decision, err)
	}
	slices.Reverse(snapshot.Nodes)
	again, err := Plan(request, snapshot)
	if err != nil || again.Assignments[0] != decision.Assignments[0] {
		t.Fatal("input order changed placement")
	}
	if err := CheckDecision(request, snapshot, decision); err != nil {
		t.Fatal(err)
	}
	for _, kind := range []string{"presence", "sample", "session", "recovery", "cordon", "maintenance", "architecture", "runtime", "storage", "engine", "label", "affinity", "headroom", "cpu", "memory", "disk", "backup"} {
		t.Run(kind, func(t *testing.T) {
			r, s := fixture()
			s.Nodes = s.Nodes[:1]
			n := &s.Nodes[0]
			switch kind {
			case "presence":
				n.LastSeenAt = s.ObservedAt.Add(-PresenceAge)
			case "sample":
				n.Probe.ValidUntil = s.ObservedAt
			case "session":
				n.SessionID = uuid.New()
			case "recovery":
				n.RecoveryRequired = true
			case "cordon":
				n.Policy.Cordoned = true
			case "maintenance":
				n.Policy.Maintenance = true
			case "architecture":
				r.Resources[0].Architecture = "amd64"
			case "runtime":
				r.Resources[0].RuntimeVersion = "other"
			case "storage":
				r.Resources[0].StorageID = "other"
			case "engine":
				r.Resources[0].Version = "18.6"
			case "label":
				r.Resources[0].Labels = map[string]string{"zone": "needed"}
			case "affinity":
				r.Resources[0].NodeAffinity = uuid.New()
			case "headroom":
				n.Policy.Headroom.MemoryBytes = 9000
			case "cpu":
				r.Resources[0].Budgets.Workload.CPUMillis = 9000
			case "memory":
				r.Resources[0].Budgets.Workload.MemoryBytes = 9000
			case "disk":
				r.Resources[0].Budgets.Workload.DiskBytes = 9000
			case "backup":
				r.Resources[0].Budgets.Backup.DiskBytes = 9000
			}
			if _, err := Plan(r, s); !errors.Is(err, ErrUnsatisfied) {
				t.Fatalf("hard filter %s: %v", kind, err)
			}
		})
	}
}

func TestReservationAccountingAndStatefulReuse(t *testing.T) {
	request, snapshot := fixture()
	snapshot.Nodes = snapshot.Nodes[:1]
	node := &snapshot.Nodes[0]
	resource := &request.Resources[0]
	node.Report.Capacity = Vector{3000, 3000, 3000}
	node.Report.Used = Vector{1500, 1500, 1500}
	node.Policy.Headroom = Vector{100, 100, 100}
	held := Allocation{ID: resource.ReservationID, InstallationID: request.InstallationID, ClusterID: request.ClusterID, NodeID: node.ID, ResourceID: resource.ID, OwnedResourceIDs: resource.OwnedResourceIDs, Budget: Vector{2000, 2000, 2000}}
	snapshot.Allocations = []Allocation{held}
	node.Report.Reservations = []Usage{{ReservationID: held.ID, ResourceID: held.ResourceID, Used: Vector{1000, 1000, 1000}}}
	if _, err := Plan(request, snapshot); err != nil {
		t.Fatal("known allocation was charged twice", err)
	}
	node.Report.Reservations = nil
	if _, err := Plan(request, snapshot); !errors.Is(err, ErrUnsatisfied) {
		t.Fatal("unknown attribution incorrectly offset observed use", err)
	}
	node.Report.Reservations = []Usage{{ReservationID: held.ID, ResourceID: held.ResourceID, Used: node.Report.Used}}
	resource.Budgets.Workload = new(Vector{2500, 2500, 2400})
	resource.Budgets.Supporting = new(Vector{})
	resource.Budgets.Backup = new(Vector{})
	if _, err := Plan(request, snapshot); err != nil {
		t.Fatal("budget growth did not account for observed attribution", err)
	}
	resource.NodeAffinity = uuid.New()
	if _, err := Plan(request, snapshot); !errors.Is(err, ErrUnsatisfied) {
		t.Fatal("stateful allocation relocated", err)
	}
	resource.NodeAffinity = uuid.Nil
	snapshot.Allocations[0].Released = true
	if _, err := Plan(request, snapshot); !errors.Is(err, ErrConflict) {
		t.Fatal("released identity reused", err)
	}
}

func TestHAAndConstrainedSearch(t *testing.T) {
	request, snapshot := fixture()
	first := request.Resources[0]
	data, _ := json.Marshal(first)
	for range 2 {
		var resource Resource
		json.Unmarshal(data, &resource)
		resource.ID, resource.ReservationID = uuid.New(), uuid.New()
		resource.OwnedResourceIDs = []uuid.UUID{resource.ID}
		request.Resources = append(request.Resources, resource)
	}
	request.Profile = "ha"
	group := Group{ID: "voters", DomainKey: "rack"}
	for _, resource := range request.Resources {
		group.Members = append(group.Members, resource.ID)
	}
	request.Groups = []Group{group}
	decision, err := Plan(request, snapshot)
	if err != nil || len(decision.Assignments) != 3 {
		t.Fatal("three-domain placement", err)
	}
	if err := CheckDecision(request, snapshot, decision); err != nil {
		t.Fatal(err)
	}
	snapshot.Nodes[1].Policy.FailureDomains["rack"] = snapshot.Nodes[0].Policy.FailureDomains["rack"]
	if _, err := Plan(request, snapshot); !errors.Is(err, ErrUnsatisfied) {
		t.Fatal("HA protection weakened", err)
	}
	request.Profile = "compact"
	request.Groups = nil
	if _, err := Plan(request, snapshot); err != nil {
		t.Fatal("explicit compact profile failed", err)
	}
	// Tight labels force the constrained member onto its only node before flexible members.
	request.Resources[2].Labels = map[string]string{"dedicated": "yes"}
	snapshot.Nodes[0].Policy.Labels = map[string]string{"dedicated": "yes"}
	decision, err = Plan(request, snapshot)
	if err != nil {
		t.Fatal(err)
	}
	for _, a := range decision.Assignments {
		if a.ResourceID == request.Resources[2].ID && a.NodeID != snapshot.Nodes[0].ID {
			t.Fatal("hard affinity ignored")
		}
	}
}

func TestPortsAndInvalidBudgets(t *testing.T) {
	for _, test := range []struct {
		a, b     Port
		conflict bool
	}{
		{Port{"0.0.0.0", "tcp", 5432}, Port{"127.0.0.1", "tcp", 5432}, true},
		{Port{"::", "tcp", 5432}, Port{"127.0.0.1", "tcp", 5432}, true},
		{Port{"127.0.0.1", "tcp", 5432}, Port{"127.0.0.2", "tcp", 5432}, false},
		{Port{"127.0.0.1", "tcp", 5432}, Port{"127.0.0.1", "udp", 5432}, false},
	} {
		if PortsConflict(test.a, test.b) != test.conflict {
			t.Fatal("wrong wildcard overlap")
		}
	}
	request, snapshot := fixture()
	snapshot.Nodes = snapshot.Nodes[:1]
	request.Resources[0].Ports = []Port{{"127.0.0.1", "tcp", 5432}}
	snapshot.Allocations = []Allocation{{ID: uuid.New(), InstallationID: request.InstallationID, NodeID: snapshot.Nodes[0].ID, ClusterID: uuid.New(), ResourceID: uuid.New(), Budget: Vector{}, Ports: []Port{{"0.0.0.0", "tcp", 5432}}}}
	if _, err := Plan(request, snapshot); !errors.Is(err, ErrUnsatisfied) {
		t.Fatal("occupied reservation ignored", err)
	}
	for _, port := range []Port{{"::ffff:127.0.0.1", "tcp", 5432}, {"fe80::1%eth0", "tcp", 5432}, {"224.0.0.1", "tcp", 5432}, {"127.0.0.1", "tcp", 0}} {
		if port.Validate() == nil {
			t.Fatal("invalid address accepted")
		}
	}
	request.Resources[0].Budgets.Backup = nil
	if _, err := Plan(request, snapshot); !errors.Is(err, ErrInvalid) {
		t.Fatal("implicit zero budget accepted", err)
	}
	if _, err := (Vector{CPUMillis: math.MaxInt64}).Add(Vector{CPUMillis: 1}); !errors.Is(err, ErrInvalid) {
		t.Fatal("overflow accepted")
	}
	if compareRatio(math.MaxUint64-1, math.MaxUint64, math.MaxUint64-2, math.MaxUint64) <= 0 {
		t.Fatal("ratio overflow")
	}
}
