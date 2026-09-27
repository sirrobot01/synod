package placement

import (
	"encoding/json"
	"errors"
	"math"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

func fixture() (Request, Snapshot) {
	now := time.Now().UTC()
	installation := uuid.New()
	resource := Resource{ID: uuid.New(), ReservationID: uuid.New(), Kind: "instance", Role: "primary", Engine: "postgres", Version: "17.11", Architecture: "arm64", RuntimeVersion: "docker-29.1.3", StorageID: "local-data", Budgets: Budgets{Workload: new(Vector{1000, 1000, 1000}), Supporting: new(Vector{100, 100, 100}), Backup: new(Vector{0, 0, 100})}}
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
	if err != nil || !reflect.DeepEqual(again.Assignments[0], decision.Assignments[0]) {
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

// A colocated service lands on its partner's node, and nothing else can pull it apart.
func TestColocatedServiceSharesItsPartnersNode(t *testing.T) {
	request, snapshot := fixture()
	partner := request.Resources[0]
	service := Resource{ID: uuid.New(), ReservationID: uuid.New(), Kind: "service", Engine: "postgres", Version: "17.11", Architecture: "arm64", Colocate: partner.ID, Budgets: Budgets{Workload: new(Vector{100, 100, 100}), Supporting: new(Vector{}), Backup: new(Vector{})}}
	service.OwnedResourceIDs = []uuid.UUID{service.ID}
	peer := partner
	peer.ID, peer.ReservationID = uuid.New(), uuid.New()
	peer.OwnedResourceIDs = []uuid.UUID{peer.ID}
	request.Profile = "ha"
	request.Groups = []Group{{ID: "members", DomainKey: "rack", Members: []uuid.UUID{partner.ID, peer.ID}}}
	request.Resources[0].Role, peer.Role = "primary", "replica"
	request.Resources = append(request.Resources, service, peer)
	decision, err := Plan(request, snapshot)
	if err != nil {
		t.Fatal(err)
	}
	nodes := map[uuid.UUID]uuid.UUID{}
	for _, assignment := range decision.Assignments {
		nodes[assignment.ResourceID] = assignment.NodeID
	}
	if nodes[service.ID] != nodes[partner.ID] {
		t.Fatalf("service on %s, partner on %s", nodes[service.ID], nodes[partner.ID])
	}
	if err := CheckDecision(request, snapshot, decision); err != nil {
		t.Fatal(err)
	}
	// A decision that splits the pair is refused.
	for index := range decision.Assignments {
		if decision.Assignments[index].ResourceID != service.ID {
			continue
		}
		for _, node := range snapshot.Nodes {
			if node.ID != nodes[partner.ID] {
				decision.Assignments[index].NodeID = node.ID
				decision.Assignments[index].PolicyRevision, decision.Assignments[index].ProbeRevision = node.Policy.Revision, node.Probe.Revision
				break
			}
		}
	}
	if err := CheckDecision(request, snapshot, decision); err == nil {
		t.Fatal("a split pair passed the check")
	}
	for name, change := range map[string]func(*Request){
		"self":       func(r *Request) { r.Resources[1].Colocate = r.Resources[1].ID },
		"unknown":    func(r *Request) { r.Resources[1].Colocate = uuid.New() },
		"other role": func(r *Request) { r.Resources[1].Role = "router" },
	} {
		changed := request
		changed.Resources = slices.Clone(request.Resources)
		change(&changed)
		if _, err := Plan(changed, snapshot); !errors.Is(err, ErrInvalid) {
			t.Errorf("%s: %v, want invalid", name, err)
		}
	}
}

// A request without a runtime version or storage ID takes a node whatever it reports.
func TestEmptyRuntimeAndStorageAcceptAnyNode(t *testing.T) {
	request, snapshot := fixture()
	request.Resources[0].RuntimeVersion, request.Resources[0].StorageID = "", ""
	snapshot.Nodes = snapshot.Nodes[:1]
	node := &snapshot.Nodes[0]
	node.Policy.RuntimeVersion, node.Report.RuntimeVersion = "docker-28.0.0", "docker-28.0.0"
	node.Policy.StorageID, node.Report.StorageID = "fast-nvme", "fast-nvme"
	decision, err := Plan(request, snapshot)
	if err != nil || decision.Assignments[0].NodeID != node.ID {
		t.Fatalf("Plan=%v, %v; want node %s", decision, err, node.ID)
	}
	if err := CheckDecision(request, snapshot, decision); err != nil {
		t.Fatal(err)
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
		{Port{"0.0.0.0", "tcp", 5432, ""}, Port{"127.0.0.1", "tcp", 5432, ""}, true},
		{Port{"::", "tcp", 5432, ""}, Port{"127.0.0.1", "tcp", 5432, ""}, true},
		{Port{"127.0.0.1", "tcp", 5432, ""}, Port{"127.0.0.2", "tcp", 5432, ""}, false},
		{Port{"127.0.0.1", "tcp", 5432, ""}, Port{"127.0.0.1", "udp", 5432, ""}, false},
	} {
		if PortsConflict(test.a, test.b) != test.conflict {
			t.Fatal("wrong wildcard overlap")
		}
	}
	request, snapshot := fixture()
	snapshot.Nodes = snapshot.Nodes[:1]
	request.Resources[0].Ports = []Port{{"127.0.0.1", "tcp", 5432, ""}}
	snapshot.Allocations = []Allocation{{ID: uuid.New(), InstallationID: request.InstallationID, NodeID: snapshot.Nodes[0].ID, ClusterID: uuid.New(), ResourceID: uuid.New(), Budget: Vector{}, Ports: []Port{{"0.0.0.0", "tcp", 5432, ""}}}}
	if _, err := Plan(request, snapshot); !errors.Is(err, ErrUnsatisfied) {
		t.Fatal("occupied reservation ignored", err)
	}
	for _, port := range []Port{{"::ffff:127.0.0.1", "tcp", 5432, ""}, {"fe80::1%eth0", "tcp", 5432, ""}, {"224.0.0.1", "tcp", 5432, ""}, {"127.0.0.1", "tcp", 0, ""}} {
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

// This profile publishes on the host's own network, so a port is a host resource. Every
// cluster asking for the same number means one cluster per node: the second is refused
// while the node has room for it. Placement knows what each node holds, so it chooses.
func TestPlanChoosesAFreePortForEachResource(t *testing.T) {
	request, snapshot := fixture()
	snapshot.Nodes = snapshot.Nodes[:1]
	node := snapshot.Nodes[0].ID
	request.PortRange = PortRange{First: 5432, Last: 5434}
	request.Resources[0].NodeAffinity = node
	request.Resources[0].Ports = []Port{{Address: "0.0.0.0", Transport: "tcp"}}
	second := request.Resources[0]
	second.ID, second.ReservationID = uuid.New(), uuid.New()
	second.OwnedResourceIDs = []uuid.UUID{second.ID}
	request.Resources = append(request.Resources, second)

	decision, err := Plan(request, snapshot)
	if err != nil {
		t.Fatal(err)
	}
	bound := make(map[uint16]bool)
	for _, assignment := range decision.Assignments {
		if assignment.NodeID != node || len(assignment.Ports) != 1 {
			t.Fatalf("assignment=%+v, want one port on the one node", assignment)
		}
		port := assignment.Ports[0]
		if port.Number < request.PortRange.First || port.Number > request.PortRange.Last {
			t.Fatalf("port=%d, want one inside %+v", port.Number, request.PortRange)
		}
		if bound[port.Number] {
			t.Fatalf("port %d was bound twice on one node", port.Number)
		}
		bound[port.Number] = true
	}
	if err := CheckDecision(request, snapshot, decision); err != nil {
		t.Fatalf("the decision it made was refused: %v", err)
	}

	// A port already held on the node is not offered again, and a range with nothing
	// free refuses rather than double-binding.
	t.Run("held ports are skipped", func(t *testing.T) {
		request, snapshot := fixture()
		snapshot.Nodes = snapshot.Nodes[:1]
		node := snapshot.Nodes[0].ID
		request.PortRange = PortRange{First: 5432, Last: 5433}
		request.Resources[0].Ports = []Port{{Address: "0.0.0.0", Transport: "tcp"}}
		snapshot.Allocations = []Allocation{{
			ID: uuid.New(), InstallationID: request.InstallationID, NodeID: node,
			ClusterID: uuid.New(), ResourceID: uuid.New(), Budget: Vector{1, 1, 1},
			Ports: []Port{{Address: "0.0.0.0", Transport: "tcp", Number: 5432}},
		}}
		decision, err := Plan(request, snapshot)
		if err != nil {
			t.Fatal(err)
		}
		if got := decision.Assignments[0].Ports[0].Number; got != 5433 {
			t.Fatalf("port=%d, want the lowest free one, 5433", got)
		}
		snapshot.Allocations[0].Ports[0].Number = 5433
		snapshot.Allocations = append(snapshot.Allocations, Allocation{
			ID: uuid.New(), InstallationID: request.InstallationID, NodeID: node,
			ClusterID: uuid.New(), ResourceID: uuid.New(), Budget: Vector{1, 1, 1},
			Ports: []Port{{Address: "0.0.0.0", Transport: "tcp", Number: 5432}},
		})
		if _, err := Plan(request, snapshot); !errors.Is(err, ErrUnsatisfied) {
			t.Fatalf("err=%v, want a refusal when the range has nothing free", err)
		}
	})

	// A retry must land on the port the reservation already holds. Choosing again
	// would move an engine that is already listening.
	t.Run("a retry keeps its port", func(t *testing.T) {
		request, snapshot := fixture()
		snapshot.Nodes = snapshot.Nodes[:1]
		node := snapshot.Nodes[0].ID
		request.PortRange = PortRange{First: 5432, Last: 5500}
		request.Resources[0].Ports = []Port{{Address: "0.0.0.0", Transport: "tcp"}}
		snapshot.Allocations = []Allocation{{
			ID: request.Resources[0].ReservationID, InstallationID: request.InstallationID, NodeID: node,
			ClusterID: request.ClusterID, ResourceID: request.Resources[0].ID, Budget: Vector{1, 1, 1},
			OwnedResourceIDs: request.Resources[0].OwnedResourceIDs,
			Ports:            []Port{{Address: "0.0.0.0", Transport: "tcp", Number: 5479}},
		}}
		decision, err := Plan(request, snapshot)
		if err != nil {
			t.Fatal(err)
		}
		if got := decision.Assignments[0].Ports[0].Number; got != 5479 {
			t.Fatalf("port=%d, want the port the reservation holds, 5479", got)
		}
	})
}

func TestAutomaticPortPreservesFixedBindings(t *testing.T) {
	for _, fixedFirst := range []bool{false, true} {
		request, snapshot := fixture()
		request.PortRange = PortRange{First: 5432, Last: 5433}
		request.Resources[0].Ports = []Port{
			{Address: "0.0.0.0", Transport: "tcp"},
			{Address: "0.0.0.0", Transport: "tcp", Number: 5432},
		}
		if fixedFirst {
			slices.Reverse(request.Resources[0].Ports)
		}
		decision, err := Plan(request, snapshot)
		if err != nil {
			t.Fatal(err)
		}
		if err := CheckDecision(request, snapshot, decision); err != nil {
			t.Fatal(err)
		}
		ports := decision.Assignments[0].Ports
		if len(ports) != 2 || PortsConflict(ports[0], ports[1]) {
			t.Fatalf("conflicting bindings: %+v", ports)
		}
		held := slices.Clone(ports)
		slices.SortFunc(held, func(a, b Port) int { return int(a.Number) - int(b.Number) })
		snapshot.Allocations = []Allocation{{
			ID: request.Resources[0].ReservationID, InstallationID: request.InstallationID,
			NodeID: decision.Assignments[0].NodeID, ClusterID: request.ClusterID,
			ResourceID: request.Resources[0].ID, Budget: Vector{1, 1, 1},
			OwnedResourceIDs: request.Resources[0].OwnedResourceIDs, Ports: held,
		}}
		replayed, err := Plan(request, snapshot)
		if err != nil || !slices.Equal(replayed.Assignments[0].Ports, ports) {
			t.Fatalf("did not reuse held ports: %+v, %v", replayed, err)
		}
		snapshot.Allocations = nil
		decision.Assignments[0].Ports[0].Number = 5432
		decision.Assignments[0].Ports[1].Number = 5432
		if err := CheckDecision(request, snapshot, decision); err == nil {
			t.Fatal("accepted conflicting ports in a decision")
		}
	}
}

// An instance is a member of the cluster, so it must name which member it is. Nothing
// else is a member, so nothing else may carry a role.
func TestInstanceMustDeclareItsMemberRole(t *testing.T) {
	for name, role := range map[string]string{"missing": "", "unknown": "arbiter", "wrong case": "Primary"} {
		t.Run(name, func(t *testing.T) {
			request, snapshot := fixture()
			request.Resources[0].Role = role
			if _, err := Plan(request, snapshot); !errors.Is(err, ErrInvalid) {
				t.Fatalf("role %q accepted: %v", role, err)
			}
		})
	}
	request, snapshot := fixture()
	request.Resources[0].Role = "replica"
	if _, err := Plan(request, snapshot); err != nil {
		t.Fatalf("a replica was refused: %v", err)
	}
	// A backup reservation is not a member, so a role on one is a malformed request.
	request, snapshot = fixture()
	request.Resources[0].Kind, request.Resources[0].Role = "backup", "primary"
	if _, err := Plan(request, snapshot); !errors.Is(err, ErrInvalid) {
		t.Fatal("a role on a backup reservation was accepted")
	}
}

// Two chosen ports of one resource are told apart by name, so a retry that lists them
// in another order still gives each its own number.
func TestNamedPortsKeepTheirNumbersOnRetry(t *testing.T) {
	request, snapshot := fixture()
	request.PortRange = PortRange{First: 5432, Last: 5440}
	request.Resources[0].Ports = []Port{{Address: "0.0.0.0", Transport: "tcp", Name: "database"}, {Address: "0.0.0.0", Transport: "tcp", Name: "rest"}}
	decision, err := Plan(request, snapshot)
	if err != nil {
		t.Fatal(err)
	}
	numbers := map[string]uint16{}
	for _, port := range decision.Assignments[0].Ports {
		numbers[port.Name] = port.Number
	}
	if len(numbers) != 2 || numbers["database"] == numbers["rest"] {
		t.Fatalf("named ports: %+v", decision.Assignments[0].Ports)
	}
	snapshot.Allocations = []Allocation{{
		ID: request.Resources[0].ReservationID, InstallationID: request.InstallationID,
		NodeID: decision.Assignments[0].NodeID, ClusterID: request.ClusterID,
		ResourceID: request.Resources[0].ID, Budget: Vector{1, 1, 1},
		OwnedResourceIDs: request.Resources[0].OwnedResourceIDs, Ports: decision.Assignments[0].Ports,
	}}
	slices.Reverse(request.Resources[0].Ports)
	replayed, err := Plan(request, snapshot)
	if err != nil {
		t.Fatal(err)
	}
	for _, port := range replayed.Assignments[0].Ports {
		if numbers[port.Name] != port.Number {
			t.Fatalf("retry moved %s from %d to %d", port.Name, numbers[port.Name], port.Number)
		}
	}
	for name, ports := range map[string][]Port{
		"duplicate name": {{Address: "0.0.0.0", Transport: "tcp", Name: "database"}, {Address: "0.0.0.0", Transport: "tcp", Name: "database"}},
		"long name":      {{Address: "0.0.0.0", Transport: "tcp", Name: strings.Repeat("n", 33)}},
		"spaced name":    {{Address: "0.0.0.0", Transport: "tcp", Name: "data base"}},
	} {
		invalid, snapshot := fixture()
		invalid.PortRange = request.PortRange
		invalid.Resources[0].Ports = ports
		if _, err := Plan(invalid, snapshot); !errors.Is(err, ErrInvalid) {
			t.Fatalf("%s accepted: %v", name, err)
		}
	}
}
