package placement

import (
	"errors"
	"reflect"
	"slices"
	"testing"

	"github.com/google/uuid"
)

func TestGenericPlacementUsesBothArchitecturesAndSoftSpread(t *testing.T) {
	request, snapshot := fixture()
	original := request.Resources[0]
	request.Resources = nil
	group := Group{ID: "copies", DomainKey: "device", Soft: true}
	for range 5 {
		resource := original
		resource.ID, resource.ReservationID = uuid.New(), uuid.New()
		resource.OwnedResourceIDs = []uuid.UUID{resource.ID}
		resource.Kind, resource.Role, resource.Engine, resource.Version, resource.Architecture = "service", "", "generic", "", ""
		resource.SupportedArchitectures = []string{"amd64", "arm64"}
		// Preference does not put all copies on one device when others can run them.
		resource.PreferredNodes = []uuid.UUID{snapshot.Nodes[0].ID}
		resource.Budgets = Budgets{Workload: new(Vector{100, 100, 0}), Supporting: new(Vector{}), Backup: new(Vector{})}
		request.Resources = append(request.Resources, resource)
		group.Members = append(group.Members, resource.ID)
	}
	request.Groups = []Group{group}
	for i := range snapshot.Nodes {
		node := &snapshot.Nodes[i]
		node.Policy.Engines = nil
		node.Policy.FailureDomains = nil
		if i == 0 {
			node.Policy.Architecture = "amd64"
			node.Report.Architecture = "amd64"
		}
	}
	decision, err := Plan(request, snapshot)
	if err != nil {
		t.Fatal(err)
	}
	counts := map[uuid.UUID]int{}
	for _, assignment := range decision.Assignments {
		counts[assignment.NodeID]++
	}
	if len(counts) != 3 || counts[snapshot.Nodes[0].ID] != 2 {
		t.Fatalf("spread ignored eligible mixed devices: %v", counts)
	}
	for _, count := range counts {
		if count < 1 || count > 2 {
			t.Fatalf("unbalanced spread: %v", counts)
		}
	}
	if err := CheckDecision(request, snapshot, decision); err != nil {
		t.Fatal(err)
	}
	slices.Reverse(request.Resources)
	slices.Reverse(snapshot.Nodes)
	again, err := Plan(request, snapshot)
	if err != nil || !reflect.DeepEqual(decision, again) {
		t.Fatalf("input order changed the app plan: %v %v", again, err)
	}
	// Replaying committed copies preserves placements and does not count capacity twice.
	for _, assignment := range decision.Assignments {
		resource := request.Resources[slices.IndexFunc(request.Resources, func(r Resource) bool { return r.ID == assignment.ResourceID })]
		budget, _ := resource.Budgets.Total()
		snapshot.Allocations = append(snapshot.Allocations, Allocation{ID: resource.ReservationID, InstallationID: request.InstallationID, NodeID: assignment.NodeID, ClusterID: request.ClusterID, ResourceID: resource.ID, Budget: budget, OwnedResourceIDs: resource.OwnedResourceIDs, Groups: map[string]string{group.ID: group.DomainKey}})
	}
	again, err = Plan(request, snapshot)
	if err != nil || !reflect.DeepEqual(decision, again) {
		t.Fatalf("committed spread could not replay: %v %v", again, err)
	}
}

func TestGenericPlacementHardFiltersBeatPreference(t *testing.T) {
	for _, reason := range []string{"architecture", "report architecture", "runtime", "storage", "never", "allowed", "offline", "cordoned", "capacity", "port"} {
		t.Run(reason, func(t *testing.T) {
			request, snapshot := fixture()
			resource := &request.Resources[0]
			resource.Kind, resource.Role, resource.Engine, resource.Version, resource.Architecture = "service", "", "generic", "", ""
			resource.SupportedArchitectures = []string{"amd64"}
			for i := range snapshot.Nodes {
				snapshot.Nodes[i].Policy.Architecture = "amd64"
				snapshot.Nodes[i].Report.Architecture = "amd64"
			}
			node := &snapshot.Nodes[0]
			resource.PreferredNodes = []uuid.UUID{node.ID}
			switch reason {
			case "architecture":
				node.Policy.Architecture = "arm64"
				node.Report.Architecture = "arm64"
			case "report architecture":
				node.Report.Architecture = "arm64"
			case "runtime":
				node.Report.RuntimeVersion = "other"
			case "storage":
				node.Report.StorageID = "other"
			case "never":
				resource.ExcludedNodes = []uuid.UUID{node.ID}
			case "allowed":
				resource.AllowedNodes = []uuid.UUID{snapshot.Nodes[1].ID, snapshot.Nodes[2].ID}
			case "offline":
				node.LastSeenAt = snapshot.ObservedAt.Add(-PresenceAge)
			case "cordoned":
				node.Policy.Cordoned = true
			case "capacity":
				node.Report.Used = node.Report.Capacity
			case "port":
				resource.Ports = []Port{{Address: "0.0.0.0", Transport: "tcp", Number: 8080}}
				snapshot.Allocations = []Allocation{{ID: uuid.New(), ResourceID: uuid.New(), InstallationID: request.InstallationID, NodeID: node.ID, Budget: Vector{}, Ports: []Port{{Address: "127.0.0.1", Transport: "tcp", Number: 8080}}}}
			}
			decision, err := Plan(request, snapshot)
			if err != nil {
				t.Fatal(err)
			}
			if decision.Assignments[0].NodeID == node.ID {
				t.Fatalf("preferred node bypassed %s", reason)
			}
			if err := CheckDecision(request, snapshot, decision); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestGenericPlacementPrefersAlwaysOnAndHonoursLocalImageAffinity(t *testing.T) {
	request, snapshot := fixture()
	resource := &request.Resources[0]
	resource.Kind, resource.Role, resource.Engine, resource.Version, resource.Architecture = "service", "", "generic", "", ""
	resource.SupportedArchitectures = []string{"arm64"}
	for i := 0; i < 2; i++ {
		snapshot.Nodes[i].Policy.Labels = map[string]string{"synod.availability": "part-time"}
	}
	decision, err := Plan(request, snapshot)
	if err != nil || decision.Assignments[0].NodeID != snapshot.Nodes[2].ID {
		t.Fatalf("baseline did not prefer always-on: %v %v", decision, err)
	}
	resource.NodeAffinity = snapshot.Nodes[0].ID
	decision, err = Plan(request, snapshot)
	if err != nil || decision.Assignments[0].NodeID != snapshot.Nodes[0].ID {
		t.Fatalf("local-only image escaped its node: %v %v", decision, err)
	}
	resource.ExcludedNodes = []uuid.UUID{resource.NodeAffinity}
	if _, err := Plan(request, snapshot); !errors.Is(err, ErrUnsatisfied) {
		t.Fatalf("local-only exclusion: %v", err)
	}
}

func TestSoftSpreadCannotWeakenDatabaseHA(t *testing.T) {
	request, snapshot := fixture()
	first := request.Resources[0]
	second := first
	second.ID, second.ReservationID = uuid.New(), uuid.New()
	second.OwnedResourceIDs = []uuid.UUID{second.ID}
	request.Resources = append(request.Resources, second)
	request.Groups = []Group{{ID: "copies", DomainKey: "rack", Members: []uuid.UUID{first.ID, second.ID}, Soft: true}}
	for _, profile := range []string{"compact", "ha"} {
		request.Profile = profile
		if _, err := Plan(request, snapshot); !errors.Is(err, ErrInvalid) {
			t.Fatalf("soft database group accepted with %s: %v", profile, err)
		}
	}
}

func TestGenericCopiesUseDistinctPhysicalDomains(t *testing.T) {
	request, snapshot := fixture()
	original := request.Resources[0]
	request.Resources = nil
	group := Group{ID: "copies", DomainKey: "device", Soft: true}
	for range 2 {
		resource := original
		resource.ID, resource.ReservationID = uuid.New(), uuid.New()
		resource.OwnedResourceIDs = []uuid.UUID{resource.ID}
		resource.Kind, resource.Role, resource.Engine, resource.Version, resource.Architecture = "service", "", "generic", "", ""
		resource.SupportedArchitectures = []string{"arm64"}
		resource.PreferredNodes = []uuid.UUID{snapshot.Nodes[0].ID, snapshot.Nodes[1].ID}
		request.Resources = append(request.Resources, resource)
		group.Members = append(group.Members, resource.ID)
	}
	request.Groups = []Group{group}
	for i := range snapshot.Nodes {
		snapshot.Nodes[i].Policy.FailureDomains = map[string]string{"device": "host-a"}
	}
	snapshot.Nodes[2].Policy.FailureDomains["device"] = "host-b"
	decision, err := Plan(request, snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.ContainsFunc(decision.Assignments, func(a Assignment) bool { return a.NodeID == snapshot.Nodes[2].ID }) {
		t.Fatal("preferred devices hid the second physical failure domain")
	}
}

func TestGenericArchitectureSetMustBeVerifiedAndUnambiguous(t *testing.T) {
	for _, architectures := range [][]string{nil, {"arm64", "arm64"}, {"riscv64"}} {
		request, _ := fixture()
		resource := &request.Resources[0]
		resource.Kind, resource.Role, resource.Engine, resource.Version, resource.Architecture = "service", "", "generic", "", ""
		resource.SupportedArchitectures = architectures
		if !errors.Is(request.Validate(), ErrInvalid) {
			t.Fatalf("invalid architecture set accepted: %v", architectures)
		}
	}
}

func TestWriterPlacementRequiresPathsAndOrdersPreferredNodes(t *testing.T) {
	request, snapshot := fixture()
	r := &request.Resources[0]
	r.Kind, r.Role, r.Engine, r.Version, r.Architecture = "service", "active", "generic", "", ""
	r.SupportedArchitectures = []string{"arm64"}
	r.RequiredPaths = []string{"/media"}
	r.PreferredNodes = []uuid.UUID{snapshot.Nodes[1].ID, snapshot.Nodes[0].ID}
	r.AllowedNodes = []uuid.UUID{snapshot.Nodes[2].ID}
	for i := range snapshot.Nodes {
		snapshot.Nodes[i].Probe.Paths = []string{"/media"}
		snapshot.Nodes[i].Report.ExistingPaths = []string{"/media"}
	}
	decision, err := Plan(request, snapshot)
	if err != nil || decision.Assignments[0].NodeID != snapshot.Nodes[1].ID {
		t.Fatalf("first preferred: %+v %v", decision, err)
	}
	snapshot.Nodes[1].Report.ExistingPaths = nil
	decision, err = Plan(request, snapshot)
	if err != nil || decision.Assignments[0].NodeID != snapshot.Nodes[0].ID {
		t.Fatalf("second preferred: %+v %v", decision, err)
	}
	snapshot.Nodes[0].Probe.Paths = nil
	decision, err = Plan(request, snapshot)
	if err != nil || decision.Assignments[0].NodeID != snapshot.Nodes[2].ID {
		t.Fatalf("allowed fallback: %+v %v", decision, err)
	}
	snapshot.Nodes[2].Report.ExistingPaths = nil
	if _, err := Plan(request, snapshot); !errors.Is(err, ErrUnsatisfied) {
		t.Fatalf("missing paths: %v", err)
	}
}

func TestStandbyReservesDiskWithoutCPUOrMemory(t *testing.T) {
	request, snapshot := fixture()
	r := &request.Resources[0]
	r.Kind, r.Role, r.Engine, r.Version, r.Architecture = "service", "standby", "generic", "", ""
	r.SupportedArchitectures = []string{"arm64"}
	r.Budgets = Budgets{Workload: &Vector{DiskBytes: 1000}, Supporting: &Vector{}, Backup: &Vector{}}
	snapshot.Nodes = snapshot.Nodes[:1]
	n := &snapshot.Nodes[0]
	n.Report.Used.CPUMillis = n.Report.Capacity.CPUMillis - n.Policy.Headroom.CPUMillis
	n.Report.Used.MemoryBytes = n.Report.Capacity.MemoryBytes - n.Policy.Headroom.MemoryBytes
	if _, err := Plan(request, snapshot); err != nil {
		t.Fatal(err)
	}
	r.Role = "active"
	r.Budgets.Workload.CPUMillis = 100
	r.Budgets.Workload.MemoryBytes = 100
	if _, err := Plan(request, snapshot); !errors.Is(err, ErrUnsatisfied) {
		t.Fatalf("promotion did not require compute: %v", err)
	}
}

func TestStandbyRejectsMissingBudget(t *testing.T) {
	request, _ := fixture()
	request.Resources[0].Role = "standby"
	request.Resources[0].Budgets.Workload = nil
	if err := request.Validate(); !errors.Is(err, ErrInvalid) {
		t.Fatalf("missing standby budget: %v", err)
	}
}
