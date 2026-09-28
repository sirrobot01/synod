package placement

import (
	"cmp"
	"math/bits"
	"slices"
	"strings"

	"github.com/google/uuid"
)

type planner struct {
	request  Request
	snapshot Snapshot
	loads    []Vector
	existing map[uuid.UUID]Allocation
	nodes    map[uuid.UUID]int
	assigned map[uuid.UUID]Assignment
}

type candidate struct {
	node                         int
	load                         Vector
	ports                        []Port
	numerator, denominator       uint64
	spread, preference, partTime int
}

func prepare(request Request, snapshot Snapshot) (*planner, error) {
	if request.Validate() != nil || snapshot.InstallationID != request.InstallationID || snapshot.ObservedAt.IsZero() || len(snapshot.Nodes) > MaxNodes || len(snapshot.Allocations) > MaxAllocations {
		return nil, ErrInvalid
	}
	p := &planner{request: request, snapshot: snapshot, existing: make(map[uuid.UUID]Allocation), nodes: make(map[uuid.UUID]int), assigned: make(map[uuid.UUID]Assignment), loads: make([]Vector, len(snapshot.Nodes))}
	reservations, owned := make(map[uuid.UUID]uuid.UUID), make(map[uuid.UUID]uuid.UUID)
	for i, node := range snapshot.Nodes {
		if node.ID == uuid.Nil || node.InstallationID != request.InstallationID {
			return nil, ErrInvalid
		}
		if _, exists := p.nodes[node.ID]; exists {
			return nil, ErrInvalid
		}
		p.nodes[node.ID] = i
		if node.Report != nil {
			if node.Report.Validate() != nil {
				return nil, ErrInvalid
			}
			p.loads[i] = node.Report.Used
		}
		if node.Policy != nil {
			if node.Policy.Validate() != nil {
				return nil, ErrInvalid
			}
			var err error
			p.loads[i], err = p.loads[i].Add(node.Policy.Headroom)
			if err != nil {
				return nil, err
			}
		}
	}
	for _, allocation := range snapshot.Allocations {
		if allocation.ID == uuid.Nil || allocation.ResourceID == uuid.Nil || allocation.InstallationID != request.InstallationID || !allocation.Budget.Valid() {
			return nil, ErrInvalid
		}
		if _, exists := p.existing[allocation.ResourceID]; exists {
			return nil, ErrInvalid
		}
		if _, exists := reservations[allocation.ID]; exists {
			return nil, ErrInvalid
		}
		p.existing[allocation.ResourceID] = allocation
		reservations[allocation.ID] = allocation.ResourceID
		for _, id := range allocation.OwnedResourceIDs {
			if previous, exists := owned[id]; exists && previous != allocation.ResourceID {
				return nil, ErrInvalid
			}
			owned[id] = allocation.ResourceID
		}
		if allocation.Released {
			continue
		}
		i, exists := p.nodes[allocation.NodeID]
		if !exists {
			return nil, ErrInvalid
		}
		observed := p.observed(allocation, i)
		extra := Vector{max(allocation.Budget.CPUMillis-observed.CPUMillis, 0), max(allocation.Budget.MemoryBytes-observed.MemoryBytes, 0), max(allocation.Budget.DiskBytes-observed.DiskBytes, 0)}
		var err error
		p.loads[i], err = p.loads[i].Add(extra)
		if err != nil {
			return nil, err
		}
	}
	for _, resource := range request.Resources {
		if prior, exists := reservations[resource.ReservationID]; exists && prior != resource.ID {
			return nil, ErrConflict
		}
		for _, id := range resource.OwnedResourceIDs {
			if prior, exists := owned[id]; exists && prior != resource.ID {
				return nil, ErrConflict
			}
		}
		if prior, exists := p.existing[resource.ID]; exists {
			if prior.Released || prior.AppID != request.AppID || prior.ID != resource.ReservationID || len(prior.OwnedResourceIDs) != len(resource.OwnedResourceIDs) {
				return nil, ErrConflict
			}
			for _, id := range resource.OwnedResourceIDs {
				if !slices.Contains(prior.OwnedResourceIDs, id) {
					return nil, ErrConflict
				}
			}
			for id, key := range prior.Groups {
				if !slices.ContainsFunc(request.Groups, func(g Group) bool { return g.ID == id && g.DomainKey == key && slices.Contains(g.Members, resource.ID) }) {
					return nil, ErrConflict
				}
			}
		}
		if request.Profile == "ha" && resource.Kind == "instance" && !slices.ContainsFunc(request.Groups, func(g Group) bool { return slices.Contains(g.Members, resource.ID) }) {
			return nil, ErrInvalid
		}
	}
	for _, group := range request.Groups {
		domains := make(map[string]bool)
		for _, member := range group.Members {
			if prior, exists := p.existing[member]; exists && !prior.Released {
				i, found := p.nodes[prior.NodeID]
				if !found || snapshot.Nodes[i].Policy == nil {
					if group.Soft {
						continue
					}
					return nil, ErrUnsatisfied
				}
				domain := snapshot.Nodes[i].Policy.FailureDomains[group.DomainKey]
				if !group.Soft && (domain == "" || domains[domain]) {
					return nil, ErrUnsatisfied
				}
				domains[domain] = true
			}
		}
		for _, member := range group.Members {
			if slices.ContainsFunc(request.Resources, func(r Resource) bool { return r.ID == member }) {
				continue
			}
			prior, exists := p.existing[member]
			if !exists || prior.Released || prior.AppID != request.AppID {
				return nil, ErrInvalid
			}
		}
		for _, prior := range snapshot.Allocations {
			if prior.Released || prior.AppID != request.AppID {
				continue
			}
			if key, exists := prior.Groups[group.ID]; exists && (key != group.DomainKey || !slices.Contains(group.Members, prior.ResourceID)) {
				return nil, ErrConflict
			}
		}
	}
	return p, nil
}

func (p *planner) observed(allocation Allocation, index int) Vector {
	node := p.snapshot.Nodes[index]
	if node.Report != nil {
		for _, usage := range node.Report.Reservations {
			if usage.ReservationID == allocation.ID && usage.ResourceID == allocation.ResourceID {
				return usage.Used
			}
		}
	}
	return Vector{}
}

func (p *planner) viable(resource Resource, index int) (candidate, bool) {
	node := p.snapshot.Nodes[index]
	policy, probe, report := node.Policy, node.Probe, node.Report
	now := p.snapshot.ObservedAt
	if node.Revoked || node.RecoveryRequired || node.SessionID == uuid.Nil || node.LastSeenAt.After(now) || !node.LastSeenAt.Add(PresenceAge).After(now) || policy == nil || probe == nil || report == nil || policy.Cordoned || policy.Maintenance {
		return candidate{}, false
	}
	if policy.InstallationID != p.request.InstallationID || policy.NodeID != node.ID || policy.Revision < 1 || probe.InstallationID != node.InstallationID || probe.NodeID != node.ID || probe.SessionID != node.SessionID || probe.BootID != node.BootID || probe.JournalGeneration != node.JournalGeneration || probe.PolicyRevision != policy.Revision || probe.IssuedAt.After(now) || !probe.ValidUntil.After(now) || probe.ValidUntil.After(probe.IssuedAt.Add(CapacityAge)) || report.ProbeID != probe.ID || report.ProbeRevision != probe.Revision || report.InstallationID != node.InstallationID || report.NodeID != node.ID || report.SessionID != node.SessionID || report.BootID != node.BootID || report.JournalGeneration != node.JournalGeneration {
		return candidate{}, false
	}
	if (len(resource.AllowedNodes) != 0 && !slices.Contains(resource.AllowedNodes, node.ID) && !((resource.Role == "active" || resource.Role == "standby") && slices.Contains(resource.PreferredNodes, node.ID))) || slices.Contains(resource.ExcludedNodes, node.ID) {
		return candidate{}, false
	}
	for _, name := range resource.RequiredPaths {
		if !slices.Contains(report.ExistingPaths, name) || !slices.Contains(probe.Paths, name) {
			return candidate{}, false
		}
	}
	if resource.NodeAffinity != uuid.Nil && resource.NodeAffinity != node.ID {
		return candidate{}, false
	}
	if prior, exists := p.existing[resource.ID]; exists && prior.NodeID != node.ID {
		return candidate{}, false
	}
	// Colocated resources share a node, whichever of the two is placed first.
	for _, other := range p.request.Resources {
		if other.ID != resource.Colocate && other.Colocate != resource.ID {
			continue
		}
		if assignment, exists := p.assigned[other.ID]; exists && assignment.NodeID != node.ID {
			return candidate{}, false
		}
	}
	if resource.Engine == "generic" {
		if !slices.Contains(resource.SupportedArchitectures, policy.Architecture) || report.Architecture != policy.Architecture || report.RuntimeVersion != policy.RuntimeVersion || report.StorageID != policy.StorageID {
			return candidate{}, false
		}
	} else if policy.Architecture != resource.Architecture || report.Architecture != resource.Architecture || !slices.Contains(policy.Engines, EngineVersion{resource.Engine, resource.Version}) {
		return candidate{}, false
	}
	// An empty runtime version or storage ID in the request accepts any node value.
	if resource.RuntimeVersion != "" && (policy.RuntimeVersion != resource.RuntimeVersion || report.RuntimeVersion != resource.RuntimeVersion) {
		return candidate{}, false
	}
	if resource.StorageID != "" && (policy.StorageID != resource.StorageID || report.StorageID != resource.StorageID) {
		return candidate{}, false
	}
	for key, value := range resource.Labels {
		if policy.Labels[key] != value {
			return candidate{}, false
		}
	}
	chosen, ok := p.ports(resource, index)
	if !ok {
		return candidate{}, false
	}
	spread := 0
	for _, group := range p.request.Groups {
		if !slices.Contains(group.Members, resource.ID) {
			continue
		}
		domain := policy.FailureDomains[group.DomainKey]
		if domain == "" && (group.Soft || group.DomainKey == "node") {
			domain = node.ID.String()
		}
		if domain == "" {
			return candidate{}, false
		}
		for _, member := range group.Members {
			if member == resource.ID {
				continue
			}
			var otherNode uuid.UUID
			if assignment, exists := p.assigned[member]; exists {
				otherNode = assignment.NodeID
			} else if allocation, exists := p.existing[member]; exists {
				otherNode = allocation.NodeID
			} else {
				continue
			}
			otherIndex, exists := p.nodes[otherNode]
			if !exists {
				return candidate{}, false
			}
			otherDomain := ""
			if p.snapshot.Nodes[otherIndex].Policy != nil {
				otherDomain = p.snapshot.Nodes[otherIndex].Policy.FailureDomains[group.DomainKey]
			}
			if otherDomain == "" && group.DomainKey == "node" {
				otherDomain = otherNode.String()
			}
			if group.Soft {
				if otherDomain == "" {
					otherDomain = otherNode.String()
				}
				if otherDomain == domain {
					spread++
				}
			} else if otherDomain == "" || otherDomain == domain {
				return candidate{}, false
			}
		}
	}
	budget, _ := resource.Budgets.Total()
	extra := budget
	if prior, exists := p.existing[resource.ID]; exists {
		observed := p.observed(prior, index)
		extra = Vector{max(budget.CPUMillis, prior.Budget.CPUMillis, observed.CPUMillis) - max(prior.Budget.CPUMillis, observed.CPUMillis), max(budget.MemoryBytes, prior.Budget.MemoryBytes, observed.MemoryBytes) - max(prior.Budget.MemoryBytes, observed.MemoryBytes), max(budget.DiskBytes, prior.Budget.DiskBytes, observed.DiskBytes) - max(prior.Budget.DiskBytes, observed.DiskBytes)}
	}
	load, err := p.loads[index].Add(extra)
	if err != nil || load.CPUMillis > report.Capacity.CPUMillis || load.MemoryBytes > report.Capacity.MemoryBytes || load.DiskBytes > report.Capacity.DiskBytes {
		return candidate{}, false
	}
	result := candidate{node: index, load: load, ports: chosen, spread: spread, numerator: uint64(load.CPUMillis), denominator: uint64(report.Capacity.CPUMillis)}
	if len(resource.PreferredNodes) != 0 {
		result.preference = slices.Index(resource.PreferredNodes, node.ID)
		if result.preference < 0 {
			result.preference = len(resource.PreferredNodes)
		}
	}
	if resource.Engine == "generic" && policy.Labels["synod.availability"] == "part-time" {
		result.partTime = 1
	}
	for _, ratio := range [][2]int64{{load.MemoryBytes, report.Capacity.MemoryBytes}, {load.DiskBytes, report.Capacity.DiskBytes}} {
		if compareRatio(uint64(ratio[0]), uint64(ratio[1]), result.numerator, result.denominator) > 0 {
			result.numerator, result.denominator = uint64(ratio[0]), uint64(ratio[1])
		}
	}
	return result, true
}

// ports resolves what one resource will bind on a node. A requested number of zero is
// a request for one from the installation's range, so the choice is made here, where
// every binding already held on the node is known. It reports false when a requested
// binding is taken and when the range has nothing left.
func (p *planner) ports(resource Resource, index int) ([]Port, bool) {
	// A retry reuses the ports its reservation already holds, matched to the bindings
	// the request asks for. Choosing again would move an engine that is already
	// listening, and the request states its ports in whatever order it likes.
	if prior, exists := p.existing[resource.ID]; exists {
		if len(prior.Ports) != len(resource.Ports) {
			return nil, false
		}
		chosen := make([]Port, 0, len(resource.Ports))
		matched := make([]bool, len(prior.Ports))
		for _, asked := range resource.Ports {
			found := -1
			for i, port := range prior.Ports {
				if asked.Number == 0 && slices.ContainsFunc(resource.Ports, func(fixed Port) bool {
					return fixed.Number != 0 && PortsConflict(port, fixed)
				}) {
					continue
				}
				if !matched[i] && port.Address == asked.Address && port.Transport == asked.Transport && port.Name == asked.Name && (asked.Number == 0 || asked.Number == port.Number) {
					found = i
					break
				}
			}
			if found < 0 {
				return nil, false
			}
			matched[found] = true
			chosen = append(chosen, prior.Ports[found])
		}
		for _, port := range chosen {
			if p.held(resource, index, port) {
				return nil, false
			}
		}
		return chosen, true
	}
	chosen := make([]Port, 0, len(resource.Ports))
	for _, port := range resource.Ports {
		if port.Number == 0 {
			// The lowest free number wins, so the same inputs always choose the same
			// port. Counting in int keeps the last port in the range from wrapping.
			for try := int(p.request.PortRange.First); try <= int(p.request.PortRange.Last); try++ {
				attempt := port
				attempt.Number = uint16(try)
				if p.held(resource, index, attempt) || slices.ContainsFunc(chosen, func(taken Port) bool { return PortsConflict(attempt, taken) }) ||
					slices.ContainsFunc(resource.Ports, func(asked Port) bool { return asked.Number != 0 && PortsConflict(attempt, asked) }) {
					continue
				}
				port.Number = attempt.Number
				break
			}
			if port.Number == 0 {
				return nil, false
			}
		} else if p.held(resource, index, port) || slices.ContainsFunc(chosen, func(taken Port) bool { return PortsConflict(port, taken) }) {
			return nil, false
		}
		chosen = append(chosen, port)
	}
	return chosen, true
}

// held reports one binding already taken on a node by anything but this resource's own
// identical one.
func (p *planner) held(resource Resource, index int, port Port) bool {
	node := p.snapshot.Nodes[index]
	for _, allocation := range p.snapshot.Allocations {
		if allocation.Released || allocation.NodeID != node.ID {
			continue
		}
		for _, taken := range allocation.Ports {
			if PortsConflict(port, taken) && (allocation.ResourceID != resource.ID || port != taken) {
				return true
			}
		}
	}
	for id, assignment := range p.assigned {
		if assignment.NodeID != node.ID || id == resource.ID {
			continue
		}
		for _, taken := range assignment.Ports {
			if PortsConflict(port, taken) {
				return true
			}
		}
	}
	return false
}

// Compare exact utilization fractions without float rounding or integer overflow.
func compareRatio(an, ad, bn, bd uint64) int {
	ah, al := bits.Mul64(an, bd)
	bh, bl := bits.Mul64(bn, ad)
	return cmp.Or(cmp.Compare(ah, bh), cmp.Compare(al, bl))
}

// Plan filters hard requirements before scoring. Search has an explicit work bound.
func Plan(request Request, snapshot Snapshot) (Decision, error) {
	p, err := prepare(request, snapshot)
	if err != nil {
		return Decision{}, err
	}
	explored := 0
	var search func() (bool, error)
	search = func() (bool, error) {
		if len(p.assigned) == len(request.Resources) {
			return true, nil
		}
		var next Resource
		var options []candidate
		for _, resource := range request.Resources {
			if _, exists := p.assigned[resource.ID]; exists {
				continue
			}
			var candidates []candidate
			for i := range snapshot.Nodes {
				if value, ok := p.viable(resource, i); ok {
					candidates = append(candidates, value)
				}
			}
			if len(candidates) == 0 {
				return false, nil
			}
			if next.ID == uuid.Nil || (resource.Role == "active" && next.Role != "active") || ((resource.Role == "active") == (next.Role == "active") && (len(candidates) < len(options) || (len(candidates) == len(options) && strings.Compare(resource.ID.String(), next.ID.String()) < 0))) {
				next, options = resource, candidates
			}
		}
		slices.SortFunc(options, func(a, b candidate) int {
			return cmp.Or(cmp.Compare(a.spread, b.spread), cmp.Compare(a.preference, b.preference), cmp.Compare(a.partTime, b.partTime), compareRatio(a.numerator, a.denominator, b.numerator, b.denominator), strings.Compare(snapshot.Nodes[a.node].ID.String(), snapshot.Nodes[b.node].ID.String()))
		})
		for _, option := range options {
			if explored >= MaxSearch {
				return false, ErrSearchLimit
			}
			explored++
			node := snapshot.Nodes[option.node]
			before := p.loads[option.node]
			p.loads[option.node] = option.load
			p.assigned[next.ID] = Assignment{ResourceID: next.ID, ReservationID: next.ReservationID, NodeID: node.ID, PolicyRevision: node.Policy.Revision, ProbeRevision: node.Probe.Revision, Ports: option.ports}
			found, err := search()
			if err != nil || found {
				return found, err
			}
			delete(p.assigned, next.ID)
			p.loads[option.node] = before
		}
		return false, nil
	}
	found, err := search()
	if err != nil {
		return Decision{}, err
	}
	if !found {
		return Decision{}, ErrUnsatisfied
	}
	result := Decision{Assignments: make([]Assignment, 0, len(p.assigned))}
	for _, assignment := range p.assigned {
		result.Assignments = append(result.Assignments, assignment)
	}
	slices.SortFunc(result.Assignments, func(a, b Assignment) int { return strings.Compare(a.ResourceID.String(), b.ResourceID.String()) })
	return result, nil
}

// CheckDecision validates the proposed assignment without rerunning scoring.
// The Store supplies fresh locked records and database time before plan commit.
func CheckDecision(request Request, snapshot Snapshot, decision Decision) error {
	p, err := prepare(request, snapshot)
	if err != nil {
		return err
	}
	if len(decision.Assignments) != len(request.Resources) {
		return ErrInvalid
	}
	for _, assignment := range decision.Assignments {
		if _, exists := p.assigned[assignment.ResourceID]; exists {
			return ErrInvalid
		}
		index := slices.IndexFunc(request.Resources, func(r Resource) bool { return r.ID == assignment.ResourceID })
		if index < 0 {
			return ErrInvalid
		}
		resource := request.Resources[index]
		nodeIndex, exists := p.nodes[assignment.NodeID]
		if !exists || assignment.ReservationID != resource.ReservationID {
			return ErrChanged
		}
		// The decision names the ports it chose. Checking them against the request
		// keeps a decision from binding a port the request never asked for.
		if len(assignment.Ports) != len(resource.Ports) {
			return ErrChanged
		}
		for i, asked := range resource.Ports {
			bound := assignment.Ports[i]
			if asked.Address != bound.Address || asked.Transport != bound.Transport || bound.Validate() != nil {
				return ErrChanged
			}
			if asked.Number == 0 && (bound.Number < request.PortRange.First || bound.Number > request.PortRange.Last) {
				return ErrChanged
			}
			if asked.Number != 0 && asked.Number != bound.Number {
				return ErrChanged
			}
		}
		// Ask whether the very bindings the decision named are still free, rather
		// than choosing again: a second choice could land elsewhere and then reject
		// a decision that is still sound.
		resource.Ports = assignment.Ports
		value, ok := p.viable(resource, nodeIndex)
		if !ok || !slices.Equal(value.ports, assignment.Ports) {
			return ErrChanged
		}
		node := snapshot.Nodes[nodeIndex]
		if assignment.PolicyRevision != node.Policy.Revision || assignment.ProbeRevision != node.Probe.Revision {
			return ErrChanged
		}
		p.loads[nodeIndex] = value.load
		p.assigned[resource.ID] = assignment
	}
	return nil
}
