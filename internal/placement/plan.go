package placement

import (
	"cmp"
	"github.com/google/uuid"
	"math/bits"
	"slices"
	"strings"
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
	node                   int
	load                   Vector
	numerator, denominator uint64
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
			if prior.Released || prior.ClusterID != request.ClusterID || prior.ID != resource.ReservationID || len(prior.OwnedResourceIDs) != len(resource.OwnedResourceIDs) {
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
					return nil, ErrUnsatisfied
				}
				domain := snapshot.Nodes[i].Policy.FailureDomains[group.DomainKey]
				if domain == "" || domains[domain] {
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
			if !exists || prior.Released || prior.ClusterID != request.ClusterID {
				return nil, ErrInvalid
			}
		}
		for _, prior := range snapshot.Allocations {
			if prior.Released || prior.ClusterID != request.ClusterID {
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
	if resource.NodeAffinity != uuid.Nil && resource.NodeAffinity != node.ID {
		return candidate{}, false
	}
	if prior, exists := p.existing[resource.ID]; exists && prior.NodeID != node.ID {
		return candidate{}, false
	}
	if policy.Architecture != resource.Architecture || report.Architecture != resource.Architecture || policy.RuntimeVersion != resource.RuntimeVersion || report.RuntimeVersion != resource.RuntimeVersion || policy.StorageID != resource.StorageID || report.StorageID != resource.StorageID || !slices.Contains(policy.Engines, EngineVersion{resource.Engine, resource.Version}) {
		return candidate{}, false
	}
	for key, value := range resource.Labels {
		if policy.Labels[key] != value {
			return candidate{}, false
		}
	}
	for _, port := range resource.Ports {
		for _, allocation := range p.snapshot.Allocations {
			if allocation.Released || allocation.NodeID != node.ID {
				continue
			}
			for _, held := range allocation.Ports {
				if PortsConflict(port, held) && (allocation.ResourceID != resource.ID || port != held) {
					return candidate{}, false
				}
			}
		}
		for id, assignment := range p.assigned {
			if assignment.NodeID != node.ID {
				continue
			}
			for _, other := range p.request.Resources {
				if other.ID != id {
					continue
				}
				for _, held := range other.Ports {
					if PortsConflict(port, held) {
						return candidate{}, false
					}
				}
			}
		}
	}
	for _, group := range p.request.Groups {
		if !slices.Contains(group.Members, resource.ID) {
			continue
		}
		domain := policy.FailureDomains[group.DomainKey]
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
			if !exists || p.snapshot.Nodes[otherIndex].Policy == nil {
				return candidate{}, false
			}
			otherDomain := p.snapshot.Nodes[otherIndex].Policy.FailureDomains[group.DomainKey]
			if otherDomain == "" || otherDomain == domain {
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
	result := candidate{node: index, load: load, numerator: uint64(load.CPUMillis), denominator: uint64(report.Capacity.CPUMillis)}
	for _, ratio := range [][2]int64{{load.MemoryBytes, report.Capacity.MemoryBytes}, {load.DiskBytes, report.Capacity.DiskBytes}} {
		if compareRatio(uint64(ratio[0]), uint64(ratio[1]), result.numerator, result.denominator) > 0 {
			result.numerator, result.denominator = uint64(ratio[0]), uint64(ratio[1])
		}
	}
	return result, true
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
			if next.ID == uuid.Nil || len(candidates) < len(options) || (len(candidates) == len(options) && strings.Compare(resource.ID.String(), next.ID.String()) < 0) {
				next, options = resource, candidates
			}
		}
		slices.SortFunc(options, func(a, b candidate) int {
			return cmp.Or(compareRatio(a.numerator, a.denominator, b.numerator, b.denominator), strings.Compare(snapshot.Nodes[a.node].ID.String(), snapshot.Nodes[b.node].ID.String()))
		})
		for _, option := range options {
			if explored >= MaxSearch {
				return false, ErrSearchLimit
			}
			explored++
			node := snapshot.Nodes[option.node]
			before := p.loads[option.node]
			p.loads[option.node] = option.load
			p.assigned[next.ID] = Assignment{ResourceID: next.ID, ReservationID: next.ReservationID, NodeID: node.ID, PolicyRevision: node.Policy.Revision, ProbeRevision: node.Probe.Revision}
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
		value, ok := p.viable(resource, nodeIndex)
		if !ok {
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
