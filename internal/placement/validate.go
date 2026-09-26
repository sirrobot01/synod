package placement

import (
	"math"
	"net/netip"
	"slices"
	"strings"

	"github.com/google/uuid"
)

// Valid reports whether every component is zero or positive.
func (v Vector) Valid() bool { return v.CPUMillis >= 0 && v.MemoryBytes >= 0 && v.DiskBytes >= 0 }

// Add returns the sum of two vectors, or ErrInvalid on invalid input or overflow.
func (v Vector) Add(other Vector) (Vector, error) {
	if !v.Valid() || !other.Valid() || v.CPUMillis > math.MaxInt64-other.CPUMillis || v.MemoryBytes > math.MaxInt64-other.MemoryBytes || v.DiskBytes > math.MaxInt64-other.DiskBytes {
		return Vector{}, ErrInvalid
	}
	return Vector{v.CPUMillis + other.CPUMillis, v.MemoryBytes + other.MemoryBytes, v.DiskBytes + other.DiskBytes}, nil
}

// Total returns the sum of the three budgets, or ErrInvalid if any is missing.
func (b Budgets) Total() (Vector, error) {
	if b.Workload == nil || b.Supporting == nil || b.Backup == nil {
		return Vector{}, ErrInvalid
	}
	value, err := b.Workload.Add(*b.Supporting)
	if err != nil {
		return Vector{}, err
	}
	return value.Add(*b.Backup)
}

func validText(value string) bool {
	return len(value) > 0 && len(value) <= 128 && !strings.ContainsFunc(value, func(r rune) bool { return r < '!' || r > '~' })
}
func validLabels(values map[string]string) bool {
	if len(values) > 32 {
		return false
	}
	for key, value := range values {
		if !validText(key) || !validText(value) {
			return false
		}
	}
	return true
}

// Validate checks the policy identity, architecture, engines, labels, and headroom.
func (p NodePolicy) Validate() error {
	if p.InstallationID == uuid.Nil || p.NodeID == uuid.Nil || p.Revision < 0 || (p.Architecture != "amd64" && p.Architecture != "arm64") || !validText(p.RuntimeVersion) || !validText(p.StorageID) || len(p.Engines) == 0 || len(p.Engines) > 32 || !validLabels(p.Labels) || !validLabels(p.FailureDomains) || !p.Headroom.Valid() || p.Headroom.CPUMillis == 0 || p.Headroom.MemoryBytes == 0 || p.Headroom.DiskBytes == 0 {
		return ErrInvalid
	}
	if p.ClientAddress != "" {
		address, err := netip.ParseAddr(p.ClientAddress)
		if err != nil || !address.IsGlobalUnicast() || address.IsLoopback() || address.Zone() != "" || address.Unmap().String() != p.ClientAddress {
			return ErrInvalid
		}
	}
	seen := make(map[EngineVersion]bool)
	for _, engine := range p.Engines {
		if (engine.Engine != "postgres" && engine.Engine != "redis") || !validText(engine.Version) || seen[engine] {
			return ErrInvalid
		}
		seen[engine] = true
	}
	return nil
}

// Validate checks the report identity, capacity, and reservation totals.
func (r CapacityReport) Validate() error {
	for _, id := range []uuid.UUID{r.ProbeID, r.InstallationID, r.NodeID, r.SessionID, r.BootID, r.JournalGeneration} {
		if id == uuid.Nil {
			return ErrInvalid
		}
	}
	if r.ProbeRevision < 1 || (r.Architecture != "arm64" && r.Architecture != "amd64") || !validText(r.RuntimeVersion) || !validText(r.StorageID) || !r.Capacity.Valid() || r.Capacity.CPUMillis == 0 || r.Capacity.MemoryBytes == 0 || r.Capacity.DiskBytes == 0 || !r.Used.Valid() || len(r.Reservations) > 512 {
		return ErrInvalid
	}
	seen := make(map[uuid.UUID]bool)
	resources := make(map[uuid.UUID]bool)
	var sum Vector
	for _, item := range r.Reservations {
		if item.ReservationID == uuid.Nil || item.ResourceID == uuid.Nil || seen[item.ReservationID] || resources[item.ResourceID] {
			return ErrInvalid
		}
		seen[item.ReservationID] = true
		resources[item.ResourceID] = true
		var err error
		sum, err = sum.Add(item.Used)
		if err != nil {
			return err
		}
	}
	if sum.CPUMillis > r.Used.CPUMillis || sum.MemoryBytes > r.Used.MemoryBytes || sum.DiskBytes > r.Used.DiskBytes {
		return ErrInvalid
	}
	return nil
}

// Validate checks the address, number, and transport of the port.
func (p Port) Validate() error {
	// A number of zero is a request for one from the installation's range, not a
	// binding. Request.Validate accepts it; a committed binding never carries it.
	if p.Number == 0 {
		return ErrInvalid
	}
	return p.binding()
}

// binding checks everything about a port except its number.
func (p Port) binding() error {
	address, err := netip.ParseAddr(p.Address)
	if err != nil || address.Zone() != "" || address.IsMulticast() || address.Unmap().String() != p.Address || (p.Transport != "tcp" && p.Transport != "udp") || len(p.Name) > 32 || (p.Name != "" && !validText(p.Name)) {
		return ErrInvalid
	}
	return nil
}

// PortsConflict reports whether two bindings can not coexist on one host.
// An address that does not parse conflicts, so an unreadable binding never looks free.
func PortsConflict(a, b Port) bool {
	if a.Transport != b.Transport || a.Number != b.Number {
		return false
	}
	first, err := netip.ParseAddr(a.Address)
	if err != nil {
		return true
	}
	second, err := netip.ParseAddr(b.Address)
	if err != nil {
		return true
	}
	first, second = first.Unmap(), second.Unmap()
	return first == second || (first.IsUnspecified() && (first.Is6() || second.Is4())) || (second.IsUnspecified() && (second.Is6() || first.Is4()))
}

// Validate checks the request version, identity, profile, resources, and groups.
func (r Request) Validate() error {
	if r.Version != 1 || r.InstallationID == uuid.Nil || r.ClusterID == uuid.Nil || r.OperationID == uuid.Nil || r.DesiredRevision < 1 || (r.Profile != "compact" && r.Profile != "ha") || len(r.Resources) == 0 || len(r.Resources) > MaxResources || len(r.Groups) > 64 || (r.Profile == "ha" && len(r.Groups) == 0) {
		return ErrInvalid
	}
	if r.PortRange != (PortRange{}) && (r.PortRange.First == 0 || r.PortRange.First > r.PortRange.Last) {
		return ErrInvalid
	}
	resources, reservations, owned := make(map[uuid.UUID]bool), make(map[uuid.UUID]bool), make(map[uuid.UUID]bool)
	for _, resource := range r.Resources {
		if resource.ID == uuid.Nil || resource.ReservationID == uuid.Nil || resources[resource.ID] || reservations[resource.ReservationID] || len(resource.OwnedResourceIDs) == 0 || len(resource.OwnedResourceIDs) > 32 || !slices.Contains(resource.OwnedResourceIDs, resource.ID) || len(resource.Ports) > 32 || !validLabels(resource.Labels) || (resource.Kind != "instance" && resource.Kind != "service" && resource.Kind != "backup") || (resource.Engine != "postgres" && resource.Engine != "redis") || !validText(resource.Version) || (resource.Architecture != "arm64" && resource.Architecture != "amd64") || (resource.RuntimeVersion != "" && !validText(resource.RuntimeVersion)) || (resource.StorageID != "" && !validText(resource.StorageID)) {
			return ErrInvalid
		}
		resources[resource.ID], reservations[resource.ReservationID] = true, true
		total, err := resource.Budgets.Total()
		if err != nil || total == (Vector{}) {
			return ErrInvalid
		}
		// An instance is a member of the cluster, so it names which member it is.
		// A Patroni member has no Synod-declared role, because Patroni elects its primary.
		// It only exists in an HA placement that spreads it from its peers.
		// The only other role is etcd: a service beside one Patroni member.
		if (resource.Kind == "instance") != (resource.Role == "primary" || resource.Role == "replica" || resource.Role == "patroni") || (resource.Role == "patroni" && r.Profile != "ha") ||
			(resource.Kind != "instance" && resource.Role != "" && (resource.Role != "etcd" || resource.Kind != "service" || r.Profile != "ha")) {
			return ErrInvalid
		}
		if resource.Colocate != uuid.Nil && (resource.Colocate == resource.ID || !slices.ContainsFunc(r.Resources, func(other Resource) bool { return other.ID == resource.Colocate })) {
			return ErrInvalid
		}
		if resource.Kind == "instance" && (resource.Budgets.Workload.CPUMillis == 0 || resource.Budgets.Workload.MemoryBytes == 0 || resource.Budgets.Workload.DiskBytes == 0) {
			return ErrInvalid
		}
		for _, id := range resource.OwnedResourceIDs {
			if id == uuid.Nil || owned[id] {
				return ErrInvalid
			}
			owned[id] = true
		}
		for i, port := range resource.Ports {
			if port.binding() != nil {
				return ErrInvalid
			}
			// A request for a chosen port needs a range to choose from.
			if port.Number == 0 && r.PortRange.First == 0 {
				return ErrInvalid
			}
			for _, previous := range resource.Ports[:i] {
				// Two chosen ports never collide, because placement gives them
				// different numbers.
				if port.Number != 0 && PortsConflict(port, previous) || port.Name != "" && port.Name == previous.Name {
					return ErrInvalid
				}
			}
		}
	}
	groups := make(map[string]bool)
	for _, group := range r.Groups {
		if !validText(group.ID) || !validText(group.DomainKey) || groups[group.ID] || len(group.Members) < 2 || len(group.Members) > MaxResources {
			return ErrInvalid
		}
		groups[group.ID] = true
		members := make(map[uuid.UUID]bool)
		for _, id := range group.Members {
			if id == uuid.Nil || members[id] {
				return ErrInvalid
			}
			members[id] = true
		}
	}
	return nil
}
