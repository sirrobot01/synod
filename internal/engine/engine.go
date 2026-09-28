// Package engine shares lifecycle contracts between database engines without forcing
// one engine's shape onto another.
//
// Specification 06 states that a single engine-wide boolean is not sufficient: what a
// database can do comes from its engine, version, topology, and persistence together,
// and that combination is what carries a qualification status.
package engine

import (
	"errors"
	"fmt"
	"slices"
	"strings"
)

// ErrUnknownEngine reports an engine this installation does not serve.
var ErrUnknownEngine = errors.New("unknown_engine")

// Engine names served by the catalog.
const (
	PostgreSQL = "postgres"
	Redis      = "redis"
)

// Status is how far one advertised combination has been proved. Specification 36
// forbids marking anything qualified without recorded evidence.
type Status string

const (
	Unsupported     Status = "unsupported"
	RequiredPending Status = "required_pending"
	Qualified       Status = "qualified"
)

// Combination is one advertised engine, version, topology, and persistence.
// Persistence is part of the key because it changes what the combination can do: an
// unpersisted engine has no backup to take and nothing to restore.
type Combination struct {
	Engine      string `json:"engine"`
	Version     string `json:"version"`
	Topology    string `json:"topology"`
	Persistence string `json:"persistence,omitempty"`
}

// Replicas is the replica count a combination accepts.
type Replicas struct {
	Minimum int `json:"minimum"`
	Maximum int `json:"maximum"`
}

// Capabilities is what one combination can do. Every field is machine readable,
// because specification 06 requires limits a CLI and a UI can render.
type Capabilities struct {
	Combination Combination `json:"combination"`
	Status      Status      `json:"status"`
	Replicas    Replicas    `json:"replicas"`
	// NativeFailover means the engine elects its own primary. Specification 07
	// forbids Synod promoting a competing one.
	NativeFailover     bool     `json:"native_failover"`
	Sharding           bool     `json:"sharding"`
	ProxyRouting       bool     `json:"proxy_routing"`
	ClientDiscovery    bool     `json:"client_discovery"`
	PITR               bool     `json:"pitr"`
	BackupFormats      []string `json:"backup_formats"`
	RestoreScopes      []string `json:"restore_scopes"`
	ScaleActions       []string `json:"scale_actions"`
	CredentialRotation bool     `json:"credential_rotation"`
	// UpgradePairs needs a qualified source and target, so empty is the default.
	UpgradePairs []string `json:"upgrade_pairs"`
	EndpointType string   `json:"endpoint_type"`
	TLS          bool     `json:"tls"`
}

// Engine answers what one database engine can do. Planning and side-effect
// execution stay out: something else decides what to run.
type Engine interface {
	Identity() string
	// Advertised lists every known combination in a stable order.
	Advertised() []Capabilities
}

// Catalog is the set of engines one installation serves.
type Catalog struct{ engines map[string]Engine }

// NewCatalog binds the engines an installation serves. Two claiming one identity is a
// wiring mistake, not a precedence question.
func NewCatalog(engines ...Engine) (*Catalog, error) {
	catalog := &Catalog{engines: make(map[string]Engine, len(engines))}
	for _, found := range engines {
		if found == nil {
			return nil, errors.New("engine catalog requires non-nil engines")
		}
		identity := found.Identity()
		if identity != PostgreSQL && identity != Redis {
			return nil, fmt.Errorf("%w: %s", ErrUnknownEngine, identity)
		}
		if _, exists := catalog.engines[identity]; exists {
			return nil, fmt.Errorf("two engines claim %s", identity)
		}
		catalog.engines[identity] = found
	}
	if len(catalog.engines) == 0 {
		return nil, errors.New("engine catalog requires at least one engine")
	}
	return catalog, nil
}

// Serves reports whether the catalog serves a supported release of an engine version.
// A major version such as "17" matches any advertised release in it, such as "17.11".
func (c *Catalog) Serves(name, version string) bool {
	found, ok := c.engines[name]
	if !ok {
		return false
	}
	return slices.ContainsFunc(found.Advertised(), func(item Capabilities) bool {
		return item.Status != Unsupported && (item.Combination.Version == version || strings.HasPrefix(item.Combination.Version, version+"."))
	})
}

// Advertised lists every combination this installation serves, in a stable order.
func (c *Catalog) Advertised() []Capabilities {
	var all []Capabilities
	for _, found := range c.engines {
		all = append(all, found.Advertised()...)
	}
	slices.SortFunc(all, func(a, b Capabilities) int {
		return strings.Compare(a.Combination.String(), b.Combination.String())
	})
	return all
}

// String renders a combination for an operator and for a stable sort.
func (c Combination) String() string {
	found := fmt.Sprintf("%s %s %s", c.Engine, c.Version, c.Topology)
	if c.Persistence != "" {
		found += " " + c.Persistence
	}
	return found
}
