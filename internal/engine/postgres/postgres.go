// Package postgres is the PostgreSQL engine's knowledge of itself.
package postgres

import "github.com/sirrobot01/synod/internal/engine"

const DataDirectory = "/var/lib/postgresql/data"
const DataVersionFile = DataDirectory + "/PG_VERSION"

// Engine answers for PostgreSQL. It holds no state, so one value serves every caller.
type Engine struct{}

// New returns the PostgreSQL engine.
func New() Engine { return Engine{} }

// Identity names the engine this implementation speaks for.
func (Engine) Identity() string { return engine.PostgreSQL }

// Advertised lists the PostgreSQL release that tier 1 apps run. A primary publishes
// every table and up to three standbys subscribe. Synod promotes a standby, so the
// engine elects nothing itself. Nothing is qualified, because specification 36 forbids
// claiming it without evidence. Upgrade pairs are empty for the same reason.
func (Engine) Advertised() []engine.Capabilities {
	return []engine.Capabilities{{
		Combination:  engine.Combination{Engine: engine.PostgreSQL, Version: "17.11", Topology: "primary-standby"},
		Status:       engine.RequiredPending,
		Replicas:     engine.Replicas{Minimum: 0, Maximum: 3},
		ProxyRouting: true,
		EndpointType: "router",
		TLS:          true,
	}}
}
