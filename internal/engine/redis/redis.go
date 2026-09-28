// Package redis is the Redis engine's knowledge of itself.
package redis

import "github.com/sirrobot01/synod/internal/engine"

// Engine answers for Redis. It holds no state, so one value serves every caller.
type Engine struct{}

// New returns the Redis engine.
func New() Engine { return Engine{} }

// Identity names the engine this implementation speaks for.
func (Engine) Identity() string { return engine.Redis }

// Advertised lists the Redis release that tier 1 apps run. The primary appends every
// write to its log and up to three replicas follow it. Nothing is qualified, because
// specification 36 forbids claiming it without evidence.
func (Engine) Advertised() []engine.Capabilities {
	return []engine.Capabilities{{
		Combination:  engine.Combination{Engine: engine.Redis, Version: "8.2.7", Topology: "primary-standby", Persistence: "aof"},
		Status:       engine.RequiredPending,
		Replicas:     engine.Replicas{Minimum: 0, Maximum: 3},
		ProxyRouting: true,
		EndpointType: "router",
		TLS:          true,
	}}
}
