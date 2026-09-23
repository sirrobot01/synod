// Package redis is the Redis engine's knowledge of itself.
package redis

import (
	"fmt"

	"github.com/sirrobot01/synod/internal/cluster"
	"github.com/sirrobot01/synod/internal/engine"
)

// Engine answers for Redis. It holds no state, so one value serves every caller.
type Engine struct{}

// New returns the Redis engine.
func New() Engine { return Engine{} }

// Identity names the engine this implementation speaks for.
func (Engine) Identity() cluster.Engine { return cluster.Redis }

// Validate checks the Redis rules a stored spec cannot express.
func (Engine) Validate(spec cluster.CreateSpec) error {
	if spec.Engine != cluster.Redis || spec.Redis == nil {
		return fmt.Errorf("%w: not a redis spec", engine.ErrUnknownEngine)
	}
	if spec.PostgreSQL != nil {
		return fmt.Errorf("%w: a redis spec carries postgres settings", engine.ErrUnsupportedCombination)
	}
	return nil
}

// Resolve answers what one Redis spec can do. Persistence is part of the lookup: an
// unpersisted Redis is a different combination, not the same one with a flag.
func (e Engine) Resolve(spec cluster.CreateSpec) (engine.Capabilities, error) {
	if err := e.Validate(spec); err != nil {
		return engine.Capabilities{}, err
	}
	combination := engine.Combination{
		Engine: cluster.Redis, Version: spec.Redis.Version,
		Topology: spec.Redis.Topology, Persistence: spec.Redis.Persistence,
	}
	for _, advertised := range e.Advertised() {
		if advertised.Combination == combination {
			return advertised, nil
		}
	}
	return engine.Capabilities{}, fmt.Errorf("%w: %s", engine.ErrUnsupportedCombination, combination)
}

// Advertised lists every Redis combination this engine knows. Nothing is qualified,
// because Q05 has not closed. The persistence rows differ in what they promise, which
// stops a backup being scheduled for data that does not survive a restart.
func (Engine) Advertised() []engine.Capabilities {
	var all []engine.Capabilities
	for _, persistence := range []string{"none", "rdb", "aof", "rdb+aof"} {
		durable := persistence != "none"
		found := engine.Capabilities{
			Combination: engine.Combination{
				Engine: cluster.Redis, Version: "8.2.7", Topology: "standalone", Persistence: persistence,
			},
			Status:             engine.RequiredPending,
			Replicas:           engine.Replicas{Minimum: 0, Maximum: 0},
			NativeFailover:     false,
			Sharding:           false,
			ProxyRouting:       false,
			ClientDiscovery:    false,
			PITR:               false,
			CredentialRotation: true,
			UpgradePairs:       nil,
			EndpointType:       "direct",
			TLS:                false,
		}
		if durable {
			found.Replicas.Maximum = 1
			found.BackupFormats = []string{"snapshot"}
			found.RestoreScopes = []string{"cluster"}
		}
		all = append(all, found)
	}
	return all
}
