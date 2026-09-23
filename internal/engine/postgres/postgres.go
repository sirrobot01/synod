// Package postgres is the PostgreSQL engine's knowledge of itself.
package postgres

import (
	"fmt"

	"github.com/sirrobot01/synod/internal/cluster"
	"github.com/sirrobot01/synod/internal/engine"
)

const DataDirectory = "/var/lib/postgresql/data"
const DataVersionFile = DataDirectory + "/PG_VERSION"

// Engine answers for PostgreSQL. It holds no state, so one value serves every caller.
type Engine struct{}

// New returns the PostgreSQL engine.
func New() Engine { return Engine{} }

// Identity names the engine this implementation speaks for.
func (Engine) Identity() cluster.Engine { return cluster.PostgreSQL }

// Validate checks the PostgreSQL rules a stored spec cannot express.
func (Engine) Validate(spec cluster.CreateSpec) error {
	if spec.Engine != cluster.PostgreSQL || spec.PostgreSQL == nil {
		return fmt.Errorf("%w: not a postgres spec", engine.ErrUnknownEngine)
	}
	if spec.Redis != nil {
		return fmt.Errorf("%w: a postgres spec carries redis settings", engine.ErrUnsupportedCombination)
	}
	if spec.PostgreSQL.ReplicationPolicy == "synchronous" && spec.PostgreSQL.Version != "17.11" {
		return fmt.Errorf("%w: synchronous replication requires PostgreSQL 17.11", engine.ErrUnsupportedCombination)
	}
	return nil
}

// Resolve answers what one PostgreSQL spec can do.
func (e Engine) Resolve(spec cluster.CreateSpec) (engine.Capabilities, error) {
	if err := e.Validate(spec); err != nil {
		return engine.Capabilities{}, err
	}
	combination := engine.Combination{
		Engine: cluster.PostgreSQL, Version: spec.PostgreSQL.Version, Topology: spec.PostgreSQL.Topology,
	}
	for _, advertised := range e.Advertised() {
		if advertised.Combination == combination {
			return advertised, nil
		}
	}
	return engine.Capabilities{}, fmt.Errorf("%w: %s", engine.ErrUnsupportedCombination, combination)
}

// Advertised lists every PostgreSQL combination this engine knows. Nothing is
// qualified, because specification 36 forbids claiming it without evidence and Q02 has
// not closed. Upgrade pairs are empty for the same reason.
func (Engine) Advertised() []engine.Capabilities {
	var all []engine.Capabilities
	for _, version := range []string{"17.11", "18.6"} {
		found := engine.Capabilities{
			Combination: engine.Combination{Engine: cluster.PostgreSQL, Version: version, Topology: "standalone"},
			Status:      engine.RequiredPending,
			// Streaming replicas do not provide native failover. PITR needs pgBackRest.
			Replicas:           engine.Replicas{Minimum: 0, Maximum: 0},
			NativeFailover:     false,
			Sharding:           false,
			ProxyRouting:       false,
			ClientDiscovery:    false,
			PITR:               false,
			BackupFormats:      []string{"logical"},
			RestoreScopes:      []string{"cluster"},
			ScaleActions:       nil,
			CredentialRotation: true,
			UpgradePairs:       nil,
			EndpointType:       "direct",
			TLS:                false,
		}
		if version == "17.11" {
			found.Replicas.Maximum = 1
		}
		all = append(all, found)
	}
	return all
}
