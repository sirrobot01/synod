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
	// Standalone PostgreSQL has no replicas, so only Patroni can wait for one.
	if spec.PostgreSQL.ReplicationPolicy == "synchronous" && spec.PostgreSQL.Topology != "patroni" {
		return fmt.Errorf("%w: synchronous replication requires the patroni topology", engine.ErrUnsupportedCombination)
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
			// Standalone is one node. Replicas need the patroni topology. PITR needs pgBackRest.
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
		all = append(all, found)
	}
	// Patroni runs three members: one elected primary and two replicas. Native Patroni
	// tests cover 17.11 only. Clients reach members directly with a multi-host URL.
	all = append(all, engine.Capabilities{
		Combination:    engine.Combination{Engine: cluster.PostgreSQL, Version: "17.11", Topology: "patroni"},
		Status:         engine.RequiredPending,
		Replicas:       engine.Replicas{Minimum: 2, Maximum: 2},
		NativeFailover: true,
		// A libpq multi-host URL finds the writer. Synod runs no proxy yet.
		ClientDiscovery: true,
		BackupFormats:   []string{},
		RestoreScopes:   []string{},
		EndpointType:    "multi_host",
		TLS:             true,
	})
	return all
}
