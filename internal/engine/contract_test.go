package engine_test

import (
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/sirrobot01/synod/internal/cluster"
	"github.com/sirrobot01/synod/internal/engine"
	"github.com/sirrobot01/synod/internal/engine/postgres"
	"github.com/sirrobot01/synod/internal/engine/redis"
)

// engines is every implementation of the boundary. Specification 06 states that
// PostgreSQL and Redis must both compile against it before it is considered stable,
// so every contract test below runs against both rather than against one.
func engines() map[string]engine.Engine {
	return map[string]engine.Engine{
		"postgres": postgres.New(),
		"redis":    redis.New(),
	}
}

// specFor builds one admissible spec for a combination an engine advertises.
func specFor(t *testing.T, found engine.Capabilities) cluster.CreateSpec {
	t.Helper()
	spec := cluster.CreateSpec{SchemaVersion: 1, Name: "example", Engine: found.Combination.Engine}
	switch found.Combination.Engine {
	case cluster.PostgreSQL:
		spec.PostgreSQL = &cluster.PostgreSQLSpec{Version: found.Combination.Version, Topology: found.Combination.Topology}
	case cluster.Redis:
		spec.Redis = &cluster.RedisSpec{
			Version: found.Combination.Version, Topology: found.Combination.Topology,
			Persistence: found.Combination.Persistence,
		}
	}
	return spec
}

// Every engine must advertise at least one combination, and each must be answerable.
// An advertisement nothing can resolve is a promise an installation cannot keep.
func TestEveryAdvertisedCombinationResolves(t *testing.T) {
	for name, found := range engines() {
		t.Run(name, func(t *testing.T) {
			advertised := found.Advertised()
			if len(advertised) == 0 {
				t.Fatal("an engine advertised nothing")
			}
			for _, one := range advertised {
				if one.Combination.Engine != found.Identity() {
					t.Fatalf("combination=%s, want %s", one.Combination, found.Identity())
				}
				resolved, err := found.Resolve(specFor(t, one))
				if err != nil {
					t.Fatalf("%s was advertised and could not be resolved: %v", one.Combination, err)
				}
				if resolved.Combination != one.Combination {
					t.Fatalf("resolved=%s, want %s", resolved.Combination, one.Combination)
				}
			}
		})
	}
}

// Capability comes from engine, version, topology, and persistence together.
// Specification 06 states a single engine-wide boolean is not sufficient, so an
// engine must answer differently for different combinations of its own.
func TestCapabilityIsNotOneBooleanPerEngine(t *testing.T) {
	// Redis persistence is the case the tree actually carries: an engine an operator
	// asked not to persist has nothing to back up, and a backup format advertised for
	// it would schedule a backup of data that does not survive a restart.
	r := redis.New()
	unpersisted, err := r.Resolve(specFor(t, engine.Capabilities{Combination: engine.Combination{
		Engine: cluster.Redis, Version: "8.2.7", Topology: "standalone", Persistence: "none",
	}}))
	if err != nil {
		t.Fatal(err)
	}
	persisted, err := r.Resolve(specFor(t, engine.Capabilities{Combination: engine.Combination{
		Engine: cluster.Redis, Version: "8.2.7", Topology: "standalone", Persistence: "rdb",
	}}))
	if err != nil {
		t.Fatal(err)
	}
	if len(unpersisted.BackupFormats) != 0 {
		t.Fatalf("formats=%v, want none for an engine that was asked not to persist", unpersisted.BackupFormats)
	}
	if len(persisted.BackupFormats) == 0 {
		t.Fatal("a persisted engine advertised no backup format")
	}
}

// A combination an engine does not serve is refused when the spec is validated, not
// when a host is touched. An operator finds out before anything is created.
func TestUnsupportedCombinationIsRefusedAtValidation(t *testing.T) {
	catalog, err := engine.NewCatalog(postgres.New(), redis.New())
	if err != nil {
		t.Fatal(err)
	}
	for name, spec := range map[string]cluster.CreateSpec{
		"a version nobody serves": {SchemaVersion: 1, Name: "example", Engine: cluster.PostgreSQL,
			PostgreSQL: &cluster.PostgreSQLSpec{Version: "9.6", Topology: "standalone"}},
		"a redis version nobody serves": {SchemaVersion: 1, Name: "example", Engine: cluster.Redis,
			Redis: &cluster.RedisSpec{Version: "6.2", Topology: "standalone", Persistence: "rdb"}},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := catalog.Resolve(spec); !errors.Is(err, engine.ErrUnsupportedCombination) {
				t.Fatalf("err=%v, want ErrUnsupportedCombination", err)
			}
		})
	}
}

// A malformed spec is refused before any capability is read, so it can never be
// answered for a combination other than the one it asked about.
func TestAMalformedSpecNeverReachesACapability(t *testing.T) {
	catalog, err := engine.NewCatalog(postgres.New(), redis.New())
	if err != nil {
		t.Fatal(err)
	}
	for name, spec := range map[string]cluster.CreateSpec{
		"no schema version": {Name: "example", Engine: cluster.PostgreSQL,
			PostgreSQL: &cluster.PostgreSQLSpec{Version: "17.11", Topology: "standalone"}},
		"a name no cluster may have": {SchemaVersion: 1, Name: "Example", Engine: cluster.PostgreSQL,
			PostgreSQL: &cluster.PostgreSQLSpec{Version: "17.11", Topology: "standalone"}},
		"two engines at once": {SchemaVersion: 1, Name: "example", Engine: cluster.PostgreSQL,
			PostgreSQL: &cluster.PostgreSQLSpec{Version: "17.11", Topology: "standalone"},
			Redis:      &cluster.RedisSpec{Version: "8.2.7", Topology: "standalone", Persistence: "rdb"}},
		"no engine settings": {SchemaVersion: 1, Name: "example", Engine: cluster.PostgreSQL},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := catalog.Resolve(spec); err == nil {
				t.Fatalf("a malformed spec was resolved: %s", name)
			}
		})
	}
}

// Specification 36 states that no combination is qualified by documentation and that
// passing evidence is never invented. Nothing may ship claiming otherwise.
func TestNothingClaimsQualificationItDoesNotHave(t *testing.T) {
	for name, found := range engines() {
		t.Run(name, func(t *testing.T) {
			for _, one := range found.Advertised() {
				if one.Status == engine.Qualified {
					t.Fatalf("%s claims qualification; no gate has closed and specification 36 forbids inventing evidence", one.Combination)
				}
				if one.Status != engine.RequiredPending && one.Status != engine.Unsupported {
					t.Fatalf("%s has status %q, which is not one of the three specification 36 defines", one.Combination, one.Status)
				}
				// A major upgrade needs a qualified source and target pair. Nothing
				// is qualified, so nothing may advertise one.
				if len(one.UpgradePairs) != 0 {
					t.Fatalf("%s advertises an upgrade pair with no qualified target", one.Combination)
				}
			}
		})
	}
}

// A CLI and a UI must be able to render the limits, so the answer is data rather than
// a yes or no, and it survives a round trip.
func TestCapabilitiesAreMachineReadable(t *testing.T) {
	catalog, err := engine.NewCatalog(postgres.New(), redis.New())
	if err != nil {
		t.Fatal(err)
	}
	advertised := catalog.Advertised()
	if len(advertised) < 2 {
		t.Fatalf("advertised=%d, want every engine's combinations", len(advertised))
	}
	encoded, err := json.Marshal(advertised)
	if err != nil {
		t.Fatal(err)
	}
	var decoded []engine.Capabilities
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	if !slices.EqualFunc(advertised, decoded, func(a, b engine.Capabilities) bool {
		return a.Combination == b.Combination && a.Status == b.Status && a.EndpointType == b.EndpointType &&
			slices.Equal(a.BackupFormats, b.BackupFormats) && a.Replicas == b.Replicas
	}) {
		t.Fatal("capabilities did not survive a round trip")
	}
	// The order is stable, so two reads of an unchanged installation agree.
	if !slices.IsSortedFunc(advertised, func(a, b engine.Capabilities) int {
		return strings.Compare(a.Combination.String(), b.Combination.String())
	}) {
		t.Fatal("the advertised list is not in a stable order")
	}
}

// The catalog serves the engines it was given and refuses a spec for anything else.
func TestCatalogServesOnlyWhatItWasGiven(t *testing.T) {
	only, err := engine.NewCatalog(postgres.New())
	if err != nil {
		t.Fatal(err)
	}
	spec := cluster.CreateSpec{SchemaVersion: 1, Name: "cache", Engine: cluster.Redis,
		Redis: &cluster.RedisSpec{Version: "8.2.7", Topology: "standalone", Persistence: "rdb"}}
	if _, err := only.Resolve(spec); !errors.Is(err, engine.ErrUnknownEngine) {
		t.Fatalf("err=%v, want ErrUnknownEngine", err)
	}
	if _, err := engine.NewCatalog(); err == nil {
		t.Fatal("an empty catalog was accepted")
	}
	if _, err := engine.NewCatalog(postgres.New(), postgres.New()); err == nil {
		t.Fatal("two engines claiming the same identity were accepted")
	}
}

// An engine answers for itself and refuses another engine's spec outright, so the
// boundary admits a second engine without either reaching into the other.
func TestAnEngineRefusesAnotherEnginesSpec(t *testing.T) {
	postgresSpec := cluster.CreateSpec{SchemaVersion: 1, Name: "db", Engine: cluster.PostgreSQL,
		PostgreSQL: &cluster.PostgreSQLSpec{Version: "17.11", Topology: "standalone"}}
	redisSpec := cluster.CreateSpec{SchemaVersion: 1, Name: "cache", Engine: cluster.Redis,
		Redis: &cluster.RedisSpec{Version: "8.2.7", Topology: "standalone", Persistence: "rdb"}}
	if err := postgres.New().Validate(redisSpec); err == nil {
		t.Fatal("the postgres engine accepted a redis spec")
	}
	if err := redis.New().Validate(postgresSpec); err == nil {
		t.Fatal("the redis engine accepted a postgres spec")
	}
}

// Topology is part of the key, not a field read afterwards. Admission refuses every
// topology but standalone today, so this asks the engine directly: the dimension has
// to work before HA becomes admissible, or an unqualified HA cluster would be created
// the moment the earlier refusal is lifted.
func TestTopologyDiscriminatesBeforeItIsAdmissible(t *testing.T) {
	ha := cluster.CreateSpec{SchemaVersion: 1, Name: "db", Engine: cluster.PostgreSQL,
		PostgreSQL: &cluster.PostgreSQLSpec{Version: "17.11", Topology: "ha"}}
	if _, err := postgres.New().Resolve(ha); !errors.Is(err, engine.ErrUnsupportedCombination) {
		t.Fatalf("err=%v, want an unadvertised topology refused", err)
	}
	// The same version in a topology the engine does advertise is served.
	standalone := cluster.CreateSpec{SchemaVersion: 1, Name: "db", Engine: cluster.PostgreSQL,
		PostgreSQL: &cluster.PostgreSQLSpec{Version: "17.11", Topology: "standalone"}}
	if _, err := postgres.New().Resolve(standalone); err != nil {
		t.Fatalf("an advertised combination was refused: %v", err)
	}
	// Redis Sentinel and Cluster are the same story on the other engine.
	sentinel := cluster.CreateSpec{SchemaVersion: 1, Name: "cache", Engine: cluster.Redis,
		Redis: &cluster.RedisSpec{Version: "8.2.7", Topology: "sentinel", Persistence: "rdb"}}
	if _, err := redis.New().Resolve(sentinel); !errors.Is(err, engine.ErrUnsupportedCombination) {
		t.Fatalf("err=%v, want an unadvertised topology refused", err)
	}
}

func TestRedisReplicaLimitsMatchPersistence(t *testing.T) {
	for _, persistence := range []string{"none", "rdb", "aof", "rdb+aof"} {
		t.Run(persistence, func(t *testing.T) {
			resolved, err := redis.New().Resolve(cluster.CreateSpec{SchemaVersion: 1, Name: "cache", Engine: cluster.Redis,
				Redis: &cluster.RedisSpec{Version: "8.2.7", Topology: "standalone", Persistence: persistence}})
			if err != nil {
				t.Fatal(err)
			}
			maximum := 1
			if persistence == "none" {
				maximum = 0
			}
			if resolved.Replicas.Minimum != 0 || resolved.Replicas.Maximum != maximum {
				t.Fatalf("replica limits %+v, want 0..%d", resolved.Replicas, maximum)
			}
			if resolved.Status != engine.RequiredPending || resolved.NativeFailover || resolved.Sharding || resolved.ClientDiscovery {
				t.Fatal("compact replicas must not claim qualification or HA")
			}
		})
	}
}

func TestPostgresReplicaLimitsMatchImplementedVersion(t *testing.T) {
	for _, version := range []string{"17.11", "18.6"} {
		t.Run(version, func(t *testing.T) {
			found, err := postgres.New().Resolve(cluster.CreateSpec{SchemaVersion: 1, Name: "db", Engine: cluster.PostgreSQL,
				PostgreSQL: &cluster.PostgreSQLSpec{Version: version, Topology: "standalone"}})
			if err != nil {
				t.Fatal(err)
			}
			// Standalone is one node.
			if found.Replicas.Minimum != 0 || found.Replicas.Maximum != 0 {
				t.Fatalf("replica limits %+v, want none", found.Replicas)
			}
			if found.Status != engine.RequiredPending || found.NativeFailover || found.ProxyRouting || found.TLS || len(found.UpgradePairs) != 0 {
				t.Fatal("standalone claimed unqualified HA or routing")
			}
		})
	}
}

// A retired topology is refused, not served.
func TestPostgresPatroniTopologyIsRefused(t *testing.T) {
	spec := cluster.CreateSpec{SchemaVersion: 1, Name: "ha", Engine: cluster.PostgreSQL, PostgreSQL: &cluster.PostgreSQLSpec{Version: "17.11", Topology: "patroni"}}
	if err := spec.Validate(); err != nil {
		t.Fatalf("a well-formed topology failed validation: %v", err)
	}
	if _, err := postgres.New().Resolve(spec); !errors.Is(err, engine.ErrUnsupportedCombination) {
		t.Fatalf("a patroni topology resolved: %v", err)
	}
}
