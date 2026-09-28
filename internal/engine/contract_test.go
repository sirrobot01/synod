package engine_test

import (
	"encoding/json"
	"slices"
	"strings"
	"testing"

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

// Every engine must advertise at least one combination, and each names its own engine.
func TestEveryEngineAdvertisesItsOwnCombinations(t *testing.T) {
	for name, found := range engines() {
		t.Run(name, func(t *testing.T) {
			advertised := found.Advertised()
			if len(advertised) == 0 {
				t.Fatal("an engine advertised nothing")
			}
			for _, one := range advertised {
				if one.Combination.Engine != found.Identity() || one.Combination.Engine != name {
					t.Fatalf("combination=%s, want %s", one.Combination, found.Identity())
				}
			}
		})
	}
}

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

// Tier 1 admission names a major version. The catalog serves a release in it, and
// refuses an engine it was not given.
func TestCatalogServesOnlyWhatItWasGiven(t *testing.T) {
	only, err := engine.NewCatalog(postgres.New())
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		engine, version string
		served          bool
	}{
		{engine.PostgreSQL, "17", true},
		{engine.PostgreSQL, "17.11", true},
		{engine.PostgreSQL, "1", false},
		{engine.PostgreSQL, "16", false},
		{engine.Redis, "8", false},
		{"mysql", "8", false},
	} {
		if got := only.Serves(test.engine, test.version); got != test.served {
			t.Fatalf("Serves(%s, %s)=%t, want %t", test.engine, test.version, got, test.served)
		}
	}
	if _, err := engine.NewCatalog(); err == nil {
		t.Fatal("an empty catalog was accepted")
	}
	if _, err := engine.NewCatalog(postgres.New(), postgres.New()); err == nil {
		t.Fatal("two engines claiming the same identity were accepted")
	}
	if _, err := engine.NewCatalog(nil); err == nil {
		t.Fatal("a nil engine was accepted")
	}
}
