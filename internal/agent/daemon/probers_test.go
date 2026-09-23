package daemon

import (
	"testing"

	"github.com/sirrobot01/synod/internal/engine"
	postgresengine "github.com/sirrobot01/synod/internal/engine/postgres"
	redisengine "github.com/sirrobot01/synod/internal/engine/redis"
)

// A readiness step proves POSTCONDITION_ENGINE_READY by asking a prober on the agent.
// An engine the control plane provisions but the agent cannot probe fails every create
// at that step, which is how a real installation found this map empty.
func TestEveryQualifiedEngineHasAProber(t *testing.T) {
	catalog, err := engine.NewCatalog(postgresengine.New(), redisengine.New())
	if err != nil {
		t.Fatal(err)
	}
	for _, advertised := range catalog.Advertised() {
		if advertised.Status != engine.Qualified {
			continue
		}
		if name := string(advertised.Combination.Engine); probers[name] == nil {
			t.Errorf("engine %s is qualified but the agent registers no prober for it", name)
		}
	}
}
