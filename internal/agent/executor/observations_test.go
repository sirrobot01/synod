package executor

import (
	"testing"
	"time"

	"github.com/google/uuid"
	noderuntime "github.com/sirrobot01/synod/internal/runtime"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
)

func TestObservationsExpireAndDoNotShareMutableState(t *testing.T) {
	id := uuid.New()
	native := &measuredRuntime{resources: []noderuntime.Resource{{ResourceID: id, State: "running", Health: "healthy", Ports: []*agentv1.PortBinding{{ContainerPort: 80, HostPort: 32100}}}, {ResourceID: uuid.New(), Kind: "volume"}}}
	e := &Executor{runtime: native, options: Options{InspectionTimeout: time.Second}}
	e.collectObservations(t.Context())
	observed := e.Observations()
	if len(observed) != 1 || observed[0].ResourceId != id.String() || observed[0].Health != "healthy" || observed[0].ObservedAt == nil {
		t.Fatalf("observations: %v", observed)
	}
	observed[0].Ports[0].HostPort = 1
	if e.Observations()[0].Ports[0].HostPort != 32100 {
		t.Fatal("caller changed the cached observation")
	}
	e.observedAt = time.Now().Add(-16 * time.Second)
	if len(e.Observations()) != 0 {
		t.Fatal("stale health is still published")
	}
	native.resources = nil
	e.collectObservations(t.Context())
	if len(e.Observations()) != 0 {
		t.Fatal("removed resource retained")
	}
}
