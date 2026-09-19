package docker

import (
	"context"
	"errors"
	"net/http"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/moby/moby/client"
	"github.com/sirrobot01/synod/internal/runtime"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
)

func TestClusterInventoryRetainsNativeEvidence(t *testing.T) {
	r, api, clusterID := runtimeFixture(t)
	ensure := createFixtureContainer(t, r, clusterID, agentv1.NetworkMode_NETWORK_MODE_BRIDGE)
	start := runtimeCommand(t, r, clusterID, &agentv1.CommandInput{ResourceId: ensure.Input.ResourceId, Payload: &agentv1.CommandInput_StartContainer{StartContainer: &agentv1.StartContainer{}}})
	if _, err := r.Execute(t.Context(), start); err != nil {
		t.Fatal(err)
	}
	native, err := api.ExecCreate(t.Context(), "synod-instance-"+ensure.Input.ResourceId, client.ExecCreateOptions{Cmd: []string{"sleep", "1"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := api.ExecStart(t.Context(), native.ID, client.ExecStartOptions{Detach: true}); err != nil {
		t.Fatal(err)
	}
	first, err := r.InspectCluster(t.Context(), clusterID, nil)
	if err != nil || len(first.Resources) != 3 || len(first.Effects) != 2 {
		t.Fatalf("native inventory: %+v %v", first, err)
	}
	if !slices.ContainsFunc(first.Effects, func(effect *agentv1.Effect) bool {
		return effect.Active && strings.HasSuffix(effect.NativeOperationId, native.ID)
	}) {
		t.Fatal("inventory omitted unjournaled native exec")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	tick := time.NewTicker(20 * time.Millisecond)
	defer tick.Stop()
	for {
		current, err := r.InspectCluster(ctx, clusterID, first.Effects)
		if err != nil {
			t.Fatal(err)
		}
		if !slices.ContainsFunc(current.Effects, func(effect *agentv1.Effect) bool { return effect.Active || effect.OutcomeUnknown }) {
			if len(current.Effects) != 2 {
				t.Fatal("inventory omitted completed effect identity")
			}
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("native exec did not drain")
		case <-tick.C:
		}
	}
}

func TestClusterInventoryRejectsForeignAndUnavailableEvidence(t *testing.T) {
	r, api, clusterID := runtimeFixture(t)
	ensure := createFixtureContainer(t, r, clusterID, agentv1.NetworkMode_NETWORK_MODE_BRIDGE)
	container, err := api.ContainerInspect(t.Context(), "synod-instance-"+ensure.Input.ResourceId, client.ContainerInspectOptions{})
	if err != nil {
		t.Fatal(err)
	}
	prior := []*agentv1.Effect{{ResourceId: ensure.Input.ResourceId, NativeOperationId: "docker-exec:" + container.Container.ID + ":" + strings.Repeat("a", 64), Active: true}}
	foreign, err := r.InspectCluster(t.Context(), uuid.New(), prior)
	if !errors.Is(err, runtime.ErrOwnershipConflict) || len(foreign.Effects) != 1 || !foreign.Effects[0].OutcomeUnknown {
		t.Fatalf("accepted another cluster's native evidence: %+v %v", foreign, err)
	}
	missingID := uuid.NewString()
	missing := []*agentv1.Effect{{ResourceId: missingID, NativeOperationId: "docker-inventory:" + missingID, Active: true, OutcomeUnknown: true}}
	absent, err := r.InspectCluster(t.Context(), clusterID, missing)
	if err != nil || !slices.ContainsFunc(absent.Effects, func(effect *agentv1.Effect) bool {
		return effect.NativeOperationId == missing[0].NativeOperationId && !effect.Active && !effect.OutcomeUnknown && effect.State == "absent"
	}) {
		t.Fatalf("missing inventory did not resolve explicitly: %+v %v", absent, err)
	}
	faulty, err := client.New(client.WithHost(os.Getenv("SYNOD_TEST_DOCKER_HOST")), client.WithResponseHook(func(response *http.Response) {
		if strings.Contains(response.Request.URL.Path, "/containers/"+container.Container.ID+"/") && strings.HasSuffix(response.Request.URL.Path, "/json") {
			response.StatusCode = http.StatusServiceUnavailable
		}
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer faulty.Close()
	injected, err := New(faulty, r.options)
	if err != nil {
		t.Fatal(err)
	}
	unknown, err := injected.InspectCluster(t.Context(), clusterID, nil)
	if err == nil || len(unknown.Effects) != 1 || !unknown.Effects[0].OutcomeUnknown {
		t.Fatalf("inspection failure claimed drain: %+v %v", unknown, err)
	}
	recovered, err := r.InspectCluster(t.Context(), clusterID, unknown.Effects)
	if err != nil || recovered.Effects[0].NativeOperationId != unknown.Effects[0].NativeOperationId || recovered.Effects[0].OutcomeUnknown {
		t.Fatalf("inventory recovery lost identity: %+v %v", recovered, err)
	}
}
