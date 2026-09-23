package docker

import (
	"errors"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"testing/iotest"

	"github.com/moby/moby/client"
	"github.com/sirrobot01/synod/internal/operation"
	"github.com/sirrobot01/synod/internal/runtime"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
)

func TestNativeContainerAbsenceIsReadOnlyAndRecoverable(t *testing.T) {
	r, api, clusterID := runtimeFixture(t)
	ensure := createFixtureContainer(t, r, clusterID, agentv1.NetworkMode_NETWORK_MODE_BRIDGE)
	id := ensure.Input.ResourceId
	command := runtimeCommand(t, r, clusterID, &agentv1.CommandInput{ResourceId: id, Payload: &agentv1.CommandInput_VerifyContainerAbsent{VerifyContainerAbsent: &agentv1.VerifyContainerAbsent{}}})
	before, err := api.ContainerInspect(t.Context(), "synod-instance-"+id, client.ContainerInspectOptions{})
	if err != nil {
		t.Fatal(err)
	}
	for _, recover := range []bool{false, true} {
		var result runtime.Result
		if recover {
			result, err = r.Recover(t.Context(), command, nil)
		} else {
			result, err = r.Execute(t.Context(), command, nil)
		}
		if !errors.Is(err, runtime.ErrConfigurationConflict) || len(result.Effects) != 1 || result.Effects[0].State != "present" || result.Effects[0].OutcomeUnknown || result.Effects[0].Active {
			t.Fatalf("present result: %+v %v", result, err)
		}
	}
	after, err := api.ContainerInspect(t.Context(), "synod-instance-"+id, client.ContainerInspectOptions{})
	if err != nil || after.Container.ID != before.Container.ID || after.Container.State.Status != before.Container.State.Status {
		t.Fatalf("absence check changed container: %v", err)
	}
	faulty, err := client.New(client.WithHost(os.Getenv("SYNOD_TEST_DOCKER_HOST")), client.WithResponseHook(func(response *http.Response) {
		if strings.HasSuffix(response.Request.URL.Path, "/containers/synod-instance-"+id+"/json") {
			response.Body.Close()
			response.Body = io.NopCloser(iotest.ErrReader(io.ErrUnexpectedEOF))
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
	result, err := injected.Execute(t.Context(), command, nil)
	if !errors.Is(err, runtime.ErrOutcomeUnknown) || len(result.Effects) != 1 || !result.Effects[0].OutcomeUnknown {
		t.Fatalf("read failure inferred absence: %+v %v", result, err)
	}
	// Only the fixture deletes its never-started container.
	if _, err := api.ContainerRemove(t.Context(), before.Container.ID, client.ContainerRemoveOptions{}); err != nil {
		t.Fatal(err)
	}
	result, err = r.Execute(t.Context(), command, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := operation.CheckStepEffects(&agentv1.PlanStep{Command: command, Postcondition: agentv1.Postcondition_POSTCONDITION_CONTAINER_ABSENT}, result.Effects); err != nil {
		t.Fatal(err)
	}
	recovered, err := r.Recover(t.Context(), command, result.Effects)
	if err != nil || len(recovered.Effects) != 1 || recovered.Effects[0].State != "absent" {
		t.Fatalf("recover: %+v %v", recovered, err)
	}
	if _, err := api.VolumeInspect(t.Context(), "synod-volume-"+ensure.Input.GetEnsureContainer().VolumeId, client.VolumeInspectOptions{}); err != nil {
		t.Fatalf("retained volume: %v", err)
	}
}
