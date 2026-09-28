package docker

import (
	"errors"
	"testing"
	"time"

	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/client"
	"github.com/sirrobot01/synod/internal/runtime"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestRejectUnsupportedContainerSecurity(t *testing.T) {
	r, api, appID := runtimeFixture(t)
	ensure := createFixtureContainer(t, r, appID)
	item, err := api.ContainerInspect(t.Context(), "synod-instance-"+ensure.Input.ResourceId, client.ContainerInspectOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := r.checkContainer(t.Context(), ensure, item.Container, nil); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name  string
		alter func(*container.HostConfig)
	}{
		{"capability", func(host *container.HostConfig) { host.CapAdd = []string{"SYS_ADMIN"} }},
		{"seccomp", func(host *container.HostConfig) { host.SecurityOpt = []string{"seccomp=unconfined"} }},
		{"ipc", func(host *container.HostConfig) { host.IpcMode = "host" }},
		{"user namespace", func(host *container.HostConfig) { host.UsernsMode = "host" }},
		{"cgroup namespace", func(host *container.HostConfig) { host.CgroupnsMode = "host" }},
		{"uts namespace", func(host *container.HostConfig) { host.UTSMode = "host" }},
		{"device request", func(host *container.HostConfig) {
			host.DeviceRequests = []container.DeviceRequest{{Count: -1, Capabilities: [][]string{{"gpu"}}}}
		}},
		{"device rules", func(host *container.HostConfig) { host.DeviceCgroupRules = []string{"a *:* rwm"} }},
		{"masked paths", func(host *container.HostConfig) { host.MaskedPaths = nil }},
		{"readonly paths", func(host *container.HostConfig) { host.ReadonlyPaths = nil }},
		{"runtime", func(host *container.HostConfig) { host.Runtime = "unapproved-runtime" }},
		{"extra groups", func(host *container.HostConfig) { host.GroupAdd = []string{"0"} }},
	} {
		t.Run(test.name, func(t *testing.T) {
			changed := item.Container
			host := *changed.HostConfig
			changed.HostConfig = &host
			test.alter(&host)
			if err := r.checkContainer(t.Context(), ensure, changed, nil); !errors.Is(err, runtime.ErrConfigurationConflict) {
				t.Fatalf("accepted unsupported security configuration: %v", err)
			}
		})
	}

	// Recreate the stopped, exactly owned container with a real unauthorized capability.
	if _, err := api.ContainerRemove(t.Context(), item.Container.ID, client.ContainerRemoveOptions{RemoveVolumes: false}); err != nil {
		t.Fatal(err)
	}
	options, err := r.containerOptions(t.Context(), ensure, ensure.Input.GetEnsureContainer(), false)
	if err != nil {
		t.Fatal(err)
	}
	options.Config.Env, err = secretEnvironment(options.Config.Env, ensure.Input.GetEnsureContainer(), redisFixtureSecrets)
	if err != nil {
		t.Fatal(err)
	}
	options.HostConfig.CapAdd = []string{"SYS_ADMIN"}
	changed, err := api.ContainerCreate(t.Context(), options)
	if err != nil {
		t.Fatal(err)
	}
	for _, input := range []*agentv1.CommandInput{
		{ResourceId: ensure.Input.ResourceId, Payload: &agentv1.CommandInput_StartContainer{StartContainer: &agentv1.StartContainer{}}},
		{ResourceId: ensure.Input.ResourceId, Payload: &agentv1.CommandInput_StopContainer{StopContainer: &agentv1.StopContainer{GracePeriod: durationpb.New(time.Second)}}},
		{ResourceId: ensure.Input.ResourceId, Payload: &agentv1.CommandInput_RemoveContainer{RemoveContainer: &agentv1.RemoveContainer{}}},
	} {
		command := runtimeCommand(t, r, appID, input)
		if _, err := r.Execute(t.Context(), command, nil); !errors.Is(err, runtime.ErrConfigurationConflict) {
			t.Fatalf("mutated unauthorized container: %v", err)
		}
	}
	preserved, err := api.ContainerInspect(t.Context(), changed.ID, client.ContainerInspectOptions{})
	if err != nil || preserved.Container.State.Running {
		t.Fatalf("conflicting container was removed or started: %v", err)
	}
}
