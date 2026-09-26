package docker

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/client"
	"github.com/sirrobot01/synod/internal/engine/postgres"
	"github.com/sirrobot01/synod/internal/runtime"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestPostgresPhaseRequiresCurrentNativeProof(t *testing.T) {
	started := time.Now().Add(-time.Minute).UTC()
	p := &agentv1.EnsureContainer{Engine: "postgres", Postgres: &agentv1.PostgresContainerConfig{Port: 5432, ApplicationCredentialId: uuid.NewString(), ApplicationCredentialVersion: 1, SuperuserCredentialId: uuid.NewString(), SuperuserCredentialVersion: 1}}
	encoded, err := proto.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	hash := sha256.Sum256(encoded)
	installation, clusterID, resource := uuid.NewString(), uuid.NewString(), uuid.NewString()
	for _, test := range []struct {
		name  string
		alter func(*container.InspectResponse)
		phase string
		known bool
	}{
		{"ready", func(*container.InspectResponse) {}, "bootstrap_ready", true},
		{"failed and inert", func(item *container.InspectResponse) {
			item.State.Health.Status = container.Unhealthy
			item.State.Health.Log[0].ExitCode, item.State.Health.Log[0].Output = 1, "failed"
		}, "bootstrap_failed", true},
		{"recovered and inert", func(item *container.InspectResponse) { item.State.Health.Log[0].Output = "recovered" }, "bootstrap_recovered", true},
		{"recovered with wrong status", func(item *container.InspectResponse) {
			item.State.Health.Status = container.Unhealthy
			item.State.Health.Log[0].Output = "recovered"
		}, "bootstrap_pending", false},
		{"starting", func(item *container.InspectResponse) { item.State.Health.Status = container.Starting }, "bootstrap_pending", false},
		{"restarting", func(item *container.InspectResponse) { item.State.Restarting = true }, "bootstrap_pending", false},
		{"missing health", func(item *container.InspectResponse) { item.State.Health = nil }, "bootstrap_pending", false},
		{"missing result", func(item *container.InspectResponse) { item.State.Health.Log = nil }, "bootstrap_pending", false},
		{"previous start", func(item *container.InspectResponse) { item.State.Health.Log[0].Start = started.Add(-time.Second) }, "bootstrap_unknown", false},
		{"malformed output", func(item *container.InspectResponse) { item.State.Health.Log[0].Output = "ready\n" }, "bootstrap_pending", false},
		{"wrong exit", func(item *container.InspectResponse) { item.State.Health.Log[0].ExitCode = 1 }, "bootstrap_pending", false},
		{"changed healthcheck", func(item *container.InspectResponse) { item.Config.Healthcheck.Test = []string{"CMD-SHELL", "exit 0"} }, "bootstrap_unknown", false},
		{"changed wrapper", func(item *container.InspectResponse) { item.Config.Cmd = []string{"postgres"} }, "bootstrap_unknown", false},
		{"changed tmpfs", func(item *container.InspectResponse) { item.HostConfig.Tmpfs = nil }, "bootstrap_unknown", false},
		{"stopped", func(item *container.InspectResponse) { item.State.Running = false; item.State.Health = nil }, "", true},
	} {
		t.Run(test.name, func(t *testing.T) {
			item := container.InspectResponse{
				Config:     &container.Config{Labels: map[string]string{labelInstallation: installation, labelCluster: clusterID, labelResource: resource, labelConfig: base64.RawStdEncoding.EncodeToString(encoded), labelHash: hex.EncodeToString(hash[:])}, Cmd: postgres.ContainerCommand(p.Postgres, installation, clusterID, resource), Healthcheck: postgresHealthcheck()},
				HostConfig: &container.HostConfig{Tmpfs: map[string]string{postgres.ConfigDirectory: postgresTmpfs}},
				State:      &container.State{Running: true, StartedAt: started.Format(time.RFC3339Nano), Health: &container.Health{Status: container.Healthy, Log: []*container.HealthcheckResult{{Start: started.Add(time.Second), End: started.Add(2 * time.Second), ExitCode: 0, Output: "ready"}}}},
			}
			test.alter(&item)
			phase, err := postgresPhase(item)
			if phase != test.phase || (err == nil) != test.known {
				t.Fatalf("phase=%q err=%v", phase, err)
			}
		})
	}
}

func TestPostgresVerifierHasStableCredentialIdentity(t *testing.T) {
	p := &agentv1.EnsureContainer{Postgres: &agentv1.PostgresContainerConfig{ApplicationCredentialId: uuid.NewString(), ApplicationCredentialVersion: 1}, SecretEnv: []*agentv1.SecretEnv{{Name: "SYNOD_APP_VERIFIER", Purpose: "application"}}}
	password := []byte("A-separated-application-password-90")
	secrets := runtime.Secrets{"application": password}
	first, err := secretEnvironment(nil, p, secrets)
	if err != nil || len(first) != 1 || !strings.HasPrefix(first[0], "SYNOD_APP_VERIFIER=SCRAM-SHA-256$4096:") || strings.Contains(first[0], string(password)) {
		t.Fatal("application verifier is missing or exposes password material")
	}
	second, err := secretEnvironment(nil, p, secrets)
	if err != nil || !slices.Equal(first, second) {
		t.Fatal("same credential identity changed verifier")
	}
	p.Postgres.ApplicationCredentialVersion++
	changed, err := secretEnvironment(nil, p, secrets)
	if err != nil || slices.Equal(first, changed) {
		t.Fatal("changed credential version reused verifier")
	}
	if _, err := secretEnvironment(nil, p, runtime.Secrets{"application": []byte("invalid\x00password")}); !errors.Is(err, runtime.ErrConfigurationConflict) {
		t.Fatal("non-ASCII credential was accepted")
	}
}

func TestPostgresNativePhaseResetAndFailedDrain(t *testing.T) {
	r, api, clusterID := runtimeFixture(t)
	ensure := postgresStorageFixture(t, r, api, clusterID)
	id, name := ensure.Input.ResourceId, "synod-instance-"+ensure.Input.ResourceId
	initial := runtimeCommand(t, r, clusterID, &agentv1.CommandInput{ResourceId: id, Payload: &agentv1.CommandInput_StartContainer{StartContainer: &agentv1.StartContainer{AllowInitialization: true}}})
	startPostgresFixture(t, r, initial)
	completed, err := r.Recover(t.Context(), initial, nil)
	if err != nil || completed.Effects[0].State != "running" || completed.Effects[0].Active || completed.Effects[0].OutcomeUnknown {
		t.Fatalf("completed start lost its controller postcondition: %+v %v", completed, err)
	}
	drainDeadline := time.Now().Add(10 * time.Second)
	var prior []*agentv1.Effect
	for {
		inventory, err := r.InspectCluster(t.Context(), clusterID, prior)
		if err != nil {
			t.Fatal(err)
		}
		prior = inventory.Effects
		if !slices.ContainsFunc(prior, func(effect *agentv1.Effect) bool { return effect.Active || effect.OutcomeUnknown }) {
			break
		}
		if time.Now().After(drainDeadline) {
			t.Fatal("completed startup did not drain with native healthcheck enabled")
		}
		time.Sleep(20 * time.Millisecond)
	}
	item, err := api.ContainerInspect(t.Context(), name, client.ContainerInspectOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if phase, err := postgresPhase(item.Container); phase != "bootstrap_ready" || err != nil {
		t.Fatalf("initial phase=%q err=%v", phase, err)
	}
	if err := r.checkContainer(t.Context(), initial, item.Container, nil); err != nil {
		t.Fatalf("lifecycle validation lost credential references: %v", err)
	}
	storageExec(t, api, name, "psql", "-h", "/run/synod-postgres", "-U", "synod_admin", "-d", "postgres", "-v", "ON_ERROR_STOP=1", "-c", "DROP DATABASE app")
	stop := runtimeCommand(t, r, clusterID, &agentv1.CommandInput{ResourceId: id, Payload: &agentv1.CommandInput_StopContainer{StopContainer: &agentv1.StopContainer{GracePeriod: durationpb.New(10 * time.Second)}}})
	if _, err := r.Execute(t.Context(), stop, nil); err != nil {
		t.Fatal(err)
	}
	restart := runtimeCommand(t, r, clusterID, &agentv1.CommandInput{ResourceId: id, Payload: &agentv1.CommandInput_StartContainer{StartContainer: &agentv1.StartContainer{}}})
	result, err := r.Execute(t.Context(), restart, nil)
	// The bootstrap can still be running, or it can have failed already on a fast host.
	// Both prove the phase reset. Reusing the stale ready phase would report a running
	// engine that has no application database, which is the answer this guards against.
	if result.Effects[0].State == "running" ||
		(!errors.Is(err, runtime.ErrOutcomeUnknown) && !errors.Is(err, runtime.ErrConfigurationConflict)) {
		t.Fatalf("restart reused old ready phase: %+v %v", result, err)
	}
	deadline := time.Now().Add(30 * time.Second)
	for errors.Is(err, runtime.ErrOutcomeUnknown) && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
		result, err = r.Recover(t.Context(), restart, result.Effects)
	}
	if !errors.Is(err, runtime.ErrConfigurationConflict) || result.Effects[0].Active || result.Effects[0].OutcomeUnknown || result.Effects[0].State != "bootstrap_failed" {
		t.Fatalf("lost application database was not an inert failure: %+v %v", result, err)
	}
	inventory, err := r.InspectCluster(t.Context(), clusterID, nil)
	if err != nil || !slices.ContainsFunc(inventory.Effects, func(effect *agentv1.Effect) bool {
		return effect.ResourceId == id && effect.State == "bootstrap_failed" && !effect.Active && !effect.OutcomeUnknown
	}) {
		t.Fatalf("failed startup cannot drain: %+v %v", inventory, err)
	}
	item, err = api.ContainerInspect(t.Context(), name, client.ContainerInspectOptions{})
	if err != nil || !item.Container.State.Running || item.Container.State.Restarting || item.Container.RestartCount != 0 {
		t.Fatalf("failed wrapper entered restart loop: %v", err)
	}
	if _, err := r.Execute(t.Context(), stop, nil); err != nil {
		t.Fatalf("failed wrapper did not stop: %v", err)
	}
}

func TestPostgresPhaseConfigurationDrift(t *testing.T) {
	r, api, clusterID := runtimeFixture(t)
	ensure := postgresStorageFixture(t, r, api, clusterID)
	item, err := api.ContainerInspect(t.Context(), "synod-instance-"+ensure.Input.ResourceId, client.ContainerInspectOptions{})
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name  string
		alter func(*container.InspectResponse)
	}{
		{"healthcheck", func(item *container.InspectResponse) {
			item.Config.Healthcheck = &container.HealthConfig{Test: []string{"NONE"}}
		}},
		{"stop signal", func(item *container.InspectResponse) { item.Config.StopSignal = "SIGTERM" }},
		{"tmpfs", func(item *container.InspectResponse) { item.HostConfig.Tmpfs = nil }},
		{"wrapper", func(item *container.InspectResponse) { item.Config.Cmd = []string{"postgres"} }},
	} {
		t.Run(test.name, func(t *testing.T) {
			changed := item.Container
			config, host := *changed.Config, *changed.HostConfig
			changed.Config, changed.HostConfig = &config, &host
			test.alter(&changed)
			if err := r.checkContainer(t.Context(), ensure, changed, nil); !errors.Is(err, runtime.ErrConfigurationConflict) {
				t.Fatalf("changed startup contract was accepted: %v", err)
			}
		})
	}
	if _, err := api.ContainerRemove(t.Context(), item.Container.ID, client.ContainerRemoveOptions{}); err != nil {
		t.Fatal(err)
	}
	options, err := r.containerOptions(t.Context(), ensure, ensure.Input.GetEnsureContainer(), false)
	if err != nil {
		t.Fatal(err)
	}
	options.Config.Env, err = secretEnvironment(options.Config.Env, ensure.Input.GetEnsureContainer(), runtime.Secrets{"superuser": []byte(uuid.NewString()), "application": []byte(uuid.NewString()), "replication": []byte(uuid.NewString())})
	if err != nil {
		t.Fatal(err)
	}
	options.Config.Healthcheck.Test = []string{"CMD-SHELL", "printf ready"}
	changed, err := api.ContainerCreate(t.Context(), options)
	if err != nil {
		t.Fatal(err)
	}
	if detail, err := r.changedContainer(t.Context(), changed.ID); err != nil || detail != driftEngineConfig {
		t.Fatalf("healthcheck drift=%q err=%v", detail, err)
	}
	if _, err := api.ContainerRemove(t.Context(), changed.ID, client.ContainerRemoveOptions{}); err != nil {
		t.Fatal(err)
	}
	options.Config.Healthcheck = postgresHealthcheck()
	originalEntrypoint := slices.Clone(options.Config.Entrypoint)
	options.Config.Entrypoint = []string{"/bin/sh", "-c", "printf ready > /run/synod-postgres/bootstrap-complete; sleep 300"}
	forged, err := api.ContainerCreate(t.Context(), options)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := api.ContainerStart(t.Context(), forged.ID, client.ContainerStartOptions{}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		live, err := api.ContainerInspect(t.Context(), forged.ID, client.ContainerInspectOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if live.Container.State.Health != nil && live.Container.State.Health.Status == container.Healthy {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("forged native phase fixture did not become healthy")
		}
		time.Sleep(20 * time.Millisecond)
	}
	// A later native read can belong to a changed container start. It cannot lend
	// its valid configuration to the first snapshot's forged healthy marker.
	var inspections atomic.Int32
	shifting, err := client.New(client.WithHost(os.Getenv("SYNOD_TEST_DOCKER_HOST")), client.WithResponseHook(func(response *http.Response) {
		if !strings.HasSuffix(response.Request.URL.Path, "/containers/"+forged.ID+"/json") || inspections.Add(1) == 1 {
			return
		}
		body, err := io.ReadAll(response.Body)
		if err != nil {
			t.Error(err)
			return
		}
		response.Body.Close()
		var item container.InspectResponse
		if err := json.Unmarshal(body, &item); err != nil {
			t.Error(err)
			return
		}
		item.Config.Entrypoint = originalEntrypoint
		item.State.StartedAt = time.Now().UTC().Format(time.RFC3339Nano)
		item.State.Health = &container.Health{Status: container.Starting}
		body, err = json.Marshal(item)
		if err != nil {
			t.Error(err)
			return
		}
		response.Body = io.NopCloser(bytes.NewReader(body))
		response.ContentLength = int64(len(body))
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer shifting.Close()
	observer, err := New(shifting, r.options)
	if err != nil {
		t.Fatal(err)
	}
	inventory, err := observer.InspectCluster(t.Context(), clusterID, nil)
	if !errors.Is(err, runtime.ErrOutcomeUnknown) || !slices.ContainsFunc(inventory.Effects, func(effect *agentv1.Effect) bool {
		return effect.ResourceId == ensure.Input.ResourceId && effect.State == "bootstrap_unknown" && effect.Active && effect.OutcomeUnknown
	}) {
		t.Fatalf("changed entrypoint forged a drained startup phase: %+v %v", inventory, err)
	}
	if inspections.Load() != 1 {
		t.Fatal("phase and configuration came from different native snapshots")
	}
}

func TestPostgresRecoveredPhaseDrainsWithoutClaimingReadiness(t *testing.T) {
	r, api, clusterID := runtimeFixture(t)
	ensure := postgresStorageFixture(t, r, api, clusterID)
	id, name := ensure.Input.ResourceId, "synod-instance-"+ensure.Input.ResourceId
	initial := runtimeCommand(t, r, clusterID, &agentv1.CommandInput{ResourceId: id, Payload: &agentv1.CommandInput_StartContainer{StartContainer: &agentv1.StartContainer{AllowInitialization: true}}})
	startPostgresFixture(t, r, initial)
	storageExec(t, api, name, "sh", "-c", "printf 'failed\\n' > "+postgres.BootstrapFailurePath)
	stop := runtimeCommand(t, r, clusterID, &agentv1.CommandInput{ResourceId: id, Payload: &agentv1.CommandInput_StopContainer{StopContainer: &agentv1.StopContainer{GracePeriod: durationpb.New(10 * time.Second)}}})
	if _, err := r.Execute(t.Context(), stop, nil); err != nil {
		t.Fatal(err)
	}
	start := runtimeCommand(t, r, clusterID, &agentv1.CommandInput{ResourceId: id, Payload: &agentv1.CommandInput_StartContainer{StartContainer: &agentv1.StartContainer{}}})
	result, err := r.Execute(t.Context(), start, nil)
	deadline := time.Now().Add(30 * time.Second)
	for errors.Is(err, runtime.ErrOutcomeUnknown) && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
		result, err = r.Recover(t.Context(), start, result.Effects)
	}
	if !errors.Is(err, runtime.ErrConfigurationConflict) || result.Effects[0].State != "bootstrap_failed" {
		t.Fatalf("expected confirmed failure: %+v %v", result, err)
	}
	program, err := postgres.RecoveryCommand(ensure.Input.GetEnsureContainer().Postgres, ensure.Epoch.InstallationId, ensure.Epoch.ClusterId, id, uuid.NewString())
	if err != nil {
		t.Fatal(err)
	}
	storageExec(t, api, name, program...)
	deadline = time.Now().Add(15 * time.Second)
	for {
		inventory, err := r.InspectCluster(t.Context(), clusterID, nil)
		if err == nil && slices.ContainsFunc(inventory.Effects, func(effect *agentv1.Effect) bool {
			return effect.ResourceId == id && effect.State == "bootstrap_recovered"
		}) && !slices.ContainsFunc(inventory.Effects, func(effect *agentv1.Effect) bool { return effect.Active || effect.OutcomeUnknown }) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("recovery did not drain: %+v %v", inventory, err)
		}
		time.Sleep(50 * time.Millisecond)
	}
	result, err = r.Execute(t.Context(), start, nil)
	if !errors.Is(err, runtime.ErrConfigurationConflict) || result.Effects[0].State != "bootstrap_recovered" || result.Effects[0].Active || result.Effects[0].OutcomeUnknown {
		t.Fatalf("inert recovery claimed readiness: %+v %v", result, err)
	}
	if _, err := r.Execute(t.Context(), stop, nil); err != nil {
		t.Fatal(err)
	}
	startPostgresFixture(t, r, start)
}
