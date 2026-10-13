package restore

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/client"
	"github.com/sirrobot01/synod/internal/agent/discovery"
	"github.com/sirrobot01/synod/internal/proof"
	"github.com/sirrobot01/synod/internal/runtime/compose"
)

func TestProofRestoresIsolatedDataChecksRunningContainersAndCleansUp(t *testing.T) {
	f := newFixture(t, true)
	w := &Worker{Backup: f.worker, Directory: t.TempDir(), Apps: t.TempDir()}
	api := f.worker.Docker
	original, err := api.ContainerInspect(t.Context(), f.container, client.ContainerInspectOptions{})
	if err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"working", "stopped", "broken"} {
		fail := mode != "working"
		request := proof.Request{Submission: proof.Submission{ID: uuid.New(), Kind: proof.Drill, NodeID: f.worker.NodeID, AppID: f.app.ID}, InstallationID: f.worker.InstallationID, RequestedAt: time.Now().UTC()}
		t.Cleanup(func() { removeLabelled(context.Background(), api, request.ID, true) })
		type outcome struct {
			result proof.Result
			err    error
		}
		done := make(chan outcome, 1)
		ctx, cancel := context.WithCancel(t.Context())
		go func() { result, err := w.ExecuteProof(ctx, request); done <- outcome{result, err}; close(done) }()
		defer func() { cancel(); <-done }()
		name := "synod-drill-" + request.ID.String() + "-app"
		// The drill loads saved images and a database dump before the app starts.
		for deadline := time.Now().Add(5 * time.Minute); ; time.Sleep(100 * time.Millisecond) {
			select {
			case out := <-done:
				t.Fatalf("drill ended before app start: %+v %v", out.result, out.err)
			default:
			}
			current, err := api.ContainerInspect(t.Context(), name, client.ContainerInspectOptions{})
			if err == nil && current.Container.State.Running {
				if len(current.Container.NetworkSettings.Networks) != 1 || current.Container.NetworkSettings.Networks[drillNetwork(request.ID)] == nil || len(current.Container.HostConfig.PortBindings) != 0 || current.Container.Config.Labels["synod.test"] != "" {
					t.Fatal("drill kept live app connections or automation labels")
				}
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("drill did not start")
			}
		}
		switch mode {
		case "stopped":
			if _, err := api.ContainerStop(t.Context(), name, client.ContainerStopOptions{Timeout: new(1)}); err != nil {
				t.Fatal(err)
			}
		case "broken":
			// The container keeps running, but the restored app answers with a server error.
			if err := execCommand(t.Context(), api, name, []string{"sh", "-c", "echo '500 Internal Server Error' > /config/status"}, nil, nil); err != nil {
				t.Fatal(err)
			}
		default:
			var data bytes.Buffer
			if err := execCommand(t.Context(), api, name, []string{"cat", "/config/state"}, nil, &data); err != nil || data.String() != "private-restored-value" {
				t.Fatalf("drill file differs: %v", err)
			}
			if value := query(t, api, "synod-drill-"+request.ID.String()+"-db", "SELECT value FROM proof WHERE id=1"); value != "private-db-value" {
				t.Fatal("drill lost the saved database row")
			}
			filters := make(client.Filters)
			filters.Add("label", operationLabel+"="+request.ID.String())
			report, err := discovery.Scan(t.Context(), api, filters)
			if err != nil || len(report.Apps) != 0 {
				t.Fatalf("discovery exposed a scratch app: %v", err)
			}
		}
		out := <-done
		cancel()
		wantStatus, wantFailure := "succeeded", ""
		switch mode {
		case "stopped":
			wantStatus, wantFailure = "failed", "drill_container_stopped"
		case "broken":
			wantStatus, wantFailure = "failed", "drill_check_failed"
			// Port probes enter the container's network namespace. They need a root agent on Linux.
			if slices.ContainsFunc(out.result.Checks, func(check proof.Check) bool { return check.Detail == "namespace_unavailable" }) {
				wantStatus, wantFailure = "succeeded", ""
			}
		}
		if out.err != nil || out.result.FailureCode != wantFailure || (proof.Event{ID: request.ID, Status: wantStatus, Result: out.result}).Validate(request) != nil {
			t.Fatalf("drill result: %+v %v", out.result, out.err)
		}
		if !fail && (out.result.Containers != 2 || out.result.CompletedAt.Sub(out.result.StartedAt) < 30*time.Second) {
			t.Fatal("drill did not observe both containers")
		}
		if mode == "working" {
			for _, kind := range []string{proof.CheckDataFiles, proof.CheckDatabaseContent} {
				if !slices.ContainsFunc(out.result.Checks, func(check proof.Check) bool { return check.Kind == kind && check.Status == proof.CheckPassed }) {
					t.Fatalf("drill did not pass %s: %+v", kind, out.result.Checks)
				}
			}
			if !slices.ContainsFunc(out.result.Checks, func(check proof.Check) bool {
				return check.Kind == proof.CheckHTTP && check.Status == proof.CheckPassed
			}) && !slices.ContainsFunc(out.result.Checks, func(check proof.Check) bool { return check.Detail == "namespace_unavailable" }) {
				t.Fatalf("drill did not check the app response: %+v", out.result.Checks)
			}
		}
		if mode == "broken" && wantFailure != "" && out.result.Resource != proof.CheckHTTP+" app:80" {
			t.Fatalf("drill named the wrong failed check: %q", out.result.Resource)
		}
		if _, err := os.Stat(filepath.Join(w.Directory, request.ID.String())); !os.IsNotExist(err) {
			t.Fatal("drill kept its scratch directory", err)
		}
		filters := make(client.Filters)
		filters.Add("label", operationLabel+"="+request.ID.String())
		if list, err := api.ContainerList(t.Context(), client.ContainerListOptions{All: true, Filters: filters}); err != nil || len(list.Items) != 0 {
			t.Fatal("drill left containers", err)
		}
		if list, err := api.VolumeList(t.Context(), client.VolumeListOptions{Filters: filters}); err != nil || len(list.Items) != 0 {
			t.Fatal("drill left volumes", err)
		}
		if list, err := api.NetworkList(t.Context(), client.NetworkListOptions{Filters: filters}); err != nil || len(list.Items) != 0 {
			t.Fatal("drill left its network", err)
		}
		if repeated, err := w.ExecuteProof(t.Context(), request); err != nil || !reflect.DeepEqual(repeated, out.result) {
			t.Fatal("completed drill ran again", err)
		}
	}
	current, err := api.ContainerInspect(t.Context(), f.container, client.ContainerInspectOptions{})
	if err != nil || !current.Container.State.Running || current.Container.State.StartedAt != original.Container.State.StartedAt {
		t.Fatal("drill changed the source app", err)
	}
	check := proof.Request{Submission: proof.Submission{ID: uuid.New(), Kind: proof.Integrity, NodeID: f.worker.NodeID, DestinationID: f.destinationID}, InstallationID: f.worker.InstallationID, RequestedAt: time.Now().UTC()}
	result, err := w.ExecuteProof(t.Context(), check)
	if err != nil || (proof.Event{ID: check.ID, Status: "succeeded", Result: result}).Validate(check) != nil {
		t.Fatalf("integrity check: %+v %v", result, err)
	}
}

func TestProofRecoversAnInterruptedDrillWithoutTouchingTheSource(t *testing.T) {
	f := newFixture(t, false)
	w := &Worker{Backup: f.worker, Directory: t.TempDir(), Apps: t.TempDir()}
	request := proof.Request{Submission: proof.Submission{ID: uuid.New(), Kind: proof.Drill, NodeID: f.worker.NodeID, AppID: f.app.ID}, InstallationID: f.worker.InstallationID, RequestedAt: time.Now().UTC()}
	input, _ := json.Marshal(request)
	if _, err := f.worker.Journal.Admit(t.Context(), request.ID, input); err != nil {
		t.Fatal(err)
	}
	if err := f.worker.Journal.Start(t.Context(), request.ID); err != nil {
		t.Fatal(err)
	}
	state, _ := json.Marshal(proofProgress{Version: 1, RequestID: request.ID, Result: proof.Result{StartedAt: request.RequestedAt}})
	if err := f.worker.Journal.SaveProgress(t.Context(), request.ID, state); err != nil {
		t.Fatal(err)
	}
	created, err := f.worker.Docker.ContainerCreate(t.Context(), client.ContainerCreateOptions{Config: &container.Config{Image: helperImage, Cmd: []string{"sleep", "300"}, Labels: map[string]string{operationLabel: request.ID.String(), "io.synod.proof": "true"}}, HostConfig: &container.HostConfig{NetworkMode: "none"}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { removeLabelled(context.Background(), f.worker.Docker, request.ID, true) })
	if _, err := f.worker.Docker.ContainerStart(t.Context(), created.ID, client.ContainerStartOptions{}); err != nil {
		t.Fatal(err)
	}
	directory := filepath.Join(w.Directory, request.ID.String())
	if err := os.Mkdir(directory, 0700); err != nil {
		t.Fatal(err)
	}
	result, err := w.ExecuteProof(t.Context(), request)
	if err != nil || result.FailureCode != "agent_interrupted" {
		t.Fatalf("interrupted drill: %+v %v", result, err)
	}
	if _, err := os.Stat(directory); !os.IsNotExist(err) {
		t.Fatal("interrupted drill left files")
	}
	if _, err := f.worker.Docker.ContainerInspect(t.Context(), created.ID, client.ContainerInspectOptions{}); err == nil {
		t.Fatal("interrupted drill left a container")
	}
	if source, err := f.worker.Docker.ContainerInspect(t.Context(), f.container, client.ContainerInspectOptions{}); err != nil || !source.Container.State.Running {
		t.Fatal("interrupted drill changed the source app", err)
	}
}

func TestDrillRefusesHostAccessBeforeCreatingResources(t *testing.T) {
	for _, service := range []compose.Service{
		{Privileged: true}, {PID: "host"}, {IPC: "host"}, {Devices: []string{"/dev/sda"}},
		{CapAdd: []string{"SYS_ADMIN"}}, {SecurityOpt: []string{"seccomp:unconfined"}},
		{Volumes: []compose.Mount{{Type: "bind", Source: "/var/run/docker.sock", Target: "/var/run/docker.sock"}}},
		{Volumes: []compose.Mount{{Type: "volume", Source: "live", Target: "/data"}}},
	} {
		p := &plan{Definition: &compose.Definition{Services: map[string]compose.Service{"app": service}, Volumes: map[string]compose.Volume{"live": {Name: "live-app-volume"}}}}
		if err := isolateDrill(p, uuid.New(), t.TempDir()); err == nil {
			t.Fatalf("unsafe drill accepted: %+v", service)
		}
	}
}
