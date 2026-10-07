package restore

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/network"
	"github.com/moby/moby/client"
	"github.com/sirrobot01/synod/internal/backup"
	"github.com/sirrobot01/synod/internal/restore"
)

// removeFixture removes every disposable resource of one test project.
func removeFixture(api *client.Client, project string) {
	ctx := context.Background()
	filters := make(client.Filters)
	filters.Add("label", "synod.test="+project)
	if list, err := api.ContainerList(ctx, client.ContainerListOptions{All: true, Filters: filters}); err == nil {
		for _, c := range list.Items {
			api.ContainerRemove(ctx, c.ID, client.ContainerRemoveOptions{Force: true, RemoveVolumes: true})
		}
	}
	api.VolumeRemove(ctx, project+"_pgdata", client.VolumeRemoveOptions{Force: true})
	api.VolumeRemove(ctx, project+"_appdata", client.VolumeRemoveOptions{Force: true})
	api.NetworkRemove(ctx, project+"_default", client.NetworkRemoveOptions{})
}

// query waits for the final PostgreSQL server, then runs SQL as the original administrator.
func query(t *testing.T, api *client.Client, name, sql string) string {
	t.Helper()
	var output bytes.Buffer
	var err error
	for deadline := time.Now().Add(2 * time.Minute); time.Now().Before(deadline); time.Sleep(250 * time.Millisecond) {
		output.Reset()
		err = execCommand(t.Context(), api, name, []string{"sh", "-ec", `[ "$(cat /proc/1/comm)" = postgres ]`}, nil, nil)
		if err == nil {
			err = execCommand(t.Context(), api, name, []string{"psql", "-v", "ON_ERROR_STOP=1", "-U", "app_admin", "-d", "postgres", "-At", "-c", sql}, nil, &output)
		}
		if err == nil {
			return strings.TrimSpace(output.String())
		}
	}
	t.Fatalf("query %s: %v", name, err)
	return ""
}

func newRequest(f *fixture, replace bool) restore.Request {
	return restore.Request{Submission: restore.Submission{ID: uuid.New(), AppID: f.app.ID, NodeID: f.worker.NodeID, Replace: replace}, Kind: "restore", InstallationID: f.worker.InstallationID, RequestedAt: time.Now().UTC()}
}

func TestRestoreRebuildsLostAppAndReplacesOnRequest(t *testing.T) {
	f := newFixture(t, true)
	api := f.worker.Docker
	w := &Worker{Backup: f.worker, Directory: t.TempDir()}
	run := func(request restore.Request) restore.Result {
		t.Helper()
		t.Cleanup(func() { removeLabelled(context.Background(), api, request.ID, true) })
		result, err := w.Execute(t.Context(), request)
		if err != nil {
			t.Fatal(err)
		}
		if (restore.Event{ID: request.ID, Status: map[bool]string{true: "succeeded", false: "failed"}[result.FailureCode == ""], Result: result}).Validate(request) != nil {
			t.Fatalf("invalid result: %+v", result)
		}
		return result
	}
	check := func(id uuid.UUID) {
		t.Helper()
		response, err := api.ContainerInspect(t.Context(), f.name, client.ContainerInspectOptions{})
		if err != nil || !response.Container.State.Running || response.Container.Config.Labels[operationLabel] != id.String() {
			t.Fatal("restored app is not running", err)
		}
		if bindings := response.Container.NetworkSettings.Ports; len(bindings) != 1 || bindings[network.MustParsePort("80/tcp")][0].HostPort != f.port {
			t.Fatalf("published port changed: %v", bindings)
		}
		var output bytes.Buffer
		if err := execCommand(t.Context(), api, f.name, []string{"cat", "/config/state"}, nil, &output); err != nil || output.String() != "private-restored-value" {
			t.Fatal("restored file changed", err)
		}
		if f.volume {
			output.Reset()
			if err := execCommand(t.Context(), api, f.name, []string{"stat", "-c", "%u:%g:%a:", "/data/state"}, nil, &output); err != nil || output.String() != "1234:5678:640:\n" {
				t.Fatalf("restored volume owner = %q: %v", output.String(), err)
			}
			output.Reset()
			if err := execCommand(t.Context(), api, f.name, []string{"cat", "/data/state"}, nil, &output); err != nil || output.String() != "private-volume-value\n" {
				t.Fatal("restored volume file changed", err)
			}
		}
		if value := query(t, api, f.name+"-db", "SELECT value FROM proof WHERE id=1"); value != "private-db-value" {
			t.Fatalf("restored row = %q", value)
		}
	}

	// A lost device leaves no container, volume, or network.
	removeFixture(api, f.name)
	first := newRequest(f, false)
	result := run(first)
	if result.FailureCode != "" || result.SnapshotID != f.saved.ID || result.DestinationID != f.destinationID || result.TargetAppID != uuid.NewSHA1(f.worker.NodeID, []byte("compose:"+f.name)) || result.ProjectDirectory != filepath.Join(w.Directory, first.ID.String()) {
		t.Fatalf("restore result: %+v", result)
	}
	check(first.ID)
	if _, err := api.NetworkInspect(t.Context(), f.network, client.NetworkInspectOptions{}); err != nil {
		t.Fatal("saved network was not created", err)
	}
	if again, err := w.Execute(t.Context(), first); err != nil || again.CompletedAt != result.CompletedAt {
		t.Fatal("repeated request ran again", err)
	}

	conflict := newRequest(f, false)
	if result := run(conflict); result.FailureCode != "app_conflict" || result.Resource == "" {
		t.Fatalf("conflict result: %+v", result)
	}
	if _, err := os.Stat(filepath.Join(w.Directory, conflict.ID.String())); !os.IsNotExist(err) {
		t.Fatal("refused restore left its directory", err)
	}
	filters := make(client.Filters)
	filters.Add("label", operationLabel+"="+conflict.ID.String())
	if volumes, err := api.VolumeList(t.Context(), client.VolumeListOptions{Filters: filters}); err != nil || len(volumes.Items) != 0 {
		t.Fatal("refused restore left volumes", err)
	}
	check(first.ID)

	replace := newRequest(f, true)
	result = run(replace)
	if result.FailureCode != "" || result.SafetySnapshotID != backup.SafetyID(replace.ID) {
		t.Fatalf("replace result: %+v", result)
	}
	if saved, err := f.repo.Snapshot(t.Context(), result.SafetySnapshotID); err != nil || saved.App.ID != f.app.ID {
		t.Fatal("safety snapshot is missing", err)
	}
	check(replace.ID)
	filters = make(client.Filters)
	filters.Add("label", operationLabel+"="+first.ID.String())
	if list, err := api.ContainerList(t.Context(), client.ContainerListOptions{All: true, Filters: filters}); err != nil || len(list.Items) != 0 {
		t.Fatal("replaced containers remain", err)
	}
	records, err := f.worker.Journal.Records(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	for _, record := range records {
		if record.ID == result.SafetySnapshotID {
			t.Fatal("safety record was not released")
		}
	}
}

func TestRestoreRecoversReplacedAppAfterInterruption(t *testing.T) {
	f := newFixture(t, false)
	api := f.worker.Docker
	w := &Worker{Backup: f.worker, Directory: t.TempDir()}
	request := newRequest(f, true)
	response, err := api.ContainerInspect(t.Context(), f.container, client.ContainerInspectOptions{})
	if err != nil {
		t.Fatal(err)
	}
	input, _ := json.Marshal(request)
	state, _ := json.Marshal(progress{Version: 1, RequestID: request.ID, StartedAt: time.Now().UTC(), Phase: phaseStarting, SnapshotID: f.saved.ID, DestinationID: f.destinationID, Project: f.name, ReplacedKey: "compose:" + f.name, Replaced: []container.InspectResponse{response.Container}})
	if _, err := f.worker.Journal.Admit(t.Context(), request.ID, input); err != nil {
		t.Fatal(err)
	}
	if err := f.worker.Journal.Start(t.Context(), request.ID); err != nil {
		t.Fatal(err)
	}
	if err := f.worker.Journal.SaveProgress(t.Context(), request.ID, state); err != nil {
		t.Fatal(err)
	}
	// The agent stopped after it removed the existing app and before Compose started the restore.
	if _, err := api.ContainerRemove(t.Context(), f.container, client.ContainerRemoveOptions{Force: true}); err != nil {
		t.Fatal(err)
	}
	result, err := w.Execute(t.Context(), request)
	if err != nil || result.FailureCode != "agent_interrupted" || result.TargetAppID != uuid.Nil {
		t.Fatalf("recovery result: %+v %v", result, err)
	}
	recovered, err := api.ContainerInspect(t.Context(), f.name, client.ContainerInspectOptions{})
	if err != nil || !recovered.Container.State.Running || recovered.Container.Config.Labels["com.docker.compose.project"] != f.name || recovered.Container.Image != response.Container.Image {
		t.Fatal("replaced app was not recovered", err)
	}
	var output bytes.Buffer
	if err := execCommand(t.Context(), api, f.name, []string{"cat", "/config/state"}, nil, &output); err != nil || output.String() != "private-restored-value" {
		t.Fatal("recovered app lost its bind data", err)
	}
}
