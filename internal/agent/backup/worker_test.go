package backup

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/mount"
	"github.com/moby/moby/client"
	"github.com/sirrobot01/synod/internal/agent/discovery"
	"github.com/sirrobot01/synod/internal/agent/journal"
	"github.com/sirrobot01/synod/internal/backup"
	"github.com/sirrobot01/synod/internal/destination"
	"github.com/sirrobot01/synod/internal/inventory"
	"github.com/sirrobot01/synod/internal/repository"
	"github.com/sirrobot01/synod/internal/secret"
)

type workerFixture struct {
	worker                   *Worker
	request                  backup.Request
	project, id, journalPath string
	location                 repository.Location
	credentials              repository.Credentials
}

func newWorkerFixture(t *testing.T, method backup.Method, stopAction string) *workerFixture {
	t.Helper()
	host := os.Getenv("SYNOD_TEST_DOCKER_HOST")
	if host == "" {
		t.Skip("set SYNOD_TEST_DOCKER_HOST for the Docker backup proof")
	}
	api, err := client.New(client.WithHost(host), client.WithAPIVersionNegotiation())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { api.Close() })
	installation, node, destinationID := uuid.New(), uuid.New(), uuid.New()
	fixture := &workerFixture{project: t.TempDir(), journalPath: filepath.Join(t.TempDir(), "agent.db"), location: repository.Location{Kind: "local", Path: filepath.Join(t.TempDir(), "repository")}, credentials: repository.Credentials{Password: "private-worker-password"}}
	keyID := uuid.New()
	key := make([]byte, 32)
	rand.Read(key)
	ring, err := secret.NewKeyring(keyID, map[uuid.UUID][]byte{keyID: key})
	if err != nil {
		t.Fatal(err)
	}
	cache, err := journal.Open(t.Context(), fixture.journalPath, installation, node)
	if err != nil {
		t.Fatal(err)
	}
	fixture.worker = &Worker{Docker: api, Journal: cache, Keyring: ring, InstallationID: installation, NodeID: node, NodeName: "fixture", Architecture: "arm64"}
	t.Cleanup(func() { fixture.worker.Journal.Close() })
	if err := cache.SaveDestinationPlan(t.Context(), ring, destination.Plan{InstallationID: installation, Revision: 1, Destinations: []destination.Entry{{Destination: destination.Destination{ID: destinationID, Name: "test", NodeID: node, Location: fixture.location, CreatedAt: time.Now().UTC()}, Credentials: fixture.credentials}}}); err != nil {
		t.Fatal(err)
	}
	for name, content := range map[string]string{"compose.yaml": "services: {}\n", "state": "before-stop", ".env": "PRIVATE=private-worker-data"} {
		if err := os.WriteFile(filepath.Join(fixture.project, name), []byte(content), 0600); err != nil {
			t.Fatal(err)
		}
	}
	project := "synod-backup-" + uuid.NewString()
	const image = "alpine@sha256:5291449c3df73caf6ed85e649dec1b9e818b39a5d8c871e97afc13e9cd5e8fa8"
	created, err := api.ContainerCreate(t.Context(), client.ContainerCreateOptions{Name: project, Config: &container.Config{Image: image, Cmd: []string{"sh", "-c", "trap 'printf after-stop > /config/state; " + stopAction + "; exit 0' TERM; touch /config/ready; while :; do sleep 0.1; done"}, Env: []string{"PRIVATE=private-worker-data"}, StopTimeout: new(2), Labels: map[string]string{"synod.test": project, "com.docker.compose.project": project, "com.docker.compose.project.working_dir": fixture.project, "com.docker.compose.project.config_files": filepath.Join(fixture.project, "compose.yaml"), "com.docker.compose.service": "app"}}, HostConfig: &container.HostConfig{NetworkMode: "none", Mounts: []mount.Mount{{Type: mount.TypeBind, Source: fixture.project, Target: "/config"}}}})
	if err != nil {
		t.Fatal(err)
	}
	fixture.id = created.ID
	t.Cleanup(func() {
		if _, err := api.ContainerRemove(context.Background(), fixture.id, client.ContainerRemoveOptions{Force: true}); err != nil {
			t.Error(err)
		}
	})
	if _, err := api.ContainerStart(t.Context(), fixture.id, client.ContainerStartOptions{}); err != nil {
		t.Fatal(err)
	}
	for deadline := time.Now().Add(10 * time.Second); ; {
		if _, err := os.Stat(filepath.Join(fixture.project, "ready")); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("fixture did not become ready")
		}
		time.Sleep(20 * time.Millisecond)
	}
	keyName := "compose:" + project
	fixture.request = backup.Request{ID: uuid.New(), InstallationID: installation, NodeID: node, AppID: uuid.NewSHA1(node, []byte(keyName)), SourceKey: keyName, DestinationID: destinationID, Method: method, Initialize: true, RequestedAt: time.Now().UTC()}
	filters := make(client.Filters)
	filters.Add("label", "com.docker.compose.project="+project)
	report, err := discovery.Scan(t.Context(), api, filters)
	if err != nil || len(report.Apps) != 1 {
		t.Fatalf("catalog fixture scan: %v", err)
	}
	plan, err := cache.DestinationPlan(t.Context(), ring)
	if err != nil {
		t.Fatal(err)
	}
	plan.Revision++
	settings := backup.DefaultSettings()
	settings.Destinations = []uuid.UUID{destinationID}
	settings.Method = method
	catalog := repository.Catalog{Version: 1, InstallationID: installation, Generation: uint64(plan.Revision), WrittenAt: time.Now().UTC(), Devices: []repository.Device{{ID: node, Name: "fixture", Architecture: "arm64"}}, Apps: []inventory.App{{ID: fixture.request.AppID, NodeID: node, NodeName: "fixture", ObservedAt: report.ObservedAt.AsTime(), Present: true, Definition: report.Apps[0]}}, Defaults: &settings, Protection: []backup.Selection{{AppID: fixture.request.AppID, Enabled: true, Settings: settings}}, Destinations: []repository.CatalogDestination{{ID: destinationID, Name: "test", NodeID: node, Location: fixture.location, Credentials: fixture.credentials}}}
	plan.Catalog, err = repository.EncodeCatalog(catalog)
	if err != nil {
		t.Fatal(err)
	}
	if err := cache.SaveDestinationPlan(t.Context(), ring, plan); err != nil {
		t.Fatal(err)
	}
	fixture.request.Retention = new(settings.Retention)
	return fixture
}

func TestWorkerCopiesAndRestoresRealDockerApp(t *testing.T) {
	for _, method := range []backup.Method{backup.Auto, backup.Stop, backup.Live} {
		t.Run(string(method), func(t *testing.T) {
			fixture := newWorkerFixture(t, method, ":")
			before, err := fixture.worker.Docker.ContainerInspect(t.Context(), fixture.id, client.ContainerInspectOptions{})
			if err != nil {
				t.Fatal(err)
			}
			result, err := fixture.worker.Execute(t.Context(), fixture.request)
			if err != nil || result.FailureCode != "" || len(result.WarningCodes) != 0 || result.SnapshotID != fixture.request.ID {
				t.Fatalf("backup: %+v %v", result, err)
			}
			after, err := fixture.worker.Docker.ContainerInspect(t.Context(), fixture.id, client.ContainerInspectOptions{})
			if err != nil || !after.Container.State.Running {
				t.Fatalf("app was not resumed: %v", err)
			}
			if method != backup.Live && (result.DowntimeMillis <= 0 || after.Container.State.StartedAt == before.Container.State.StartedAt) {
				t.Fatal("Stop method did not stop and resume the app")
			}
			if method == backup.Live && (result.DowntimeMillis != 0 || after.Container.State.StartedAt != before.Container.State.StartedAt) {
				t.Fatal("Live method stopped the app")
			}
			replay, err := fixture.worker.Execute(t.Context(), fixture.request)
			if err != nil || !reflect.DeepEqual(result, replay) {
				t.Fatalf("job replay changed the result: %v", err)
			}
			r, err := repository.Open(t.Context(), fixture.location, fixture.credentials)
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close(context.Background())
			recovered, err := r.ReadCatalog(t.Context())
			if err != nil || len(recovered.Protection) != 1 || recovered.Protection[0].Settings.Retention.Monthly != 6 || recovered.Destinations[0].Credentials.Password != fixture.credentials.Password {
				t.Fatalf("backup did not save the recovery settings: %v", err)
			}
			saved, err := r.Snapshot(t.Context(), result.SnapshotID)
			if err != nil {
				t.Fatal(err)
			}
			if len(saved.Images) != 1 || saved.Images[0].ContainerID != fixture.id || !slices.Contains(saved.Images[0].Architectures, fixture.worker.Architecture) || saved.Images[0].Reference == "" {
				t.Fatal("snapshot lost running image evidence")
			}
			if saved.Method == backup.Auto || (method == backup.Live && saved.Method != backup.Live) {
				t.Fatal("snapshot did not record the resolved copy method")
			}
			target := filepath.Join(t.TempDir(), "restored")
			if err := r.RestoreFiles(t.Context(), result.SnapshotID, target); err != nil {
				t.Fatal(err)
			}
			expected := "before-stop"
			if method != backup.Live {
				expected = "after-stop"
			}
			found := false
			for _, path := range saved.Paths {
				if path.Source == fixture.project && path.Kind == "project" {
					found = true
					data, err := os.ReadFile(filepath.Join(target, path.Archive, "state"))
					if err != nil || string(data) != expected {
						t.Fatalf("restored state differs: %v", err)
					}
				}
			}
			if !found {
				t.Fatal("project path is absent from the manifest")
			}
			data, err := os.ReadFile(filepath.Join(target, "definition", "inspect.json"))
			if err != nil || !bytes.Contains(data, []byte("private-worker-data")) {
				t.Fatal("encrypted snapshot lost its app definition")
			}
			public, _ := json.Marshal(result)
			if bytes.Contains(public, []byte("private-worker")) {
				t.Fatal("result contains private data")
			}
		})
	}
}

func TestWorkerResumesAfterFinalCopyFailure(t *testing.T) {
	fixture := newWorkerFixture(t, backup.Stop, "rm /config/compose.yaml")
	result, err := fixture.worker.Execute(t.Context(), fixture.request)
	if err != nil || result.FailureCode != "final_copy_failed" || result.SnapshotID != uuid.Nil {
		t.Fatalf("final failure: %+v %v", result, err)
	}
	response, err := fixture.worker.Docker.ContainerInspect(t.Context(), fixture.id, client.ContainerInspectOptions{})
	if err != nil || !response.Container.State.Running {
		t.Fatalf("failed backup left app stopped: %v", err)
	}
	r, err := repository.Open(t.Context(), fixture.location, fixture.credentials)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close(context.Background())
	if _, err := r.Snapshot(t.Context(), fixture.request.ID); !errors.Is(err, repository.ErrSnapshotMissing) {
		t.Fatalf("failed copy published a snapshot: %v", err)
	}
}

func TestWorkerKeepsGoodCopyWhenCatalogCannotBeReplaced(t *testing.T) {
	for _, newer := range []bool{false, true} {
		t.Run(map[bool]string{false: "conflict", true: "newer"}[newer], func(t *testing.T) {
			fixture := newWorkerFixture(t, backup.Live, ":")
			plan, err := fixture.worker.Journal.DestinationPlan(t.Context(), fixture.worker.Keyring)
			if err != nil {
				t.Fatal(err)
			}
			catalog, err := repository.DecodeCatalog(plan.Catalog)
			if err != nil {
				t.Fatal(err)
			}
			catalog.WrittenAt = catalog.WrittenAt.Add(time.Second)
			if newer {
				catalog.Generation++
			}
			r, err := repository.Create(t.Context(), fixture.location, fixture.credentials)
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close(context.Background())
			if err := r.WriteCatalog(t.Context(), catalog); err != nil {
				t.Fatal(err)
			}
			result, err := fixture.worker.Execute(t.Context(), fixture.request)
			if err != nil || result.FailureCode != "" || result.SnapshotID != fixture.request.ID {
				t.Fatalf("catalog issue lost a good snapshot: %+v %v", result, err)
			}
			if newer && len(result.WarningCodes) != 0 || !newer && !slices.Equal(result.WarningCodes, []string{"catalog_write_failed"}) {
				t.Fatalf("incorrect catalog warning: %v", result.WarningCodes)
			}
			saved, err := r.ReadCatalog(t.Context())
			if err != nil || saved.Generation != catalog.Generation || !saved.WrittenAt.Equal(catalog.WrittenAt) {
				t.Fatal("agent replaced an incompatible or newer catalog")
			}
			metadata, err := r.Snapshot(t.Context(), result.SnapshotID)
			if err != nil {
				t.Fatal(err)
			}
			target := filepath.Join(t.TempDir(), "restore")
			if err := r.RestoreFiles(t.Context(), result.SnapshotID, target); err != nil {
				t.Fatal(err)
			}
			found := false
			for _, path := range metadata.Paths {
				if path.Kind == "project" && path.Source == fixture.project {
					data, err := os.ReadFile(filepath.Join(target, path.Archive, "state"))
					if err != nil || string(data) != "before-stop" {
						t.Fatalf("good copy cannot be restored: %v", err)
					}
					found = true
				}
			}
			if !found {
				t.Fatal("backup lost the project files")
			}
		})
	}
}

func TestWorkerRecoversStopAfterJournalRestart(t *testing.T) {
	fixture := newWorkerFixture(t, backup.Stop, ":")
	input, _ := json.Marshal(fixture.request)
	if _, err := fixture.worker.Journal.Admit(t.Context(), fixture.request.ID, input); err != nil {
		t.Fatal(err)
	}
	if err := fixture.worker.Journal.Start(t.Context(), fixture.request.ID); err != nil {
		t.Fatal(err)
	}
	state := progress{Version: 1, RequestID: fixture.request.ID, StartedAt: time.Now().Add(-time.Second).UTC(), StoppingAt: time.Now().UTC(), Restart: []restartIntent{{ID: fixture.id, Timeout: 2}}}
	if err := fixture.worker.save(t.Context(), &state); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.worker.Docker.ContainerStop(t.Context(), fixture.id, client.ContainerStopOptions{Timeout: new(2)}); err != nil {
		t.Fatal(err)
	}
	// The process lost the stop response and never committed confirmation.
	if err := fixture.worker.Journal.Close(); err != nil {
		t.Fatal(err)
	}
	cache, err := journal.Open(t.Context(), fixture.journalPath, fixture.worker.InstallationID, fixture.worker.NodeID)
	if err != nil {
		t.Fatal(err)
	}
	fixture.worker.Journal = cache
	result, err := fixture.worker.Execute(t.Context(), fixture.request)
	if err != nil || result.FailureCode != "agent_interrupted" || result.DowntimeMillis <= 0 {
		t.Fatalf("recovery: %+v %v", result, err)
	}
	response, err := fixture.worker.Docker.ContainerInspect(t.Context(), fixture.id, client.ContainerInspectOptions{})
	if err != nil || !response.Container.State.Running {
		t.Fatalf("recovery left app stopped: %v", err)
	}
	records, err := cache.Records(t.Context())
	if err != nil || len(records) != 1 || records[0].State != journal.Terminal || len(records[0].Progress) != 0 {
		t.Fatalf("recovery did not commit its result: %v", err)
	}
}
