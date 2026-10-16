package restore

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"net/netip"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/mount"
	"github.com/moby/moby/api/types/network"
	"github.com/moby/moby/client"
	agentbackup "github.com/sirrobot01/synod/internal/agent/backup"
	"github.com/sirrobot01/synod/internal/agent/discovery"
	"github.com/sirrobot01/synod/internal/agent/journal"
	"github.com/sirrobot01/synod/internal/backup"
	"github.com/sirrobot01/synod/internal/destination"
	"github.com/sirrobot01/synod/internal/inventory"
	"github.com/sirrobot01/synod/internal/repository"
	"github.com/sirrobot01/synod/internal/secret"
)

type fixture struct {
	worker                        *agentbackup.Worker
	saved                         backup.Snapshot
	repo                          *repository.Repository
	container, project, directory string
	name, database, network       string
	volume                        bool
	app                           inventory.App
	destinationID                 uuid.UUID
	port, media                   string
}

const helperImage = "alpine@sha256:5291449c3df73caf6ed85e649dec1b9e818b39a5d8c871e97afc13e9cd5e8fa8"

const postgresImage = "postgres@sha256:051f7b7b3abdd564d5d1bd1e8c4b9c1b6e77087d1dd22020ede611c096a272e0"

// newFixture backs up one Compose project. With a database, the project also has a
// PostgreSQL service with a named volume, a user network, and a published port that answers HTTP.
func newFixture(t *testing.T, database bool) *fixture {
	t.Helper()
	if database {
		return buildFixture(t, database, false, helperImage, serveStatus)
	}
	return buildFixture(t, database, false, helperImage)
}

// serveStatus answers HTTP on port 80 with the status line in /config/status, or 200 OK.
// A drill can make the restored copy fail while its container keeps running.
func serveStatus(config *container.Config, _ *container.HostConfig) {
	config.Cmd = []string{"sh", "-c", `exec nc -lk -p 80 -e sh -c 'while IFS= read -r line; do [ "$line" = "$(printf "\r")" ] && break; done; status=$(cat /config/status 2>/dev/null || echo "200 OK"); printf "HTTP/1.1 %s\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok" "$status"'`}
}

// buildFixture can also mount a media directory. Discovery marks it Large, and the backup includes it.
func buildFixture(t *testing.T, database, media bool, sourceImage string, configure ...func(*container.Config, *container.HostConfig)) *fixture {
	t.Helper()
	host := os.Getenv("SYNOD_TEST_DOCKER_HOST")
	if host == "" {
		t.Skip("set SYNOD_TEST_DOCKER_HOST for app restore proof")
	}
	api, err := client.New(client.WithHost(host), client.WithAPIVersionNegotiation())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { api.Close() })
	version, err := api.ServerVersion(t.Context(), client.ServerVersionOptions{})
	if err != nil {
		t.Fatal(err)
	}
	f := &fixture{project: t.TempDir(), directory: t.TempDir()}
	if root := os.Getenv("SYNOD_TEST_SNAPSHOT_DIR"); root != "" {
		f.project, err = os.MkdirTemp(root, "synod-project-")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { os.RemoveAll(f.project) })
	}
	for name, data := range map[string]string{"compose.yaml": "services: {}\n", "state": "private-restored-value", ".env": "SECRET=private-restored-env"} {
		if err := os.WriteFile(filepath.Join(f.project, name), []byte(data), 0640); err != nil {
			t.Fatal(err)
		}
	}
	project := "synod-restore-test-" + uuid.NewString()
	f.name = project
	labels := func(service string) map[string]string {
		return map[string]string{"synod.test": project, "com.docker.compose.project": project, "com.docker.compose.service": service, "com.docker.compose.project.working_dir": f.project, "com.docker.compose.project.config_files": filepath.Join(f.project, "compose.yaml")}
	}
	t.Cleanup(func() { removeFixture(api, project) })
	hostConfig := &container.HostConfig{NetworkMode: "none", Mounts: []mount.Mount{{Type: mount.TypeBind, Source: f.project, Target: "/config"}}}
	if media {
		f.media = filepath.Join(t.TempDir(), "media")
		if err := os.MkdirAll(f.media, 0750); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(f.media, "movie"), []byte("private-included-media"), 0640); err != nil {
			t.Fatal(err)
		}
		hostConfig.Mounts = append(hostConfig.Mounts, mount.Mount{Type: mount.TypeBind, Source: f.media, Target: "/media"})
	}
	var endpoints map[string]*network.EndpointSettings
	if database {
		f.network = project + "_default"
		if _, err := api.NetworkCreate(t.Context(), f.network, client.NetworkCreateOptions{Driver: "bridge", Labels: map[string]string{"synod.test": project, "com.docker.compose.project": project, "com.docker.compose.network": "default"}}); err != nil {
			t.Fatal(err)
		}
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		_, f.port, _ = net.SplitHostPort(listener.Addr().String())
		listener.Close()
		hostConfig.NetworkMode = container.NetworkMode(f.network)
		hostConfig.PortBindings = network.PortMap{network.MustParsePort("80/tcp"): {{HostIP: netip.MustParseAddr("127.0.0.1"), HostPort: f.port}}}
		endpoints = map[string]*network.EndpointSettings{f.network: {Aliases: []string{"app"}}}
		// Only a Linux agent can read volume data from the Docker data root.
		if runtime.GOOS == "linux" {
			f.volume = true
			hostConfig.Mounts = append(hostConfig.Mounts, mount.Mount{Type: mount.TypeVolume, Source: project + "_appdata", Target: "/data"})
		}
	}
	config := &container.Config{Image: sourceImage, Cmd: []string{"sleep", "3600"}, Env: []string{"SECRET=literal-$HOME-value"}, StopTimeout: new(1), Labels: labels("app")}
	for _, configure := range configure {
		configure(config, hostConfig)
	}
	created, err := api.ContainerCreate(t.Context(), client.ContainerCreateOptions{Name: project, Config: config, HostConfig: hostConfig, NetworkingConfig: &network.NetworkingConfig{EndpointsConfig: endpoints}})
	if err != nil {
		t.Fatal(err)
	}
	f.container = created.ID
	if _, err := api.ContainerStart(t.Context(), f.container, client.ContainerStartOptions{}); err != nil {
		t.Fatal(err)
	}
	if f.volume {
		if err := execCommand(t.Context(), api, f.container, []string{"sh", "-ec", "echo private-volume-value > /data/state; chown 1234:5678 /data/state; chmod 0640 /data/state"}, nil, nil); err != nil {
			t.Fatal(err)
		}
	}
	if database {
		created, err := api.ContainerCreate(t.Context(), client.ContainerCreateOptions{Name: project + "-db", Config: &container.Config{Image: postgresImage, Env: []string{"POSTGRES_USER=app_admin", "POSTGRES_PASSWORD=private-db-password"}, StopTimeout: new(5), Labels: labels("db")}, HostConfig: &container.HostConfig{NetworkMode: container.NetworkMode(f.network), Mounts: []mount.Mount{{Type: mount.TypeVolume, Source: project + "_pgdata", Target: "/var/lib/postgresql/data"}}}, NetworkingConfig: &network.NetworkingConfig{EndpointsConfig: map[string]*network.EndpointSettings{f.network: {Aliases: []string{"db"}}}}})
		if err != nil {
			t.Fatal(err)
		}
		f.database = created.ID
		if _, err := api.ContainerStart(t.Context(), f.database, client.ContainerStartOptions{}); err != nil {
			t.Fatal(err)
		}
		query(t, api, project+"-db", "CREATE TABLE proof(id integer PRIMARY KEY, value text); INSERT INTO proof VALUES (1, 'private-db-value');")
	}
	installation, node, destID, keyID := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	ring, err := secret.NewKeyring(keyID, map[uuid.UUID][]byte{keyID: bytes.Repeat([]byte{63}, 32)})
	if err != nil {
		t.Fatal(err)
	}
	cache, err := journal.Open(t.Context(), filepath.Join(t.TempDir(), "agent.db"), installation, node)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cache.Close() })
	f.worker = &agentbackup.Worker{Docker: api, Journal: cache, Keyring: ring, InstallationID: installation, NodeID: node, NodeName: "fixture", Architecture: version.Arch}
	location := repository.Location{Kind: "local", Path: filepath.Join(t.TempDir(), "repository")}
	credentials := repository.Credentials{Password: "private-restore-password"}
	filters := make(client.Filters)
	filters.Add("label", "com.docker.compose.project="+project)
	report, err := discovery.Scan(t.Context(), api, filters)
	if err != nil || len(report.Apps) != 1 {
		t.Fatal("source scan failed", err)
	}
	f.destinationID = destID
	app := inventory.App{ID: uuid.NewSHA1(node, []byte("compose:"+project)), NodeID: node, NodeName: "fixture", Present: true, ObservedAt: report.ObservedAt.AsTime(), Definition: report.Apps[0]}
	catalog, err := repository.EncodeCatalog(repository.Catalog{Version: 1, InstallationID: installation, Generation: 1, WrittenAt: time.Now().UTC(), Devices: []repository.Device{{ID: node, Name: "fixture", Architecture: version.Arch}}, Apps: []inventory.App{app}, Destinations: []repository.CatalogDestination{{ID: destID, Name: "fixture", NodeID: node, Location: location, Credentials: credentials}}})
	if err != nil {
		t.Fatal(err)
	}
	if err := cache.SaveDestinationPlan(t.Context(), ring, destination.Plan{InstallationID: installation, Revision: 1, Catalog: catalog, Destinations: []destination.Entry{{Destination: destination.Destination{ID: destID, Name: "fixture", NodeID: node, Location: location, CreatedAt: time.Now().UTC()}, Credentials: credentials}}}); err != nil {
		t.Fatal(err)
	}
	f.app = app
	request := backup.Request{ID: uuid.New(), InstallationID: installation, AppID: app.ID, NodeID: node, SourceKey: app.Definition.Key, DestinationID: destID, Method: backup.Auto, Initialize: true, RequestedAt: time.Now().UTC()}
	if media {
		request.IncludeLarge = []string{f.media}
	}
	result, err := f.worker.Execute(t.Context(), request)
	if err != nil || result.FailureCode != "" || len(result.WarningCodes) != 0 {
		t.Fatalf("source backup: %+v %v", result, err)
	}
	f.repo, err = repository.Open(t.Context(), location, credentials)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { f.repo.Close(context.Background()) })
	f.saved, err = f.repo.Snapshot(t.Context(), result.SnapshotID)
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func TestPrepareRestoredDataPreservesOverlappingMounts(t *testing.T) {
	f := newFixture(t, false)
	id := uuid.New()
	p, err := makePlan(t.Context(), f.repo, f.saved, id, f.directory, f.worker.Architecture, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(p.Data) != 1 || p.Data[0].Relative != "project" {
		t.Fatalf("overlapping project copies were not joined: %+v", p.Data)
	}
	// The containerd image store knows a saved image only after a load.
	if err := loadImages(t.Context(), f.worker.Docker, f.repo, p, f.worker.Architecture); err != nil {
		t.Fatal(err)
	}
	helper, err := prepareData(t.Context(), f.worker.Docker, f.repo, p, id, f.directory)
	if helper != "" {
		t.Cleanup(func() {
			if _, err := f.worker.Docker.ContainerRemove(context.Background(), helper, client.ContainerRemoveOptions{Force: true, RemoveVolumes: true}); err != nil {
				t.Error(err)
			}
		})
	}
	if err != nil {
		t.Fatal(err)
	}
	if actual, err := os.ReadFile(filepath.Join(f.directory, "project", "state")); err != nil || string(actual) != "private-restored-value" {
		t.Fatal("restored bind data changed", err)
	}
	service := p.Definition.Services["app"]
	if len(service.Volumes) != 1 || service.Volumes[0].Source != filepath.Join(f.directory, "project") || service.Volumes[0].Bind.CreateHostPath {
		t.Fatal("bind path did not use the restored project")
	}
	raw, _ := json.Marshal(service.Environment)
	if !bytes.Contains(raw, []byte("literal-$HOME-value")) {
		t.Fatal("environment value changed")
	}
	other := "amd64"
	if f.worker.Architecture == "amd64" {
		other = "arm64"
	}
	f.saved.Images[0].Architectures = []string{other}
	if _, err := makePlan(t.Context(), f.repo, f.saved, uuid.New(), f.directory, f.worker.Architecture, false); err == nil {
		t.Fatal("unsupported architecture accepted")
	}
}
