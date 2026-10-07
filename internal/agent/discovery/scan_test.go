package discovery

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/google/uuid"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/mount"
	"github.com/moby/moby/client"
	"github.com/sirrobot01/synod/internal/inventory"
)

func TestScanGroupsProjectsAndKeepsDataOutOfReports(t *testing.T) {
	project := t.TempDir()
	for _, file := range []string{"compose.yaml", ".env"} {
		if err := os.WriteFile(filepath.Join(project, file), []byte("password=do-not-report"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	var broken atomic.Bool
	apiServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := strings.TrimPrefix(r.URL.Path, "/v1.56")
		if r.Method != "GET" {
			t.Errorf("discovery mutated Docker: %s", r.Method)
		}
		switch {
		case path == "/containers/json":
			if r.URL.Query().Get("all") != "1" {
				t.Error("stopped containers were excluded")
			}
			json.NewEncoder(w).Encode([]container.Summary{{ID: "web"}, {ID: "db"}, {ID: "solo"}})
		case strings.HasPrefix(path, "/containers/"):
			id := strings.TrimSuffix(strings.TrimPrefix(path, "/containers/"), "/json")
			if broken.Load() && id == "db" {
				http.Error(w, "container disappeared", 404)
				return
			}
			c := container.InspectResponse{ID: id, Name: "/" + id, Image: "sha256:test", Config: &container.Config{Image: "nginx:stable", Env: []string{"PASSWORD=do-not-report"}}}
			if id != "solo" {
				c.Config.Labels = map[string]string{"com.docker.compose.project": "notes", "com.docker.compose.project.working_dir": project, "com.docker.compose.project.config_files": "compose.yaml", "com.docker.compose.service": id}
			}
			if id == "db" {
				c.Config.Image = "registry.example:5000/team/postgres:18"
				c.Config.Env = append(c.Config.Env, "PGDATA=/var/lib/postgresql/18/docker")
				c.Mounts = []container.MountPoint{{Type: mount.TypeBind, Source: project, Destination: "/var/lib/postgresql"}}
			} else if id == "solo" {
				c.Mounts = []container.MountPoint{{Type: mount.TypeVolume, Name: "host-root", Driver: "local", Source: project, Destination: "/state"}, {Type: mount.TypeTmpfs, Destination: "/cache"}}
			}
			json.NewEncoder(w).Encode(c)
		case strings.HasPrefix(path, "/images/"):
			w.Write([]byte(`{"Id":"sha256:test","RepoDigests":["nginx@sha256:test"],"Os":"linux","Architecture":"arm64"}`))
		case path == "/volumes/host-root":
			w.Write([]byte(`{"Name":"host-root","Driver":"local","Options":{"type":"none","o":"bind","device":"/"}}`))
		default:
			t.Errorf("unexpected endpoint %s", path)
			http.NotFound(w, r)
		}
	}))
	defer apiServer.Close()
	api, err := client.New(client.WithHost(apiServer.URL), client.WithVersion("1.56"))
	if err != nil {
		t.Fatal(err)
	}
	defer api.Close()
	report, err := Scan(t.Context(), api, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := inventory.Validate(report); err != nil {
		t.Fatal(err)
	}
	if len(report.Apps) != 2 || len(report.Apps[0].Containers) != 2 || report.Apps[0].Key != "compose:notes" {
		t.Fatalf("apps: %+v", report)
	}
	statuses := map[string]int{}
	for _, app := range report.Apps {
		for _, p := range app.Paths {
			statuses[p.Status]++
		}
	}
	if statuses["Dumped"] != 1 || statuses["Never"] != 2 || statuses["Included"] != 4 {
		t.Fatalf("path statuses: %v", statuses)
	}
	for _, p := range report.Apps[0].Paths {
		if p.Status == "Dumped" && (p.Target != "/var/lib/postgresql/18/docker" || p.Source != filepath.Join(project, "18/docker")) {
			t.Fatalf("database classification includes unrelated mount data: %+v", p)
		}
	}
	encoded, err := json.Marshal(report)
	if err != nil || strings.Contains(string(encoded), "do-not-report") {
		t.Fatal("file or environment contents reached inventory")
	}
	broken.Store(true)
	if report, err := Scan(t.Context(), api, nil); err == nil || report != nil {
		t.Fatalf("partial report on Docker error: %+v %v", report, err)
	}
}

func TestScanRealDocker(t *testing.T) {
	host := os.Getenv("SYNOD_TEST_DOCKER_HOST")
	if host == "" {
		t.Skip("set SYNOD_TEST_DOCKER_HOST for the Docker discovery proof")
	}
	api, err := client.New(client.WithHost(host), client.WithAPIVersionNegotiation())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { api.Close() })
	const image = "alpine@sha256:5291449c3df73caf6ed85e649dec1b9e818b39a5d8c871e97afc13e9cd5e8fa8"
	if _, err := api.ImageInspect(t.Context(), image); err != nil {
		t.Fatalf("load the pinned fixture image %s: %v", image, err)
	}
	name := "synod-discovery-" + uuid.NewString()
	project := t.TempDir()
	if err := os.WriteFile(filepath.Join(project, "compose.yaml"), []byte("services: {}\n"), 0600); err != nil {
		t.Fatal(err)
	}
	volume, err := api.VolumeCreate(t.Context(), client.VolumeCreateOptions{Name: name})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, err := api.VolumeRemove(context.Background(), volume.Volume.Name, client.VolumeRemoveOptions{}); err != nil {
			t.Error(err)
		}
	})
	for _, service := range []string{"web", "worker", "solo"} {
		labels := map[string]string{"synod.test": name}
		if service != "solo" {
			labels["com.docker.compose.project"] = name
			labels["com.docker.compose.project.working_dir"] = project
			labels["com.docker.compose.project.config_files"] = filepath.Join(project, "compose.yaml")
			labels["com.docker.compose.service"] = service
		}
		created, err := api.ContainerCreate(t.Context(), client.ContainerCreateOptions{Name: name + "-" + service, Config: &container.Config{Image: image, Cmd: []string{"true"}, Labels: labels}, HostConfig: &container.HostConfig{NetworkMode: "none", Mounts: []mount.Mount{{Type: mount.TypeBind, Source: project, Target: "/config", ReadOnly: true}, {Type: mount.TypeVolume, Source: volume.Volume.Name, Target: "/state"}}}})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if _, err := api.ContainerRemove(context.Background(), created.ID, client.ContainerRemoveOptions{Force: true}); err != nil {
				t.Error(err)
			}
		})
	}
	report, err := Scan(t.Context(), api, make(client.Filters).Add("label", "synod.test="+name))
	if err != nil {
		t.Fatal(err)
	}
	if inventory.Validate(report) != nil || len(report.Apps) != 2 || len(report.Apps[0].Containers) != 2 {
		t.Fatalf("inventory: %+v", report)
	}
	for _, app := range report.Apps {
		for _, c := range app.Containers {
			if c.ImageId == "" || len(c.RepoDigests) == 0 || !strings.HasPrefix(c.Platform, "linux/") {
				t.Fatalf("missing image evidence: %+v", c)
			}
		}
		found := false
		for _, p := range app.Paths {
			if p.Kind == "volume" && p.Volume == name && p.Status == "Included" {
				found = true
			}
		}
		if !found {
			t.Fatalf("named volume missing: %+v", app)
		}
	}
}

func TestScanClassifiesCustomDatabaseDirectories(t *testing.T) {
	for _, database := range []struct{ image, flag string }{{"postgres", "-D"}, {"mysql", "--datadir"}, {"mariadb", "--datadir"}, {"redis", "--dir"}} {
		t.Run(database.image, func(t *testing.T) {
			parent, child := t.TempDir(), t.TempDir()
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch strings.TrimPrefix(r.URL.Path, "/v1.56") {
				case "/containers/json":
					json.NewEncoder(w).Encode([]container.Summary{{ID: "database"}})
				case "/containers/database/json":
					json.NewEncoder(w).Encode(container.InspectResponse{ID: "database", Name: "/database", Image: "sha256:test", Config: &container.Config{Image: database.image, Cmd: []string{database.flag, "/state/database"}}, Mounts: []container.MountPoint{{Type: mount.TypeBind, Source: parent, Destination: "/state"}, {Type: mount.TypeBind, Source: child, Destination: "/state/database/child"}}})
				case "/images/sha256:test/json":
					w.Write([]byte(`{"Id":"sha256:test","Os":"linux","Architecture":"arm64"}`))
				default:
					t.Errorf("unexpected request: %s", r.URL.Path)
					http.NotFound(w, r)
				}
			}))
			defer server.Close()
			api, err := client.New(client.WithHost(server.URL), client.WithVersion("1.56"))
			if err != nil {
				t.Fatal(err)
			}
			defer api.Close()
			report, err := Scan(t.Context(), api, nil)
			if err != nil {
				t.Fatal(err)
			}
			statuses := map[string]string{}
			for _, p := range report.Apps[0].Paths {
				statuses[p.Source] = p.Status
			}
			if statuses[parent] != "Included" || statuses[filepath.Join(parent, "database")] != "Dumped" || statuses[child] != "Dumped" {
				t.Fatalf("raw database path classification: %v", statuses)
			}
		})
	}
}
