package restore

import (
	"bufio"
	"context"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/kopia/kopia/snapshot"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/network"
	agentbackup "github.com/sirrobot01/synod/internal/agent/backup"
	"github.com/sirrobot01/synod/internal/backup"
	"github.com/sirrobot01/synod/internal/inventory"
	"github.com/sirrobot01/synod/internal/proof"
	"github.com/sirrobot01/synod/internal/repository"
	"github.com/sirrobot01/synod/internal/runtime/compose"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
)

func TestDrillTargetsProbeServedPortsAndNameUncheckableServices(t *testing.T) {
	web := container.InspectResponse{ID: "web", Name: "/shop-web", Config: &container.Config{ExposedPorts: network.PortSet{network.MustParsePort("80/tcp"): {}, network.MustParsePort("9000/tcp"): {}, network.MustParsePort("53/udp"): {}}, Healthcheck: &container.HealthConfig{Test: []string{"CMD", "true"}}}, HostConfig: &container.HostConfig{PortBindings: network.PortMap{network.MustParsePort("80/tcp"): nil}}}
	db := container.InspectResponse{ID: "db", Config: &container.Config{ExposedPorts: network.PortSet{network.MustParsePort("5432/tcp"): {}}, Healthcheck: &container.HealthConfig{Test: []string{"NONE"}}}, HostConfig: &container.HostConfig{}}
	edge := container.InspectResponse{ID: "edge", Config: &container.Config{}, HostConfig: &container.HostConfig{NetworkMode: "host"}}
	worker := container.InspectResponse{ID: "worker", Config: &container.Config{}, HostConfig: &container.HostConfig{}}
	p := &plan{Containers: []container.InspectResponse{web, db, edge, worker}, Services: map[string]string{"web": "web", "db": "db", "edge": "edge", "worker": "worker"}}
	p.Snapshot.App.Definition = &agentv1.DiscoveredApp{Containers: []*agentv1.DiscoveredContainer{{Id: "db", Database: "postgres"}}}
	p.Snapshot.Host = []backup.HostRequirements{{ContainerID: "edge", PortsKnown: true, Ports: []backup.HostPort{{Address: "0.0.0.0", Port: 8443, Protocol: "tcp"}, {Address: "192.168.1.5", Port: 53, Protocol: "tcp"}, {Address: "0.0.0.0", Port: 53, Protocol: "udp"}}}}
	targets := drillTargets(p)
	byService := make(map[string]drillTarget)
	for _, target := range targets {
		byService[target.Service] = target
	}
	if got := byService["web"]; !got.Healthcheck || got.Database || !slices.Equal(got.Probes, []probe{{Port: 80, Required: true}, {Port: 9000}}) {
		t.Errorf("web target: %+v", got)
	}
	if got := byService["db"]; got.Healthcheck || !got.Database || !slices.Equal(got.Probes, []probe{{Port: 5432}}) {
		t.Errorf("database target: %+v", got)
	}
	if got := byService["edge"]; got.Unprobed != "host_address_listener" || !slices.Equal(got.Probes, []probe{{Port: 8443, Required: true}}) {
		t.Errorf("host-network target: %+v", got)
	}
	if got := byService["worker"]; len(got.Probes) != 0 || got.Healthcheck {
		t.Errorf("worker target: %+v", got)
	}
}

func TestIsolatedDrillKeepsServiceNamesOnAnInternalNetwork(t *testing.T) {
	id := uuid.New()
	directory := t.TempDir()
	p := &plan{
		Containers: []container.InspectResponse{{ID: "db", Name: "/shop-db"}},
		Services:   map[string]string{"db": "db"},
		Definition: &compose.Definition{Name: "shop", Networks: map[string]compose.Network{"default": {Name: "shop_default", External: true}}, Services: map[string]compose.Service{
			"db": {Image: "postgres", NetworkMode: "", Networks: map[string]compose.Attachment{"default": {Aliases: []string{"database"}}}, Ports: []compose.Port{{Published: "5432", Target: 5432, Protocol: "tcp"}}},
		}},
	}
	if err := isolateDrill(p, id, directory); err != nil {
		t.Fatal(err)
	}
	if network := p.Definition.Networks["drill"]; len(p.Definition.Networks) != 1 || !network.Internal || network.External || network.Name != drillNetwork(id) || network.Labels[operationLabel] != id.String() {
		t.Fatalf("drill network: %+v", p.Definition.Networks)
	}
	service := p.Definition.Services["db"]
	if len(service.Ports) != 0 || service.NetworkMode != "" || len(service.Networks) != 1 || !slices.Equal(service.Networks["drill"].Aliases, []string{"database", "db", "shop-db"}) {
		t.Fatalf("drill service: %+v", service)
	}
}

func TestHTTPCheckFailsOnServerErrors(t *testing.T) {
	for _, test := range []struct {
		name, reply string
		checks      int
		status      string
	}{
		{name: "ok", reply: "HTTP/1.1 302 Found\r\nContent-Length: 0\r\n\r\n", checks: 1, status: proof.CheckPassed},
		{name: "server error", reply: "HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\n\r\n", checks: 1, status: proof.CheckFailed},
		{name: "not http", reply: "-ERR unknown command\r\n", checks: 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			client, server := net.Pipe()
			defer client.Close()
			go func() {
				defer server.Close()
				request, err := http.ReadRequest(bufio.NewReader(server))
				if err != nil || request.URL.Path != "/" {
					return
				}
				io.WriteString(server, test.reply)
			}()
			checks := checkHTTP(client)
			if len(checks) != test.checks || (test.checks == 1 && (checks[0].Kind != proof.CheckHTTP || checks[0].Status != test.status)) {
				t.Fatalf("checks: %+v", checks)
			}
		})
	}
}

func TestPortCheckRunsInTheContainerNamespace(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				if _, err := http.ReadRequest(bufio.NewReader(conn)); err == nil {
					io.WriteString(conn, "HTTP/1.1 500 Internal Server Error\r\nContent-Length: 0\r\n\r\n")
				}
			}()
		}
	}()
	port := uint16(listener.Addr().(*net.TCPAddr).Port)
	checks := checkPort(t.Context(), os.Getpid(), probe{Port: port, Required: true}, true, time.Now())
	if checks[0].Detail == "namespace_unavailable" {
		// Entering a network namespace needs Linux and root.
		if checks[0].Status != proof.CheckUnchecked || len(checks) != 1 {
			t.Fatalf("an unavailable namespace failed the check: %+v", checks)
		}
		t.Skip("this test needs a root agent on Linux")
	}
	if len(checks) != 2 || checks[0].Status != proof.CheckPassed || checks[1].Kind != proof.CheckHTTP || checks[1].Status != proof.CheckFailed {
		t.Fatalf("checks: %+v", checks)
	}
	listener.Close()
	if checks := checkPort(t.Context(), os.Getpid(), probe{Port: port}, false, time.Now()); checks[0].Status != proof.CheckUnchecked || checks[0].Detail != "exposed_port_unused" {
		t.Fatalf("an unused exposed port failed the drill: %+v", checks)
	}
	if checks := checkPort(t.Context(), os.Getpid(), probe{Port: port, Required: true}, false, time.Now()); checks[0].Status != proof.CheckFailed || checks[0].Detail != "no_answer" {
		t.Fatalf("a silent published port passed: %+v", checks)
	}
}

func TestDataCheckComparesRestoredFilesWithTheSnapshot(t *testing.T) {
	source := t.TempDir()
	for name, data := range map[string]string{"state": "private-state", "sub/config": "private-config"} {
		if err := os.MkdirAll(filepath.Join(source, filepath.Dir(name)), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(source, name), []byte(data), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	r, err := repository.Create(t.Context(), repository.Location{Kind: "local", Path: t.TempDir()}, repository.Credentials{Password: "data-check-password"})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close(context.Background())
	app := &agentv1.DiscoveredApp{Key: "compose:data", Name: "data", Kind: "compose", Paths: []*agentv1.DiscoveredPath{{Source: source, Kind: "bind", Status: "Included"}}}
	tree, err := agentbackup.NewTree(t.Context(), app, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tree.Close()
	root, err := tree.Root(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := r.UploadFiles(t.Context(), root, snapshot.SourceInfo{Host: "test", UserName: "test", Path: app.Key}, false)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	saved, err := r.PublishSnapshot(t.Context(), prepared, backup.Snapshot{ID: uuid.New(), InstallationID: uuid.New(), DestinationID: uuid.New(), App: inventory.App{ID: uuid.New(), NodeID: uuid.New(), Definition: app}, Method: backup.Live, Architecture: "arm64", StartedAt: now, CompletedAt: now, Paths: tree.Paths})
	if err != nil {
		t.Fatal(err)
	}
	directory := t.TempDir()
	restored := filepath.Join(directory, "data", "path0")
	if err := os.CopyFS(restored, os.DirFS(source)); err != nil {
		t.Fatal(err)
	}
	p := &plan{Snapshot: saved, Data: []dataRoot{{Source: source, Archive: saved.Paths[0].Archive, Relative: filepath.Join("data", "path0")}, {Source: "/srv/large", Relative: filepath.Join("data", "path1")}}}
	checks := checkData(t.Context(), nil, r, p, directory)
	if len(checks) != 1 || checks[0].Status != proof.CheckPassed || checks[0].Resource != source {
		t.Fatalf("checks: %+v", checks)
	}
	if err := os.Remove(filepath.Join(restored, "sub", "config")); err != nil {
		t.Fatal(err)
	}
	if checks := checkData(t.Context(), nil, r, p, directory); checks[0].Status != proof.CheckFailed || checks[0].Detail != "files=1/2 bytes=13/27" {
		t.Fatalf("a missing restored file passed: %+v", checks)
	}
}
