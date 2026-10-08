package restore

import (
	"bytes"
	"context"
	"io"
	"net"
	"net/http"
	"net/netip"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/network"
	"github.com/moby/moby/client"
	agentbackup "github.com/sirrobot01/synod/internal/agent/backup"
	"github.com/sirrobot01/synod/internal/agent/journal"
	"github.com/sirrobot01/synod/internal/destination"
	"github.com/sirrobot01/synod/internal/repository"
	"github.com/sirrobot01/synod/internal/secret"
)

func TestRestoreHostNetworkAndPublishedPorts(t *testing.T) {
	if os.Getenv("SYNOD_TEST_DOCKER_HOST") == "" || runtime.GOOS != "linux" {
		t.Skip("set SYNOD_TEST_DOCKER_HOST on Linux for host requirement recovery")
	}
	for _, mode := range []string{"host", "bridge"} {
		t.Run(mode, func(t *testing.T) {
			listener, err := net.Listen("tcp4", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			address := listener.Addr().String()
			_, port, _ := net.SplitHostPort(address)
			listener.Close()
			f := buildFixture(t, false, false, helperImage, func(config *container.Config, host *container.HostConfig) {
				host.NetworkMode = container.NetworkMode(mode)
				config.User = strconv.Itoa(os.Getuid())
				bindPort, bindIP := port, "127.0.0.1"
				if mode == "bridge" {
					bindPort, bindIP = "8080", "0.0.0.0"
					host.PortBindings = network.PortMap{network.MustParsePort("8080/tcp"): {{HostIP: netip.MustParseAddr("127.0.0.1"), HostPort: port}}}
				}
				host.Devices = []container.DeviceMapping{{PathOnHost: "/dev/null", PathInContainer: "/dev/synod-null", CgroupPermissions: "rwm"}}
				config.Cmd = []string{"nc", "-lk", "-s", bindIP, "-p", bindPort, "-e", "sh", "-ec", `test -c /dev/synod-null; printf 'HTTP/1.0 200 OK\r\nContent-Length: 22\r\nConnection: close\r\n\r\n'; cat /config/state`}
			})
			if len(f.saved.Host) != 1 || (mode == "host" && (!f.saved.Host[0].PortsKnown || len(f.saved.Host[0].Ports) != 1)) || len(f.saved.Host[0].Paths) != 1 || !f.saved.Host[0].Paths[0].Known {
				t.Fatalf("host requirements not saved: %+v", f.saved.Host)
			}
			api := f.worker.Docker
			w := &Worker{Backup: f.worker, Directory: t.TempDir()}
			removeFixture(api, f.name)
			listener, err = net.Listen("tcp4", address)
			if err != nil {
				t.Fatal(err)
			}
			request := newRequest(f, false)
			result, err := w.Execute(t.Context(), request)
			listener.Close()
			if err != nil || result.FailureCode != "port_conflict" {
				t.Fatalf("occupied host port: %+v %v", result, err)
			}
			if _, err := os.Stat(filepath.Join(w.Directory, request.ID.String())); !os.IsNotExist(err) {
				t.Fatal("refused restore left a directory", err)
			}
			if err := os.RemoveAll(f.project); err != nil {
				t.Fatal(err)
			}
			for _, replace := range []bool{false, true} {
				request := newRequest(f, replace)
				t.Cleanup(func() { removeLabelled(context.Background(), api, request.ID, true) })
				result, err := w.Execute(t.Context(), request)
				if err != nil || result.FailureCode != "" {
					t.Fatalf("restore replace=%t: %+v %v", replace, result, err)
				}
				response, err := (&http.Client{Timeout: 5 * time.Second}).Get("http://" + address + "/state")
				if err != nil {
					t.Fatal(err)
				}
				data, err := io.ReadAll(response.Body)
				response.Body.Close()
				if err != nil || response.StatusCode != http.StatusOK || string(data) != "private-restored-value" {
					t.Fatal("restored HTTP app has wrong data", err)
				}
				actual, err := api.ContainerInspect(t.Context(), f.name, client.ContainerInspectOptions{})
				if err != nil || string(actual.Container.HostConfig.NetworkMode) != mode || len(actual.Container.HostConfig.Devices) != 1 {
					t.Fatal("host requirements changed", err)
				}
			}
		})
	}
}

// TestExportHostNetworkRecovery saves only a repository for the import test on another host.
func TestExportHostNetworkRecovery(t *testing.T) {
	output := os.Getenv("SYNOD_TEST_HOST_EXPORT")
	if output == "" || os.Getenv("SYNOD_TEST_DOCKER_HOST") == "" || runtime.GOOS != "linux" {
		t.Skip("set SYNOD_TEST_HOST_EXPORT and SYNOD_TEST_DOCKER_HOST on the source Linux host")
	}
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	_, port, _ := net.SplitHostPort(listener.Addr().String())
	listener.Close()
	f := buildFixture(t, false, false, helperImage, func(config *container.Config, host *container.HostConfig) {
		host.NetworkMode = "host"
		host.Devices = []container.DeviceMapping{{PathOnHost: "/dev/null", PathInContainer: "/dev/synod-null", CgroupPermissions: "rwm"}}
		config.User = strconv.Itoa(os.Getuid())
		config.Cmd = []string{"nc", "-lk", "-s", "127.0.0.1", "-p", port, "-e", "sh", "-ec", `test -c /dev/synod-null; printf 'HTTP/1.0 200 OK\r\nContent-Length: 22\r\nConnection: close\r\n\r\n'; cat /config/state`}
	})
	if len(f.saved.Host) != 1 || !f.saved.Host[0].PortsKnown || len(f.saved.Host[0].Ports) != 1 || len(f.saved.Images[0].Variants) != 2 {
		t.Fatalf("cross-device requirements not saved: %+v", f.saved.Host)
	}
	plan, err := f.worker.Journal.DestinationPlan(t.Context(), f.worker.Keyring)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(output); !os.IsNotExist(err) {
		t.Fatal("export needs a new directory")
	}
	if err := os.CopyFS(output, os.DirFS(plan.Destinations[0].Destination.Location.Path)); err != nil {
		t.Fatal(err)
	}
	removeFixture(f.worker.Docker, f.name)
	if err := os.RemoveAll(f.project); err != nil {
		t.Fatal(err)
	}
	t.Log("Saved the repository. Removed the source app and its files.")
}

func TestImportHostNetworkRecovery(t *testing.T) {
	input := os.Getenv("SYNOD_TEST_HOST_IMPORT")
	dockerHost := os.Getenv("SYNOD_TEST_DOCKER_HOST")
	if input == "" || dockerHost == "" || runtime.GOOS != "linux" {
		t.Skip("set SYNOD_TEST_HOST_IMPORT and SYNOD_TEST_DOCKER_HOST on the target Linux host")
	}
	location := repository.Location{Kind: "local", Path: input}
	credentials := repository.Credentials{Password: "private-restore-password"}
	r, err := repository.Open(t.Context(), location, credentials)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close(context.Background())
	catalog, err := r.ReadCatalog(t.Context())
	if err != nil || len(catalog.Apps) != 1 {
		t.Fatal("read source catalog", err)
	}
	saved, err := r.Snapshots(t.Context(), catalog.InstallationID, catalog.Apps[0].ID)
	if err != nil || len(saved) != 1 || len(saved[0].Host) != 1 || len(saved[0].Host[0].Ports) != 1 {
		t.Fatal("read saved host app", err)
	}
	api, err := client.New(client.WithHost(dockerHost), client.WithAPIVersionNegotiation())
	if err != nil {
		t.Fatal(err)
	}
	defer api.Close()
	version, err := api.ServerVersion(t.Context(), client.ServerVersionOptions{})
	if err != nil || version.Arch == saved[0].Architecture {
		t.Fatal("import must run on another architecture", err)
	}
	node, keyID := uuid.New(), uuid.New()
	ring, err := secret.NewKeyring(keyID, map[uuid.UUID][]byte{keyID: bytes.Repeat([]byte{29}, 32)})
	if err != nil {
		t.Fatal(err)
	}
	cache, err := journal.Open(t.Context(), filepath.Join(t.TempDir(), "agent.db"), catalog.InstallationID, node)
	if err != nil {
		t.Fatal(err)
	}
	defer cache.Close()
	b := &agentbackup.Worker{Docker: api, Journal: cache, Keyring: ring, InstallationID: catalog.InstallationID, NodeID: node, NodeName: "target", Architecture: version.Arch}
	entry := destination.Entry{Destination: destination.Destination{ID: saved[0].DestinationID, Name: "transferred", NodeID: node, Location: location, CreatedAt: time.Now().UTC()}, Credentials: credentials}
	if err := cache.SaveDestinationPlan(t.Context(), ring, destination.Plan{InstallationID: catalog.InstallationID, Revision: 1, Destinations: []destination.Entry{entry}}); err != nil {
		t.Fatal(err)
	}
	f := &fixture{worker: b, app: catalog.Apps[0]}
	request := newRequest(f, false)
	t.Cleanup(func() { removeLabelled(context.Background(), api, request.ID, true) })
	w := &Worker{Backup: b, Directory: t.TempDir()}
	result, err := w.Execute(t.Context(), request)
	if err != nil || result.FailureCode != "" {
		t.Fatalf("cross-device restore: %+v %v", result, err)
	}
	port := saved[0].Host[0].Ports[0]
	response, err := (&http.Client{Timeout: 5 * time.Second}).Get("http://" + net.JoinHostPort(port.Address, strconv.Itoa(int(port.Port))))
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(response.Body)
	response.Body.Close()
	if err != nil || response.StatusCode != http.StatusOK || string(data) != "private-restored-value" {
		t.Fatal("cross-device HTTP data changed", err)
	}
	t.Logf("Restored host networking, the device mapping, and HTTP data from %s on %s.", saved[0].Architecture, version.Arch)
}
