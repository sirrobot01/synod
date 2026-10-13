package restore

import (
	"encoding/json/v2"
	"errors"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/client"
	"github.com/sirrobot01/synod/internal/backup"
	"github.com/sirrobot01/synod/internal/runtime/compose"
	"github.com/sirrobot01/synod/internal/runtime/host"
)

func TestHostPreflightBeforeMutation(t *testing.T) {
	socket := filepath.Join(t.TempDir(), "docker.sock")
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	hostname, _ := os.Hostname()
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			t.Error("preflight mutated Docker:", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.HasSuffix(r.URL.Path, "/containers/json"):
			w.Write([]byte("[]"))
		case strings.HasSuffix(r.URL.Path, "/info"):
			json.MarshalWrite(w, map[string]string{"Name": hostname, "OSType": "linux"})
		default:
			t.Error("unexpected Docker read:", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	})}
	go server.Serve(listener)
	t.Cleanup(func() { server.Close() })
	api, err := client.New(client.WithHost("unix://"+socket), client.WithAPIVersion("1.53"))
	if err != nil {
		t.Fatal(err)
	}
	defer api.Close()
	for _, protocol := range []string{"tcp", "udp"} {
		t.Run(protocol, func(t *testing.T) {
			var address string
			var closeSocket func() error
			if protocol == "tcp" {
				bound, err := net.Listen("tcp4", "127.0.0.1:0")
				if err != nil {
					t.Fatal(err)
				}
				address, closeSocket = bound.Addr().String(), bound.Close
			} else {
				bound, err := net.ListenPacket("udp4", "127.0.0.1:0")
				if err != nil {
					t.Fatal(err)
				}
				address, closeSocket = bound.LocalAddr().String(), bound.Close
			}
			defer closeSocket()
			_, number, _ := net.SplitHostPort(address)
			n, _ := strconv.ParseUint(number, 10, 16)
			c := container.InspectResponse{ID: "app", HostConfig: &container.HostConfig{NetworkMode: "host"}}
			p := &plan{Containers: []container.InspectResponse{c}, Services: map[string]string{"app": "app"}, Definition: &compose.Definition{Name: "test", Services: map[string]compose.Service{"app": {NetworkMode: "host", ContainerName: "app"}}}, Snapshot: backup.Snapshot{Host: []backup.HostRequirements{{ContainerID: "app", PortsKnown: true, Ports: []backup.HostPort{{Address: "127.0.0.1", Port: uint16(n), Protocol: protocol}}}}}}
			_, _, err := findConflicts(t.Context(), api, p, false)
			if failed, ok := errors.AsType[*failure](err); !ok || failed.Code != "port_conflict" || !strings.Contains(failed.Resource, number) {
				t.Fatal("host process was not reported:", err)
			}
			closeSocket()
			if _, _, err := findConflicts(t.Context(), api, p, false); err != nil {
				t.Fatal("free host port was refused:", err)
			}
			p.Snapshot.Host[0].Ports[0].Address = "203.0.113.99"
			_, _, err = findConflicts(t.Context(), api, p, false)
			if failed, ok := errors.AsType[*failure](err); !ok || failed.Code != "host_port_unavailable" {
				t.Fatal("missing host address was not reported:", err)
			}
			p.Snapshot.Host[0].PortsKnown = false
			_, _, err = findConflicts(t.Context(), api, p, false)
			if failed, ok := errors.AsType[*failure](err); !ok || failed.Code != "host_requirements_unverified" {
				t.Fatal("unknown source ports were accepted:", err)
			}
		})
	}
	for _, kind := range []string{"device", "mount"} {
		t.Run(kind, func(t *testing.T) {
			missing := filepath.Join(t.TempDir(), "missing")
			c := container.InspectResponse{ID: "app", HostConfig: &container.HostConfig{NetworkMode: "none"}}
			s := compose.Service{ContainerName: "app", NetworkMode: "none"}
			required := backup.HostPath{Path: missing, Known: true, Mode: uint32(os.ModeSocket)}
			if kind == "device" {
				c.HostConfig.Devices = []container.DeviceMapping{{PathOnHost: missing, PathInContainer: "/dev/required", CgroupPermissions: "rwm"}}
				s.Devices = []string{missing + ":/dev/required:rwm"}
				required.Mode, required.Device = uint32(os.ModeDevice|os.ModeCharDevice), 259
			} else {
				c.Mounts = []container.MountPoint{{Type: "bind", Source: missing, Destination: "/required"}}
				s.Volumes = []compose.Mount{{Type: "bind", Source: missing, Target: "/required"}}
			}
			p := &plan{Containers: []container.InspectResponse{c}, Services: map[string]string{"app": "app"}, Definition: &compose.Definition{Name: "test", Services: map[string]compose.Service{"app": s}}, Snapshot: backup.Snapshot{Host: []backup.HostRequirements{{ContainerID: "app", Paths: []backup.HostPath{required}}}}}
			_, _, err := findConflicts(t.Context(), api, p, false)
			if failed, ok := errors.AsType[*failure](err); !ok || failed.Code != kind+"_unavailable" || failed.Resource != missing {
				t.Fatal("missing requirement reached mutation:", err)
			}
			if _, err := os.Stat(missing); !os.IsNotExist(err) {
				t.Fatal("preflight created a required path", err)
			}
		})
	}
}

func TestRequiredDeviceAndMountTypes(t *testing.T) {
	file := filepath.Join(t.TempDir(), "required")
	if err := os.WriteFile(file, []byte("unchanged"), 0600); err != nil {
		t.Fatal(err)
	}
	required := backup.HostRequirements{Paths: []backup.HostPath{host.Path(file), host.Path("/dev/null")}}
	if err := checkHostPath(nil, required, file, false); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(file, 0644); err != nil {
		t.Fatal(err)
	}
	if failed, ok := errors.AsType[*failure](checkHostPath(nil, required, file, false)); !ok || failed.Code != "mount_unavailable" {
		t.Fatal("changed permissions accepted")
	}
	if err := os.Chmod(file, 0600); err != nil {
		t.Fatal(err)
	}
	required.Paths[0].UserID++
	if failed, ok := errors.AsType[*failure](checkHostPath(nil, required, file, false)); !ok || failed.Code != "mount_unavailable" {
		t.Fatal("changed owner accepted")
	}
	required.Paths[0].UserID--
	if err := checkHostPath(nil, required, "/dev/null", true); err != nil {
		t.Fatal(err)
	}
	required.Paths[1].Device++
	if failed, ok := errors.AsType[*failure](checkHostPath(nil, required, "/dev/null", true)); !ok || failed.Code != "device_unavailable" {
		t.Fatal("different device accepted")
	}
	if failed, ok := errors.AsType[*failure](checkHostPath(nil, required, file, true)); !ok || failed.Code != "device_unavailable" {
		t.Fatal("regular file accepted as device")
	}
	if err := os.Remove(file); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(file, 0700); err != nil {
		t.Fatal(err)
	}
	if failed, ok := errors.AsType[*failure](checkHostPath(nil, required, file, false)); !ok || failed.Code != "mount_unavailable" {
		t.Fatal("changed mount type accepted")
	}
	if failed, ok := errors.AsType[*failure](checkHostPath(nil, required, file+"-unknown", false)); !ok || failed.Code != "host_requirements_unverified" {
		t.Fatal("unknown mount requirement accepted")
	}
}
