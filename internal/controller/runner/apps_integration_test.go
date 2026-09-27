package runner

import (
	"bufio"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	goruntime "runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/moby/moby/client"
	agentcapacity "github.com/sirrobot01/synod/internal/agent/capacity"
	"github.com/sirrobot01/synod/internal/agent/executor"
	"github.com/sirrobot01/synod/internal/agent/journal"
	"github.com/sirrobot01/synod/internal/agent/transport"
	"github.com/sirrobot01/synod/internal/app"
	"github.com/sirrobot01/synod/internal/controller/capacity"
	"github.com/sirrobot01/synod/internal/controller/provision"
	"github.com/sirrobot01/synod/internal/controlstream"
	"github.com/sirrobot01/synod/internal/engine"
	postgresengine "github.com/sirrobot01/synod/internal/engine/postgres"
	"github.com/sirrobot01/synod/internal/operation"
	"github.com/sirrobot01/synod/internal/placement"
	"github.com/sirrobot01/synod/internal/router"
	"github.com/sirrobot01/synod/internal/runtime/docker"
	"github.com/sirrobot01/synod/internal/store/postgres"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/proto"
)

func TestAppAdmissionRunsThroughRealAgentAndRouter(t *testing.T) {
	host := os.Getenv("SYNOD_TEST_DOCKER_HOST")
	if host == "" {
		t.Skip("set SYNOD_TEST_DOCKER_HOST")
	}
	inst := newInstallation(t)
	inst.runner.config.Logger = slog.Default()
	generation := uuid.New()
	if err := inst.store.InitializeGeneration(t.Context(), inst.id, generation); err != nil {
		t.Fatal(err)
	}
	api, err := client.New(client.WithHost(host))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		defer api.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		list, err := api.ContainerList(ctx, client.ContainerListOptions{All: true, Filters: client.Filters{}.Add("label", "io.synod.installation="+inst.id.String())})
		if err != nil {
			t.Error(err)
			return
		}
		for _, item := range list.Items {
			if _, err := api.ContainerRemove(ctx, item.ID, client.ContainerRemoveOptions{Force: true}); err != nil {
				t.Error(err)
			}
		}
	})
	native, err := docker.New(api, docker.Options{InstallationID: inst.id, NodeID: inst.node})
	if err != nil {
		t.Fatal(err)
	}
	j, err := journal.Open(filepath.Join(t.TempDir(), "journal"), journal.Options{Identity: journal.Identity{InstallationID: inst.id.String(), NodeID: inst.node.String(), RecoveryGeneration: generation.String()}, AllowCreate: true, EmptyInventory: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { j.Close() })
	routes, err := router.New(t.Context(), router.Options{ProbeInterval: 100 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { routes.Close() })
	proc := t.TempDir()
	for name, data := range map[string]string{"meminfo": "MemTotal: 16000000 kB\nMemAvailable: 8000000 kB\n", "stat": "cpu  1 0 1 1 1\n"} {
		if err := os.WriteFile(filepath.Join(proc, name), []byte(data), 0600); err != nil {
			t.Fatal(err)
		}
	}
	counters, stopCounters := context.WithCancel(t.Context())
	countersDone := make(chan struct{})
	go func() {
		defer close(countersDone)
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		step := int64(100)
		for {
			select {
			case <-counters.Done():
				return
			case <-ticker.C:
				step += 100
				next := filepath.Join(proc, "stat.next")
				if err := os.WriteFile(next, fmt.Appendf(nil, "cpu  1 0 1 %d 1\n", step), 0600); err != nil {
					t.Error(err)
					return
				}
				if err := os.Rename(next, filepath.Join(proc, "stat")); err != nil {
					t.Error(err)
					return
				}
			}
		}
	}()
	t.Cleanup(func() { stopCounters(); <-countersDone })
	boot := uuid.NewString()
	agent, err := executor.New(j, native, executor.Options{Host: agentcapacity.Host{ProcRoot: proc, StoragePath: t.TempDir()}, BootID: boot, StorageID: "app-test", RuntimeVersion: "test", PollInterval: 20 * time.Millisecond, Router: routes})
	if err != nil {
		t.Fatal(err)
	}
	capabilities := []string{"runtime.docker", controlstream.CommandInspectionCapability, controlstream.RouterCapability}
	agentClient, err := transport.New(transport.Config{Endpoints: []string{inst.address}, TLS: &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: inst.roots, Certificates: []tls.Certificate{inst.agentTLS}}, Hello: &agentv1.Hello{InstallationId: inst.id.String(), NodeId: inst.node.String(), BootId: boot, JournalGeneration: j.Identity().JournalGeneration, Protocol: &agentv1.VersionRange{Minimum: 1, Maximum: 1}, ActivePayloadVersions: []uint32{1}, Capabilities: capabilities}, Handle: agent.Handle, Observations: agent.Observations})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	doneAgent, doneClient := make(chan error, 1), make(chan error, 1)
	go func() { doneAgent <- agent.Run(ctx, agentClient) }()
	go func() { doneClient <- agentClient.Run(ctx) }()
	t.Cleanup(func() { cancel(); <-doneClient; <-doneAgent })
	// The local proof uses real Docker and PostgreSQL. A proc fixture supplies Linux CPU and memory counters on macOS.
	var address string
	interfaces, err := net.InterfaceAddrs()
	if err != nil {
		t.Fatal(err)
	}
	for _, candidate := range interfaces {
		if ip, ok := candidate.(*net.IPNet); ok && ip.IP.To4() != nil && !ip.IP.IsLoopback() {
			address = ip.IP.String()
			break
		}
	}
	if address == "" {
		t.Fatal("no reachable local IPv4 address")
	}
	if _, err := inst.store.PutNodePlacementPolicy(t.Context(), placement.NodePolicy{InstallationID: inst.id, NodeID: inst.node, Architecture: goruntime.GOARCH, RuntimeVersion: "test", StorageID: "app-test", ClientAddress: address, Headroom: placement.Vector{CPUMillis: 100, MemoryBytes: 64 << 20, DiskBytes: 1 << 20}}, 0); err != nil {
		t.Fatal(err)
	}
	var ports []uint16
	for range 2 {
		listener, err := net.Listen("tcp", "0.0.0.0:0")
		if err != nil {
			t.Fatal(err)
		}
		ports = append(ports, uint16(listener.Addr().(*net.TCPAddr).Port))
		listener.Close()
	}
	driver, err := capacity.New(capacity.Config{Store: inst.store, Transport: inst.runner.config.Transport})
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := engine.NewCatalog(postgresengine.New())
	if err != nil {
		t.Fatal(err)
	}
	planner, err := provision.New(provision.Config{Store: inst.store, Decider: driver, Catalog: catalog, Images: []provision.Image{{Digest: "unused"}}, Budget: placement.Vector{CPUMillis: 500, MemoryBytes: 256 << 20, DiskBytes: 1}, EndpointPorts: placement.PortRange{First: ports[0], Last: ports[0]}})
	if err != nil {
		t.Fatal(err)
	}
	inst.runner.config.Planner = planner
	runnerCtx, stopRunner := context.WithCancel(t.Context())
	doneRunner := make(chan error, 1)
	go func() { doneRunner <- inst.runner.Run(runnerCtx) }()
	t.Cleanup(func() { stopRunner(); <-doneRunner })
	external, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(external, "keep"), []byte("external data"), 0600); err != nil {
		t.Fatal(err)
	}
	request := postgres.CreateAppRequest{Identity: postgres.RequestIdentity{InstallationID: inst.id, ActorID: "operator", APIOperation: postgres.CreateAppAPIOperation, IdempotencyKey: uuid.NewString()}, RequestID: uuid.NewString(), Spec: app.AppSpec{SchemaVersion: 1, Name: "echo", Tier: app.TierParallel, Copies: 1, Service: app.ServiceSpec{Image: "redis@sha256:d30960f73a599496d8b2802c97758bab6b1cd421fd06337f837779c47a57e1f3", Ports: []app.PortSpec{{ContainerPort: 6379}}, Mounts: []app.MountSpec{{Type: "tmpfs", Target: "/data"}, {Type: "bind", Source: external, Target: "/external", ReadOnly: true}}, HealthCheck: &app.HealthCheckSpec{Type: "tcp", Port: 6379, IntervalSeconds: 1}}, Router: app.RouterSpec{Protocol: "tcp", Port: ports[1]}}}
	admitted, err := inst.store.AdmitApp(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	publishRoutes := func() {
		t.Helper()
		targets, err := inst.store.AppRouteTargets(t.Context(), inst.id)
		if err != nil {
			t.Fatalf("publish routes: %v: %v", err, errors.Unwrap(err))
		}
		for node, target := range targets {
			table, acknowledged, err := inst.store.PrepareAppRoutes(t.Context(), inst.id, node, target.Table)
			if err != nil {
				t.Fatalf("publish routes: %v: %v", err, errors.Unwrap(err))
			}
			if !acknowledged {
				if err := inst.runner.config.Transport.Send(node, table.SessionId, &agentv1.ControllerMessage{Message: &agentv1.ControllerMessage_Routes{Routes: table}}); err != nil {
					t.Fatalf("publish routes: %v: %v", err, errors.Unwrap(err))
				}
			}
		}
	}
	var found app.App
	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) {
		publishRoutes()
		found, err = inst.store.GetAppStatus(t.Context(), inst.id, admitted.AppID)
		if err != nil {
			t.Fatal(err)
		}
		if found.Status.State == "failed" {
			t.Fatalf("failed: %+v", found.Status)
		}
		if found.Status.State == "running" {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if found.Status.State != "running" {
		op, _ := inst.store.GetOperation(t.Context(), inst.id, admitted.OperationID)
		t.Fatalf("app never ran: %+v operation %+v", found.Status, op)
	}
	ingress := net.JoinHostPort(address, strconv.Itoa(int(ports[1])))
	var response string
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); {
		connection, err := net.DialTimeout("tcp", ingress, time.Second)
		if err == nil {
			connection.SetDeadline(time.Now().Add(time.Second))
			_, err = connection.Write([]byte("PING\r\n"))
			if err == nil {
				response, err = bufio.NewReader(connection).ReadString('\n')
			}
			connection.Close()
			if response == "+PONG\r\n" {
				break
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	if response != "+PONG\r\n" {
		t.Fatalf("router response %q", response)
	}
	replay, err := postgres.New(inst.pool).AdmitApp(t.Context(), request)
	if err != nil || !replay.Replayed || replay.OperationID != admitted.OperationID {
		t.Fatalf("retry: %+v %v", replay, err)
	}
	var planBytes []byte
	if err := inst.pool.QueryRow(t.Context(), `SELECT plan_bytes FROM operations WHERE id=$1`, admitted.OperationID).Scan(&planBytes); err != nil {
		t.Fatal(err)
	}
	var plan agentv1.ExecutionPlan
	if err := proto.Unmarshal(planBytes, &plan); err != nil {
		t.Fatal(err)
	}
	for _, step := range plan.Steps {
		record, err := j.Get(step.Command.CommandId)
		if err != nil || record.State != journal.Terminal || record.Result.State != "succeeded" {
			t.Fatalf("journal proof: %+v %v", record, err)
		}
	}
	list, err := api.ContainerList(t.Context(), client.ContainerListOptions{All: true, Filters: client.Filters{}.Add("label", "io.synod.installation="+inst.id.String())})
	if err != nil || len(list.Items) != 1 {
		t.Fatalf("duplicate containers: %v %v", list, err)
	}
	// A completed start receipt does not freeze health after the container stops.
	for _, want := range []string{"failed", "running"} {
		if want == "failed" {
			_, err = api.ContainerStop(t.Context(), list.Items[0].ID, client.ContainerStopOptions{})
		} else {
			_, err = api.ContainerStart(t.Context(), list.Items[0].ID, client.ContainerStartOptions{})
		}
		if err != nil {
			t.Fatal(err)
		}
		for deadline := time.Now().Add(20 * time.Second); time.Now().Before(deadline); {
			found, err = inst.store.GetAppStatus(t.Context(), inst.id, admitted.AppID)
			if err != nil {
				t.Fatal(err)
			}
			if found.Status.State == want {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
		if found.Status.State != want {
			t.Fatalf("health state %q, want %q: %+v", found.Status.State, want, found.Status)
		}
	}
	if !strings.Contains(found.Status.Endpoints[0].Address, strconv.Itoa(int(ports[1]))) {
		t.Fatalf("endpoint: %+v", found.Status.Endpoints)
	}
	copyID := found.Status.Copies[0].ID
	originalRoutes, err := inst.store.AppRouteTargets(t.Context(), inst.id)
	if err != nil {
		t.Fatal(err)
	}
	for _, action := range []string{"stop", "start", "delete"} {
		request := postgres.AppActionRequest{Identity: postgres.RequestIdentity{InstallationID: inst.id, ActorID: "operator", APIOperation: "app." + action, IdempotencyKey: uuid.NewString()}, RequestID: uuid.NewString(), AppID: admitted.AppID, ExpectedRevision: found.DesiredRevision}
		accepted, err := inst.store.AdmitAppAction(t.Context(), request)
		if err != nil {
			t.Fatal(err)
		}
		retry, err := postgres.New(inst.pool).AdmitAppAction(t.Context(), request)
		if err != nil || !retry.Replayed || retry.OperationID != accepted.OperationID {
			t.Fatalf("action retry: %+v %v", retry, err)
		}
		if action != "start" {
			for node, target := range originalRoutes {
				if len(target.Table.Routes) > 0 {
					if _, _, err := inst.store.PrepareAppRoutes(t.Context(), inst.id, node, target.Table); !errors.Is(err, operation.ErrCommandConflict) {
						t.Fatalf("stale route restored: %v", err)
					}
				}
			}
			// Withhold route withdrawal until every native command has a receipt.
			proved := false
			for deadline := time.Now().Add(30 * time.Second); time.Now().Before(deadline); {
				var encoded []byte
				if err := inst.pool.QueryRow(t.Context(), `SELECT plan_bytes FROM operations WHERE id=$1`, accepted.OperationID).Scan(&encoded); err != nil {
					t.Fatal(err)
				}
				var plan agentv1.ExecutionPlan
				if len(encoded) > 0 && proto.Unmarshal(encoded, &plan) == nil {
					proved = true
					for _, step := range plan.Steps {
						record, err := j.Get(step.Command.CommandId)
						if err != nil || record.Result == nil || record.Result.State != "succeeded" {
							proved = false
						}
					}
				}
				if proved {
					break
				}
				time.Sleep(100 * time.Millisecond)
			}
			if !proved {
				t.Fatal("native lifecycle commands did not finish")
			}
			op, err := inst.store.GetOperation(t.Context(), inst.id, accepted.OperationID)
			if err != nil || op.Status == operation.Succeeded {
				t.Fatalf("finished before route withdrawal: %+v %v", op, err)
			}
			var live int
			if err := inst.pool.QueryRow(t.Context(), `SELECT count(*) FROM resource_allocations WHERE cluster_id=$1 AND released_at IS NULL`, admitted.AppID).Scan(&live); err != nil || live != 2 {
				t.Fatalf("released held ports: %d %v", live, err)
			}
		}
		want := map[string]string{"stop": "stopped", "start": "running", "delete": "deleted"}[action]
		for deadline := time.Now().Add(30 * time.Second); time.Now().Before(deadline); {
			publishRoutes()
			found, err = inst.store.GetAppStatus(t.Context(), inst.id, admitted.AppID)
			if err != nil {
				t.Fatal(err)
			}
			if found.Status.State == "failed" {
				t.Fatalf("%s failed: %+v", action, found.Status)
			}
			if found.Status.State == want {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
		if found.Status.State != want || found.ObservedRevision != accepted.DesiredRevision {
			t.Fatalf("%s did not finish: %+v", action, found)
		}
		if action == "stop" && found.Status.Copies[0].ObservedState != "stopped" {
			t.Fatalf("stopped copy status: %+v", found.Status.Copies)
		}
		if action == "start" {
			if found.Status.Copies[0].ID != copyID {
				t.Fatal("start changed copy identity")
			}
			current, err := api.ContainerInspect(t.Context(), list.Items[0].ID, client.ContainerInspectOptions{})
			if err != nil || !current.Container.State.Running || current.Container.Config.Image != plan.Steps[0].Command.Input.GetEnsureContainer().ImageDigest {
				t.Fatalf("start changed pinned container: %+v %v", current, err)
			}
		} else {
			listener, err := net.Listen("tcp", ingress)
			if err != nil {
				t.Fatalf("old ingress is still bound: %v", err)
			}
			listener.Close()
		}
	}
	list, err = api.ContainerList(t.Context(), client.ContainerListOptions{All: true, Filters: client.Filters{}.Add("label", "io.synod.installation="+inst.id.String())})
	if err != nil || len(list.Items) != 0 {
		t.Fatalf("delete left containers: %+v %v", list, err)
	}
	var live int
	if err := inst.pool.QueryRow(t.Context(), `SELECT count(*) FROM resource_allocations WHERE cluster_id=$1 AND released_at IS NULL`, admitted.AppID).Scan(&live); err != nil || live != 0 {
		t.Fatalf("delete held reservations: %d %v", live, err)
	}
	kept, err := os.ReadFile(filepath.Join(external, "keep"))
	if err != nil || string(kept) != "external data" {
		t.Fatalf("delete changed external data: %q %v", kept, err)
	}

}
