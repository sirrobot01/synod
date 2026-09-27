package runner

import (
	"bufio"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
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
	"github.com/sirrobot01/synod/internal/agent/replica"
	"github.com/sirrobot01/synod/internal/agent/replication"
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
	for _, tier := range []app.Tier{app.TierParallel, app.TierSingleWriter} {
		t.Run(fmt.Sprint(tier), func(t *testing.T) {
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
			appsDir, err := filepath.EvalSymlinks(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			native, err := docker.New(api, docker.Options{InstallationID: inst.id, NodeID: inst.node, AppsDir: appsDir})
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
			if err := os.MkdirAll(filepath.Join(proc, "1"), 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink("/", filepath.Join(proc, "1", "root")); err != nil {
				t.Fatal(err)
			}
			pushClient := &replication.Client{InstallationID: inst.id, Certificate: inst.agentTLS, Roots: inst.roots}
			started := time.Now()
			boot := uuid.NewString()
			agent, err := executor.New(j, native, executor.Options{Host: agentcapacity.Host{ProcRoot: proc, StoragePath: t.TempDir()}, BootID: boot, StorageID: "app-test", RuntimeVersion: "test", PollInterval: 20 * time.Millisecond, Router: routes, AppsDir: appsDir, Replication: &replication.Table{}, PushSnapshot: pushClient.Push, BootTime: func() (time.Duration, error) { return time.Hour + time.Since(started), nil }})
			if err != nil {
				t.Fatal(err)
			}
			capabilities := []string{"runtime.docker", controlstream.CommandInspectionCapability, controlstream.RouterCapability, replication.Capability}
			agentClient, err := transport.New(transport.Config{Endpoints: []string{inst.address}, TLS: &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: inst.roots, Certificates: []tls.Certificate{inst.agentTLS}}, Hello: &agentv1.Hello{InstallationId: inst.id.String(), NodeId: inst.node.String(), BootId: boot, JournalGeneration: j.Identity().JournalGeneration, Protocol: &agentv1.VersionRange{Minimum: 1, Maximum: 1}, ActivePayloadVersions: []uint32{1}, Capabilities: capabilities}, Handle: agent.Handle, Observations: agent.Observations, WriterClock: agent.WriterClock})
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
			journals := map[string]*journal.Journal{inst.node.String(): j}
			var standbyNode uuid.UUID
			var standbyRoot, standbyAddress string
			if tier == app.TierSingleWriter {
				// A second enrolled agent owns the standby and runs native cleanup.
				standbyNode = uuid.New()
				_, private, err := ed25519.GenerateKey(rand.Reader)
				if err != nil {
					t.Fatal(err)
				}
				csr, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{}, private)
				if err != nil {
					t.Fatal(err)
				}
				verifier := sha256.Sum256([]byte(uuid.NewString()))
				if _, err := inst.store.RegisterEnrollmentToken(t.Context(), postgres.TokenRegistration{InstallationID: inst.id, NodeID: standbyNode, ActorID: "test-admin", RequestID: uuid.NewString(), IdempotencyKey: uuid.NewString(), TokenVerifier: verifier, TTLSeconds: 600}); err != nil {
					t.Fatal(err)
				}
				enrolled, err := inst.store.EnrollAgent(t.Context(), postgres.EnrollmentRequest{InstallationID: inst.id, NodeID: standbyNode, RequestID: uuid.New(), TokenVerifier: verifier, CSRDER: csr}, inst.ca)
				if err != nil {
					t.Fatal(err)
				}
				key, err := x509.MarshalPKCS8PrivateKey(private)
				if err != nil {
					t.Fatal(err)
				}
				certificate, err := tls.X509KeyPair([]byte(enrolled.CertificatePEM), pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: key}))
				if err != nil {
					t.Fatal(err)
				}
				standbyRoot, err = filepath.EvalSymlinks(t.TempDir())
				if err != nil {
					t.Fatal(err)
				}
				standbyRuntime, err := docker.New(api, docker.Options{InstallationID: inst.id, NodeID: standbyNode, AppsDir: standbyRoot})
				if err != nil {
					t.Fatal(err)
				}
				standbyJournal, err := journal.Open(filepath.Join(t.TempDir(), "journal"), journal.Options{Identity: journal.Identity{InstallationID: inst.id.String(), NodeID: standbyNode.String(), RecoveryGeneration: generation.String()}, AllowCreate: true, EmptyInventory: true})
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { standbyJournal.Close() })
				journals[standbyNode.String()] = standbyJournal
				standbyBoot := uuid.NewString()
				table, status := &replication.Table{}, &replication.Status{}
				standbyAgent, err := executor.New(standbyJournal, standbyRuntime, executor.Options{Host: agentcapacity.Host{ProcRoot: proc, StoragePath: t.TempDir()}, BootID: standbyBoot, StorageID: "standby-test", RuntimeVersion: "test", PollInterval: 20 * time.Millisecond, AppsDir: standbyRoot, Replication: table, BootTime: func() (time.Duration, error) { return time.Hour + time.Since(started), nil }})
				if err != nil {
					t.Fatal(err)
				}
				standbyClient, err := transport.New(transport.Config{Endpoints: []string{inst.address}, TLS: &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: inst.roots, Certificates: []tls.Certificate{certificate}}, Hello: &agentv1.Hello{InstallationId: inst.id.String(), NodeId: standbyNode.String(), BootId: standbyBoot, JournalGeneration: standbyJournal.Identity().JournalGeneration, Protocol: &agentv1.VersionRange{Minimum: 1, Maximum: 1}, ActivePayloadVersions: []uint32{1}, Capabilities: []string{"runtime.docker", controlstream.CommandInspectionCapability, replication.Capability}}, Handle: standbyAgent.Handle, Observations: standbyAgent.Observations, WriterClock: standbyAgent.WriterClock, Replicas: status.List})
				if err != nil {
					t.Fatal(err)
				}
				standbyCtx, stopStandby := context.WithCancel(t.Context())
				agentDone, clientDone := make(chan error, 1), make(chan error, 1)
				go func() { agentDone <- standbyAgent.Run(standbyCtx, standbyClient) }()
				go func() { clientDone <- standbyClient.Run(standbyCtx) }()
				listener, err := net.Listen("tcp", "0.0.0.0:0")
				if err != nil {
					t.Fatal(err)
				}
				standbyAddress = net.JoinHostPort(address, strconv.Itoa(listener.Addr().(*net.TCPAddr).Port))
				receiver := &replication.Server{InstallationID: inst.id, Root: standbyRoot, Table: table, Status: status}
				serverDone := make(chan error, 1)
				go func() { serverDone <- receiver.Serve(standbyCtx, listener, certificate, inst.roots) }()
				t.Cleanup(func() { stopStandby(); <-clientDone; <-agentDone; <-serverDone })
				if _, err := inst.store.PutNodePlacementPolicy(t.Context(), placement.NodePolicy{InstallationID: inst.id, NodeID: standbyNode, Architecture: goruntime.GOARCH, RuntimeVersion: "test", StorageID: "standby-test", ClientAddress: address, ReplicationPort: uint16(listener.Addr().(*net.TCPAddr).Port), Headroom: placement.Vector{CPUMillis: 100, MemoryBytes: 64 << 20, DiskBytes: 1 << 20}}, 0); err != nil {
					t.Fatal(err)
				}
				request.Spec.Tier = tier
				request.Spec.Replication = new(app.DefaultReplication())
				request.Spec.DevicePreference.Preferred = []string{inst.node.String()}
				request.Spec.Service.Mounts = []app.MountSpec{{Type: "replicated", Source: "config", Target: "/data"}}
			}
			admitted, err := inst.store.AdmitApp(t.Context(), request)
			if err != nil {
				t.Fatal(err)
			}
			publishReplication := func() {
				t.Helper()
				if tier == app.TierSingleWriter {
					tables, err := inst.store.AppReplicationTargets(t.Context(), inst.id)
					if err != nil {
						t.Fatal(err)
					}
					for node, desired := range tables {
						table, acked, err := inst.store.PrepareAppReplication(t.Context(), inst.id, node, desired)
						if errors.Is(err, operation.ErrCommandConflict) || errors.Is(err, postgres.ErrStaleSession) {
							continue
						}
						if err != nil {
							t.Fatal(err)
						}
						if !acked {
							if err := inst.runner.config.Transport.Send(node, table.SessionId, &agentv1.ControllerMessage{Message: &agentv1.ControllerMessage_Replication{Replication: table}}); err != nil {
								if errors.Is(err, controlstream.ErrDisconnected) || errors.Is(err, controlstream.ErrBusy) {
									continue
								}
								t.Fatal(err)
							}
						} else {
							for _, assignment := range table.Apps {
								if assignment.Role == replication.RoleActive {
									grant, err := inst.store.PrepareAppWriterLease(t.Context(), inst.id, admitted.AppID, node, uuid.MustParse(table.SessionId), assignment.Epoch, false)
									if errors.Is(err, operation.ErrCommandConflict) || errors.Is(err, postgres.ErrStaleSession) {
										continue
									}
									if err != nil {
										t.Fatal(err)
									}
									if grant != nil {
										if err := inst.runner.config.Transport.Send(node, table.SessionId, &agentv1.ControllerMessage{Message: &agentv1.ControllerMessage_WriterLease{WriterLease: grant}}); err != nil {
											if errors.Is(err, controlstream.ErrDisconnected) || errors.Is(err, controlstream.ErrBusy) {
												continue
											}
											t.Fatal(err)
										}
									}
								}
							}
						}
					}
				}
			}
			publishRoutes := func() {
				publishReplication()
				targets, err := inst.store.AppRouteTargets(t.Context(), inst.id)
				if err != nil {
					t.Fatalf("publish routes: %v: %v", err, errors.Unwrap(err))
				}
				for node, target := range targets {
					table, acknowledged, err := inst.store.PrepareAppRoutes(t.Context(), inst.id, node, target.Table)
					if errors.Is(err, operation.ErrCommandConflict) || errors.Is(err, postgres.ErrStaleSession) {
						continue
					}
					if err != nil {
						t.Fatalf("publish routes: %v: %v", err, errors.Unwrap(err))
					}
					if !acknowledged {
						if err := inst.runner.config.Transport.Send(node, table.SessionId, &agentv1.ControllerMessage{Message: &agentv1.ControllerMessage_Routes{Routes: table}}); err != nil {
							if errors.Is(err, controlstream.ErrDisconnected) || errors.Is(err, controlstream.ErrBusy) {
								continue
							}
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
				record, err := journals[step.Command.Epoch.NodeId].Get(step.Command.CommandId)
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
				if tier == app.TierSingleWriter && want == "failed" {
					want = "down"
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
				if action == "delete" && tier == app.TierSingleWriter {
					// Seed the initial replica before new writes. The final copy must retain the later Redis write.
					staging := t.TempDir()
					initial, err := (&replica.Snapshotter{Live: filepath.Join(appsDir, admitted.AppID.String(), "live"), Snapshot: staging}).TakeStopped(t.Context(), 1, 1)
					if err != nil {
						t.Fatal(err)
					}
					for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); {
						publishReplication()
						pushed, pushErr := pushClient.Push(t.Context(), &agentv1.ReplicaPeer{NodeId: standbyNode.String(), Address: standbyAddress}, admitted.AppID.String(), initial, staging)
						if pushErr == nil && pushed.FailureCode == "" {
							break
						}
						time.Sleep(100 * time.Millisecond)
					}
					// Reserve the test's initial sequence before the real final-snapshot command.
					if err := os.WriteFile(filepath.Join(appsDir, admitted.AppID.String(), "sequence"), []byte("1"), 0600); err != nil {
						t.Fatal(err)
					}
					connection, err := net.DialTimeout("tcp", ingress, time.Second)
					if err != nil {
						t.Fatal(err)
					}
					connection.SetDeadline(time.Now().Add(2 * time.Second))
					if _, err := connection.Write([]byte("SET committed retained\r\n")); err != nil {
						t.Fatal(err)
					}
					reply, err := bufio.NewReader(connection).ReadString('\n')
					connection.Close()
					if err != nil || reply != "+OK\r\n" {
						t.Fatalf("write: %q %v", reply, err)
					}
					moved, err := inst.store.AdmitAppAction(t.Context(), postgres.AppActionRequest{Identity: postgres.RequestIdentity{InstallationID: inst.id, ActorID: "operator", APIOperation: postgres.MoveAppAPIOperation, IdempotencyKey: uuid.NewString()}, RequestID: uuid.NewString(), AppID: admitted.AppID, ExpectedRevision: found.DesiredRevision, TargetNodeID: standbyNode})
					if err != nil {
						t.Fatal(err)
					}
					for deadline := time.Now().Add(60 * time.Second); time.Now().Before(deadline); {
						publishRoutes()
						found, err = inst.store.GetAppStatus(t.Context(), inst.id, admitted.AppID)
						if err != nil {
							t.Fatal(err)
						}
						if found.Status.OperationID == moved.OperationID && found.Status.OperationState == "succeeded" && found.Status.State == "running" {
							break
						}
						time.Sleep(100 * time.Millisecond)
					}
					if found.Status.OperationState != "succeeded" || found.Status.ActiveNodeID != standbyNode || found.Status.WriterEpoch != 2 {
						t.Fatalf("move did not finish: %+v", found.Status)
					}
					connection, err = net.DialTimeout("tcp", ingress, time.Second)
					if err != nil {
						t.Fatal(err)
					}
					connection.SetDeadline(time.Now().Add(2 * time.Second))
					if _, err := connection.Write([]byte("GET committed\r\n")); err != nil {
						t.Fatal(err)
					}
					reader := bufio.NewReader(connection)
					size, err := reader.ReadString('\n')
					if err != nil {
						t.Fatal(err)
					}
					value, err := reader.ReadString('\n')
					connection.Close()
					if err != nil || size != "$8\r\n" || value != "retained\r\n" {
						t.Fatalf("final copy lost committed write: %q %q %v", size, value, err)
					}
				}
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
						publishReplication()
						var encoded []byte
						if err := inst.pool.QueryRow(t.Context(), `SELECT plan_bytes FROM operations WHERE id=$1`, accepted.OperationID).Scan(&encoded); err != nil {
							t.Fatal(err)
						}
						var plan agentv1.ExecutionPlan
						if len(encoded) > 0 && proto.Unmarshal(encoded, &plan) == nil {
							proved = true
							for _, step := range plan.Steps {
								record, err := journals[step.Command.Epoch.NodeId].Get(step.Command.CommandId)
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
					expected := 2
					if tier == app.TierSingleWriter {
						expected++
					}
					if err := inst.pool.QueryRow(t.Context(), `SELECT count(*) FROM resource_allocations WHERE cluster_id=$1 AND released_at IS NULL`, admitted.AppID).Scan(&live); err != nil || live != expected {
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
			if tier == app.TierSingleWriter {
				for _, root := range []string{appsDir, standbyRoot} {
					if _, err := os.Lstat(filepath.Join(root, admitted.AppID.String())); !errors.Is(err, os.ErrNotExist) {
						t.Fatalf("delete left app data on %s: %v", root, err)
					}
				}
			}
			kept, err := os.ReadFile(filepath.Join(external, "keep"))
			if err != nil || string(kept) != "external data" {
				t.Fatalf("delete changed external data: %q %v", kept, err)
			}

		})
	}

}
