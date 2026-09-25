package daemon_test

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"io"
	"maps"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/containerd/errdefs"
	"github.com/google/uuid"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/mount"
	"github.com/moby/moby/client"
	"github.com/sirrobot01/synod/internal/agent/daemon"
	"github.com/sirrobot01/synod/internal/agent/enrollment"
	"github.com/sirrobot01/synod/internal/agent/journal"
	"github.com/sirrobot01/synod/internal/controlstream"
	"github.com/sirrobot01/synod/internal/operation"
	"github.com/sirrobot01/synod/internal/pki"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestExecutableLifecycleWithDocker(t *testing.T) {
	host := os.Getenv("SYNOD_TEST_DOCKER_HOST")
	if host == "" {
		t.Skip("set SYNOD_TEST_DOCKER_HOST for actual Docker")
	}
	if !strings.HasPrefix(host, "unix://") {
		t.Fatal("fixture requires local Unix Docker socket")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
	defer cancel()
	parent := t.TempDir()
	if err := os.Chmod(parent, 0o700); err != nil {
		t.Fatal(err)
	}
	config := daemon.Config{InstallationID: uuid.NewString(), NodeID: uuid.NewString(), RecoveryGeneration: uuid.NewString(), EnrollmentDir: filepath.Join(parent, "enrollment"), JournalDir: filepath.Join(parent, "journal"), DockerSocket: strings.TrimPrefix(host, "unix://"), StorageID: "pool-lifecycle", JournalMaxRetainedBytes: 128 << 20, JournalMaxStoredBytes: 512 << 20, JournalRecoveryWindowSeconds: 3600}
	assigned := enrollment.Identity{InstallationID: uuid.MustParse(config.InstallationID), NodeID: uuid.MustParse(config.NodeID)}
	caPEM, caKey, err := pki.CreateCA(assigned.InstallationID, time.Now(), 7*24*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	ca, err := pki.LoadCA(assigned.InstallationID, caPEM, caKey)
	if err != nil {
		t.Fatal(err)
	}
	token := base64.RawURLEncoding.EncodeToString(make([]byte, 32))
	tokenPath := filepath.Join(parent, "token")
	if err := os.WriteFile(tokenPath, []byte(token), 0o600); err != nil {
		t.Fatal(err)
	}
	// This fixture issues the reviewed enrollment wire response. It does not
	// claim Store enrollment or controller worker policy integration.
	issuer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request enrollment.Request
		if r.Header.Get("Authorization") != "Bearer "+token || json.NewDecoder(r.Body).Decode(&request) != nil {
			w.WriteHeader(401)
			return
		}
		leaf, err := ca.Issue(request.CSRDER, pki.Identity{InstallationID: assigned.InstallationID, Role: pki.Agent, SubjectID: assigned.NodeID}, nil, time.Now(), time.Hour)
		if err != nil {
			t.Error(err)
			w.WriteHeader(500)
			return
		}
		w.WriteHeader(201)
		_ = json.NewEncoder(w).Encode(enrollment.Response{NodeID: assigned.NodeID, CertificateID: uuid.New(), CertificatePEM: string(leaf), CACertificatePEM: string(caPEM), AcceptedAt: time.Now()})
	}))
	state, err := enrollment.Open(config.EnrollmentDir, enrollment.Options{Identity: assigned, AllowCreate: true})
	if err != nil {
		t.Fatal(err)
	}
	trust := x509.NewCertPool()
	trust.AddCert(issuer.Certificate())
	credentials, err := state.Submit(ctx, enrollment.Config{Endpoint: issuer.URL, TokenFile: tokenPath, RootCAs: trust})
	if err != nil {
		t.Fatal(err)
	}
	if err := state.Close(); err != nil {
		t.Fatal(err)
	}
	issuer.Close()
	if err := os.Remove(tokenPath); err != nil {
		t.Fatal(err)
	}
	_, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	csr, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{}, key)
	if err != nil {
		t.Fatal(err)
	}
	leaf, err := ca.Issue(csr, pki.Identity{InstallationID: assigned.InstallationID, Role: pki.Controller, SubjectID: uuid.New()}, []string{"127.0.0.1"}, time.Now(), time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	pair, err := tls.X509KeyPair(leaf, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}))
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(caPEM) {
		t.Fatal("invalid CA")
	}
	barriers := make(chan *agentv1.BarrierResult, 16)
	admissions := make(chan *agentv1.AdmissionAck, 4)
	results := make(chan *agentv1.TerminalResult, 4)
	hellos := make(chan *agentv1.Hello, 16)
	heartbeats := make(chan struct{}, 16)
	var rejectHeartbeat atomic.Bool
	server, err := controlstream.New(controlstream.Config{InstallationID: assigned.InstallationID, TLS: &tls.Config{MinVersion: tls.VersionTLS12, ClientCAs: roots, Certificates: []tls.Certificate{pair}}, HeartbeatInterval: 100 * time.Millisecond, StaleAfter: time.Second, Authorize: func(_ context.Context, installation, node uuid.UUID, certificate *x509.Certificate) error {
		if installation != assigned.InstallationID || node != assigned.NodeID || !bytes.Equal(certificate.Raw, credentials.Certificate.Leaf.Raw) {
			return errors.New("wrong enrolled identity")
		}
		return nil
	}, Handle: func(_ context.Context, _ controlstream.Peer, message *agentv1.AgentMessage) error {
		if hello := message.GetHello(); hello != nil {
			hellos <- hello
			return nil
		}
		if message.GetHeartbeat() != nil {
			if rejectHeartbeat.CompareAndSwap(true, false) {
				return errors.New("disconnect fixture")
			}
			select {
			case heartbeats <- struct{}{}:
			default:
			}
			return nil
		}
		if barrier := message.GetBarrierResult(); barrier != nil {
			barriers <- barrier
			return nil
		}
		if admission := message.GetAdmissionAck(); admission != nil {
			admissions <- admission
			return nil
		}
		if result := message.GetTerminalResult(); result != nil {
			// This bounded fixture commits the exact receipt to a private file before
			// permitting transport ACK. Production uses Store.RecordAgentResult.
			data, err := proto.Marshal(result)
			if err != nil {
				return err
			}
			file, err := os.OpenFile(filepath.Join(parent, "fixture-receipt"), os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
			if err != nil {
				return err
			}
			_, writeErr := file.Write(data)
			syncErr := file.Sync()
			closeErr := file.Close()
			if err := errors.Join(writeErr, syncErr, closeErr); err != nil {
				return err
			}
			results <- result
			return nil
		}
		return errors.New("unsupported fixture controller message")
	}})
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	served := make(chan error, 1)
	go func() { served <- server.Serve(listener) }()
	t.Cleanup(func() { server.Stop(); <-served })
	config.ControllerEndpoints = []string{listener.Addr().String()}
	configPath := filepath.Join(parent, "config.json")
	data, err := json.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(configPath, data, 0o600); err != nil {
		t.Fatal(err)
	}
	binary := os.Getenv("SYNOD_TEST_AGENT_BINARY")
	if binary == "" {
		binary = filepath.Join(parent, "synod-agent")
		command := exec.CommandContext(ctx, "go", "build", "-race", "-o", binary, "../../../cmd/synod-agent")
		if output, err := command.CombinedOutput(); err != nil {
			t.Fatalf("build: %v %s", err, output)
		}
	}
	api, err := client.New(client.WithHost(host))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { api.Close() })
	clusterID, volumeID, containerID := uuid.NewString(), uuid.NewString(), uuid.NewString()
	labels := map[string]string{"io.synod.installation": config.InstallationID, "io.synod.node": config.NodeID, "io.synod.cluster": clusterID, "io.synod.resource": volumeID, "io.synod.kind": "volume", "io.synod.fixture": "task-048"}
	volumeName := "synod-volume-" + volumeID
	if _, err := api.VolumeCreate(ctx, client.VolumeCreateOptions{Name: volumeName, Labels: labels}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), 15*time.Second)
		defer stop()
		if _, err := api.VolumeRemove(cleanup, volumeName, client.VolumeRemoveOptions{}); err != nil && !errdefs.IsNotFound(err) {
			t.Error(err)
		}
	})
	invoke := func(args ...string) ([]byte, []byte, error) {
		t.Helper()
		command := exec.CommandContext(ctx, binary, args...)
		var stdout, stderr bytes.Buffer
		command.Stdout = &stdout
		command.Stderr = &stderr
		err := command.Run()
		if bytes.Contains(stderr.Bytes(), []byte(token)) || bytes.Contains(stdout.Bytes(), []byte("PRIVATE KEY")) || bytes.Contains(stderr.Bytes(), []byte(parent)) {
			t.Fatalf("unredacted executable output: %s", stderr.Bytes())
		}
		return stdout.Bytes(), stderr.Bytes(), err
	}
	if _, _, err := invoke("journal", "init", "--config", configPath); err == nil {
		t.Fatal("initialized over retained volume")
	}
	if _, err := os.Stat(config.JournalDir); !os.IsNotExist(err) {
		t.Fatal("failed initialization created journal")
	}
	if _, err := api.VolumeRemove(ctx, volumeName, client.VolumeRemoveOptions{}); err != nil {
		t.Fatal(err)
	}
	stdout, stderr, err := invoke("journal", "init", "--config", configPath)
	if err != nil {
		t.Fatalf("initialize: %v %s", err, stderr)
	}
	var initial journal.Identity
	if json.Unmarshal(stdout, &initial) != nil || initial.JournalGeneration == "" {
		t.Fatal("invalid initialized identity")
	}
	if _, _, err := invoke("journal", "init", "--config", configPath); err == nil {
		t.Fatal("initialized existing journal")
	}
	if _, err := api.VolumeCreate(ctx, client.VolumeCreateOptions{Name: volumeName, Labels: labels}); err != nil {
		t.Fatal(err)
	}
	labels = maps.Clone(labels)
	labels["io.synod.resource"] = containerID
	labels["io.synod.kind"] = "container"
	created, err := api.ContainerCreate(ctx, client.ContainerCreateOptions{Name: "synod-instance-" + containerID, Config: &container.Config{Image: "alpine@sha256:28bd5fe8b56d1bd048e5babf5b10710ebe0bae67db86916198a6eec434943f8b", Cmd: []string{"sh", "-c", "echo task-048-data > /data/marker; exec sleep 300"}, Labels: labels}, HostConfig: &container.HostConfig{NetworkMode: "none", Resources: container.Resources{Memory: 32 << 20, NanoCPUs: 100000000, PidsLimit: new(int64(32))}, Mounts: []mount.Mount{{Type: mount.TypeVolume, Source: volumeName, Target: "/data"}}, LogConfig: container.LogConfig{Type: "json-file", Config: map[string]string{"max-size": "1m", "max-file": "1"}}}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), 15*time.Second)
		defer stop()
		if _, err := api.ContainerRemove(cleanup, created.ID, client.ContainerRemoveOptions{Force: true}); err != nil {
			t.Error(err)
		}
	})
	if _, err := api.ContainerStart(ctx, created.ID, client.ContainerStartOptions{}); err != nil {
		t.Fatal(err)
	}
	runArgs := []string{"run", "--config", configPath}
	if runtime.GOOS == "darwin" {
		runArgs = append(runArgs, "--test-boot-id", uuid.NewString())
	}
	// Hold the native journal lock without holding enrollment to test both locks.
	held, err := journal.Open(config.JournalDir, journal.Options{Identity: initial})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := invoke(runArgs...); err == nil {
		t.Fatal("second process bypassed journal lock")
	}
	if err := held.Close(); err != nil {
		t.Fatal(err)
	}
	managedVolumeID := uuid.NewString()
	managedVolume := "synod-volume-" + managedVolumeID
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), 15*time.Second)
		defer stop()
		if _, err := api.VolumeRemove(cleanup, managedVolume, client.VolumeRemoveOptions{}); err != nil && !errdefs.IsNotFound(err) {
			t.Error(err)
		}
	})
	for cycle := range 2 {
		command := exec.CommandContext(ctx, binary, runArgs...)
		var out, diagnostics bytes.Buffer
		command.Stdout = &out
		command.Stderr = &diagnostics
		if err := command.Start(); err != nil {
			t.Fatal(err)
		}
		ended := make(chan error, 1)
		go func() { ended <- command.Wait() }()
		waited := false
		t.Cleanup(func() {
			if !waited {
				_ = command.Process.Kill()
				<-ended
			}
		})
		select {
		case hello := <-hellos:
			if hello.JournalGeneration != initial.JournalGeneration || hello.InstallationId != config.InstallationID || hello.NodeId != config.NodeID || hello.RuntimeVersion == "" {
				t.Fatal("wrong Hello")
			}
		case err := <-ended:
			waited = true
			t.Fatalf("agent exited before Hello: %v %s", err, diagnostics.Bytes())
		case <-ctx.Done():
			t.Fatal("agent did not connect")
		}
		if _, _, err := invoke(runArgs...); err == nil {
			t.Fatal("second process bypassed enrollment lock")
		}
		if cycle == 0 {
			rejectHeartbeat.Store(true)
			select {
			case <-hellos:
			case <-ctx.Done():
				t.Fatal("agent did not reconnect")
			}
		}
		select {
		case <-heartbeats:
		case <-ctx.Done():
			t.Fatal("agent did not heartbeat")
		}
		if cycle == 1 {
			peer, ok := server.Session(assigned.NodeID)
			if !ok {
				t.Fatal("no current session")
			}
			epoch := &agentv1.Epoch{InstallationId: config.InstallationID, NodeId: config.NodeID, ClusterId: uuid.NewString(), RecoveryGeneration: config.RecoveryGeneration, ExecutionEpoch: 1}
			if err := server.Send(assigned.NodeID, peer.SessionID, &agentv1.ControllerMessage{Message: &agentv1.ControllerMessage_PrepareEpoch{PrepareEpoch: &agentv1.PrepareEpoch{Epoch: epoch}}}); err != nil {
				t.Fatal(err)
			}
			drained := false
			for !drained {
				select {
				case barrier := <-barriers:
					drained = barrier.Drained
				case <-ctx.Done():
					t.Fatal("epoch did not drain")
				}
			}
			if err := server.Send(assigned.NodeID, peer.SessionID, &agentv1.ControllerMessage{Message: &agentv1.ControllerMessage_ActivateEpoch{ActivateEpoch: &agentv1.ActivateEpoch{Epoch: epoch}}}); err != nil {
				t.Fatal(err)
			}
			work := &agentv1.Command{Epoch: epoch, OperationId: uuid.NewString(), StepId: uuid.NewString(), CommandId: uuid.NewString(), DesiredRevision: 1, PlanVersion: 1, Input: &agentv1.CommandInput{PayloadVersion: 1, ResourceId: managedVolumeID, Timeout: durationpb.New(5 * time.Second), Payload: &agentv1.CommandInput_EnsureVolume{EnsureVolume: &agentv1.EnsureVolume{VolumeId: managedVolumeID}}}}
			hash, err := operation.CommandHash(work)
			if err != nil {
				t.Fatal(err)
			}
			work.InputHash = hash[:]
			if err := server.Send(assigned.NodeID, peer.SessionID, &agentv1.ControllerMessage{DeliveryAttemptId: uuid.NewString(), Message: &agentv1.ControllerMessage_Command{Command: work}}); err != nil {
				t.Fatal(err)
			}
			select {
			case admission := <-admissions:
				if admission.State != "queued" || admission.CommandId != work.CommandId {
					t.Fatal("command not durably admitted")
				}
			case <-ctx.Done():
				t.Fatal("no command admission")
			}
			select {
			case result := <-results:
				if result.CommandId != work.CommandId || result.State != "succeeded" {
					t.Fatal("native command did not succeed")
				}
			case <-ctx.Done():
				t.Fatal("no durable native result")
			}
			volume, err := api.VolumeInspect(ctx, managedVolume, client.VolumeInspectOptions{})
			if err != nil || volume.Volume.Labels["io.synod.node"] != config.NodeID {
				t.Fatal("executor did not create owned volume")
			}
		}
		signal := os.Signal(syscall.SIGTERM)
		if cycle == 0 {
			signal = os.Kill
		}
		if err := command.Process.Signal(signal); err != nil {
			t.Fatal(err)
		}
		select {
		case err := <-ended:
			waited = true
			if cycle == 1 && err != nil {
				t.Fatalf("graceful stop: %v %s", err, diagnostics.Bytes())
			}
		case <-ctx.Done():
			t.Fatal("agent did not exit")
		}
		if out.Len() != 0 || bytes.Contains(diagnostics.Bytes(), []byte(parent)) || bytes.Contains(diagnostics.Bytes(), []byte(token)) {
			t.Fatalf("unredacted daemon output: %s", diagnostics.Bytes())
		}
		current, err := api.ContainerInspect(ctx, created.ID, client.ContainerInspectOptions{})
		if err != nil || !current.Container.State.Running || len(current.Container.Mounts) != 1 || current.Container.Mounts[0].Name != volumeName {
			t.Fatal("management exit changed workload")
		}
	}
	// Missing and corrupt state must not create a replacement or alter native work.
	saved := config.JournalDir + "-saved"
	if err := os.Rename(config.JournalDir, saved); err != nil {
		t.Fatal(err)
	}
	if _, _, err := invoke(runArgs...); err == nil {
		t.Fatal("missing journal accepted")
	}
	if _, err := os.Stat(config.JournalDir); !os.IsNotExist(err) {
		t.Fatal("ordinary run created missing journal")
	}
	if err := os.Mkdir(config.JournalDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(config.JournalDir, "CURRENT"), []byte("invalid-sensitive-path\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := invoke(runArgs...); err == nil {
		t.Fatal("corrupt journal accepted")
	}
	if err := os.RemoveAll(config.JournalDir); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(saved, config.JournalDir); err != nil {
		t.Fatal(err)
	}
	config.RecoveryGeneration = uuid.NewString()
	data, err = json.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(configPath, data, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := invoke(runArgs...); err == nil {
		t.Fatal("wrong recovery generation accepted")
	}
	copied, err := api.CopyFromContainer(ctx, created.ID, client.CopyFromContainerOptions{SourcePath: "/data/marker"})
	if err != nil {
		t.Fatal(err)
	}
	defer copied.Content.Close()
	archive := tar.NewReader(copied.Content)
	if _, err := archive.Next(); err != nil {
		t.Fatal(err)
	}
	marker, err := io.ReadAll(archive)
	if err != nil || string(marker) != "task-048-data\n" {
		t.Fatal("workload data changed")
	}
	final, err := api.ContainerInspect(ctx, created.ID, client.ContainerInspectOptions{})
	if err != nil || !final.Container.State.Running {
		t.Fatal("recovery refusal stopped workload")
	}
}
