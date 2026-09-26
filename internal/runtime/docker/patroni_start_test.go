package docker

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"errors"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/netip"
	"os"
	"os/exec"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"testing/iotest"
	"time"

	"github.com/containerd/errdefs"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/mount"
	"github.com/moby/moby/api/types/network"
	"github.com/moby/moby/client"
	"github.com/sirrobot01/synod/internal/agent/journal"
	"github.com/sirrobot01/synod/internal/engine/postgres"
	"github.com/sirrobot01/synod/internal/operation"
	"github.com/sirrobot01/synod/internal/runtime"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestRealDockerPatroniStartAndRestart(t *testing.T) {
	image := os.Getenv("SYNOD_TEST_PATRONI_IMAGE")
	if image == "" {
		t.Skip("set SYNOD_TEST_PATRONI_IMAGE for the native runtime fixture")
	}
	r, api, clusterID := runtimeFixture(t)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Minute)
	defer cancel()
	networkID := uuid.NewString()
	networkCommand := runtimeCommand(t, r, clusterID, &agentv1.CommandInput{ResourceId: networkID, Payload: &agentv1.CommandInput_EnsureNetwork{EnsureNetwork: &agentv1.EnsureNetwork{NetworkId: networkID}}})
	if _, err := r.Execute(ctx, networkCommand, nil); err != nil {
		t.Fatal(err)
	}
	networkName := "synod-network-" + networkID
	inspected, err := api.NetworkInspect(ctx, networkName, client.NetworkInspectOptions{})
	if err != nil || len(inspected.Network.IPAM.Config) != 1 {
		t.Fatal("fixture network", err)
	}
	subnet := inspected.Network.IPAM.Config[0].Subnet
	addresses := []netip.Addr{subnet.Addr().Next().Next(), subnet.Addr().Next().Next().Next(), subnet.Addr().Next().Next().Next().Next()}
	now := time.Now()
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	ca := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "runtime-patroni-" + clusterID.String()}, NotBefore: now.Add(-time.Hour), NotAfter: now.Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	caDER, err := x509.CreateCertificate(rand.Reader, ca, ca, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	ca, err = x509.ParseCertificate(caDER)
	if err != nil {
		t.Fatal(err)
	}
	caPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER})
	roots := x509.NewCertPool()
	roots.AppendCertsFromPEM(caPEM)
	materials := make([]postgres.PatroniTLSMaterial, len(addresses))
	for index, address := range addresses {
		key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		leaf := &x509.Certificate{SerialNumber: big.NewInt(int64(index + 2)), NotBefore: now.Add(-time.Hour), NotAfter: now.Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}, IPAddresses: []net.IP{net.ParseIP(address.String()), net.ParseIP("127.0.0.1")}}
		der, err := x509.CreateCertificate(rand.Reader, leaf, ca, &key.PublicKey, caKey)
		if err != nil {
			t.Fatal(err)
		}
		private, err := x509.MarshalPKCS8PrivateKey(key)
		if err != nil {
			t.Fatal(err)
		}
		materials[index] = postgres.PatroniTLSMaterial{CA: caPEM, Certificate: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), PrivateKey: pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: private})}
	}
	dcsPort, err := network.ParsePort("2379/tcp")
	if err != nil {
		t.Fatal(err)
	}
	dcsContainer, err := api.ContainerCreate(ctx, client.ContainerCreateOptions{
		Name:             "synod-patroni-dcs-" + clusterID.String(),
		Config:           &container.Config{Image: image, Entrypoint: []string{"etcd"}, Cmd: []string{"--data-dir", "/tmp/etcd", "--listen-client-urls", "https://0.0.0.0:2379", "--advertise-client-urls", "https://" + net.JoinHostPort(addresses[0].String(), "2379"), "--cert-file", "/etc/synod-dcs/member.crt", "--key-file", "/etc/synod-dcs/member.key", "--trusted-ca-file", "/etc/synod-dcs/ca.crt", "--client-cert-auth=true", "--listen-peer-urls", "http://127.0.0.1:2380", "--initial-advertise-peer-urls", "http://127.0.0.1:2380", "--initial-cluster", "default=http://127.0.0.1:2380"}, ExposedPorts: network.PortSet{dcsPort: struct{}{}}},
		HostConfig:       &container.HostConfig{NetworkMode: container.NetworkMode(networkName), PortBindings: network.PortMap{dcsPort: []network.PortBinding{{HostIP: netip.MustParseAddr("127.0.0.1")}}}},
		NetworkingConfig: &network.NetworkingConfig{EndpointsConfig: map[string]*network.EndpointSettings{networkName: {IPAMConfig: &network.EndpointIPAMConfig{IPv4Address: addresses[0]}}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanup, finish := context.WithTimeout(context.Background(), 20*time.Second)
		defer finish()
		if _, err := api.ContainerRemove(cleanup, dcsContainer.ID, client.ContainerRemoveOptions{Force: true, RemoveVolumes: true}); err != nil {
			t.Error(err)
		}
	})
	var archive bytes.Buffer
	writer := tar.NewWriter(&archive)
	if err := writer.WriteHeader(&tar.Header{Name: "synod-dcs/", Typeflag: tar.TypeDir, Mode: 0700}); err != nil {
		t.Fatal(err)
	}
	for name, data := range map[string][]byte{"ca.crt": caPEM, "member.crt": materials[0].Certificate, "member.key": materials[0].PrivateKey} {
		if err := writer.WriteHeader(&tar.Header{Name: "synod-dcs/" + name, Mode: 0600, Size: int64(len(data))}); err != nil {
			t.Fatal(err)
		}
		if _, err := writer.Write(data); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := api.CopyToContainer(ctx, dcsContainer.ID, client.CopyToContainerOptions{DestinationPath: "/etc", Content: &archive, CopyUIDGID: true}); err != nil {
		t.Fatal(err)
	}
	if _, err := api.ContainerStart(ctx, dcsContainer.ID, client.ContainerStartOptions{}); err != nil {
		t.Fatal(err)
	}
	secrets := runtime.Secrets{}
	refs := make([]*agentv1.SecretReference, 0, 7)
	for _, purpose := range []string{"superuser", "application", "replication", "patroni_rewind", "patroni_rest", "patroni_member_tls", "patroni_dcs_tls"} {
		refs = append(refs, &agentv1.SecretReference{CredentialId: uuid.NewString(), Version: 1, Purpose: purpose})
		secrets[purpose] = []byte(uuid.NewString())
	}
	secrets["patroni_dcs_tls"], err = json.Marshal(materials[0])
	if err != nil {
		t.Fatal(err)
	}
	// copyRecord replaces one private native record with the ownership of a saved record header.
	copyRecord := func(name, directory, filename string, data []byte, owner *tar.Header) {
		t.Helper()
		var record bytes.Buffer
		recordWriter := tar.NewWriter(&record)
		if err := recordWriter.WriteHeader(&tar.Header{Name: filename, Mode: 0600, Uid: owner.Uid, Gid: owner.Gid, Size: int64(len(data))}); err != nil {
			t.Fatal(err)
		}
		if _, err := recordWriter.Write(data); err != nil {
			t.Fatal(err)
		}
		if err := recordWriter.Close(); err != nil {
			t.Fatal(err)
		}
		if _, err := api.CopyToContainer(ctx, name, client.CopyToContainerOptions{DestinationPath: directory, Content: &record, CopyUIDGID: true}); err != nil {
			t.Fatal(err)
		}
	}
	// readRecord returns one private native record and its archive header.
	readRecord := func(name, filename string) ([]byte, *tar.Header) {
		t.Helper()
		copied, err := api.CopyFromContainer(ctx, name, client.CopyFromContainerOptions{SourcePath: filename})
		if err != nil {
			t.Fatal(err)
		}
		defer copied.Content.Close()
		reader := tar.NewReader(copied.Content)
		header, err := reader.Next()
		if err != nil {
			t.Fatal(err)
		}
		data, err := io.ReadAll(io.LimitReader(reader, 80<<10))
		if err != nil {
			t.Fatal(err)
		}
		return data, header
	}
	var writes atomic.Int64
	counting, err := client.New(client.WithHost(os.Getenv("SYNOD_TEST_DOCKER_HOST")), client.WithResponseHook(func(response *http.Response) {
		if response.Request.Method != http.MethodGet && response.Request.Method != http.MethodHead {
			writes.Add(1)
		}
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer counting.Close()
	reader, err := New(counting, r.options)
	if err != nil {
		t.Fatal(err)
	}

	// The first member's application hook dies after it records its attempt.
	// Patroni then cancels the initial bootstrap, removes the initialize key, and moves the data aside.
	// The later members bootstrap the same cluster again. The stopped member releases the address they expect.
	cancelledVolume := uuid.NewString()
	if _, err := r.Execute(ctx, runtimeCommand(t, r, clusterID, &agentv1.CommandInput{ResourceId: cancelledVolume, Payload: &agentv1.CommandInput_EnsureVolume{EnsureVolume: &agentv1.EnsureVolume{VolumeId: cancelledVolume}}}), nil); err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	cancelledPort := uint32(listener.Addr().(*net.TCPAddr).Port)
	listener.Close()
	cancelledRefs := make([]*agentv1.SecretReference, len(refs))
	for index, ref := range refs {
		cancelledRefs[index] = proto.Clone(ref).(*agentv1.SecretReference)
	}
	cancelledRefs[6].CredentialId = uuid.NewString()
	cancelled := &agentv1.EnsureContainer{Engine: "postgres", ImageDigest: image, VolumeId: cancelledVolume, NetworkId: networkID, NetworkMode: agentv1.NetworkMode_NETWORK_MODE_BRIDGE, MemoryBytes: 512 << 20, CpuMillis: 1000, Ports: []*agentv1.PortBinding{{HostAddress: "127.0.0.1", HostPort: cancelledPort, ContainerPort: 5432, Transport: "tcp"}}, Postgres: &agentv1.PostgresContainerConfig{Port: 5432, SuperuserCredentialId: refs[0].CredentialId, SuperuserCredentialVersion: 1, ApplicationCredentialId: refs[1].CredentialId, ApplicationCredentialVersion: 1, ReplicationCredentialId: refs[2].CredentialId, ReplicationCredentialVersion: 1, Patroni: &agentv1.PatroniContainerConfig{Address: addresses[1].String(), RestPort: 8008, DcsEndpoints: []string{net.JoinHostPort(addresses[0].String(), "2379")}, PeerNetworks: []string{subnet.String()}, ClientNetworks: []string{subnet.String()}, ProvisionId: uuid.NewString(), AllowInitialization: true, RequiredFreeBytes: 1 << 30, Credentials: cancelledRefs}}}
	cancelledPrepare := runtimeCommand(t, r, clusterID, &agentv1.CommandInput{ResourceId: uuid.NewString(), Secrets: cancelledRefs, Payload: &agentv1.CommandInput_EnsureContainer{EnsureContainer: cancelled}})
	secrets["patroni_member_tls"], err = json.Marshal(materials[1])
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.Execute(ctx, cancelledPrepare, secrets); err != nil {
		t.Fatal("prepare cancelled member", err)
	}
	cancelledName := "synod-instance-" + cancelledPrepare.Input.ResourceId
	if _, err := r.Execute(ctx, runtimeCommand(t, r, clusterID, &agentv1.CommandInput{ResourceId: cancelledPrepare.Input.ResourceId, Payload: &agentv1.CommandInput_StartPatroniMember{StartPatroniMember: &agentv1.StartPatroniMember{Container: cancelled, AllowInitialization: true}}}), nil); err != nil && !errors.Is(err, runtime.ErrOutcomeUnknown) {
		t.Fatal("start cancelled member", err)
	}
	killer := exec.CommandContext(ctx, "docker", "--host", os.Getenv("SYNOD_TEST_DOCKER_HOST"), "exec", cancelledName, "/usr/bin/python3", "-c", `import os, signal, time
deadline = time.monotonic() + 90
while time.monotonic() < deadline:
    for name in os.listdir('/proc'):
        if not name.isdecimal():
            continue
        try:
            with open('/proc/' + name + '/cmdline', 'rb') as source:
                args = source.read().split(b'\x00')
        except OSError:
            continue
        if b'/run/synod-patroni/application.py' in args and b'bootstrap' in args:
            attempt = '/var/lib/synod-patroni/application-attempt.json'
            # The hook creates the record, then writes it. Wait for the written record.
            while not os.path.exists(attempt) or os.path.getsize(attempt) == 0:
                if time.monotonic() > deadline:
                    raise SystemExit(1)
            os.kill(int(name), signal.SIGKILL)
            raise SystemExit(0)
raise SystemExit(1)
`)
	if output, err := killer.CombinedOutput(); err != nil {
		t.Fatalf("application hook was not interrupted: %v %s", err, output)
	}
	deadline := time.Now().Add(60 * time.Second)
	for {
		_, err := api.ContainerStatPath(ctx, cancelledName, client.ContainerStatPathOptions{Path: postgres.PatroniDataDirectory + ".failed"})
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("Patroni did not move the cancelled bootstrap data", err)
		}
		time.Sleep(250 * time.Millisecond)
	}
	if _, err := r.Execute(ctx, runtimeCommand(t, r, clusterID, &agentv1.CommandInput{ResourceId: cancelledPrepare.Input.ResourceId, Payload: &agentv1.CommandInput_StopContainer{StopContainer: &agentv1.StopContainer{GracePeriod: durationpb.New(10 * time.Second)}}}), nil); err != nil {
		t.Fatal("stop cancelled member", err)
	}
	cancelledProof := runtimeCommand(t, r, clusterID, &agentv1.CommandInput{ResourceId: cancelledPrepare.Input.ResourceId, Payload: &agentv1.CommandInput_VerifyPatroniStartFailure{VerifyPatroniStartFailure: &agentv1.VerifyPatroniStartFailure{Container: cancelled}}})
	cancelledStep := &agentv1.PlanStep{Command: cancelledProof, Postcondition: agentv1.Postcondition_POSTCONDITION_PATRONI_START_FAILED}
	proved, err := reader.Execute(ctx, cancelledProof, nil)
	if err != nil || proved.Effects[0].State != "patroni_bootstrap_cancelled" || operation.CheckStepEffects(cancelledStep, proved.Effects) != nil {
		t.Fatal("cancelled initial bootstrap was not proved", proved, err)
	}
	proved, err = reader.Recover(ctx, cancelledProof, proved.Effects)
	if err != nil || operation.CheckStepEffects(cancelledStep, proved.Effects) != nil || writes.Load() != 0 {
		t.Fatal("recovered cancelled-bootstrap proof differs or wrote", proved, writes.Load(), err)
	}
	receipt, owner := readRecord(cancelledName, "/var/lib/synod-patroni/application-attempt.json")
	var changed map[string]any
	if err := json.Unmarshal(receipt, &changed); err != nil {
		t.Fatal(err)
	}
	changed["system_id"] = "1"
	mismatched, err := json.Marshal(changed)
	if err != nil {
		t.Fatal(err)
	}
	copyRecord(cancelledName, "/var/lib/synod-patroni", "application-attempt.json", mismatched, owner)
	if _, err := r.Execute(ctx, cancelledProof, nil); !errors.Is(err, runtime.ErrConfigurationConflict) {
		t.Fatal("mismatched application attempt proved a cancelled bootstrap", err)
	}
	// A hook killed inside its first write leaves an empty record and no SQL effect.
	copyRecord(cancelledName, "/var/lib/synod-patroni", "application-attempt.json", nil, owner)
	if _, err := r.Execute(ctx, cancelledProof, nil); err != nil {
		t.Fatal("empty application attempt did not prove a cancelled bootstrap", err)
	}
	copyRecord(cancelledName, "/var/lib/synod-patroni", "application-attempt.json", receipt, owner)
	attemptRecord, attemptOwner := readRecord(cancelledName, "/var/lib/synod-patroni/attempt.json")
	copyRecord(cancelledName, "/var/lib/synod-patroni", "attempt.json", bytes.Replace(attemptRecord, []byte(`"init"`), []byte(`"clone"`), 1), attemptOwner)
	if _, err := r.Execute(ctx, cancelledProof, nil); !errors.Is(err, runtime.ErrConfigurationConflict) {
		t.Fatal("completed clone attempt proved a cancelled bootstrap", err)
	}
	copyRecord(cancelledName, "/var/lib/synod-patroni", "attempt.json", attemptRecord, attemptOwner)
	if _, err := r.Execute(ctx, cancelledProof, nil); err != nil {
		t.Fatal("restored cancelled bootstrap was not proved", err)
	}
	var present bytes.Buffer
	presentWriter := tar.NewWriter(&present)
	if err := presentWriter.WriteHeader(&tar.Header{Name: "patroni/", Typeflag: tar.TypeDir, Mode: 0700, Uid: owner.Uid, Gid: owner.Gid}); err != nil {
		t.Fatal(err)
	}
	if err := presentWriter.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := api.CopyToContainer(ctx, cancelledName, client.CopyToContainerOptions{DestinationPath: postgres.DataDirectory, Content: &present, CopyUIDGID: true}); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Execute(ctx, cancelledProof, nil); !errors.Is(err, runtime.ErrConfigurationConflict) {
		t.Fatal("present native data proved a cancelled bootstrap", err)
	}

	commands := make([]*agentv1.Command, 2)
	databases := make([]*pgx.ConnConfig, 2)
	for index := range 2 {
		volumeID := uuid.NewString()
		volume := runtimeCommand(t, r, clusterID, &agentv1.CommandInput{ResourceId: volumeID, Payload: &agentv1.CommandInput_EnsureVolume{EnsureVolume: &agentv1.EnsureVolume{VolumeId: volumeID}}})
		if _, err := r.Execute(ctx, volume, nil); err != nil {
			t.Fatal(err)
		}
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		port := uint32(listener.Addr().(*net.TCPAddr).Port)
		listener.Close()
		memberRefs := make([]*agentv1.SecretReference, len(refs))
		for index, ref := range refs {
			memberRefs[index] = proto.Clone(ref).(*agentv1.SecretReference)
		}
		memberRefs[6].CredentialId = uuid.NewString()
		configuration := &agentv1.EnsureContainer{Engine: "postgres", ImageDigest: image, VolumeId: volumeID, NetworkId: networkID, NetworkMode: agentv1.NetworkMode_NETWORK_MODE_BRIDGE, MemoryBytes: 512 << 20, CpuMillis: 1000, Ports: []*agentv1.PortBinding{{HostAddress: "127.0.0.1", HostPort: port, ContainerPort: 5432, Transport: "tcp"}}, Postgres: &agentv1.PostgresContainerConfig{Port: 5432, SuperuserCredentialId: refs[0].CredentialId, SuperuserCredentialVersion: 1, ApplicationCredentialId: refs[1].CredentialId, ApplicationCredentialVersion: 1, ReplicationCredentialId: refs[2].CredentialId, ReplicationCredentialVersion: 1, Patroni: &agentv1.PatroniContainerConfig{Address: addresses[index+1].String(), RestPort: 8008, DcsEndpoints: []string{net.JoinHostPort(addresses[0].String(), "2379")}, PeerNetworks: []string{subnet.String()}, ClientNetworks: []string{subnet.String()}, ProvisionId: uuid.NewString(), AllowInitialization: true, RequiredFreeBytes: 1 << 30, Credentials: memberRefs}}}
		ensure := runtimeCommand(t, r, clusterID, &agentv1.CommandInput{ResourceId: uuid.NewString(), Secrets: memberRefs, Payload: &agentv1.CommandInput_EnsureContainer{EnsureContainer: configuration}})
		secrets["patroni_member_tls"], err = json.Marshal(materials[index+1])
		if err != nil {
			t.Fatal(err)
		}
		if _, err := r.Execute(ctx, ensure, secrets); err != nil {
			t.Fatalf("prepare member %d: %v", index, err)
		}
		commands[index] = runtimeCommand(t, r, clusterID, &agentv1.CommandInput{ResourceId: ensure.Input.ResourceId, Payload: &agentv1.CommandInput_StartPatroniMember{StartPatroniMember: &agentv1.StartPatroniMember{Container: configuration, AllowInitialization: true}}})
		denied := runtimeCommand(t, r, clusterID, &agentv1.CommandInput{ResourceId: ensure.Input.ResourceId, Payload: &agentv1.CommandInput_StartPatroniMember{StartPatroniMember: &agentv1.StartPatroniMember{Container: configuration}}})
		if _, err := r.Execute(ctx, denied, nil); !errors.Is(err, runtime.ErrStorageLost) {
			t.Fatal("empty member started without first-start permission", err)
		}
		active := r
		var dropped, blockInspection atomic.Bool
		var startCalls atomic.Int64
		if index == 0 {
			blockInspection.Store(true)
			faulty, err := client.New(client.WithHost(os.Getenv("SYNOD_TEST_DOCKER_HOST")), client.WithResponseHook(func(response *http.Response) {
				path := response.Request.URL.Path
				start := response.Request.Method == http.MethodPost && strings.HasSuffix(path, "/start")
				if start {
					startCalls.Add(1)
				}
				if start && response.StatusCode < 300 && dropped.CompareAndSwap(false, true) || dropped.Load() && blockInspection.Load() && response.Request.Method == http.MethodGet && strings.Contains(path, "/containers/") {
					response.Body.Close()
					response.StatusCode = http.StatusInternalServerError
					response.Body = io.NopCloser(iotest.ErrReader(io.ErrUnexpectedEOF))
				}
			}))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { faulty.Close() })
			active, err = New(faulty, r.options)
			if err != nil {
				t.Fatal(err)
			}
		}
		j, options, journalPath := runtimeJournal(t, commands[index])
		observed, err := active.Execute(ctx, commands[index], nil)
		if err != nil && !errors.Is(err, runtime.ErrOutcomeUnknown) {
			t.Fatalf("start member %d: %v / %v", index, err, errors.Unwrap(err))
		}
		if index == 0 && (!dropped.Load() || !errors.Is(err, runtime.ErrOutcomeUnknown)) {
			t.Fatal("lost successful start was not retained", err)
		}
		if err := j.Observe(commands[index].CommandId, observed.Effects); err != nil {
			t.Fatal(err)
		}
		if err := j.Close(); err != nil {
			t.Fatal(err)
		}
		options.AllowCreate = false
		reopened, openErr := journal.Open(journalPath, options)
		if openErr != nil {
			t.Fatal(openErr)
		}
		t.Cleanup(func() { reopened.Close() })
		retained, readErr := reopened.Get(commands[index].CommandId)
		if readErr != nil {
			t.Fatal(readErr)
		}
		blockInspection.Store(false)
		observed, err = active.Recover(ctx, commands[index], retained.Effects)
		deadline := time.Now().Add(60 * time.Second)
		step := &agentv1.PlanStep{Command: commands[index], Postcondition: agentv1.Postcondition_POSTCONDITION_CONTAINER_RUNNING}
		for err != nil || operation.CheckStepEffects(step, observed.Effects) != nil {
			if time.Now().After(deadline) {
				t.Fatalf("member %d did not finish startup: %+v %v", index, observed, err)
			}
			time.Sleep(250 * time.Millisecond)
			observed, err = active.Recover(ctx, commands[index], observed.Effects)
		}
		if err := operation.CheckStepEffects(&agentv1.PlanStep{Command: commands[index], Postcondition: agentv1.Postcondition_POSTCONDITION_CONTAINER_RUNNING}, observed.Effects); err != nil {
			t.Fatal("startup proof incomplete", observed, err)
		}
		if index == 0 && startCalls.Load() != 1 {
			t.Fatal("recovery repeated native startup")
		}
		if err := reopened.Complete(&agentv1.TerminalResult{Epoch: commands[index].Epoch, CommandId: commands[index].CommandId, InputHash: commands[index].InputHash, Sequence: 1, State: "succeeded", Effects: observed.Effects}); err != nil {
			t.Fatal(err)
		}
		config, err := pgx.ParseConfig("user=synod_app dbname=app sslmode=verify-full")
		if err != nil {
			t.Fatal(err)
		}
		config.Host, config.Port, config.Password = "127.0.0.1", uint16(port), string(secrets["application"])
		config.TLSConfig = &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots, ServerName: addresses[index+1].String()}
		config.Fallbacks = nil
		config.ConnectTimeout = 2 * time.Second
		databases[index] = config
	}
	primary, err := pgx.ConnectConfig(ctx, databases[0])
	if err != nil {
		t.Fatal(err)
	}
	if _, err := primary.Exec(ctx, "CREATE TABLE runtime_native(value int); INSERT INTO runtime_native VALUES(152)"); err != nil {
		t.Fatal(err)
	}
	primary.Close(ctx)
	replica, err := pgx.ConnectConfig(ctx, databases[1])
	if err != nil {
		t.Fatal(err)
	}
	deadline = time.Now().Add(20 * time.Second)
	for {
		var value int
		var standby bool
		err := replica.QueryRow(ctx, "SELECT value, pg_is_in_recovery() FROM runtime_native").Scan(&value, &standby)
		if err == nil && value == 152 && standby {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("native replica did not copy data", err)
		}
		time.Sleep(100 * time.Millisecond)
	}
	replica.Close(ctx)
	stop := runtimeCommand(t, r, clusterID, &agentv1.CommandInput{ResourceId: commands[0].Input.ResourceId, Payload: &agentv1.CommandInput_StopContainer{StopContainer: &agentv1.StopContainer{GracePeriod: durationpb.New(10 * time.Second)}}})
	if _, err := r.Execute(ctx, stop, nil); err != nil {
		t.Fatal("stop native primary", err)
	}
	if _, err := r.Execute(ctx, commands[0], nil); !errors.Is(err, runtime.ErrConfigurationConflict) {
		t.Fatal("completed member accepted first-start permission again", err)
	}
	deadline = time.Now().Add(30 * time.Second)
	for {
		promoted, err := pgx.ConnectConfig(ctx, databases[1])
		var recovery bool
		if err == nil {
			err = promoted.QueryRow(ctx, "SELECT pg_is_in_recovery()").Scan(&recovery)
			promoted.Close(ctx)
		}
		if err == nil && !recovery {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("native follower did not promote", err)
		}
		time.Sleep(100 * time.Millisecond)
	}
	restart := runtimeCommand(t, r, clusterID, &agentv1.CommandInput{ResourceId: commands[0].Input.ResourceId, Payload: &agentv1.CommandInput_StartPatroniMember{StartPatroniMember: &agentv1.StartPatroniMember{Container: commands[0].Input.GetStartPatroniMember().Container}}})
	name := "synod-instance-" + restart.Input.ResourceId
	saved, err := api.CopyFromContainer(ctx, name, client.CopyFromContainerOptions{SourcePath: "/var/lib/synod-patroni/complete.json"})
	if err != nil {
		t.Fatal(err)
	}
	savedArchive, err := io.ReadAll(io.LimitReader(saved.Content, 80<<10))
	saved.Content.Close()
	if err != nil {
		t.Fatal(err)
	}
	header, err := tar.NewReader(bytes.NewReader(savedArchive)).Next()
	if err != nil {
		t.Fatal(err)
	}
	var conflicting bytes.Buffer
	conflictWriter := tar.NewWriter(&conflicting)
	if err := conflictWriter.WriteHeader(&tar.Header{Name: "complete.json", Mode: 0600, Uid: header.Uid, Gid: header.Gid, Size: 2}); err != nil {
		t.Fatal(err)
	}
	if _, err := conflictWriter.Write([]byte("{}")); err != nil {
		t.Fatal(err)
	}
	if err := conflictWriter.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := api.CopyToContainer(ctx, name, client.CopyToContainerOptions{DestinationPath: "/var/lib/synod-patroni", Content: &conflicting, CopyUIDGID: true}); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Execute(ctx, restart, nil); !errors.Is(err, runtime.ErrConfigurationConflict) {
		t.Fatal("restart accepted conflicting native completion", err)
	}
	stopped, err := api.ContainerInspect(ctx, name, client.ContainerInspectOptions{})
	if err != nil || stopped.Container.State.Running || stopped.Container.HostConfig.RestartPolicy.Name != container.RestartPolicyDisabled {
		t.Fatal("refused restart changed the stopped member", err)
	}
	if _, err := api.CopyToContainer(ctx, name, client.CopyToContainerOptions{DestinationPath: "/var/lib/synod-patroni", Content: bytes.NewReader(savedArchive), CopyUIDGID: true}); err != nil {
		t.Fatal(err)
	}
	observed, err := r.Execute(ctx, restart, nil)
	if err != nil && !errors.Is(err, runtime.ErrOutcomeUnknown) {
		t.Fatalf("restart native member: %v / %v", err, errors.Unwrap(err))
	}
	deadline = time.Now().Add(60 * time.Second)
	for err != nil {
		if time.Now().After(deadline) {
			t.Fatal("native restart did not finish", observed, err)
		}
		time.Sleep(250 * time.Millisecond)
		observed, err = r.Recover(ctx, restart, observed.Effects)
	}
	rejoined, err := pgx.ConnectConfig(ctx, databases[0])
	if err != nil {
		t.Fatal(err)
	}
	defer rejoined.Close(ctx)
	var recovery bool
	var value int
	if err := rejoined.QueryRow(ctx, "SELECT value, pg_is_in_recovery() FROM runtime_native").Scan(&value, &recovery); err != nil || value != 152 || !recovery {
		t.Fatal("restarted member did not rejoin with data", value, recovery, err)
	}
	rejoined.Close(ctx)
	finalStop := runtimeCommand(t, r, clusterID, &agentv1.CommandInput{ResourceId: restart.Input.ResourceId, Payload: &agentv1.CommandInput_StopContainer{StopContainer: &agentv1.StopContainer{GracePeriod: durationpb.New(10 * time.Second)}}})
	if _, err := r.Execute(ctx, finalStop, nil); err != nil {
		t.Fatal(err)
	}
	resume := runtimeCommand(t, r, clusterID, &agentv1.CommandInput{ResourceId: restart.Input.ResourceId, Payload: &agentv1.CommandInput_StartPatroniMember{StartPatroniMember: &agentv1.StartPatroniMember{Container: restart.Input.GetStartPatroniMember().Container}}})
	var updateLost atomic.Bool
	var unexpectedStarts atomic.Int64
	faulty, err := client.New(client.WithHost(os.Getenv("SYNOD_TEST_DOCKER_HOST")), client.WithResponseHook(func(response *http.Response) {
		path := response.Request.URL.Path
		if response.Request.Method == http.MethodPost && strings.HasSuffix(path, "/start") {
			unexpectedStarts.Add(1)
		}
		if response.Request.Method == http.MethodPost && strings.HasSuffix(path, "/update") && response.StatusCode < 300 && updateLost.CompareAndSwap(false, true) {
			response.Body.Close()
			response.StatusCode = http.StatusInternalServerError
			response.Body = io.NopCloser(iotest.ErrReader(io.ErrUnexpectedEOF))
		}
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer faulty.Close()
	injected, err := New(faulty, r.options)
	if err != nil {
		t.Fatal(err)
	}
	observed, err = injected.Execute(ctx, resume, nil)
	if !errors.Is(err, runtime.ErrOutcomeUnknown) || !updateLost.Load() {
		t.Fatal("lost restart-policy update was not retained", err)
	}
	observed, err = injected.Recover(ctx, resume, observed.Effects)
	if !errors.Is(err, runtime.ErrOutcomeUnknown) || unexpectedStarts.Load() != 0 || operation.CheckStepEffects(&agentv1.PlanStep{Command: resume, Postcondition: agentv1.Postcondition_POSTCONDITION_CONTAINER_RUNNING}, observed.Effects) == nil {
		t.Fatal("recovery started or completed unstarted native work", err)
	}
	unchanged, err := api.ContainerInspect(ctx, name, client.ContainerInspectOptions{})
	if err != nil || unchanged.Container.State.Running || unchanged.Container.HostConfig.RestartPolicy.Name != container.RestartPolicyUnlessStopped {
		t.Fatal("lost update fixture did not reach the native boundary", err)
	}

	// A completed member is a known refusal of the failed-attempt precondition.
	completedProof := runtimeCommand(t, r, clusterID, &agentv1.CommandInput{ResourceId: resume.Input.ResourceId, Payload: &agentv1.CommandInput_VerifyPatroniStartFailure{VerifyPatroniStartFailure: &agentv1.VerifyPatroniStartFailure{Container: resume.Input.GetStartPatroniMember().Container}}})
	if _, err := r.Execute(ctx, completedProof, nil); !errors.Is(err, runtime.ErrConfigurationConflict) {
		t.Fatal("completed member proved a failed storage attempt", err)
	}

	// A wrong replication password fails the first copy after the guard records its attempt.
	// The healthy leader keeps its lease, so the copy is attempted without a timing window.
	secrets["replication"] = []byte(uuid.NewString())
	failedVolume := uuid.NewString()
	if _, err := r.Execute(ctx, runtimeCommand(t, r, clusterID, &agentv1.CommandInput{ResourceId: failedVolume, Payload: &agentv1.CommandInput_EnsureVolume{EnsureVolume: &agentv1.EnsureVolume{VolumeId: failedVolume}}}), nil); err != nil {
		t.Fatal(err)
	}
	listener, err = net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	failedPort := uint32(listener.Addr().(*net.TCPAddr).Port)
	listener.Close()
	failed := proto.Clone(commands[1].Input.GetStartPatroniMember().Container).(*agentv1.EnsureContainer)
	failed.VolumeId, failed.Ports[0].HostPort = failedVolume, failedPort
	// The bridge fixture assigns the lowest free address. The stopped first member released it.
	failed.Postgres.Patroni.Address, failed.Postgres.Patroni.ProvisionId = addresses[1].String(), uuid.NewString()
	failed.Postgres.Patroni.Credentials[6].CredentialId = uuid.NewString()
	prepare := runtimeCommand(t, r, clusterID, &agentv1.CommandInput{ResourceId: uuid.NewString(), Secrets: failed.Postgres.Patroni.Credentials, Payload: &agentv1.CommandInput_EnsureContainer{EnsureContainer: failed}})
	secrets["patroni_member_tls"], err = json.Marshal(materials[1])
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.Execute(ctx, prepare, secrets); err != nil {
		t.Fatal("prepare failing member", err)
	}
	failedName := "synod-instance-" + prepare.Input.ResourceId
	proof := runtimeCommand(t, r, clusterID, &agentv1.CommandInput{ResourceId: prepare.Input.ResourceId, Payload: &agentv1.CommandInput_VerifyPatroniStartFailure{VerifyPatroniStartFailure: &agentv1.VerifyPatroniStartFailure{Container: failed}}})
	if _, err := r.Execute(ctx, proof, nil); !errors.Is(err, runtime.ErrConfigurationConflict) {
		t.Fatal("never-started member proved a failed storage attempt", err)
	}
	firstStart := runtimeCommand(t, r, clusterID, &agentv1.CommandInput{ResourceId: prepare.Input.ResourceId, Payload: &agentv1.CommandInput_StartPatroniMember{StartPatroniMember: &agentv1.StartPatroniMember{Container: failed, AllowInitialization: true}}})
	if _, err := r.Execute(ctx, firstStart, nil); err != nil && !errors.Is(err, runtime.ErrOutcomeUnknown) {
		t.Fatal("start failing member", err)
	}
	assigned, err := api.ContainerInspect(ctx, failedName, client.ContainerInspectOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if got := assigned.Container.NetworkSettings.Networks[networkName].IPAddress; got != addresses[1] {
		t.Fatalf("failing member received %s and declared %s", got, addresses[1])
	}
	deadline = time.Now().Add(60 * time.Second)
	for {
		_, err := api.ContainerStatPath(ctx, failedName, client.ContainerStatPathOptions{Path: "/var/lib/synod-patroni/attempt.json"})
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("failing member did not record a copy attempt", err)
		}
		time.Sleep(250 * time.Millisecond)
	}
	if _, err := r.Execute(ctx, proof, nil); !errors.Is(err, runtime.ErrOutcomeUnknown) {
		t.Fatal("running member proved a failed storage attempt", err)
	}
	if _, err := r.Execute(ctx, runtimeCommand(t, r, clusterID, &agentv1.CommandInput{ResourceId: prepare.Input.ResourceId, Payload: &agentv1.CommandInput_StopContainer{StopContainer: &agentv1.StopContainer{GracePeriod: durationpb.New(10 * time.Second)}}}), nil); err != nil {
		t.Fatal("stop failing member", err)
	}
	writes.Store(0)
	before, err := api.ContainerInspect(ctx, failedName, client.ContainerInspectOptions{})
	if err != nil {
		t.Fatal(err)
	}
	proofStep := &agentv1.PlanStep{Command: proof, Postcondition: agentv1.Postcondition_POSTCONDITION_PATRONI_START_FAILED}
	observed, err = reader.Execute(ctx, proof, nil)
	if err != nil || operation.CheckStepEffects(proofStep, observed.Effects) != nil {
		t.Fatal("stopped failed copy was not proved", observed, err)
	}
	// A lost reply recovers the same proof from the retained effect.
	observed, err = reader.Recover(ctx, proof, observed.Effects)
	if err != nil || operation.CheckStepEffects(proofStep, observed.Effects) != nil {
		t.Fatal("recovered proof differs", observed, err)
	}
	after, err := api.ContainerInspect(ctx, failedName, client.ContainerInspectOptions{})
	if err != nil || writes.Load() != 0 || after.Container.State.Running || after.Container.State.StartedAt != before.Container.State.StartedAt || after.Container.State.FinishedAt != before.Container.State.FinishedAt {
		t.Fatal("failure proof changed the member", writes.Load(), err)
	}

	// Substituted records check the accepted methods and the completion refusal on the same stopped member.
	saved, err = api.CopyFromContainer(ctx, failedName, client.CopyFromContainerOptions{SourcePath: "/var/lib/synod-patroni/attempt.json"})
	if err != nil {
		t.Fatal(err)
	}
	savedArchive, err = io.ReadAll(io.LimitReader(saved.Content, 80<<10))
	saved.Content.Close()
	if err != nil {
		t.Fatal(err)
	}
	header, err = tar.NewReader(bytes.NewReader(savedArchive)).Next()
	if err != nil {
		t.Fatal(err)
	}
	identity := patroniIdentity{Installation: r.options.InstallationID.String(), Cluster: clusterID.String(), Resource: prepare.Input.ResourceId, Volume: failedVolume, Provision: failed.Postgres.Patroni.ProvisionId, Format: 1}
	write := func(name string, value any) {
		t.Helper()
		data, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		copyRecord(failedName, "/var/lib/synod-patroni", name, data, header)
	}
	type attempt struct {
		Identity patroniIdentity `json:"identity"`
		Method   string          `json:"method"`
	}
	write("attempt.json", attempt{identity, "init"})
	if _, err := r.Execute(ctx, proof, nil); err != nil {
		t.Fatal("stopped failed init attempt was not proved", err)
	}
	write("attempt.json", attempt{identity, "restore"})
	if _, err := r.Execute(ctx, proof, nil); !errors.Is(err, runtime.ErrConfigurationConflict) {
		t.Fatal("unknown attempt method proved a failure", err)
	}
	if _, err := api.CopyToContainer(ctx, failedName, client.CopyToContainerOptions{DestinationPath: "/var/lib/synod-patroni", Content: bytes.NewReader(savedArchive), CopyUIDGID: true}); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Execute(ctx, proof, nil); err != nil {
		t.Fatal("restored failed copy was not proved", err)
	}
	write("application-attempt.json", map[string]string{"system_id": "1"})
	if _, err := r.Execute(ctx, proof, nil); !errors.Is(err, runtime.ErrConfigurationConflict) {
		t.Fatal("application record proved a failed storage attempt", err)
	}

	// The guard writes both identities, then refuses for space before any attempt record.
	refusedVolume := uuid.NewString()
	if _, err := r.Execute(ctx, runtimeCommand(t, r, clusterID, &agentv1.CommandInput{ResourceId: refusedVolume, Payload: &agentv1.CommandInput_EnsureVolume{EnsureVolume: &agentv1.EnsureVolume{VolumeId: refusedVolume}}}), nil); err != nil {
		t.Fatal(err)
	}
	refused := proto.Clone(failed).(*agentv1.EnsureContainer)
	refused.VolumeId, refused.Postgres.Patroni.ProvisionId, refused.Postgres.Patroni.RequiredFreeBytes = refusedVolume, uuid.NewString(), 1<<50
	refused.Postgres.Patroni.Credentials[6].CredentialId = uuid.NewString()
	refusedPrepare := runtimeCommand(t, r, clusterID, &agentv1.CommandInput{ResourceId: uuid.NewString(), Secrets: refused.Postgres.Patroni.Credentials, Payload: &agentv1.CommandInput_EnsureContainer{EnsureContainer: refused}})
	if _, err := r.Execute(ctx, refusedPrepare, secrets); err != nil {
		t.Fatal("prepare space-refused member", err)
	}
	refusedName := "synod-instance-" + refusedPrepare.Input.ResourceId
	if _, err := r.Execute(ctx, runtimeCommand(t, r, clusterID, &agentv1.CommandInput{ResourceId: refusedPrepare.Input.ResourceId, Payload: &agentv1.CommandInput_StartPatroniMember{StartPatroniMember: &agentv1.StartPatroniMember{Container: refused, AllowInitialization: true}}}), nil); err != nil && !errors.Is(err, runtime.ErrOutcomeUnknown) {
		t.Fatal("start space-refused member", err)
	}
	deadline = time.Now().Add(60 * time.Second)
	for {
		_, err := api.ContainerStatPath(ctx, refusedName, client.ContainerStatPathOptions{Path: postgres.DataDirectory + "/.synod-patroni-identity.json"})
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("space-refused member did not record its identity", err)
		}
		time.Sleep(250 * time.Millisecond)
	}
	if _, err := r.Execute(ctx, runtimeCommand(t, r, clusterID, &agentv1.CommandInput{ResourceId: refusedPrepare.Input.ResourceId, Payload: &agentv1.CommandInput_StopContainer{StopContainer: &agentv1.StopContainer{GracePeriod: durationpb.New(10 * time.Second)}}}), nil); err != nil {
		t.Fatal("stop space-refused member", err)
	}
	refusedProof := runtimeCommand(t, r, clusterID, &agentv1.CommandInput{ResourceId: refusedPrepare.Input.ResourceId, Payload: &agentv1.CommandInput_VerifyPatroniStartFailure{VerifyPatroniStartFailure: &agentv1.VerifyPatroniStartFailure{Container: refused}}})
	refusedStep := &agentv1.PlanStep{Command: refusedProof, Postcondition: agentv1.Postcondition_POSTCONDITION_PATRONI_START_FAILED}
	proveState := func(want string) {
		t.Helper()
		writes.Store(0)
		observed, err := reader.Execute(ctx, refusedProof, nil)
		if err != nil || observed.Effects[0].State != want || operation.CheckStepEffects(refusedStep, observed.Effects) != nil {
			t.Fatalf("want %s: %v %v", want, observed, err)
		}
		observed, err = reader.Recover(ctx, refusedProof, observed.Effects)
		if err != nil || observed.Effects[0].State != want || writes.Load() != 0 {
			t.Fatalf("recovered %s differs or wrote: %v %d %v", want, observed, writes.Load(), err)
		}
	}
	proveState("patroni_storage_not_started")
	if _, err := api.ContainerStatPath(ctx, refusedName, client.ContainerStatPathOptions{Path: "/var/lib/synod-patroni/attempt.json"}); err == nil {
		t.Fatal("space refusal recorded an attempt")
	}
	outsideIdentity, outsideOwner := readRecord(refusedName, "/var/lib/synod-patroni/identity.json")
	insideIdentity, insideOwner := readRecord(refusedName, postgres.DataDirectory+"/.synod-patroni-identity.json")
	copyRecord(refusedName, postgres.DataDirectory, ".synod-patroni-identity.json", nil, insideOwner)
	proveState("patroni_storage_not_started")
	// Records are written outside first. An empty outside record beside an inside record cannot occur.
	copyRecord(refusedName, "/var/lib/synod-patroni", "identity.json", nil, outsideOwner)
	if _, err := r.Execute(ctx, refusedProof, nil); !errors.Is(err, runtime.ErrConfigurationConflict) {
		t.Fatal("empty outside identity beside an inside identity proved a failure", err)
	}
	copyRecord(refusedName, "/var/lib/synod-patroni", "identity.json", outsideIdentity, outsideOwner)
	copyRecord(refusedName, postgres.DataDirectory, ".synod-patroni-identity.json", insideIdentity, insideOwner)
	copyRecord(refusedName, "/var/lib/synod-patroni", "attempt.json", nil, outsideOwner)
	proveState("patroni_storage_attempt_failed")
	copyRecord(refusedName, postgres.DataDirectory, ".synod-patroni-identity.json", nil, insideOwner)
	if _, err := r.Execute(ctx, refusedProof, nil); !errors.Is(err, runtime.ErrConfigurationConflict) {
		t.Fatal("empty attempt without an identity pair proved a failure", err)
	}
	copyRecord(refusedName, postgres.DataDirectory, ".synod-patroni-identity.json", insideIdentity, insideOwner)
	var staged bytes.Buffer
	stagedWriter := tar.NewWriter(&staged)
	if err := stagedWriter.WriteHeader(&tar.Header{Name: ".synod-patroni-stage/", Typeflag: tar.TypeDir, Mode: 0700, Uid: insideOwner.Uid, Gid: insideOwner.Gid}); err != nil {
		t.Fatal(err)
	}
	if err := stagedWriter.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := api.CopyToContainer(ctx, refusedName, client.CopyToContainerOptions{DestinationPath: postgres.DataDirectory, Content: &staged, CopyUIDGID: true}); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Execute(ctx, refusedProof, nil); !errors.Is(err, runtime.ErrConfigurationConflict) {
		t.Fatal("empty attempt beside staging data proved a failure", err)
	}

	// A running member proves nothing. A stopped member is not serving, but its storage
	// still restarts until the disk loses its data.
	proveMember := func(resource string, stored *agentv1.EnsureContainer, failed bool) (runtime.Result, error) {
		t.Helper()
		input := &agentv1.CommandInput{ResourceId: resource, Payload: &agentv1.CommandInput_VerifyPatroniMemberNotServing{VerifyPatroniMemberNotServing: &agentv1.VerifyPatroniMemberNotServing{Container: stored}}}
		if failed {
			input.Payload = &agentv1.CommandInput_VerifyPatroniMemberFailed{VerifyPatroniMemberFailed: &agentv1.VerifyPatroniMemberFailed{Container: stored}}
		}
		command := runtimeCommand(t, r, clusterID, input)
		writes.Store(0)
		observed, err := reader.Execute(ctx, command, nil)
		if err == nil {
			observed, err = reader.Recover(ctx, command, observed.Effects)
		}
		if writes.Load() != 0 {
			t.Fatal("a member proof wrote to Docker")
		}
		return observed, err
	}
	leader := commands[1].Input.GetStartPatroniMember().Container
	deadline = time.Now().Add(60 * time.Second)
	for {
		_, err := proveMember(commands[1].Input.ResourceId, leader, false)
		if errors.Is(err, runtime.ErrConfigurationConflict) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("a serving leader was not refused", err)
		}
		time.Sleep(time.Second)
	}
	retired := resume.Input.GetStartPatroniMember().Container
	if observed, err := proveMember(resume.Input.ResourceId, retired, false); err != nil || observed.Effects[0].State != "patroni_member_not_serving" {
		t.Fatalf("a stopped member was not proved not serving: %+v %v", observed, err)
	}
	if _, err := proveMember(resume.Input.ResourceId, retired, true); !errors.Is(err, runtime.ErrConfigurationConflict) {
		t.Fatal("restartable storage was proved failed", err)
	}
	// Certificate renewal recreates the stopped member with new protected files. It keeps the
	// volume and the storage records, so the member restarts on its own data. Each case stops
	// the recreate at one crash point and runs it again.
	resource := resume.Input.ResourceId
	memberName, retiredName := "synod-instance-"+resource, "synod-retired-"+resource
	issue := func(serial int64, address string) []byte {
		t.Helper()
		key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		leaf := &x509.Certificate{SerialNumber: big.NewInt(serial), NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}, IPAddresses: []net.IP{net.ParseIP(address), net.ParseIP("127.0.0.1")}}
		der, err := x509.CreateCertificate(rand.Reader, leaf, ca, &key.PublicKey, caKey)
		if err != nil {
			t.Fatal(err)
		}
		private, err := x509.MarshalPKCS8PrivateKey(key)
		if err != nil {
			t.Fatal(err)
		}
		encoded, err := json.Marshal(postgres.PatroniTLSMaterial{CA: caPEM, Certificate: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), PrivateKey: pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: private})})
		if err != nil {
			t.Fatal(err)
		}
		return encoded
	}
	// renew moves the named certificate credentials of a configuration to a new version.
	renew := func(previous *agentv1.EnsureContainer, purposes ...string) *agentv1.EnsureContainer {
		next := proto.Clone(previous).(*agentv1.EnsureContainer)
		for _, reference := range next.Postgres.Patroni.Credentials {
			if slices.Contains(purposes, reference.Purpose) {
				reference.Version++
			}
		}
		return next
	}
	recreate := func(target string, previous, next *agentv1.EnsureContainer) *agentv1.Command {
		t.Helper()
		return runtimeCommand(t, r, clusterID, &agentv1.CommandInput{ResourceId: target, Secrets: next.Postgres.Patroni.Credentials, Payload: &agentv1.CommandInput_RecreatePatroniMember{RecreatePatroniMember: &agentv1.RecreatePatroniMember{Previous: previous, Next: next}}})
	}
	recreated := func(command *agentv1.Command) {
		t.Helper()
		observed, err := r.Execute(ctx, command, secrets)
		step := &agentv1.PlanStep{Command: command, Postcondition: agentv1.Postcondition_POSTCONDITION_PATRONI_MEMBER_RECREATED}
		if err != nil || operation.CheckStepEffects(step, observed.Effects) != nil {
			t.Fatalf("recreate did not finish: %+v %v / %v", observed, err, errors.Unwrap(err))
		}
		if _, err := api.ContainerInspect(ctx, retiredName, client.ContainerInspectOptions{}); !errdefs.IsNotFound(err) {
			t.Fatal("recreate kept the retired container", err)
		}
		if observed, err := r.Recover(ctx, command, nil); err != nil || operation.CheckStepEffects(step, observed.Effects) != nil {
			t.Fatal("completed recreate was not recovered", observed, err)
		}
	}
	address := retired.Postgres.Patroni.Address
	// A running member and a container that holds another configuration are refused unchanged.
	if _, err := r.Execute(ctx, recreate(commands[1].Input.ResourceId, leader, renew(leader, "patroni_member_tls")), secrets); !errors.Is(err, runtime.ErrConfigurationConflict) {
		t.Fatal("a running member was recreated", err)
	}
	other := proto.Clone(retired).(*agentv1.EnsureContainer)
	other.Postgres.Patroni.ProvisionId = uuid.NewString()
	if _, err := r.Execute(ctx, recreate(resource, other, renew(other, "patroni_member_tls")), secrets); !errors.Is(err, runtime.ErrConfigurationConflict) {
		t.Fatal("a container with another configuration was recreated", err)
	}
	// Crash after the rename: only the retired container exists.
	secrets["patroni_member_tls"] = issue(100, address)
	second := renew(retired, "patroni_member_tls")
	if _, err := api.ContainerRename(ctx, memberName, client.ContainerRenameOptions{NewName: retiredName}); err != nil {
		t.Fatal(err)
	}
	command := recreate(resource, retired, second)
	if _, err := r.Recover(ctx, command, nil); !errors.Is(err, runtime.ErrOutcomeUnknown) {
		t.Fatal("a half recreate was proved", err)
	}
	recreated(command)
	// Crash after the create: the new container has no storage records yet.
	secrets["patroni_member_tls"] = issue(101, address)
	third := renew(second, "patroni_member_tls")
	if _, err := api.ContainerRename(ctx, memberName, client.ContainerRenameOptions{NewName: retiredName}); err != nil {
		t.Fatal(err)
	}
	command = recreate(resource, second, third)
	if err := r.createPatroni(ctx, command, third, secrets); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Recover(ctx, command, nil); !errors.Is(err, runtime.ErrOutcomeUnknown) {
		t.Fatal("a recreate without records was proved", err)
	}
	recreated(command)
	// Crash after the record copy: both containers hold the records. This case also renews the DCS client certificate.
	secrets["patroni_member_tls"], secrets["patroni_dcs_tls"] = issue(102, address), issue(103, addresses[0].String())
	fourth := renew(third, "patroni_member_tls", "patroni_dcs_tls")
	if _, err := api.ContainerRename(ctx, memberName, client.ContainerRenameOptions{NewName: retiredName}); err != nil {
		t.Fatal(err)
	}
	command = recreate(resource, third, fourth)
	if err := r.createPatroni(ctx, command, fourth, secrets); err != nil {
		t.Fatal(err)
	}
	records, err := api.CopyFromContainer(ctx, retiredName, client.CopyFromContainerOptions{SourcePath: patroniRecords})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := api.CopyToContainer(ctx, memberName, client.CopyToContainerOptions{DestinationPath: "/var/lib", Content: records.Content, CopyUIDGID: true}); err != nil {
		t.Fatal(err)
	}
	records.Content.Close()
	recreated(command)
	// A repeat of a completed recreate changes nothing.
	recreated(command)
	// The member restarts on its own data and serves the renewed certificate. Joining the
	// cluster also proves the renewed DCS client certificate.
	renewedStart := runtimeCommand(t, r, clusterID, &agentv1.CommandInput{ResourceId: resource, Payload: &agentv1.CommandInput_StartPatroniMember{StartPatroniMember: &agentv1.StartPatroniMember{Container: fourth}}})
	observed, err = r.Execute(ctx, renewedStart, nil)
	deadline = time.Now().Add(90 * time.Second)
	for err != nil || operation.CheckStepEffects(&agentv1.PlanStep{Command: renewedStart, Postcondition: agentv1.Postcondition_POSTCONDITION_CONTAINER_RUNNING}, observed.Effects) != nil {
		if err != nil && !errors.Is(err, runtime.ErrOutcomeUnknown) || time.Now().After(deadline) {
			t.Fatalf("renewed member did not restart: %+v %v / %v", observed, err, errors.Unwrap(err))
		}
		time.Sleep(250 * time.Millisecond)
		observed, err = r.Recover(ctx, renewedStart, observed.Effects)
	}
	deadline = time.Now().Add(60 * time.Second)
	for {
		renewed, err := pgx.ConnectConfig(ctx, databases[0])
		var serial int64
		if err == nil {
			value, recovery = 0, false
			err = renewed.QueryRow(ctx, "SELECT value, pg_is_in_recovery() FROM runtime_native").Scan(&value, &recovery)
			if state := renewed.PgConn().Conn().(*tls.Conn).ConnectionState(); len(state.PeerCertificates) > 0 {
				serial = state.PeerCertificates[0].SerialNumber.Int64()
			}
			renewed.Close(ctx)
		}
		if err == nil && value == 152 && recovery && serial == 102 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("renewed member did not rejoin with its data and new certificate", value, recovery, serial, err)
		}
		time.Sleep(250 * time.Millisecond)
	}
	if _, err := r.Execute(ctx, runtimeCommand(t, r, clusterID, &agentv1.CommandInput{ResourceId: resource, Payload: &agentv1.CommandInput_StopContainer{StopContainer: &agentv1.StopContainer{GracePeriod: durationpb.New(10 * time.Second)}}}), nil); err != nil {
		t.Fatal(err)
	}
	retired = fourth
	// Disk loss: a throwaway container removes the member's data directory from its volume.
	wipe, err := api.ContainerCreate(ctx, client.ContainerCreateOptions{
		Config:     &container.Config{Image: image, User: "0", Entrypoint: []string{"sh", "-c", "rm -rf " + postgres.PatroniDataDirectory}},
		HostConfig: &container.HostConfig{Mounts: []mount.Mount{{Type: mount.TypeVolume, Source: "synod-volume-" + retired.VolumeId, Target: postgres.DataDirectory}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer api.ContainerRemove(context.WithoutCancel(ctx), wipe.ID, client.ContainerRemoveOptions{Force: true})
	if _, err := api.ContainerStart(ctx, wipe.ID, client.ContainerStartOptions{}); err != nil {
		t.Fatal(err)
	}
	waited := api.ContainerWait(ctx, wipe.ID, client.ContainerWaitOptions{Condition: container.WaitConditionNotRunning})
	select {
	case result := <-waited.Result:
		if result.StatusCode != 0 {
			t.Fatal("disk-loss fixture failed", result.StatusCode)
		}
	case err := <-waited.Error:
		t.Fatal(err)
	}
	if observed, err := proveMember(resume.Input.ResourceId, retired, true); err != nil || observed.Effects[0].State != "patroni_member_storage_failed" {
		t.Fatalf("lost storage was not proved failed: %+v %v", observed, err)
	}
}
