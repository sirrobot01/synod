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
	"strings"
	"sync/atomic"
	"testing"
	"testing/iotest"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/network"
	"github.com/moby/moby/client"
	"github.com/sirrobot01/synod/internal/agent/journal"
	"github.com/sirrobot01/synod/internal/engine/postgres"
	"github.com/sirrobot01/synod/internal/operation"
	"github.com/sirrobot01/synod/internal/runtime"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestRealDockerPatroniStartAndRestart(t *testing.T) {
	image := os.Getenv("SYNOD_TEST_PATRONI_IMAGE")
	if image == "" {
		t.Skip("set SYNOD_TEST_PATRONI_IMAGE for the native runtime fixture")
	}
	r, api, clusterID := runtimeFixture(t)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
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
	materials := make([]postgres.PatroniTLSMaterial, 3)
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
	dcsInspect, err := api.ContainerInspect(ctx, dcsContainer.ID, client.ContainerInspectOptions{})
	if err != nil {
		t.Fatal(err)
	}
	binding := dcsInspect.Container.NetworkSettings.Ports[dcsPort][0]
	pair, err := tls.X509KeyPair(materials[0].Certificate, materials[0].PrivateKey)
	if err != nil {
		t.Fatal(err)
	}
	dcs, err := clientv3.New(clientv3.Config{Endpoints: []string{"https://" + net.JoinHostPort(binding.HostIP.String(), binding.HostPort)}, DialTimeout: 5 * time.Second, TLS: &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots, Certificates: []tls.Certificate{pair}, ServerName: addresses[0].String()}})
	if err != nil {
		t.Fatal(err)
	}
	defer dcs.Close()
	secrets := runtime.Secrets{}
	refs := make([]*agentv1.SecretReference, 0, 8)
	for _, purpose := range []string{"superuser", "application", "replication", "patroni_rewind", "patroni_dcs", "patroni_rest", "patroni_member_tls", "patroni_dcs_tls"} {
		refs = append(refs, &agentv1.SecretReference{CredentialId: uuid.NewString(), Version: 1, Purpose: purpose})
		secrets[purpose] = []byte(uuid.NewString())
	}
	dcsUser := "synod_patroni_" + clusterID.String()
	dcsContext, cancelDCS := context.WithTimeout(ctx, 30*time.Second)
	defer cancelDCS()
	if _, err := dcs.Auth.UserAdd(dcsContext, "root", uuid.NewString()); err != nil {
		t.Fatal("fixture root setup", err)
	}
	if _, err := dcs.Auth.UserGrantRole(dcsContext, "root", "root"); err != nil {
		t.Fatal(err)
	}
	if _, err := dcs.Auth.UserAdd(dcsContext, dcsUser, string(secrets["patroni_dcs"])); err != nil {
		t.Fatal(err)
	}
	if _, err := dcs.Auth.RoleAdd(dcsContext, dcsUser); err != nil {
		t.Fatal(err)
	}
	prefix := "/synod-patroni/" + r.options.InstallationID.String() + "/" + clusterID.String() + "/"
	if _, err := dcs.Auth.RoleGrantPermission(dcsContext, dcsUser, prefix, clientv3.GetPrefixRangeEnd(prefix), clientv3.PermissionType(clientv3.PermReadWrite)); err != nil {
		t.Fatal(err)
	}
	if _, err := dcs.Auth.UserGrantRole(dcsContext, dcsUser, dcsUser); err != nil {
		t.Fatal(err)
	}
	if _, err := dcs.Auth.AuthEnable(dcsContext); err != nil {
		t.Fatal(err)
	}
	secrets["patroni_dcs_tls"], err = json.Marshal(materials[0])
	if err != nil {
		t.Fatal(err)
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
	deadline := time.Now().Add(20 * time.Second)
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
}
