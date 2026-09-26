package docker

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"math/rand/v2"
	"net"
	"net/netip"
	"os"
	"os/exec"
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/client"
	"github.com/sirrobot01/synod/internal/engine"
	"github.com/sirrobot01/synod/internal/engine/postgres"
	"github.com/sirrobot01/synod/internal/operation"
	"github.com/sirrobot01/synod/internal/pki"
	"github.com/sirrobot01/synod/internal/runtime"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/types/known/durationpb"
)

// A production member runs on the host network. The agent proves readiness on loopback
// with TLS for the member IP. Docker Desktop hides host networking, so this runs on Linux.
func TestRealDockerPatroniHostNetwork(t *testing.T) {
	image, hostAddress := os.Getenv("SYNOD_TEST_PATRONI_IMAGE"), os.Getenv("SYNOD_TEST_PATRONI_HOST_ADDRESS")
	if image == "" || hostAddress == "" {
		t.Skip("set SYNOD_TEST_PATRONI_IMAGE and SYNOD_TEST_PATRONI_HOST_ADDRESS on a Linux Docker host")
	}
	address, err := netip.ParseAddr(hostAddress)
	if err != nil || !address.IsGlobalUnicast() {
		t.Fatal("SYNOD_TEST_PATRONI_HOST_ADDRESS must be a routable local address")
	}
	r, api, clusterID := runtimeFixture(t)
	ctx, cancel := context.WithTimeout(t.Context(), 4*time.Minute)
	defer cancel()
	now, installation := time.Now(), r.options.InstallationID
	nativeCert, nativeKey, err := pki.CreatePatroniCA(installation, now, 24*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	native, err := pki.LoadPatroniCA(installation, nativeCert, nativeKey)
	if err != nil {
		t.Fatal(err)
	}
	// Host ports are shared with the host, so each run takes its own span.
	base := 42000 + rand.IntN(3000)*2
	etcdPort, peerPort, databasePort, restPort := base, base+6000, base+1, base+6001
	etcdServer, err := postgres.IssuePatroniMemberMaterial(native, pki.PatroniIdentity{InstallationID: installation, ClusterID: clusterID, ResourceID: uuid.New()}, address, now, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	dcsMaterial, err := postgres.IssuePatroniDCSMaterial(native, pki.PatroniIdentity{InstallationID: installation, ClusterID: clusterID}, nativeCert, now, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	etcdEndpoint := net.JoinHostPort(address.String(), strconv.Itoa(etcdPort))
	dcs, err := api.ContainerCreate(ctx, client.ContainerCreateOptions{
		Name: "synod-patroni-host-dcs-" + clusterID.String(),
		Config: &container.Config{Image: image, Entrypoint: []string{"etcd"}, Cmd: []string{"--data-dir", "/tmp/etcd",
			"--listen-client-urls", "https://" + etcdEndpoint, "--advertise-client-urls", "https://" + etcdEndpoint,
			"--cert-file", "/etc/synod-dcs/server.crt", "--key-file", "/etc/synod-dcs/server.key", "--trusted-ca-file", "/etc/synod-dcs/ca.crt", "--client-cert-auth=true",
			"--listen-peer-urls", "http://127.0.0.1:" + strconv.Itoa(peerPort), "--initial-advertise-peer-urls", "http://127.0.0.1:" + strconv.Itoa(peerPort),
			"--initial-cluster", "default=http://127.0.0.1:" + strconv.Itoa(peerPort)}},
		HostConfig: &container.HostConfig{NetworkMode: "host"},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanup, finish := context.WithTimeout(context.Background(), 20*time.Second)
		defer finish()
		if _, err := api.ContainerRemove(cleanup, dcs.ID, client.ContainerRemoveOptions{Force: true, RemoveVolumes: true}); err != nil {
			t.Error(err)
		}
	})
	var archive bytes.Buffer
	writer := tar.NewWriter(&archive)
	if err := writer.WriteHeader(&tar.Header{Name: "synod-dcs/", Typeflag: tar.TypeDir, Mode: 0700}); err != nil {
		t.Fatal(err)
	}
	for name, data := range map[string][]byte{"ca.crt": nativeCert, "server.crt": etcdServer.Certificate, "server.key": etcdServer.PrivateKey} {
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
	if _, err := api.CopyToContainer(ctx, dcs.ID, client.CopyToContainerOptions{DestinationPath: "/etc", Content: &archive}); err != nil {
		t.Fatal(err)
	}
	if _, err := api.ContainerStart(ctx, dcs.ID, client.ContainerStartOptions{}); err != nil {
		t.Fatal(err)
	}
	secrets := runtime.Secrets{}
	refs := make([]*agentv1.SecretReference, 0, 7)
	for _, purpose := range []string{"superuser", "application", "replication", "patroni_rewind", "patroni_rest", "patroni_member_tls", "patroni_dcs_tls"} {
		refs = append(refs, &agentv1.SecretReference{CredentialId: uuid.NewString(), Version: 1, Purpose: purpose})
		secrets[purpose] = []byte(uuid.NewString())
	}
	resourceID := uuid.NewString()
	member, err := postgres.IssuePatroniMemberMaterial(native, pki.PatroniIdentity{InstallationID: installation, ClusterID: clusterID, ResourceID: uuid.MustParse(resourceID)}, address, now, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	for purpose, material := range map[string]postgres.PatroniTLSMaterial{"patroni_member_tls": member, "patroni_dcs_tls": dcsMaterial} {
		if secrets[purpose], err = json.Marshal(material); err != nil {
			t.Fatal(err)
		}
	}
	volumeID := uuid.NewString()
	if _, err := r.Execute(ctx, runtimeCommand(t, r, clusterID, &agentv1.CommandInput{ResourceId: volumeID, Payload: &agentv1.CommandInput_EnsureVolume{EnsureVolume: &agentv1.EnsureVolume{VolumeId: volumeID}}}), nil); err != nil {
		t.Fatal(err)
	}
	host := netip.PrefixFrom(address, address.BitLen()).String()
	configuration := &agentv1.EnsureContainer{Engine: "postgres", ImageDigest: image, VolumeId: volumeID, NetworkMode: agentv1.NetworkMode_NETWORK_MODE_HOST, MemoryBytes: 512 << 20, CpuMillis: 1000,
		Postgres: &agentv1.PostgresContainerConfig{Port: uint32(databasePort), SuperuserCredentialId: refs[0].CredentialId, SuperuserCredentialVersion: 1, ApplicationCredentialId: refs[1].CredentialId, ApplicationCredentialVersion: 1, ReplicationCredentialId: refs[2].CredentialId, ReplicationCredentialVersion: 1,
			Patroni: &agentv1.PatroniContainerConfig{Address: address.String(), RestPort: uint32(restPort), DcsEndpoints: []string{etcdEndpoint}, PeerNetworks: []string{host}, ClientNetworks: []string{host}, ProvisionId: uuid.NewString(), AllowInitialization: true, RequiredFreeBytes: 1 << 30, Credentials: refs}}}
	if _, err := r.Execute(ctx, runtimeCommand(t, r, clusterID, &agentv1.CommandInput{ResourceId: resourceID, Secrets: refs, Payload: &agentv1.CommandInput_EnsureContainer{EnsureContainer: configuration}}), secrets); err != nil {
		t.Fatal("prepare host-network member", err)
	}
	start := runtimeCommand(t, r, clusterID, &agentv1.CommandInput{ResourceId: resourceID, Payload: &agentv1.CommandInput_StartPatroniMember{StartPatroniMember: &agentv1.StartPatroniMember{Container: configuration, AllowInitialization: true}}})
	observed, err := r.Execute(ctx, start, nil)
	step := &agentv1.PlanStep{Command: start, Postcondition: agentv1.Postcondition_POSTCONDITION_CONTAINER_RUNNING}
	deadline := time.Now().Add(120 * time.Second)
	for err != nil || operation.CheckStepEffects(step, observed.Effects) != nil {
		if err != nil && !errors.Is(err, runtime.ErrOutcomeUnknown) {
			t.Fatal("start host-network member", err)
		}
		if time.Now().After(deadline) {
			t.Fatal("host-network member did not prove startup", observed, err)
		}
		time.Sleep(time.Second)
		observed, err = r.Recover(ctx, start, observed.Effects)
	}
	probe := func(endpoint engine.Endpoint) engine.Observation {
		return postgres.Engine{}.Probe(ctx, endpoint, secrets["application"], 1)
	}
	ready := engine.Endpoint{PostgresTLS: &engine.PostgresTLS{RootCAPEM: nativeCert, ServerName: address.String()}, PostgresRole: "primary", Address: postgres.ReadinessLoopback(address).Addr().String(), Port: uint16(databasePort), Database: "app", Role: "synod_app"}
	if observation := probe(ready); observation.Ready.State != engine.ProbePassed {
		t.Fatalf("loopback TLS readiness failed: %+v", observation)
	}
	wrongName := ready
	wrongName.PostgresTLS = &engine.PostgresTLS{RootCAPEM: nativeCert, ServerName: "192.0.2.1"}
	if observation := probe(wrongName); observation.Ready.State == engine.ProbePassed {
		t.Fatal("readiness accepted a certificate for another member")
	}
	plaintext := ready
	plaintext.PostgresTLS = nil
	if observation := probe(plaintext); observation.Ready.State == engine.ProbePassed {
		t.Fatal("readiness accepted a connection without TLS")
	}
	// A pooler and a router run beside the member on the host network. A client reaches the
	// member through each with TLS verify-full for the member address.
	poolerPort, writerPort, readerPort := base+2, base+3, base+4
	memberRef := refs[6]
	routes := map[string]*agentv1.EnsureContainer{
		"pooler": {Engine: "postgres", ImageDigest: image, NetworkMode: agentv1.NetworkMode_NETWORK_MODE_HOST, MemoryBytes: 64 << 20, CpuMillis: 250,
			Route: &agentv1.PostgresRouteConfig{Pooler: &agentv1.PostgresPoolerConfig{Address: address.String(), Port: uint32(poolerPort), DatabasePort: uint32(databasePort), RestPort: uint32(restPort)}, Credentials: []*agentv1.SecretReference{refs[1], memberRef}}},
		"router": {Engine: "postgres", ImageDigest: image, NetworkMode: agentv1.NetworkMode_NETWORK_MODE_HOST, MemoryBytes: 64 << 20, CpuMillis: 250,
			Route: &agentv1.PostgresRouteConfig{Router: &agentv1.PostgresRouterConfig{Address: address.String(), WriterPort: uint32(writerPort), ReaderPort: uint32(readerPort),
				Backends: []*agentv1.PostgresRouteBackend{{Address: address.String(), PoolerPort: uint32(poolerPort), RestPort: uint32(restPort)}}}, Credentials: []*agentv1.SecretReference{memberRef}}},
	}
	routeIDs := map[string]string{"pooler": uuid.NewString(), "router": uuid.NewString()}
	startRoute := func(kind string) {
		t.Helper()
		start := runtimeCommand(t, r, clusterID, &agentv1.CommandInput{ResourceId: routeIDs[kind], Payload: &agentv1.CommandInput_StartContainer{StartContainer: &agentv1.StartContainer{}}})
		observed, err := r.Execute(ctx, start, nil)
		step := &agentv1.PlanStep{Command: start, Postcondition: agentv1.Postcondition_POSTCONDITION_CONTAINER_RUNNING}
		deadline := time.Now().Add(30 * time.Second)
		for err != nil || operation.CheckStepEffects(step, observed.Effects) != nil {
			if err != nil && !errors.Is(err, runtime.ErrOutcomeUnknown) || time.Now().After(deadline) {
				t.Fatalf("start %s: %+v %v", kind, observed, err)
			}
			time.Sleep(500 * time.Millisecond)
			observed, err = r.Recover(ctx, start, observed.Effects)
		}
	}
	for _, kind := range []string{"pooler", "router"} {
		configuration := routes[kind]
		command := runtimeCommand(t, r, clusterID, &agentv1.CommandInput{ResourceId: routeIDs[kind], Secrets: configuration.Route.Credentials, Payload: &agentv1.CommandInput_EnsureContainer{EnsureContainer: configuration}})
		routeSecrets := runtime.Secrets{"patroni_member_tls": secrets["patroni_member_tls"]}
		if kind == "pooler" {
			routeSecrets["application"] = secrets["application"]
		}
		if observed, err := r.Execute(ctx, command, routeSecrets); err != nil {
			t.Fatalf("prepare %s: %v / %v: %+v", kind, err, errors.Unwrap(err), observed)
		}
		// A repeated create keeps the same container and files.
		if _, err := r.Execute(ctx, command, routeSecrets); err != nil {
			t.Fatalf("repeat %s: %v", kind, err)
		}
		inspected, err := api.ContainerInspect(ctx, "synod-instance-"+routeIDs[kind], client.ContainerInspectOptions{})
		if err != nil || len(inspected.Container.Mounts) != 0 || inspected.Container.HostConfig.RestartPolicy.Name != container.RestartPolicyDisabled {
			t.Fatalf("%s container shape: mounts %d %v", kind, len(inspected.Container.Mounts), err)
		}
		t.Cleanup(func() {
			cleanup, finish := context.WithTimeout(context.Background(), 20*time.Second)
			defer finish()
			api.ContainerRemove(cleanup, "synod-instance-"+routeIDs[kind], client.ContainerRemoveOptions{Force: true})
		})
		startRoute(kind)
	}
	through := func(port int, label string) {
		t.Helper()
		endpoint := ready
		endpoint.Address, endpoint.Port = address.String(), uint16(port)
		deadline := time.Now().Add(30 * time.Second)
		for {
			observation := probe(endpoint)
			if observation.Ready.State == engine.ProbePassed {
				return
			}
			if time.Now().After(deadline) {
				logs, _ := exec.Command("docker", "logs", "--tail", "20", "synod-instance-"+routeIDs["pooler"]).CombinedOutput()
				t.Fatalf("readiness through the %s: %+v\npooler: %s", label, observation, logs)
			}
			time.Sleep(500 * time.Millisecond)
		}
	}
	through(poolerPort, "pooler")
	through(writerPort, "router writer route")
	// The protected files survive a restart, and the route serves again.
	for _, kind := range []string{"pooler", "router"} {
		if _, err := r.Execute(ctx, runtimeCommand(t, r, clusterID, &agentv1.CommandInput{ResourceId: routeIDs[kind], Payload: &agentv1.CommandInput_StopContainer{StopContainer: &agentv1.StopContainer{GracePeriod: durationpb.New(5 * time.Second)}}}), nil); err != nil {
			t.Fatalf("stop %s: %v", kind, err)
		}
		startRoute(kind)
	}
	through(writerPort, "router after restart")
	for _, kind := range []string{"router", "pooler"} {
		if _, err := r.Execute(ctx, runtimeCommand(t, r, clusterID, &agentv1.CommandInput{ResourceId: routeIDs[kind], Payload: &agentv1.CommandInput_StopContainer{StopContainer: &agentv1.StopContainer{GracePeriod: durationpb.New(5 * time.Second)}}}), nil); err != nil {
			t.Fatalf("stop %s: %v", kind, err)
		}
		if _, err := r.Execute(ctx, runtimeCommand(t, r, clusterID, &agentv1.CommandInput{ResourceId: routeIDs[kind], Payload: &agentv1.CommandInput_RemoveContainer{RemoveContainer: &agentv1.RemoveContainer{}}}), nil); err != nil {
			t.Fatalf("remove %s: %v", kind, err)
		}
	}
	if _, err := r.Execute(ctx, runtimeCommand(t, r, clusterID, &agentv1.CommandInput{ResourceId: resourceID, Payload: &agentv1.CommandInput_StopContainer{StopContainer: &agentv1.StopContainer{GracePeriod: durationpb.New(10 * time.Second)}}}), nil); err != nil {
		t.Fatal("stop host-network member", err)
	}
}
