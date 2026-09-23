package docker

import (
	"archive/tar"
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"errors"
	"io"
	"maps"
	"math/big"
	"net"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"testing/iotest"
	"time"

	"github.com/google/uuid"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/client"
	"github.com/sirrobot01/synod/internal/agent/journal"
	"github.com/sirrobot01/synod/internal/engine/postgres"
	"github.com/sirrobot01/synod/internal/runtime"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestRealDockerPatroniPreparation(t *testing.T) {
	image := os.Getenv("SYNOD_TEST_PATRONI_IMAGE")
	if image == "" {
		t.Skip("set SYNOD_TEST_PATRONI_IMAGE for the native fixture")
	}
	now := time.Now()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	ca := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "runtime-test"}, NotBefore: now.Add(-time.Hour), NotAfter: now.Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	caDER, err := x509.CreateCertificate(rand.Reader, ca, ca, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	ca, err = x509.ParseCertificate(caDER)
	if err != nil {
		t.Fatal(err)
	}
	leaf := &x509.Certificate{SerialNumber: big.NewInt(2), NotBefore: now.Add(-time.Hour), NotAfter: now.Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}, IPAddresses: []net.IP{net.ParseIP("192.0.2.10")}}
	cert, err := x509.CreateCertificate(rand.Reader, leaf, ca, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	private, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	material := postgres.PatroniTLSMaterial{CA: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER}), Certificate: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cert}), PrivateKey: pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: private})}
	encoded, err := json.Marshal(material)
	if err != nil {
		t.Fatal(err)
	}
	for _, fault := range []string{"none", "lost_create_reply", "lost_copy_reply", "lost_copy_and_inspection"} {
		t.Run(fault, func(t *testing.T) {
			r, api, clusterID := runtimeFixture(t)
			volumeID := uuid.NewString()
			volume := runtimeCommand(t, r, clusterID, &agentv1.CommandInput{ResourceId: volumeID, Payload: &agentv1.CommandInput_EnsureVolume{EnsureVolume: &agentv1.EnsureVolume{VolumeId: volumeID}}})
			if _, err := r.Execute(t.Context(), volume, nil); err != nil {
				t.Fatal(err)
			}
			var refs []*agentv1.SecretReference
			secrets := runtime.Secrets{}
			for _, purpose := range []string{"superuser", "application", "replication", "patroni_rewind", "patroni_dcs", "patroni_rest", "patroni_member_tls", "patroni_dcs_tls"} {
				refs = append(refs, &agentv1.SecretReference{CredentialId: uuid.NewString(), Version: 1, Purpose: purpose})
				secrets[purpose] = []byte(uuid.NewString())
			}
			secrets["patroni_member_tls"], secrets["patroni_dcs_tls"] = encoded, encoded
			configuration := &agentv1.EnsureContainer{Engine: "postgres", ImageDigest: image, VolumeId: volumeID, NetworkMode: agentv1.NetworkMode_NETWORK_MODE_HOST, MemoryBytes: 512 << 20, CpuMillis: 1000, Postgres: &agentv1.PostgresContainerConfig{Port: 5432, SuperuserCredentialId: refs[0].CredentialId, SuperuserCredentialVersion: 1, ApplicationCredentialId: refs[1].CredentialId, ApplicationCredentialVersion: 1, ReplicationCredentialId: refs[2].CredentialId, ReplicationCredentialVersion: 1, Patroni: &agentv1.PatroniContainerConfig{RequiredFreeBytes: 1 << 30, Address: "192.0.2.10", RestPort: 8008, DcsEndpoints: []string{"192.0.2.20:2379"}, PeerNetworks: []string{"192.0.2.0/24"}, ClientNetworks: []string{"198.51.100.0/24"}, ProvisionId: uuid.NewString(), AllowInitialization: true, Credentials: refs}}}
			command := runtimeCommand(t, r, clusterID, &agentv1.CommandInput{ResourceId: uuid.NewString(), Secrets: refs, Payload: &agentv1.CommandInput_EnsureContainer{EnsureContainer: configuration}})
			var copies, creates atomic.Int64
			var lost atomic.Bool
			faulty, err := client.New(client.WithHost(os.Getenv("SYNOD_TEST_DOCKER_HOST")), client.WithResponseHook(func(response *http.Response) {
				path := response.Request.URL.Path
				if response.Request.Method == http.MethodPost && strings.HasSuffix(path, "/containers/create") {
					creates.Add(1)
					if fault == "lost_create_reply" && lost.CompareAndSwap(false, true) {
						response.Body.Close()
						response.Body = io.NopCloser(iotest.ErrReader(io.ErrUnexpectedEOF))
					}
				}
				copy := response.Request.Method == http.MethodPut && strings.HasSuffix(path, "/archive")
				if copy {
					copies.Add(1)
				}
				if fault != "none" && copy && response.StatusCode < 300 && lost.CompareAndSwap(false, true) || fault == "lost_copy_and_inspection" && lost.Load() && response.Request.Method == http.MethodGet && strings.Contains(path, "/containers/") {
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
			result, err := injected.Execute(t.Context(), command, secrets)
			if fault == "lost_create_reply" {
				if !errors.Is(err, runtime.ErrOutcomeUnknown) || len(result.Effects) != 1 || !result.Effects[0].OutcomeUnknown {
					t.Fatal("partial creation was not retained", err)
				}
				result, err = injected.Execute(t.Context(), command, secrets)
				if !errors.Is(err, runtime.ErrOutcomeUnknown) || !result.Effects[0].OutcomeUnknown || copies.Load() != 0 || creates.Load() != 1 {
					t.Fatal("partial preparation was overwritten", err)
				}
				return
			}
			if fault == "lost_copy_and_inspection" {
				if !errors.Is(err, runtime.ErrOutcomeUnknown) {
					t.Fatal("uncertain copy was not retained", err)
				}
				j, options, path := runtimeJournal(t, command)
				if err := j.Observe(command.CommandId, result.Effects); err != nil {
					t.Fatal(err)
				}
				if err := j.Close(); err != nil {
					t.Fatal(err)
				}
				options.AllowCreate = false
				reopened, openErr := journal.Open(path, options)
				if openErr != nil {
					t.Fatal(openErr)
				}
				defer reopened.Close()
				retained, readErr := reopened.Get(command.CommandId)
				if readErr != nil {
					t.Fatal(readErr)
				}
				result, err = r.Recover(t.Context(), command, retained.Effects)
				if err != nil {
					t.Fatal(err)
				}
				if err := reopened.Complete(&agentv1.TerminalResult{Epoch: command.Epoch, CommandId: command.CommandId, InputHash: command.InputHash, Sequence: 1, State: "succeeded", Effects: result.Effects}); err != nil {
					t.Fatal(err)
				}
			}
			if err != nil {
				t.Fatalf("preparation: %v / %v", err, errors.Unwrap(err))
			}
			if len(result.Effects) != 1 || result.Effects[0].OutcomeUnknown || result.Effects[0].Active {
				t.Fatalf("incomplete preparation: %+v", result)
			}
			name := "synod-instance-" + command.Input.ResourceId
			item, err := api.ContainerInspect(t.Context(), name, client.ContainerInspectOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if item.Container.State.Running || item.Container.HostConfig.RestartPolicy.Name != container.RestartPolicyDisabled {
				t.Fatal("preparation started native work")
			}
			metadata, err := json.Marshal(item.Container.Config)
			if err != nil {
				t.Fatal(err)
			}
			for purpose, value := range secrets {
				if bytes.Contains(metadata, value) {
					t.Fatal("secret in container metadata", purpose)
				}
			}
			if bytes.Contains(metadata, []byte("BEGIN PRIVATE KEY")) || bytes.Contains(metadata, []byte("SCRAM-SHA-256$")) {
				t.Fatal("private material in metadata")
			}
			if detail, err := r.containerChanges(t.Context(), item.Container); err != nil || detail != "" {
				t.Fatal("prepared container drift", detail, err)
			}
			if fault != "lost_copy_and_inspection" {
				if _, err := injected.Execute(t.Context(), command, secrets); err != nil {
					t.Fatal("duplicate preparation", err)
				}
			} else if _, err := r.Execute(t.Context(), command, secrets); err != nil {
				t.Fatal("duplicate preparation", err)
			}
			if copies.Load() != 1 || creates.Load() != 1 {
				t.Fatal("preparation recopied or recreated", copies.Load(), creates.Load())
			}
			changed := maps.Clone(secrets)
			changed["patroni_rest"] = []byte(uuid.NewString())
			if _, err := r.Execute(t.Context(), command, changed); !errors.Is(err, runtime.ErrConfigurationConflict) {
				t.Fatal("changed material accepted", err)
			}
			start := runtimeCommand(t, r, clusterID, &agentv1.CommandInput{ResourceId: command.Input.ResourceId, Payload: &agentv1.CommandInput_StartContainer{StartContainer: &agentv1.StartContainer{AllowInitialization: true}}})
			if _, err := r.Execute(t.Context(), start, nil); !errors.Is(err, runtime.ErrUnsupportedConfiguration) {
				t.Fatal("native start admitted", err)
			}
			item, err = api.ContainerInspect(t.Context(), name, client.ContainerInspectOptions{})
			if err != nil || item.Container.State.Running {
				t.Fatal("refused start changed state", err)
			}
			{
				var occupied bytes.Buffer
				writer := tar.NewWriter(&occupied)
				if err := writer.WriteHeader(&tar.Header{Name: "retained-native-data", Mode: 0600, Size: 8}); err != nil {
					t.Fatal(err)
				}
				if _, err := writer.Write([]byte("retained")); err != nil {
					t.Fatal(err)
				}
				if err := writer.Close(); err != nil {
					t.Fatal(err)
				}
				if _, err := api.CopyToContainer(t.Context(), name, client.CopyToContainerOptions{DestinationPath: postgres.DataDirectory, Content: &occupied, CopyUIDGID: true}); err != nil {
					t.Fatal(err)
				}
				native := runtimeCommand(t, r, clusterID, &agentv1.CommandInput{ResourceId: command.Input.ResourceId, Payload: &agentv1.CommandInput_StartPatroniMember{StartPatroniMember: &agentv1.StartPatroniMember{Container: configuration, AllowInitialization: true}}})
				if _, err := r.Execute(t.Context(), native, nil); !errors.Is(err, runtime.ErrConfigurationConflict) {
					t.Fatal("native initialization accepted occupied storage", err)
				}
				unchanged, err := api.ContainerInspect(t.Context(), name, client.ContainerInspectOptions{})
				if err != nil || unchanged.Container.State.Running || unchanged.Container.HostConfig.RestartPolicy.Name != container.RestartPolicyDisabled {
					t.Fatal("refused initial work changed container state", err)
				}
			}
			// An external file change must remain an unresolved effect. Preparation cannot repair it.
			var archive bytes.Buffer
			writer := tar.NewWriter(&archive)
			if err := writer.WriteHeader(&tar.Header{Name: "config.json", Mode: 0644, Size: 3}); err != nil {
				t.Fatal(err)
			}
			if _, err := writer.Write([]byte("bad")); err != nil {
				t.Fatal(err)
			}
			if err := writer.Close(); err != nil {
				t.Fatal(err)
			}
			if _, err := api.CopyToContainer(t.Context(), name, client.CopyToContainerOptions{DestinationPath: postgres.PatroniSourceDirectory, Content: &archive, CopyUIDGID: true}); err != nil {
				t.Fatal(err)
			}
			result, err = r.Execute(t.Context(), command, secrets)
			if !errors.Is(err, runtime.ErrOutcomeUnknown) || len(result.Effects) != 1 || !result.Effects[0].OutcomeUnknown || !result.Effects[0].Active {
				t.Fatalf("unsafe source was repaired or forgotten: %+v %v", result, err)
			}
			if detail, err := r.containerChanges(t.Context(), item.Container); err != nil || detail != driftEngineConfig {
				t.Fatal("unsafe source drift not reported", detail, err)
			}
			inspect := runtimeCommand(t, r, clusterID, &agentv1.CommandInput{ResourceId: command.Input.ResourceId, Payload: &agentv1.CommandInput_InspectContainer{InspectContainer: &agentv1.InspectContainer{}}})
			if observed, err := r.Execute(t.Context(), inspect, nil); !errors.Is(err, runtime.ErrOutcomeUnknown) || len(observed.Effects) != 1 || !observed.Effects[0].OutcomeUnknown {
				t.Fatal("inspection skipped unsafe source proof", err)
			}
			copied, err := api.CopyFromContainer(t.Context(), name, client.CopyFromContainerOptions{SourcePath: postgres.PatroniSourceDirectory + "/config.json"})
			if err != nil {
				t.Fatal(err)
			}
			reader := tar.NewReader(copied.Content)
			header, err := reader.Next()
			if err != nil {
				t.Fatal(err)
			}
			data, err := io.ReadAll(reader)
			copied.Content.Close()
			if err != nil || header.Mode != 0644 || string(data) != "bad" {
				t.Fatal("unsafe source was overwritten")
			}
			stop := runtimeCommand(t, r, clusterID, &agentv1.CommandInput{ResourceId: command.Input.ResourceId, Payload: &agentv1.CommandInput_StopContainer{StopContainer: &agentv1.StopContainer{GracePeriod: durationpb.New(time.Second)}}})
			if _, err := r.Execute(t.Context(), stop, nil); err != nil {
				t.Fatal("owned stop", err)
			}
			remove := runtimeCommand(t, r, clusterID, &agentv1.CommandInput{ResourceId: command.Input.ResourceId, Payload: &agentv1.CommandInput_RemoveContainer{RemoveContainer: &agentv1.RemoveContainer{}}})
			if _, err := r.Execute(t.Context(), remove, nil); err != nil {
				t.Fatal("owned remove", err)
			}
		})
	}
}
