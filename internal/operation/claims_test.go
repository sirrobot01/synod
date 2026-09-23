package operation

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

func commandFixture() *agentv1.Command {
	return &agentv1.Command{
		Epoch:       &agentv1.Epoch{InstallationId: uuid.NewString(), NodeId: uuid.NewString(), ClusterId: uuid.NewString(), RecoveryGeneration: uuid.NewString(), ExecutionEpoch: 1},
		OperationId: uuid.NewString(), StepId: uuid.NewString(), CommandId: uuid.NewString(), DesiredRevision: 1, PlanVersion: 1,
		Input: &agentv1.CommandInput{ResourceId: uuid.NewString(), PayloadVersion: 1, Timeout: durationpb.New(time.Minute), Payload: &agentv1.CommandInput_StartContainer{StartContainer: &agentv1.StartContainer{}}},
	}
}

func TestVersionNegotiation(t *testing.T) {
	for _, tt := range []struct{ cmin, cmax, amin, amax, want uint32 }{{1, 1, 1, 1, 1}, {1, 2, 1, 1, 1}, {1, 3, 2, 4, 3}, {1, 1, 2, 3, 0}, {0, 1, 1, 1, 0}, {3, 1, 1, 3, 0}} {
		got, err := NegotiateVersion(&agentv1.VersionRange{Minimum: tt.cmin, Maximum: tt.cmax}, &agentv1.VersionRange{Minimum: tt.amin, Maximum: tt.amax})
		if tt.want == 0 {
			if !errors.Is(err, ErrIncompatibleProtocol) {
				t.Fatal("accepted incompatible versions")
			}
			continue
		}
		if err != nil || got != tt.want {
			t.Fatalf("version %d, want %d: %v", got, tt.want, err)
		}
	}
	hello := &agentv1.Hello{InstallationId: uuid.NewString(), NodeId: uuid.NewString(), BootId: uuid.NewString(), JournalGeneration: uuid.NewString(), Protocol: &agentv1.VersionRange{Minimum: 1, Maximum: 2}, Capabilities: []string{"start", "inspect", "start", "other"}, ActivePayloadVersions: []uint32{1}}
	got, err := NegotiateHello(hello, []string{"start", "inspect"})
	if err != nil || strings.Join(got, ",") != "inspect,start" {
		t.Fatalf("capabilities %v: %v", got, err)
	}
	hello.ActivePayloadVersions = []uint32{2}
	if _, err := NegotiateHello(hello, []string{"start"}); !errors.Is(err, ErrIncompatibleProtocol) {
		t.Fatal("accepted unknown active journal payload")
	}
}

func TestCommandHashAndDelayedEpoch(t *testing.T) {
	command := commandFixture()
	hash, err := CommandHash(command)
	if err != nil {
		t.Fatal(err)
	}
	command.InputHash = hash[:]
	if err := ValidateCommand(command); err != nil {
		t.Fatal(err)
	}
	encoded, err := proto.Marshal(command)
	if err != nil {
		t.Fatal(err)
	}
	var decoded agentv1.Command
	if err := proto.Unmarshal(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	if err := ValidateCommand(&decoded); err != nil {
		t.Fatal(err)
	}
	newOwner := proto.Clone(command).(*agentv1.Command)
	newOwner.Epoch.ExecutionEpoch++
	newOwner.TraceId = "another-trace"
	if err := ValidateCommand(newOwner); err != nil {
		t.Fatal("ownership changed immutable hash", err)
	}
	if err := CheckEpoch(command.Epoch, newOwner.Epoch); !errors.Is(err, ErrStaleOwner) {
		t.Fatal("admitted delayed command")
	}
	oldGeneration := proto.Clone(command.Epoch).(*agentv1.Epoch)
	oldGeneration.RecoveryGeneration = uuid.NewString()
	if err := CheckEpoch(command.Epoch, oldGeneration); !errors.Is(err, ErrStaleOwner) {
		t.Fatal("admitted old recovery generation")
	}
	for _, change := range []func(*agentv1.Command){
		func(c *agentv1.Command) { c.Input.GetStartContainer().AllowInitialization = true },
		func(c *agentv1.Command) { c.Input.Timeout = durationpb.New(2 * time.Minute) },
		func(c *agentv1.Command) { c.Epoch.NodeId = uuid.NewString() },
		func(c *agentv1.Command) { c.CommandId = uuid.NewString() },
		func(c *agentv1.Command) {
			c.Input.Secrets = []*agentv1.SecretReference{{CredentialId: uuid.NewString(), Version: 2, Purpose: "database-password"}}
		},
	} {
		changed := proto.Clone(command).(*agentv1.Command)
		change(changed)
		if err := ValidateCommand(changed); !errors.Is(err, ErrCommandConflict) {
			t.Fatalf("accepted changed input: %v", err)
		}
	}
	command.Input.Payload = nil
	if _, err := CommandHash(command); err == nil {
		t.Fatal("accepted unsupported payload")
	}
}

func TestBarrierRetainsActiveNativeEffects(t *testing.T) {
	result := &agentv1.BarrierResult{Epoch: commandFixture().Epoch, AdmissionsClosed: true, Drained: true}
	if err := BarrierDrained(result); err != nil {
		t.Fatal(err)
	}
	for _, effect := range []*agentv1.Effect{{NativeOperationId: "native-backup", Active: true}, {CommandId: uuid.NewString(), OutcomeUnknown: true}, nil} {
		result.Effects = []*agentv1.Effect{effect}
		if err := BarrierDrained(result); !errors.Is(err, ErrBarrierPending) {
			t.Fatal("accepted unresolved native effect")
		}
	}
	result.Effects = []*agentv1.Effect{{State: "container remains stopped"}}
	if err := BarrierDrained(result); err != nil {
		t.Fatal("known residual can proceed to controller reconciliation", err)
	}
	result.Drained = false
	if err := BarrierDrained(result); !errors.Is(err, ErrBarrierPending) {
		t.Fatal("accepted undrained work")
	}
}

func TestContainerNetworkMode(t *testing.T) {
	command := commandFixture()
	config := &agentv1.EnsureContainer{
		ImageDigest: "redis@sha256:" + strings.Repeat("a", 64), Engine: "redis", VolumeId: uuid.NewString(), NetworkMode: agentv1.NetworkMode_NETWORK_MODE_HOST, MemoryBytes: 64 << 20,
		Redis:     &agentv1.RedisContainerConfig{Port: 6379, Persistence: "none", MaxmemoryBytes: 48 << 20, EvictionPolicy: "noeviction"},
		SecretEnv: []*agentv1.SecretEnv{{Name: "REDIS_ADMIN_PASSWORD", Purpose: "superuser"}, {Name: "REDIS_APP_PASSWORD", Purpose: "application"}, {Name: "REDIS_REPLICATION_PASSWORD", Purpose: "replication"}},
	}
	command.Input.Secrets = []*agentv1.SecretReference{{CredentialId: uuid.NewString(), Version: 1, Purpose: "superuser"}, {CredentialId: uuid.NewString(), Version: 1, Purpose: "application"}, {CredentialId: uuid.NewString(), Version: 1, Purpose: "replication"}}
	config.Redis.Credentials = command.Input.Secrets
	command.Input.Payload = &agentv1.CommandInput_EnsureContainer{EnsureContainer: config}
	if _, err := CommandHash(command); err != nil {
		t.Fatal(err)
	}
	config.NetworkId = uuid.NewString()
	if _, err := CommandHash(command); err == nil {
		t.Fatal("host mode accepted network ID")
	}
	config.NetworkId = ""
	config.Ports = []*agentv1.PortBinding{{HostAddress: "127.0.0.1", HostPort: 6379, ContainerPort: 6379, Transport: "tcp"}}
	if _, err := CommandHash(command); err == nil {
		t.Fatal("host mode accepted published ports")
	}
	config.Ports = nil
	config.NetworkMode = agentv1.NetworkMode_NETWORK_MODE_UNSPECIFIED
	if _, err := CommandHash(command); err == nil {
		t.Fatal("accepted unspecified network mode")
	}
	config.NetworkMode = agentv1.NetworkMode_NETWORK_MODE_BRIDGE
	if _, err := CommandHash(command); err == nil {
		t.Fatal("bridge mode accepted absent network")
	}
	config.NetworkId = uuid.NewString()
	if _, err := CommandHash(command); err != nil {
		t.Fatal(err)
	}
}
