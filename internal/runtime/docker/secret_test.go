package docker

import (
	"encoding/base64"
	"errors"
	"strings"
	"testing"

	"github.com/containerd/errdefs"
	"github.com/google/uuid"
	"github.com/moby/moby/client"
	"github.com/sirrobot01/synod/internal/runtime"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/proto"
)

// A credential reaches the engine's environment and nothing else.
func TestSecretReachesTheEnvironmentAndNotTheLabel(t *testing.T) {
	r, api, clusterID := runtimeFixture(t)
	password := "credential-" + uuid.NewString()
	volumeID := uuid.NewString()
	volume := runtimeCommand(t, r, clusterID, &agentv1.CommandInput{
		ResourceId: volumeID, Payload: &agentv1.CommandInput_EnsureVolume{EnsureVolume: &agentv1.EnsureVolume{VolumeId: volumeID}},
	})
	if _, err := r.Execute(t.Context(), volume, nil); err != nil {
		t.Fatal(err)
	}
	id := uuid.NewString()
	command := runtimeCommand(t, r, clusterID, &agentv1.CommandInput{
		ResourceId: id,
		Secrets:    []*agentv1.SecretReference{{CredentialId: uuid.NewString(), Version: 1, Purpose: "superuser"}, {CredentialId: uuid.NewString(), Version: 1, Purpose: "application"}, {CredentialId: uuid.NewString(), Version: 1, Purpose: "replication"}},
		Payload: &agentv1.CommandInput_EnsureContainer{EnsureContainer: &agentv1.EnsureContainer{
			ImageDigest: redisFixture, Engine: "redis", VolumeId: volumeID,
			NetworkMode: agentv1.NetworkMode_NETWORK_MODE_HOST, MemoryBytes: 64 << 20, CpuMillis: 250,
			Redis:     &agentv1.RedisContainerConfig{Port: 6379, Persistence: "none", MaxmemoryBytes: 48 << 20, EvictionPolicy: "noeviction"},
			SecretEnv: []*agentv1.SecretEnv{{Name: "REDIS_ADMIN_PASSWORD", Purpose: "superuser"}, {Name: "REDIS_APP_PASSWORD", Purpose: "application"}, {Name: "REDIS_REPLICATION_PASSWORD", Purpose: "replication"}},
		}},
	})
	if _, err := r.Execute(t.Context(), command, runtime.Secrets{"superuser": []byte(password), "application": []byte("runtime-test-app"), "replication": []byte("unsafe\nreplication")}); !errors.Is(err, runtime.ErrConfigurationConflict) {
		t.Fatal("invalid replication material was not refused")
	}
	if _, err := api.ContainerInspect(t.Context(), "synod-instance-"+id, client.ContainerInspectOptions{}); !errdefs.IsNotFound(err) {
		t.Fatal("invalid replication material reached container creation")
	}
	if _, err := r.Execute(t.Context(), command, runtime.Secrets{"superuser": []byte(password), "application": []byte("runtime-test-app"), "replication": []byte("runtime-test-repl")}); err != nil {
		t.Fatal(err)
	}
	found, err := api.ContainerInspect(t.Context(), "synod-instance-"+id, client.ContainerInspectOptions{})
	if err != nil {
		t.Fatal(err)
	}
	// The engine can read it.
	if !containsEnv(found.Container.Config.Env, "REDIS_ADMIN_PASSWORD="+password) {
		t.Fatal("the environment does not hold the credential the engine reads")
	}
	// The configuration label is what this driver writes to the host and hashes, and
	// what an inventory read and a drift report carry. The credential is in none of
	// them, because it was applied after the payload was encoded.
	for key, value := range found.Container.Config.Labels {
		if strings.Contains(value, password) {
			t.Fatalf("label %s carries the credential", key)
		}
		if decoded, err := base64.RawStdEncoding.DecodeString(value); err == nil && strings.Contains(string(decoded), password) {
			t.Fatalf("label %s carries the credential once decoded", key)
		}
	}
	// The command this node journals holds a reference and no material. Serialising
	// the whole command is the check that matters, because the journal, the stored
	// dispatch, and the input hash are all taken from exactly these bytes.
	encoded, err := proto.Marshal(command)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), password) {
		t.Fatal("the dispatched command carries the credential")
	}
	if drift, err := r.Drift(t.Context()); err != nil {
		t.Fatal(err)
	} else {
		for _, item := range drift {
			if strings.Contains(item.Detail, password) || strings.Contains(item.Name, password) {
				t.Fatal("a drift report carries the credential")
			}
		}
	}
}

// A named variable with no resolved credential must refuse the command rather than
// start an engine with an empty password.
func TestAnUnresolvedSecretRefusesTheCommand(t *testing.T) {
	p := &agentv1.EnsureContainer{SecretEnv: []*agentv1.SecretEnv{{Name: "ENGINE_PASSWORD", Purpose: "superuser"}}}
	for name, secrets := range map[string]runtime.Secrets{
		"nothing resolved":  nil,
		"another purpose":   {"application": []byte("value")},
		"an empty material": {"superuser": []byte("")},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := secretEnvironment(nil, p, secrets); !errors.Is(err, runtime.ErrConfigurationConflict) {
				t.Fatalf("err=%v, want the command refused", err)
			}
		})
	}
	if _, err := secretEnvironment(nil, p, runtime.Secrets{"superuser": []byte("value")}); err != nil {
		t.Fatalf("a resolved credential was refused: %v", err)
	}
}

// The image must not already set a variable the command fills. Appending a second
// assignment leaves which one wins to the container runtime.
func TestASecretNeverShadowsAnImageVariable(t *testing.T) {
	p := &agentv1.EnsureContainer{SecretEnv: []*agentv1.SecretEnv{{Name: "ENGINE_PASSWORD", Purpose: "superuser"}}}
	secrets := runtime.Secrets{"superuser": []byte("value")}
	if _, err := secretEnvironment([]string{"ENGINE_PASSWORD=from-the-image"}, p, secrets); !errors.Is(err, runtime.ErrConfigurationConflict) {
		t.Fatalf("err=%v, want a shadowed image variable refused", err)
	}
}

// A malformed variable name is refused rather than escaped.
func TestSecretEnvironmentRefusesAnUnsoundName(t *testing.T) {
	secrets := runtime.Secrets{"superuser": []byte("value")}
	for name, entry := range map[string]*agentv1.SecretEnv{
		"empty":           {Name: "", Purpose: "superuser"},
		"an equals sign":  {Name: "A=B", Purpose: "superuser"},
		"lowercase":       {Name: "password", Purpose: "superuser"},
		"a leading digit": {Name: "1PASSWORD", Purpose: "superuser"},
		"no purpose":      {Name: "ENGINE_PASSWORD", Purpose: ""},
		"a space":         {Name: "ENGINE PASSWORD", Purpose: "superuser"},
	} {
		t.Run(name, func(t *testing.T) {
			p := &agentv1.EnsureContainer{SecretEnv: []*agentv1.SecretEnv{entry}}
			if _, err := secretEnvironment(nil, p, secrets); err == nil {
				t.Fatalf("an unsound variable name was accepted: %s", name)
			}
		})
	}
}

// A verification rebuild holds no credential, so it compares the image's own
// variables exactly and the credential-backed ones by name.
func TestVerificationComparesSecretsByNameOnly(t *testing.T) {
	filled := &agentv1.EnsureContainer{SecretEnv: []*agentv1.SecretEnv{{Name: "ENGINE_PASSWORD", Purpose: "superuser"}}}
	rebuilt := []string{"PATH=/usr/bin", "LANG=C"}
	live := []string{"PATH=/usr/bin", "LANG=C", "ENGINE_PASSWORD=whatever-it-is"}
	if !sameEnvironment(live, rebuilt, filled) {
		t.Fatal("a container with its credential set did not match its own rebuild")
	}
	// A missing credential variable is a changed container.
	if sameEnvironment([]string{"PATH=/usr/bin", "LANG=C"}, rebuilt, filled) {
		t.Fatal("a container missing its credential variable matched")
	}
	// A changed image variable is a changed container.
	if sameEnvironment([]string{"PATH=/sbin", "LANG=C", "ENGINE_PASSWORD=x"}, rebuilt, filled) {
		t.Fatal("a changed image variable matched")
	}
	// The same variable assigned twice leaves which one wins to the runtime.
	if sameEnvironment([]string{"PATH=/usr/bin", "LANG=C", "ENGINE_PASSWORD=a", "ENGINE_PASSWORD=b"}, rebuilt, filled) {
		t.Fatal("a doubly assigned credential variable matched")
	}
}

func containsEnv(environment []string, want string) bool {
	for _, entry := range environment {
		if entry == want {
			return true
		}
	}
	return false
}

func TestRedisReplicationSecretRejectsControlBytes(t *testing.T) {
	p := &agentv1.EnsureContainer{Redis: &agentv1.RedisContainerConfig{}, SecretEnv: []*agentv1.SecretEnv{{Name: "REDIS_REPLICATION_PASSWORD", Purpose: "replication"}}}
	for _, value := range []byte{0, '\t', '\n', '\r', 0x1f, 0x7f} {
		material := []byte{'a', value, 'b'}
		if _, err := secretEnvironment(nil, p, runtime.Secrets{"replication": material}); !errors.Is(err, runtime.ErrConfigurationConflict) {
			t.Fatalf("control byte %d accepted", value)
		}
	}
	password := `space " quote \ slash $ dollar # hash`
	if _, err := secretEnvironment(nil, p, runtime.Secrets{"replication": []byte(password)}); err != nil {
		t.Fatal("printable replication secret rejected")
	}
}
