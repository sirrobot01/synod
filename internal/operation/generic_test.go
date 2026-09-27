package operation

import (
	"strings"
	"testing"

	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/proto"
)

func TestGenericCommandValidation(t *testing.T) {
	image := "example@sha256:" + strings.Repeat("a", 64)
	valid := &agentv1.EnsureContainer{Engine: "generic", ImageDigest: image, NetworkMode: agentv1.NetworkMode_NETWORK_MODE_BRIDGE,
		Generic: &agentv1.GenericContainerConfig{Image: image, AppName: "web", CommandOverride: true, Command: []string{"serve"}, Mounts: []*agentv1.Mount{{Type: "tmpfs", Target: "/tmp"}}, HealthCheck: &agentv1.HealthCheckConfig{Type: "tcp", Port: 80}},
		Ports:   []*agentv1.PortBinding{{ContainerPort: 80, HostPort: 0, HostAddress: "0.0.0.0", Transport: "tcp"}}, PlainEnv: []*agentv1.PlainEnv{{Name: "lowercase", Value: ""}},
	}
	command := commandFixture()
	command.Input.Payload = &agentv1.CommandInput_EnsureContainer{EnsureContainer: valid}
	if _, err := CommandHash(command); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*agentv1.EnsureContainer){
		"tag":               func(p *agentv1.EnsureContainer) { p.ImageDigest = "nginx:latest"; p.Generic.Image = p.ImageDigest },
		"different pin":     func(p *agentv1.EnsureContainer) { p.Generic.Image = "other" },
		"database engine":   func(p *agentv1.EnsureContainer) { p.Engine = "postgres" },
		"database settings": func(p *agentv1.EnsureContainer) { p.Redis = &agentv1.RedisContainerConfig{} },
		"implicit command":  func(p *agentv1.EnsureContainer) { p.Generic.CommandOverride = false },
		"argument nul":      func(p *agentv1.EnsureContainer) { p.Generic.Command = []string{"a\x00b"} },
		"host network":      func(p *agentv1.EnsureContainer) { p.NetworkMode = agentv1.NetworkMode_NETWORK_MODE_HOST },
		"port overflow":     func(p *agentv1.EnsureContainer) { p.Ports[0].HostPort = 65536 },
		"relative mount": func(p *agentv1.EnsureContainer) {
			p.Generic.Mounts = []*agentv1.Mount{{Type: "bind", Source: "./data", Target: "/data"}}
		},
		"duplicate env":     func(p *agentv1.EnsureContainer) { p.PlainEnv = append(p.PlainEnv, p.PlainEnv[0]) },
		"unpublished probe": func(p *agentv1.EnsureContainer) { p.Generic.HealthCheck.Port = 90 },
	} {
		t.Run(name, func(t *testing.T) {
			changed := proto.Clone(valid).(*agentv1.EnsureContainer)
			mutate(changed)
			command.Input.Payload = &agentv1.CommandInput_EnsureContainer{EnsureContainer: changed}
			if _, err := CommandHash(command); err == nil {
				t.Fatal("invalid generic command accepted")
			}
		})
	}
}

func TestGenericReplicatedMountValidation(t *testing.T) {
	image := "example@sha256:" + strings.Repeat("a", 64)
	p := &agentv1.EnsureContainer{Engine: "generic", ImageDigest: image, NetworkMode: agentv1.NetworkMode_NETWORK_MODE_BRIDGE, Generic: &agentv1.GenericContainerConfig{Image: image, AppName: "writer", Mounts: []*agentv1.Mount{{Type: "replicated", Source: "config", Target: "/config"}}}}
	if err := ValidateGenericContainer(p); err != nil {
		t.Fatal(err)
	}
	for _, source := range []string{"../outside", "/absolute", "has/slash", ""} {
		p.Generic.Mounts[0].Source = source
		if err := ValidateGenericContainer(p); err == nil {
			t.Fatalf("accepted %q", source)
		}
	}
}
