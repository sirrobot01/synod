package operation

import (
	"strings"
	"testing"

	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/proto"
)

func TestGenericCommandValidation(t *testing.T) {
	image := "example@sha256:" + strings.Repeat("a", 64)
	valid := &agentv1.EnsureContainer{ImageDigest: image, NetworkMode: agentv1.NetworkMode_NETWORK_MODE_BRIDGE,
		Generic: &agentv1.GenericContainerConfig{Image: image, AppName: "web", CommandOverride: true, Command: []string{"serve"}, Mounts: []*agentv1.Mount{{Type: "tmpfs", Target: "/tmp"}}, HealthCheck: &agentv1.HealthCheckConfig{Type: "tcp", Port: 80}},
		Ports:   []*agentv1.PortBinding{{ContainerPort: 80, HostPort: 0, HostAddress: "0.0.0.0", Transport: "tcp"}}, PlainEnv: []*agentv1.PlainEnv{{Name: "lowercase", Value: ""}},
	}
	command := commandFixture()
	command.Input.Payload = &agentv1.CommandInput_EnsureContainer{EnsureContainer: valid}
	if _, err := CommandHash(command); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*agentv1.EnsureContainer){
		"tag":                 func(p *agentv1.EnsureContainer) { p.ImageDigest = "nginx:latest"; p.Generic.Image = p.ImageDigest },
		"different pin":       func(p *agentv1.EnsureContainer) { p.Generic.Image = "other" },
		"no generic settings": func(p *agentv1.EnsureContainer) { p.Generic = nil },
		"implicit command":    func(p *agentv1.EnsureContainer) { p.Generic.CommandOverride = false },
		"argument nul":        func(p *agentv1.EnsureContainer) { p.Generic.Command = []string{"a\x00b"} },
		"host network":        func(p *agentv1.EnsureContainer) { p.NetworkMode = agentv1.NetworkMode_NETWORK_MODE_HOST },
		"port overflow":       func(p *agentv1.EnsureContainer) { p.Ports[0].HostPort = 65536 },
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
	p := &agentv1.EnsureContainer{ImageDigest: image, NetworkMode: agentv1.NetworkMode_NETWORK_MODE_BRIDGE, Generic: &agentv1.GenericContainerConfig{Image: image, AppName: "writer", Mounts: []*agentv1.Mount{{Type: "replicated", Source: "config", Target: "/config"}}}}
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

func TestGenericEngineMountValidation(t *testing.T) {
	image := "example@sha256:" + strings.Repeat("a", 64)
	database := func() *agentv1.EnsureContainer {
		return &agentv1.EnsureContainer{ImageDigest: image, NetworkMode: agentv1.NetworkMode_NETWORK_MODE_BRIDGE, SecretEnv: []*agentv1.SecretEnv{{Name: "POSTGRES_PASSWORD", Purpose: "superuser"}},
			Generic: &agentv1.GenericContainerConfig{Image: image, AppName: "photos-db", Mounts: []*agentv1.Mount{{Type: "engine", Source: "data", Target: "/var/lib/postgresql/data"}, {Type: "engine", Source: "tls", Target: "/tls-src", ReadOnly: true}}}}
	}
	if err := ValidateGenericContainer(database()); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*agentv1.EnsureContainer){
		"escaping source": func(p *agentv1.EnsureContainer) { p.Generic.Mounts[0].Source = "../outside" },
		"repeated source": func(p *agentv1.EnsureContainer) { p.Generic.Mounts[1].Source = "data" },
		"relative target": func(p *agentv1.EnsureContainer) { p.Generic.Mounts[0].Target = "data" },
		"with replicated data": func(p *agentv1.EnsureContainer) {
			p.Generic.Mounts = append(p.Generic.Mounts, &agentv1.Mount{Type: "replicated", Source: "config", Target: "/config"})
		},
		"too many secrets": func(p *agentv1.EnsureContainer) {
			for i := range 8 {
				p.SecretEnv = append(p.SecretEnv, &agentv1.SecretEnv{Name: "S" + strings.Repeat("X", i), Purpose: "superuser"})
			}
		},
		"secrets without an engine": func(p *agentv1.EnsureContainer) { p.Generic.Mounts = nil },
	} {
		t.Run(name, func(t *testing.T) {
			p := database()
			mutate(p)
			if err := ValidateGenericContainer(p); err == nil {
				t.Fatal("invalid database copy accepted")
			}
		})
	}
}
