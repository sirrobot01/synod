package operation

import (
	"encoding/hex"
	"fmt"
	"net/netip"
	"regexp"
	"slices"
	"strings"

	"github.com/sirrobot01/synod/internal/app"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
)

var engineSourcePattern = regexp.MustCompile(`^[a-z][a-z0-9-]{0,62}$`)

// ValidateGenericContainer also checks stored Docker configuration during recovery.
// A container with an engine mount is a tier 1 database copy. Only it may take secret
// environment values, which carry its credentials.
func ValidateGenericContainer(p *agentv1.EnsureContainer) error {
	if p == nil || p.Generic == nil || p.NetworkMode != agentv1.NetworkMode_NETWORK_MODE_BRIDGE {
		return fmt.Errorf("containers require generic settings and bridge networking")
	}
	g := p.Generic
	database := slices.ContainsFunc(g.Mounts, func(m *agentv1.Mount) bool { return m != nil && m.Type == "engine" })
	if len(p.SecretEnv) > 0 && !database {
		return fmt.Errorf("only a database copy takes secret environment values")
	}
	if len(p.SecretEnv) > 8 {
		return fmt.Errorf("a database copy takes at most 8 secret environment values")
	}
	if g.Image != p.ImageDigest {
		return fmt.Errorf("generic image must match its pinned reference")
	}
	if !imageDigestPattern.MatchString(g.Image) {
		digest, ok := strings.CutPrefix(g.Image, "sha256:")
		if _, err := hex.DecodeString(digest); !ok || err != nil || len(digest) != 64 {
			return fmt.Errorf("generic image must be pinned to a digest or local image ID")
		}
	}
	if (!g.CommandOverride && len(g.Command) != 0) || (!g.EntrypointOverride && len(g.Entrypoint) != 0) {
		return fmt.Errorf("container command overrides must be explicit")
	}
	for _, args := range [][]string{g.Command, g.Entrypoint} {
		for _, arg := range args {
			if strings.ContainsRune(arg, 0) {
				return fmt.Errorf("container arguments cannot contain NUL")
			}
		}
	}
	spec := app.AppSpec{SchemaVersion: 1, Name: g.AppName, Tier: app.TierParallel, Copies: 1, Router: app.RouterSpec{Protocol: "tcp", Port: 1}, Service: app.ServiceSpec{Image: g.Image, Resources: app.ResourceSpec{MemoryBytes: p.MemoryBytes, CPUMillis: p.CpuMillis}, Environment: map[string]string{}}}
	if len(p.PlainEnv) > 256 || len(p.Ports) > 64 || len(g.Mounts) > 64 {
		return fmt.Errorf("generic container exceeds configuration limits")
	}
	for _, env := range p.PlainEnv {
		if env == nil {
			return fmt.Errorf("environment entry is absent")
		}
		if _, exists := spec.Service.Environment[env.Name]; exists {
			return fmt.Errorf("environment name is repeated")
		}
		spec.Service.Environment[env.Name] = env.Value
	}
	ports := map[uint32]bool{}
	for _, port := range p.Ports {
		if port == nil || port.ContainerPort == 0 || port.ContainerPort > 65535 || port.HostPort > 65535 || port.Transport != "tcp" || ports[port.ContainerPort] {
			return fmt.Errorf("generic ports must be unique TCP ports")
		}
		address, err := netip.ParseAddr(port.HostAddress)
		if err != nil || address.IsMulticast() {
			return fmt.Errorf("generic host address must be a unicast or wildcard IP")
		}
		ports[port.ContainerPort] = true
		spec.Service.Ports = append(spec.Service.Ports, app.PortSpec{ContainerPort: uint16(port.ContainerPort), Protocol: port.Transport})
	}
	targets, engines := map[string]bool{}, map[string]bool{}
	for _, m := range g.Mounts {
		if m == nil || targets[m.Target] || (m.Type == "tmpfs" && m.Source != "") {
			return fmt.Errorf("generic mount is invalid or repeated")
		}
		targets[m.Target] = true
		// Engine data lives in Synod-owned directories. It never passes through the app mount rules.
		if m.Type == "engine" {
			if !engineSourcePattern.MatchString(m.Source) || engines[m.Source] || !strings.HasPrefix(m.Target, "/") {
				return fmt.Errorf("engine mount is invalid or repeated")
			}
			engines[m.Source] = true
			continue
		}
		if m.Type == "replicated" {
			spec.Tier = app.TierSingleWriter
			spec.Replication = new(app.DefaultReplication())
		}
		spec.Service.Mounts = append(spec.Service.Mounts, app.MountSpec{Type: m.Type, Source: m.Source, Target: m.Target, ReadOnly: m.ReadOnly})
	}
	if database && spec.Tier == app.TierSingleWriter {
		return fmt.Errorf("a container cannot hold both engine and replicated mounts")
	}
	if h := g.HealthCheck; h != nil {
		if h.Port > 65535 || (h.Type != "exec" && !ports[h.Port]) {
			return fmt.Errorf("health probe needs a published backend port")
		}
		spec.Service.HealthCheck = &app.HealthCheckSpec{Type: h.Type, Path: h.Path, Port: uint16(h.Port), Command: h.Command}
	}
	if spec.Tier == app.TierParallel && len(p.Ports) == 0 {
		spec.Router = app.RouterSpec{}
	}
	return spec.Validate()
}
