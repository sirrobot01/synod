package operation

import (
	"encoding/hex"
	"fmt"
	"net/netip"
	"strings"

	"github.com/sirrobot01/synod/internal/app"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
)

// ValidateGenericContainer also checks stored Docker configuration during recovery.
func ValidateGenericContainer(p *agentv1.EnsureContainer) error {
	if p == nil || p.Generic == nil || p.Engine != "generic" || p.Postgres != nil || p.Redis != nil || p.VolumeId != "" || p.NetworkId != "" || p.NetworkMode != agentv1.NetworkMode_NETWORK_MODE_BRIDGE || len(p.SecretEnv) != 0 {
		return fmt.Errorf("generic containers require bridge networking without engine state")
	}
	g := p.Generic
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
	}
	targets := map[string]bool{}
	for _, m := range g.Mounts {
		if m == nil || targets[m.Target] || (m.Type == "tmpfs" && m.Source != "") {
			return fmt.Errorf("generic mount is invalid or repeated")
		}
		targets[m.Target] = true
		spec.Service.Mounts = append(spec.Service.Mounts, app.MountSpec{Type: m.Type, Source: m.Source, Target: m.Target, ReadOnly: m.ReadOnly})
	}
	if h := g.HealthCheck; h != nil {
		if h.Port > 65535 || (h.Type != "exec" && !ports[h.Port]) {
			return fmt.Errorf("health probe needs a published backend port")
		}
		spec.Service.HealthCheck = &app.HealthCheckSpec{Type: h.Type, Path: h.Path, Port: uint16(h.Port), Command: h.Command}
	}
	return spec.Validate()
}
