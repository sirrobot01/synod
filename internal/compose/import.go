// Package compose imports one Compose service as a Synod app.
package compose

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/compose-spec/compose-go/v2/loader"
	"github.com/compose-spec/compose-go/v2/types"
	"github.com/sirrobot01/synod/internal/app"
	"go.yaml.in/yaml/v4"
)

type Options struct {
	Worker    bool
	Service   string
	Name      string
	Tier      app.Tier
	Copies    string
	Port      uint16
	Preferred []string
	Allowed   []string
	Never     []string
	// Tier 2 replication settings. Zero values select the defaults.
	Standbys int
	Interval time.Duration
	Failback string
}

// Import resolves local environment files relative to filename.
// Bind sources remain paths on the destination devices.
func Import(ctx context.Context, filename string, options Options) (app.AppSpec, error) {
	var spec app.AppSpec
	file, err := os.Open(filename)
	if err != nil {
		return spec, fmt.Errorf("read the Compose file: %w", err)
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, (1<<20)+1))
	if err != nil || len(data) > 1<<20 {
		return spec, fmt.Errorf("the Compose file must be readable and at most 1 MiB")
	}
	var raw map[string]any
	decoder := yaml.NewDecoder(strings.NewReader(string(data)))
	if err := decoder.Decode(&raw); err != nil {
		return spec, fmt.Errorf("the Compose file must contain valid YAML")
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return spec, fmt.Errorf("use one Compose YAML document")
	}
	for key := range raw {
		if !slices.Contains([]string{"version", "name", "services", "networks", "volumes"}, key) && !strings.HasPrefix(key, "x-") {
			return spec, fmt.Errorf("unsupported Compose directive: %s", key)
		}
	}
	services, ok := raw["services"].(map[string]any)
	if !ok || len(services) == 0 {
		return spec, fmt.Errorf("the Compose file must define services")
	}
	name := options.Service
	if name == "" {
		if len(services) != 1 {
			return spec, fmt.Errorf("select a SERVICE when the Compose file defines several services")
		}
		for key := range services {
			name = key
		}
	}
	service, ok := services[name].(map[string]any)
	if !ok {
		return spec, fmt.Errorf("the selected service does not exist or is invalid")
	}
	for key := range service {
		if !slices.Contains([]string{"image", "command", "entrypoint", "environment", "env_file", "ports", "volumes", "tmpfs", "deploy", "healthcheck", "networks", "restart"}, key) && !strings.HasPrefix(key, "x-") {
			return spec, fmt.Errorf("unsupported Compose service directive: %s", key)
		}
	}
	// These fields have nested settings that the app contract cannot preserve.
	for _, rule := range []struct {
		path    string
		allowed []string
	}{
		{"deploy", []string{"replicas", "resources"}},
		{"deploy.resources", []string{"limits"}},
		{"deploy.resources.limits", []string{"cpus", "memory"}},
		{"healthcheck", []string{"test", "interval", "timeout", "retries"}},
	} {
		var value any = service
		for part := range strings.SplitSeq(rule.path, ".") {
			m, ok := value.(map[string]any)
			if !ok {
				value = nil
				break
			}
			value = m[part]
		}
		if m, ok := value.(map[string]any); ok {
			for key := range m {
				if !slices.Contains(rule.allowed, key) {
					return spec, fmt.Errorf("unsupported Compose directive: %s.%s", rule.path, key)
				}
			}
		}
	}
	for _, field := range []string{"volumes", "ports"} {
		entries, _ := service[field].([]any)
		for _, entry := range entries {
			if m, ok := entry.(map[string]any); ok {
				allowed := []string{"type", "source", "target", "read_only"}
				if field == "ports" {
					allowed = []string{"target", "published", "protocol", "host_ip", "app_protocol"}
				}
				for key := range m {
					if !slices.Contains(allowed, key) {
						return spec, fmt.Errorf("unsupported Compose %s setting: %s", field, key)
					}
				}
			}
		}
	}
	delete(service, "networks")
	delete(service, "restart")
	declared, _ := raw["volumes"].(map[string]any)
	raw = map[string]any{"services": map[string]any{name: service}}
	absolute, err := filepath.Abs(filename)
	if err != nil {
		return spec, fmt.Errorf("resolve the Compose file path: %w", err)
	}
	project, err := loader.LoadWithContext(ctx, types.ConfigDetails{
		WorkingDir: filepath.Dir(absolute), Environment: types.NewMapping(os.Environ()),
		ConfigFiles: []types.ConfigFile{{Filename: absolute, Config: raw}},
	}, func(o *loader.Options) {
		o.SetProjectName("synod", true)
		o.ResolvePaths = false
		o.SkipResolveEnvironment = true
		o.SkipConsistencyCheck = true
		o.SkipInclude = true
		o.SkipExtends = true
	})
	if err != nil {
		return spec, fmt.Errorf("invalid Compose service; check field types and interpolation")
	}
	svc := project.Services[name]
	for i := range svc.EnvFiles {
		if !filepath.IsAbs(svc.EnvFiles[i].Path) {
			svc.EnvFiles[i].Path = filepath.Join(filepath.Dir(absolute), svc.EnvFiles[i].Path)
		}
	}
	project.Services[name] = svc
	project, err = project.WithServicesEnvironmentResolved(true)
	if err != nil {
		return spec, fmt.Errorf("read the service environment files failed; check their paths and syntax")
	}
	svc = project.Services[name]
	spec = app.AppSpec{SchemaVersion: 1, Name: cmp.Or(options.Name, name), Tier: cmp.Or(options.Tier, app.TierParallel), Copies: 1, CopiesMode: app.CopiesModeFixed,
		DevicePreference: app.DevicePreference{Preferred: options.Preferred, Allowed: options.Allowed, Never: options.Never},
		Service:          app.ServiceSpec{Image: svc.Image, Command: svc.Command, Entrypoint: svc.Entrypoint, Environment: map[string]string{}},
		Router:           app.RouterSpec{Protocol: "tcp", Port: options.Port},
	}
	switch spec.Tier {
	case app.TierParallel:
		if options.Standbys != 0 || options.Interval != 0 || options.Failback != "" {
			return app.AppSpec{}, fmt.Errorf("replication settings apply only to tier 2")
		}
	case app.TierSingleWriter:
		if svc.Deploy != nil && svc.Deploy.Replicas != nil && *svc.Deploy.Replicas != 1 {
			return app.AppSpec{}, fmt.Errorf("tier 2 runs one active copy; remove deploy.replicas or set it to 1")
		}
		if options.Copies != "" && options.Copies != "1" {
			return app.AppSpec{}, fmt.Errorf("tier 2 runs exactly one active copy")
		}
		replication := app.DefaultReplication()
		replication.Standbys = cmp.Or(options.Standbys, replication.Standbys)
		replication.Failback = cmp.Or(options.Failback, replication.Failback)
		if options.Interval != 0 {
			if options.Interval%time.Second != 0 || options.Interval < 0 || options.Interval/time.Second > math.MaxUint32 {
				return app.AppSpec{}, fmt.Errorf("replication interval must be a whole number of seconds")
			}
			replication.IntervalSeconds = uint32(options.Interval / time.Second)
		}
		spec.Replication = &replication
	default:
		return app.AppSpec{}, fmt.Errorf("Compose import supports tier 2 and tier 3")
	}
	for key, value := range svc.Environment {
		if value != nil {
			spec.Service.Environment[key] = *value
		}
	}
	if svc.Deploy != nil {
		if svc.Deploy.Replicas != nil {
			spec.Copies = *svc.Deploy.Replicas
		}
		if limits := svc.Deploy.Resources.Limits; limits != nil {
			cpu := float64(limits.NanoCPUs) * 1000
			// Compose stores CPU counts as float32. A whole millicore must map
			// back to the same value in that format.
			if math.IsNaN(cpu) || math.IsInf(cpu, 0) || cpu < 0 || cpu >= math.MaxInt64 || types.NanoCPUs(math.Round(cpu)/1000) != limits.NanoCPUs {
				return app.AppSpec{}, fmt.Errorf("CPU limit must be a nonnegative whole number of millicores")
			}
			spec.Service.Resources = app.ResourceSpec{MemoryBytes: int64(limits.MemoryBytes), CPUMillis: int64(math.Round(cpu))}
		}
	}
	if options.Copies == "all_capable" {
		spec.CopiesMode = app.CopiesModeAllCapable
		spec.Copies = 0
	} else if options.Copies != "" {
		spec.Copies, err = strconv.Atoi(options.Copies)
		if err != nil {
			return app.AppSpec{}, fmt.Errorf("copies must be a positive integer or all_capable")
		}
	}
	if options.Worker {
		spec.Router = app.RouterSpec{}
		if spec.Tier != app.TierParallel || options.Port != 0 || len(svc.Ports) != 0 {
			return app.AppSpec{}, fmt.Errorf("worker import requires tier 3 without published ports or an ingress override")
		}
	} else {
		// One ingress route has one backend port. Refuse extra ports until the model can name their routes.
		if len(svc.Ports) != 1 {
			return app.AppSpec{}, fmt.Errorf("import requires exactly one TCP service port")
		}
		port := svc.Ports[0]
		if port.Target == 0 || port.Target > 65535 || (port.Protocol != "" && port.Protocol != "tcp") {
			return app.AppSpec{}, fmt.Errorf("the service port must be TCP in the range 1 to 65535")
		}
		if port.HostIP != "" && port.HostIP != "0.0.0.0" {
			return app.AppSpec{}, fmt.Errorf("host_ip is unsupported; Synod ingress listens on every participating device")
		}
		if port.AppProtocol != "" && port.AppProtocol != "http" && port.AppProtocol != "tcp" {
			return app.AppSpec{}, fmt.Errorf("app_protocol must be http or tcp")
		}
		spec.Router.Protocol = cmp.Or(port.AppProtocol, "tcp")
		if port.Published != "" {
			published, err := strconv.ParseUint(port.Published, 10, 16)
			if err != nil || published == 0 {
				return app.AppSpec{}, fmt.Errorf("published port must be one port in the range 1 to 65535")
			}
			if spec.Router.Port == 0 {
				spec.Router.Port = uint16(published)
			}
		}
		spec.Service.Ports = []app.PortSpec{{ContainerPort: uint16(port.Target), Protocol: "tcp"}}
	}
	for _, volume := range svc.Volumes {
		mount := app.MountSpec{Type: volume.Type, Source: volume.Source, Target: volume.Target, ReadOnly: volume.ReadOnly}
		switch {
		case volume.Type == "bind" || volume.Type == "tmpfs":
			if volume.Bind != nil && (volume.Bind.SELinux != "" || volume.Bind.Propagation != "") {
				return app.AppSpec{}, fmt.Errorf("bind mount options are unsupported")
			}
		case volume.Type == "volume" && spec.Tier == app.TierSingleWriter:
			if volume.Source == "" {
				return app.AppSpec{}, fmt.Errorf("anonymous volumes are unsupported; name each tier 2 volume")
			}
			if volume.Volume != nil && (volume.Volume.NoCopy || volume.Volume.Subpath != "") {
				return app.AppSpec{}, fmt.Errorf("volume options are unsupported")
			}
			// Synod owns replicated volumes, so a Compose volume cannot choose a driver or an external volume.
			definition, ok := declared[volume.Source]
			if !ok {
				return app.AppSpec{}, fmt.Errorf("declare volume %s in the top-level volumes section", volume.Source)
			}
			fields, isMap := definition.(map[string]any)
			if (definition != nil && !isMap) || len(fields) > 1 || (len(fields) == 1 && fields["driver"] != "local") {
				return app.AppSpec{}, fmt.Errorf("volume %s must use the local driver without options", volume.Source)
			}
			mount.Type = "replicated"
		case spec.Tier == app.TierParallel:
			return app.AppSpec{}, fmt.Errorf("tier 3 refuses named volumes; use an external absolute bind path or tmpfs")
		default:
			return app.AppSpec{}, fmt.Errorf("tier 2 supports named volumes, absolute bind paths, and tmpfs")
		}
		spec.Service.Mounts = append(spec.Service.Mounts, mount)
	}
	for _, target := range svc.Tmpfs {
		if strings.Contains(target, ":") {
			return app.AppSpec{}, fmt.Errorf("tmpfs options are unsupported")
		}
		spec.Service.Mounts = append(spec.Service.Mounts, app.MountSpec{Type: "tmpfs", Target: target})
	}
	if h := svc.HealthCheck; h != nil {
		probe := &app.HealthCheckSpec{Type: "exec", IntervalSeconds: 30, TimeoutSeconds: 30, UnhealthyThreshold: 3}
		if len(h.Test) < 2 {
			return app.AppSpec{}, fmt.Errorf("healthcheck requires CMD or CMD-SHELL with a command")
		}
		switch h.Test[0] {
		case "CMD":
			probe.Command = h.Test[1:]
		case "CMD-SHELL":
			if len(h.Test) != 2 {
				return app.AppSpec{}, fmt.Errorf("CMD-SHELL healthcheck requires one command string")
			}
			probe.Command = []string{"/bin/sh", "-c", h.Test[1]}
		default:
			return app.AppSpec{}, fmt.Errorf("healthcheck requires CMD or CMD-SHELL")
		}
		for _, duration := range []struct {
			input  *types.Duration
			output *uint32
		}{{h.Interval, &probe.IntervalSeconds}, {h.Timeout, &probe.TimeoutSeconds}} {
			if duration.input == nil {
				continue
			}
			d := time.Duration(*duration.input)
			if d <= 0 || d%time.Second != 0 || d/time.Second > math.MaxUint32 {
				return app.AppSpec{}, fmt.Errorf("healthcheck durations must be positive whole seconds")
			}
			*duration.output = uint32(d / time.Second)
		}
		if h.Retries != nil {
			if *h.Retries == 0 || *h.Retries > math.MaxUint32 {
				return app.AppSpec{}, fmt.Errorf("healthcheck retries must be between 1 and 4294967295")
			}
			probe.UnhealthyThreshold = uint32(*h.Retries)
		}
		spec.Service.HealthCheck = probe
	}
	if err := spec.Validate(); err != nil {
		return app.AppSpec{}, err
	}
	return spec, nil
}
