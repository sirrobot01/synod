package app

import (
	"encoding/json"
	"reflect"
	"testing"
)

func validTier3Spec() AppSpec {
	return AppSpec{
		SchemaVersion: 1,
		Name:          "web-frontend",
		Tier:          TierParallel,
		Copies:        3,
		CopiesMode:    CopiesModeFixed,
		DevicePreference: DevicePreference{
			Preferred: []string{"server", "odin"},
			Allowed:   []string{"*"},
			Never:     []string{"untrusted"},
		},
		Service: ServiceSpec{
			Image:   "traefik/whoami:latest",
			Command: []string{"--port", "80"},
			Environment: map[string]string{
				"ENV": "production",
			},
			Ports: []PortSpec{
				{ContainerPort: 80, Protocol: "tcp"},
			},
			Mounts: []MountSpec{
				{Type: "bind", Source: "/var/log/app", Target: "/logs", ReadOnly: false},
				{Type: "tmpfs", Target: "/tmp"},
			},
			Resources: ResourceSpec{
				MemoryBytes: 256 * 1024 * 1024,
				CPUMillis:   500,
			},
			HealthCheck: &HealthCheckSpec{
				Type:            "http",
				Path:            "/health",
				Port:            80,
				IntervalSeconds: 10,
				TimeoutSeconds:  2,
			},
			SupportedArchitectures: []string{"amd64", "arm64"},
		},
		Router: RouterSpec{
			Protocol: "http",
			Port:     8080,
		},
	}
}

func TestAppSpecValidationSuccess(t *testing.T) {
	spec := validTier3Spec()
	if err := spec.Validate(); err != nil {
		t.Fatalf("expected valid spec, got error: %v", err)
	}
}

func TestAppSpecJSONRoundtrip(t *testing.T) {
	spec := validTier3Spec()
	data, err := json.Marshal(spec)
	if err != nil {
		t.Fatalf("marshal error: %v", err)
	}

	var decoded AppSpec
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("unmarshal error: %v", err)
	}

	if decoded.Name != spec.Name || decoded.Tier != spec.Tier || decoded.Copies != spec.Copies {
		t.Fatalf("decoded mismatch: got %+v, want %+v", decoded, spec)
	}
	if err := decoded.Validate(); err != nil {
		t.Fatalf("decoded spec invalid: %v", err)
	}
}

func TestAppSpecValidationFailures(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(s *AppSpec)
	}{
		{
			name: "wrong schema version",
			mutate: func(s *AppSpec) {
				s.SchemaVersion = 2
			},
		},
		{
			name: "invalid name uppercase",
			mutate: func(s *AppSpec) {
				s.Name = "Web-Frontend"
			},
		},
		{
			name: "invalid name special chars",
			mutate: func(s *AppSpec) {
				s.Name = "web_frontend!"
			},
		},
		{
			name: "missing image",
			mutate: func(s *AppSpec) {
				s.Service.Image = ""
			},
		},
		{
			name: "zero copies in fixed mode",
			mutate: func(s *AppSpec) {
				s.Copies = 0
			},
		},
		{
			name: "invalid mount type",
			mutate: func(s *AppSpec) {
				s.Service.Mounts = []MountSpec{{Type: "volume", Source: "named_vol", Target: "/data"}}
			},
		},
		{
			name: "relative bind source",
			mutate: func(s *AppSpec) {
				s.Service.Mounts = []MountSpec{{Type: "bind", Source: "rel/path", Target: "/data"}}
			},
		},
		{
			name: "relative mount target",
			mutate: func(s *AppSpec) {
				s.Service.Mounts = []MountSpec{{Type: "bind", Source: "/abs/path", Target: "rel/target"}}
			},
		},
		{
			name: "zero router port",
			mutate: func(s *AppSpec) {
				s.Router.Port = 0
			},
		},
		{
			name: "unsupported router protocol",
			mutate: func(s *AppSpec) {
				s.Router.Protocol = "udp"
			},
		},
		{
			name: "tier 1 rejected in app spec",
			mutate: func(s *AppSpec) {
				s.Tier = TierDatabase
			},
		},
		{
			name: "tier 2 multiple copies rejected",
			mutate: func(s *AppSpec) {
				s.Tier = TierSingleWriter
				s.Copies = 2
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			spec := validTier3Spec()
			tc.mutate(&spec)
			if err := spec.Validate(); err == nil {
				t.Fatalf("expected validation error for test %q, but got nil", tc.name)
			}
		})
	}
}

func TestRejectInvalidServiceSettings(t *testing.T) {
	for name, mutate := range map[string]func(*AppSpec){
		"copy mode":                 func(s *AppSpec) { s.CopiesMode = "unknown" },
		"negative copies":           func(s *AppSpec) { s.Copies = -1; s.CopiesMode = CopiesModeAllCapable },
		"memory":                    func(s *AppSpec) { s.Service.Resources.MemoryBytes = -1 },
		"cpu":                       func(s *AppSpec) { s.Service.Resources.CPUMillis = -1 },
		"port":                      func(s *AppSpec) { s.Service.Ports[0].ContainerPort = 0 },
		"protocol":                  func(s *AppSpec) { s.Service.Ports[0].Protocol = "sctp" },
		"environment":               func(s *AppSpec) { s.Service.Environment["BAD=NAME"] = "value" },
		"probe type":                func(s *AppSpec) { s.Service.HealthCheck.Type = "unknown" },
		"http port":                 func(s *AppSpec) { s.Service.HealthCheck.Port = 0 },
		"http path":                 func(s *AppSpec) { s.Service.HealthCheck.Path = "relative" },
		"exec command":              func(s *AppSpec) { s.Service.HealthCheck = &HealthCheckSpec{Type: "exec"} },
		"single writer all capable": func(s *AppSpec) { s.Tier = TierSingleWriter; s.Copies = 1; s.CopiesMode = CopiesModeAllCapable },
	} {
		t.Run(name, func(t *testing.T) {
			spec := validTier3Spec()
			mutate(&spec)
			if err := spec.Validate(); err == nil {
				t.Fatal("invalid spec accepted")
			}
		})
	}
}

func TestContainerOverridesRoundTrip(t *testing.T) {
	for _, command := range [][]string{nil, {}, {"serve", "--port=80"}} {
		spec := ServiceSpec{Image: "example", Command: command, Entrypoint: command}
		data, err := json.Marshal(spec)
		if err != nil {
			t.Fatal(err)
		}
		var got ServiceSpec
		if err := json.Unmarshal(data, &got); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(spec, got) {
			t.Fatalf("override changed: %s", data)
		}
	}
}
