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
			name: "tier 1 with a service",
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
		{
			name: "tier 3 replication rejected",
			mutate: func(s *AppSpec) {
				r := DefaultReplication()
				s.Replication = &r
			},
		},
		{
			name: "tier 3 replicated mount rejected",
			mutate: func(s *AppSpec) {
				s.Service.Mounts = []MountSpec{{Type: "replicated", Source: "config", Target: "/config"}}
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

func validTier2Spec() AppSpec {
	r := DefaultReplication()
	return AppSpec{
		SchemaVersion:    1,
		Name:             "sonarr",
		Tier:             TierSingleWriter,
		Copies:           1,
		DevicePreference: DevicePreference{Preferred: []string{"mac"}, Allowed: []string{"server"}},
		Service: ServiceSpec{
			Image: "lscr.io/linuxserver/sonarr:4",
			Ports: []PortSpec{{ContainerPort: 8989, Protocol: "tcp"}},
			Mounts: []MountSpec{
				{Type: "replicated", Source: "config", Target: "/config"},
				{Type: "bind", Source: "/mnt/media/tv", Target: "/tv"},
				{Type: "tmpfs", Target: "/tmp"},
			},
		},
		Router:      RouterSpec{Protocol: "tcp", Port: 8989},
		Replication: &r,
	}
}

func TestTier2Validation(t *testing.T) {
	spec := validTier2Spec()
	if err := spec.Validate(); err != nil {
		t.Fatalf("valid tier 2 spec: %v", err)
	}
	for name, mutate := range map[string]func(*AppSpec){
		"no replication":       func(s *AppSpec) { s.Replication = nil },
		"all capable copies":   func(s *AppSpec) { s.CopiesMode = CopiesModeAllCapable },
		"no replicated volume": func(s *AppSpec) { s.Service.Mounts = s.Service.Mounts[1:] },
		"volume mounted twice": func(s *AppSpec) {
			s.Service.Mounts[1] = MountSpec{Type: "replicated", Source: "config", Target: "/other"}
		},
		"invalid volume name":      func(s *AppSpec) { s.Service.Mounts[0].Source = "/config" },
		"relative volume target":   func(s *AppSpec) { s.Service.Mounts[0].Target = "config" },
		"docker volume type":       func(s *AppSpec) { s.Service.Mounts[0].Type = "volume" },
		"zero standbys":            func(s *AppSpec) { s.Replication.Standbys = 0 },
		"four standbys":            func(s *AppSpec) { s.Replication.Standbys = 4 },
		"short interval":           func(s *AppSpec) { s.Replication.IntervalSeconds = 9 },
		"long interval":            func(s *AppSpec) { s.Replication.IntervalSeconds = 3601 },
		"failover inside a lease":  func(s *AppSpec) { s.Replication.FailoverAfterSeconds = 2*LeaseSeconds - 1 },
		"unknown failback":         func(s *AppSpec) { s.Replication.Failback = "later" },
		"short failback stability": func(s *AppSpec) { s.Replication.FailbackStableSeconds = 59 },
		"no router port":           func(s *AppSpec) { s.Router.Port = 0 },
	} {
		t.Run(name, func(t *testing.T) {
			spec := validTier2Spec()
			mutate(&spec)
			if err := spec.Validate(); err == nil {
				t.Fatal("invalid tier 2 spec accepted")
			}
		})
	}
}

func TestTier2JSONRoundTrip(t *testing.T) {
	spec := validTier2Spec()
	data, err := json.Marshal(spec)
	if err != nil {
		t.Fatal(err)
	}
	var decoded AppSpec
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(spec, decoded) {
		t.Fatalf("round trip changed the spec:\n%+v\n%+v", spec, decoded)
	}
}

func validTier1Spec() AppSpec {
	replication := DefaultDatabaseReplication()
	return AppSpec{
		SchemaVersion:    1,
		Name:             "photos-db",
		Tier:             TierDatabase,
		Copies:           1,
		DevicePreference: DevicePreference{Preferred: []string{"server"}, Allowed: []string{"odin"}},
		Router:           RouterSpec{Protocol: "tcp", Port: 5432, ReaderPort: 5433},
		Replication:      &replication,
		Database:         &DatabaseSpec{Engine: EnginePostgres, Version: "17", Databases: []string{"immich"}},
	}
}

func TestTier1Validation(t *testing.T) {
	for name, mutate := range map[string]func(*AppSpec){
		"postgres":      func(s *AppSpec) {},
		"no standby":    func(s *AppSpec) { s.Replication.Standbys = 0 },
		"no reader":     func(s *AppSpec) { s.Router.ReaderPort = 0 },
		"redis":         func(s *AppSpec) { s.Database = &DatabaseSpec{Engine: EngineRedis, Version: "8.2"} },
		"many standbys": func(s *AppSpec) { s.Replication.Standbys = 3 },
	} {
		t.Run(name, func(t *testing.T) {
			spec := validTier1Spec()
			mutate(&spec)
			if err := spec.Validate(); err != nil {
				t.Fatalf("valid tier 1 spec refused: %v", err)
			}
		})
	}
	for name, mutate := range map[string]func(*AppSpec){
		"no database":          func(s *AppSpec) { s.Database = nil },
		"unknown engine":       func(s *AppSpec) { s.Database.Engine = "mysql" },
		"version text":         func(s *AppSpec) { s.Database.Version = "latest" },
		"no postgres database": func(s *AppSpec) { s.Database.Databases = nil },
		"database name":        func(s *AppSpec) { s.Database.Databases = []string{"Photos"} },
		"repeated database":    func(s *AppSpec) { s.Database.Databases = []string{"a", "a"} },
		"redis database names": func(s *AppSpec) {
			s.Database = &DatabaseSpec{Engine: EngineRedis, Version: "8", Databases: []string{"a"}}
		},
		"service image":         func(s *AppSpec) { s.Service.Image = "postgres:17" },
		"service mount":         func(s *AppSpec) { s.Service.Mounts = []MountSpec{{Type: "tmpfs", Target: "/tmp"}} },
		"two primaries":         func(s *AppSpec) { s.Copies = 2 },
		"no replication":        func(s *AppSpec) { s.Replication = nil },
		"four standbys":         func(s *AppSpec) { s.Replication.Standbys = 4 },
		"snapshot interval":     func(s *AppSpec) { s.Replication.IntervalSeconds = 60 },
		"failover inside lease": func(s *AppSpec) { s.Replication.FailoverAfterSeconds = 2*LeaseSeconds - 1 },
		"http route":            func(s *AppSpec) { s.Router.Protocol = "http" },
		"no writer port":        func(s *AppSpec) { s.Router.Port = 0 },
		"reader on writer port": func(s *AppSpec) { s.Router.ReaderPort = s.Router.Port },
	} {
		t.Run(name, func(t *testing.T) {
			spec := validTier1Spec()
			mutate(&spec)
			if err := spec.Validate(); err == nil {
				t.Fatal("invalid tier 1 spec accepted")
			}
		})
	}
	for name, mutate := range map[string]func(*AppSpec){
		"database on tier 3":    func(s *AppSpec) { s.Database = &DatabaseSpec{Engine: EngineRedis, Version: "8"} },
		"reader port on tier 3": func(s *AppSpec) { s.Router.ReaderPort = 9000 },
	} {
		t.Run(name, func(t *testing.T) {
			spec := validTier3Spec()
			mutate(&spec)
			if err := spec.Validate(); err == nil {
				t.Fatal("tier 1 setting accepted on tier 3")
			}
		})
	}
}

func TestTier1JSONRoundTrip(t *testing.T) {
	spec := validTier1Spec()
	encoded, err := json.Marshal(spec)
	if err != nil {
		t.Fatal(err)
	}
	var decoded AppSpec
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(spec, decoded) {
		t.Fatalf("round trip changed the spec: %+v", decoded)
	}
}

func TestWorkerSpecHasNoIngress(t *testing.T) {
	s := validTier3Spec()
	s.Router = RouterSpec{}
	s.Service.Ports = nil
	s.Service.HealthCheck = &HealthCheckSpec{Type: "exec", Command: []string{"true"}}
	if err := s.Validate(); err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*AppSpec){
		func(s *AppSpec) { s.Router.Protocol = "tcp" },
		func(s *AppSpec) { s.Router.Port = 8000 },
		func(s *AppSpec) { s.Service.Ports = []PortSpec{{ContainerPort: 80}} },
		func(s *AppSpec) { s.Service.HealthCheck = &HealthCheckSpec{Type: "tcp", Port: 80} },
		func(s *AppSpec) { s.Tier = TierSingleWriter; s.Copies = 1; s.Replication = new(DefaultReplication()) },
	} {
		bad := s
		mutate(&bad)
		if err := bad.Validate(); err == nil {
			t.Fatalf("invalid worker accepted: %+v", bad)
		}
	}
}
