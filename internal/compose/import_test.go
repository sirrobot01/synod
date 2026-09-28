package compose

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/sirrobot01/synod/internal/app"
)

func TestImportCompose(t *testing.T) {
	directory := t.TempDir()
	t.Setenv("SYNOD_IMPORT_TEST", "from-host")
	if err := os.WriteFile(filepath.Join(directory, "app.env"), []byte("FROM_FILE=value\nOVERRIDE=file\n"), 0600); err != nil {
		t.Fatal(err)
	}
	filename := filepath.Join(directory, "compose.yml")
	data := `version: "3.8"
services:
  web:
    image: nginx:alpine
    command: 'nginx -g "daemon off;"'
    entrypoint: []
    env_file: app.env
    environment:
      OVERRIDE: service
      FROM_HOST: ${SYNOD_IMPORT_TEST}
    ports:
      - target: 80
        published: "8080"
        app_protocol: http
    volumes:
      - /mnt/media:/media:ro
      - type: tmpfs
        target: /cache
    tmpfs: /tmp
    deploy:
      replicas: 3
      resources:
        limits:
          cpus: "0.25"
          memory: 256M
    healthcheck:
      test: [CMD-SHELL, 'test -f /tmp/ready']
      interval: 10s
      timeout: 2s
      retries: 4
    networks: [custom]
    restart: always
networks:
  custom: {}
`
	if err := os.WriteFile(filename, []byte(data), 0600); err != nil {
		t.Fatal(err)
	}
	spec, err := Import(t.Context(), filename, Options{Preferred: []string{"odin"}})
	if err != nil {
		t.Fatal(err)
	}
	if spec.Name != "web" || spec.Copies != 3 || spec.Router.Port != 8080 || spec.Router.Protocol != "http" || spec.Tier != app.TierParallel {
		t.Fatalf("spec: %+v", spec)
	}
	if !reflect.DeepEqual(spec.Service.Command, []string{"nginx", "-g", "daemon off;"}) || spec.Service.Entrypoint == nil || len(spec.Service.Entrypoint) != 0 {
		t.Fatalf("overrides: %+v", spec.Service)
	}
	if !reflect.DeepEqual(spec.Service.Environment, map[string]string{"FROM_FILE": "value", "OVERRIDE": "service", "FROM_HOST": "from-host"}) {
		t.Fatalf("environment: %v", spec.Service.Environment)
	}
	if spec.Service.Resources.MemoryBytes != 256*1024*1024 || spec.Service.Resources.CPUMillis != 250 {
		t.Fatalf("limits: %+v", spec.Service.Resources)
	}
	if len(spec.Service.Mounts) != 3 || !spec.Service.Mounts[0].ReadOnly {
		t.Fatalf("mounts: %+v", spec.Service.Mounts)
	}
	if h := spec.Service.HealthCheck; h.IntervalSeconds != 10 || h.TimeoutSeconds != 2 || h.UnhealthyThreshold != 4 || !reflect.DeepEqual(h.Command, []string{"/bin/sh", "-c", "test -f /tmp/ready"}) {
		t.Fatalf("probe: %+v", h)
	}
	spec, err = Import(t.Context(), filename, Options{Name: "renamed", Copies: "all_capable", Port: 9090})
	if err != nil || spec.Name != "renamed" || spec.CopiesMode != app.CopiesModeAllCapable || spec.Copies != 0 || spec.Router.Port != 9090 {
		t.Fatalf("overrides: %+v %v", spec, err)
	}
}

func TestImportCPULimits(t *testing.T) {
	t.Setenv("SYNOD_IMPORT_CPU", "0.10")
	for _, tc := range []struct {
		value string
		want  int64
	}{
		{`"0.10"`, 100},
		{`0.127`, 127},
		{`"0.30"`, 300},
		{`"1.001"`, 1001},
		{`"123.456"`, 123456},
		{`"${SYNOD_IMPORT_CPU}"`, 100},
		{`"0"`, 0},
		{`"0.0001"`, -1},
		{`"0.1004"`, -1},
		{`"-0.1"`, -1},
		{`"1e20"`, -1},
	} {
		t.Run(tc.value, func(t *testing.T) {
			filename := filepath.Join(t.TempDir(), "compose.yml")
			content := "services:\n  web:\n    image: nginx\n    ports: ['8080:80']\n    deploy:\n      resources:\n        limits:\n          cpus: " + tc.value + "\n"
			if err := os.WriteFile(filename, []byte(content), 0600); err != nil {
				t.Fatal(err)
			}
			spec, err := Import(t.Context(), filename, Options{})
			if tc.want < 0 {
				if err == nil {
					t.Fatalf("invalid CPU limit accepted: %+v", spec.Service.Resources)
				}
				return
			}
			if err != nil || spec.Service.Resources.CPUMillis != tc.want {
				t.Fatalf("CPU limit: %d, want %d; error: %v", spec.Service.Resources.CPUMillis, tc.want, err)
			}
		})
	}
}

func TestImportRefusesUnsupportedSettings(t *testing.T) {
	for name, field := range map[string]string{
		"build":             "build: .",
		"privileged":        "privileged: true",
		"dependencies":      "depends_on: [db]",
		"named volume":      "volumes: [data:/data]",
		"anonymous volume":  "volumes: [/data]",
		"relative bind":     "volumes: [./data:/data]",
		"bind options":      "volumes: [/data:/data:z]",
		"long bind options": "volumes: [{type: bind, source: /data, target: /data, bind: {propagation: shared}}]",
		"reservation":       "deploy: {resources: {reservations: {memory: 1G}}}",
		"gpu":               "deploy: {resources: {limits: {pids: 20}}}",
		"zero copies":       "deploy: {replicas: 0}",
		"health start":      "healthcheck: {test: [CMD, true], start_period: 10s}",
		"health fraction":   "healthcheck: {test: [CMD, /bin/true], interval: 500ms}",
		"health disabled":   "healthcheck: {disable: true}",
		"bad env file":      "env_file: missing.env",
	} {
		t.Run(name, func(t *testing.T) {
			filename := filepath.Join(t.TempDir(), "compose.yml")
			content := "services:\n  web:\n    image: nginx\n    ports: ['8080:80']\n    " + field + "\n"
			if err := os.WriteFile(filename, []byte(content), 0600); err != nil {
				t.Fatal(err)
			}
			if spec, err := Import(t.Context(), filename, Options{}); err == nil {
				t.Fatalf("unsupported service accepted: %+v", spec)
			}
		})
	}
	for name, ports := range map[string]string{"udp": "['8080:80/udp']", "loopback": "['127.0.0.1:8080:80']", "multiple": "['8080:80','9090:90']", "range": "['8080-8082:80']", "missing published": "['80']"} {
		t.Run(name, func(t *testing.T) {
			filename := filepath.Join(t.TempDir(), "compose.yml")
			if err := os.WriteFile(filename, []byte("services: {web: {image: nginx, ports: "+ports+"}}"), 0600); err != nil {
				t.Fatal(err)
			}
			if _, err := Import(t.Context(), filename, Options{}); err == nil {
				t.Fatal("unsupported ports accepted")
			}
		})
	}
}

func TestImportSelectsOneService(t *testing.T) {
	filename := filepath.Join(t.TempDir(), "compose.yml")
	content := "services:\n  web:\n    image: nginx\n    ports: ['80']\n    environment: ['KEY=value']\n  db:\n    image: postgres\n    env_file: missing.env\n"
	if err := os.WriteFile(filename, []byte(content), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := Import(t.Context(), filename, Options{}); err == nil || !strings.Contains(err.Error(), "select a SERVICE") {
		t.Fatalf("selection error: %v", err)
	}
	spec, err := Import(t.Context(), filename, Options{Service: "web", Port: 8080, Copies: "2"})
	if err != nil || spec.Copies != 2 || spec.Service.Environment["KEY"] != "value" {
		t.Fatalf("selected spec: %+v %v", spec, err)
	}
	if spec.Service.Command != nil || spec.Service.Entrypoint != nil {
		t.Fatal("missing overrides became empty overrides")
	}
	for _, options := range []Options{{Service: "missing"}, {Service: "web", Tier: app.TierSingleWriter}, {Service: "web", Copies: "0", Port: 8080}, {Service: "web", Copies: "garbage", Port: 8080}} {
		if _, err := Import(t.Context(), filename, options); err == nil {
			t.Fatalf("accepted %+v", options)
		}
	}
}

func TestImportTier2(t *testing.T) {
	filename := filepath.Join(t.TempDir(), "compose.yml")
	content := `services:
  sonarr:
    image: lscr.io/linuxserver/sonarr:4
    ports: ['8989:8989']
    volumes:
      - config:/config
      - /mnt/media/tv:/tv
    tmpfs: /tmp
    deploy: {replicas: 1}
volumes:
  config:
`
	if err := os.WriteFile(filename, []byte(content), 0600); err != nil {
		t.Fatal(err)
	}
	spec, err := Import(t.Context(), filename, Options{Tier: app.TierSingleWriter, Preferred: []string{"mac"}, Allowed: []string{"server"}, Never: []string{"odin"}})
	if err != nil {
		t.Fatal(err)
	}
	want := []app.MountSpec{{Type: "replicated", Source: "config", Target: "/config"}, {Type: "bind", Source: "/mnt/media/tv", Target: "/tv"}, {Type: "tmpfs", Target: "/tmp"}}
	if spec.Tier != app.TierSingleWriter || spec.Copies != 1 || !reflect.DeepEqual(spec.Service.Mounts, want) {
		t.Fatalf("spec: %+v", spec)
	}
	if !reflect.DeepEqual(spec.DevicePreference, app.DevicePreference{Preferred: []string{"mac"}, Allowed: []string{"server"}, Never: []string{"odin"}}) {
		t.Fatalf("preference: %+v", spec.DevicePreference)
	}
	if spec.Replication == nil || *spec.Replication != app.DefaultReplication() {
		t.Fatalf("replication: %+v", spec.Replication)
	}
	spec, err = Import(t.Context(), filename, Options{Tier: app.TierSingleWriter, Standbys: 2, Interval: 30 * time.Second, Failback: app.FailbackManual})
	if err != nil {
		t.Fatal(err)
	}
	if r := spec.Replication; r.Standbys != 2 || r.IntervalSeconds != 30 || r.Failback != app.FailbackManual {
		t.Fatalf("replication overrides: %+v", r)
	}
}

func TestImportTier2Refusals(t *testing.T) {
	for name, tc := range map[string]struct {
		service, volumes string
		options          Options
	}{
		"no named volume":       {service: "volumes: [/mnt/tv:/tv]"},
		"two replicas":          {service: "volumes: [config:/config]\n    deploy: {replicas: 2}", volumes: "config:"},
		"copies option":         {service: "volumes: [config:/config]", volumes: "config:", options: Options{Copies: "2"}},
		"anonymous volume":      {service: "volumes: [/config]"},
		"undeclared volume":     {service: "volumes: [config:/config]"},
		"external volume":       {service: "volumes: [config:/config]", volumes: "config: {external: true}"},
		"volume driver":         {service: "volumes: [config:/config]", volumes: "config: {driver: nfs}"},
		"driver options":        {service: "volumes: [config:/config]", volumes: "config: {driver: local, driver_opts: {type: nfs}}"},
		"scalar volume":         {service: "volumes: [config:/config]", volumes: "config: local"},
		"nocopy":                {service: "volumes: ['config:/config:nocopy']", volumes: "config:"},
		"fractional interval":   {service: "volumes: [config:/config]", volumes: "config:", options: Options{Interval: 1500 * time.Millisecond}},
		"short interval":        {service: "volumes: [config:/config]", volumes: "config:", options: Options{Interval: 5 * time.Second}},
		"unknown failback":      {service: "volumes: [config:/config]", volumes: "config:", options: Options{Failback: "never"}},
		"too many standbys":     {service: "volumes: [config:/config]", volumes: "config:", options: Options{Standbys: 4}},
		"tier 3 replication":    {service: "volumes: [/mnt/tv:/tv]", options: Options{Tier: app.TierParallel, Standbys: 1}},
		"tier 3 named volume":   {service: "volumes: [config:/config]", volumes: "config:", options: Options{Tier: app.TierParallel}},
		"tier 1 is not Compose": {service: "volumes: [config:/config]", volumes: "config:", options: Options{Tier: app.TierDatabase}},
	} {
		t.Run(name, func(t *testing.T) {
			content := "services:\n  sonarr:\n    image: sonarr\n    ports: ['8989:8989']\n    " + tc.service + "\n"
			if tc.volumes != "" {
				content += "volumes:\n  " + tc.volumes + "\n"
			}
			filename := filepath.Join(t.TempDir(), "compose.yml")
			if err := os.WriteFile(filename, []byte(content), 0600); err != nil {
				t.Fatal(err)
			}
			if tc.options.Tier == 0 {
				tc.options.Tier = app.TierSingleWriter
			}
			if spec, err := Import(t.Context(), filename, tc.options); err == nil {
				t.Fatalf("accepted: %+v", spec)
			}
		})
	}
}

func TestImportWorker(t *testing.T) {
	filename := filepath.Join(t.TempDir(), "compose.yml")
	data := "services:\n  worker:\n    image: redis:8\n    command: [sh, -c, 'sleep 100']\n    healthcheck:\n      test: [CMD, 'true']\n"
	if err := os.WriteFile(filename, []byte(data), 0600); err != nil {
		t.Fatal(err)
	}
	spec, err := Import(t.Context(), filename, Options{Worker: true, Copies: "all_capable"})
	if err != nil {
		t.Fatal(err)
	}
	if spec.Router != (app.RouterSpec{}) || len(spec.Service.Ports) != 0 || spec.CopiesMode != app.CopiesModeAllCapable || spec.Service.HealthCheck.Type != "exec" {
		t.Fatalf("worker: %+v", spec)
	}
	for _, options := range []Options{{}, {Worker: true, Tier: app.TierSingleWriter}, {Worker: true, Port: 8080}} {
		if _, err := Import(t.Context(), filename, options); err == nil {
			t.Fatalf("invalid worker import accepted: %+v", options)
		}
	}
	if err := os.WriteFile(filename, []byte(data+"    ports: ['8080:80']\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := Import(t.Context(), filename, Options{Worker: true}); err == nil {
		t.Fatal("published worker port accepted")
	}
}
