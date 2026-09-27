package compose

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

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
