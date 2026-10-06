package daemon

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/google/uuid"
)

func TestProtectedConfiguration(t *testing.T) {
	dir := t.TempDir()
	config := Config{InstallationID: uuid.NewString(), NodeID: uuid.NewString(), EnrollmentDir: filepath.Join(dir, "enrollment"), ControllerEndpoints: []string{"localhost:8444"}}
	data, err := json.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "config")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	loaded, err := Load(path)
	if err != nil || loaded.DockerSocket != "/var/run/docker.sock" {
		t.Fatalf("valid config: %+v %v", loaded, err)
	}
	for name, content := range map[string][]byte{"oversized": bytes.Repeat([]byte("x"), (64<<10)+1), "trailing": append(bytes.Clone(data), []byte(" {}")...), "unknown": bytes.Replace(data, []byte("{"), []byte(`{"token":"private",`), 1)} {
		t.Run(name, func(t *testing.T) {
			if err := os.WriteFile(path, content, 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := Load(path); err == nil {
				t.Fatal("unsafe config accepted")
			}
		})
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(path); err == nil {
		t.Fatal("public configuration accepted")
	}
	link := filepath.Join(dir, "link")
	if err := os.Symlink(path, link); err != nil {
		t.Fatal(err)
	}
	fifo := filepath.Join(dir, "fifo")
	if err := syscall.Mkfifo(fifo, 0o600); err != nil {
		t.Fatal(err)
	}
	for _, unsafe := range []string{link, fifo, dir} {
		if _, err := Load(unsafe); err == nil {
			t.Fatal("non-regular config accepted")
		}
	}
	config.DockerSocket = "/var/run/docker.sock"
	for _, change := range []func(*Config){
		func(c *Config) { c.NodeID = uuid.Nil.String() },
		func(c *Config) { c.EnrollmentDir = "relative" },
		func(c *Config) { c.ControllerEndpoints = nil },
		func(c *Config) { c.ControllerEndpoints = []string{"https://user:secret@example:123"} },
		func(c *Config) { c.DockerSocket = "tcp://localhost:2375" },
	} {
		bad := config
		change(&bad)
		if bad.validate() == nil {
			t.Fatal("invalid configuration accepted")
		}
	}
}
