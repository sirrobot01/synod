package daemon

import (
	"bytes"
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"testing"

	"github.com/google/uuid"
)

func TestProtectedConfiguration(t *testing.T) {
	dir := t.TempDir()
	config := Config{InstallationID: uuid.NewString(), NodeID: uuid.NewString(), RecoveryGeneration: uuid.NewString(), EnrollmentDir: filepath.Join(dir, "enrollment"), JournalDir: filepath.Join(dir, "journal"), ControllerEndpoints: []string{"localhost:8444"}, StorageID: "pool-a", JournalMaxRetainedBytes: 512 << 20, JournalMaxStoredBytes: 2 << 30, JournalRecoveryWindowSeconds: 3600}
	data, err := json.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "config")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	loaded, err := Load(path)
	if err != nil || loaded.DockerSocket != "/var/run/docker.sock" || loaded.JournalMaxRetainedBytes != config.JournalMaxRetainedBytes || loaded.JournalMaxStoredBytes != config.JournalMaxStoredBytes || loaded.JournalRecoveryWindowSeconds != config.JournalRecoveryWindowSeconds {
		t.Fatalf("valid config: %v", err)
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
	// An agent without a storage pool identity would refuse every capacity probe.
	if err := os.WriteFile(path, bytes.Replace(data, []byte(`"storage_id":"pool-a"`), []byte(`"storage_id":""`), 1), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(path); err == nil || !strings.Contains(err.Error(), "storage_id") {
		t.Fatalf("missing storage_id: %v", err)
	}
	config.DockerSocket = "/var/run/docker.sock"
	for _, change := range []func(*Config){func(c *Config) { c.JournalMaxRetainedBytes = -1 }, func(c *Config) { c.JournalMaxStoredBytes = -1 }, func(c *Config) { c.JournalRecoveryWindowSeconds = -1 }, func(c *Config) { c.JournalRecoveryWindowSeconds = math.MaxInt64 }, func(c *Config) { c.RecoveryGeneration = uuid.Nil.String() }, func(c *Config) { c.JournalDir = c.EnrollmentDir }, func(c *Config) { c.JournalDir = filepath.Join(c.EnrollmentDir, "journal") }, func(c *Config) { c.ControllerEndpoints = []string{"https://user:secret@example:123"} }, func(c *Config) { c.DockerSocket = "tcp://localhost:2375" }, func(c *Config) { c.StorageID = "" }, func(c *Config) { c.StorageID = "pool a" }, func(c *Config) { c.StoragePath = "data/pool" }} {
		bad := config
		change(&bad)
		if bad.validate() == nil {
			t.Fatal("invalid configuration accepted")
		}
	}
}

func TestBootIdentity(t *testing.T) {
	if runtime.GOOS == "linux" {
		first, err := bootIdentity("")
		if err != nil {
			t.Fatal(err)
		}
		second, err := bootIdentity("")
		if err != nil || first != second {
			t.Fatal("boot identity changed")
		}
		if _, err := bootIdentity(uuid.NewString()); err == nil {
			t.Fatal("Linux boot identity override accepted")
		}
	} else if runtime.GOOS == "darwin" {
		if _, err := bootIdentity(""); err == nil {
			t.Fatal("ordinary macOS run accepted")
		}
		id := uuid.NewString()
		if got, err := bootIdentity(id); err != nil || got != id {
			t.Fatal("explicit fixture boot identity failed")
		}
	}
	if _, err := bootIdentity("invalid-secret"); err == nil {
		t.Fatal("invalid boot ID accepted")
	}
}
