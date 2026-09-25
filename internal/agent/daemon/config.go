// Package daemon owns enrolled agent startup and borrowed component lifetimes.
package daemon

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/google/uuid"
)

var ErrConfiguration = errors.New("invalid protected agent configuration")
var ErrEnrollment = errors.New("completed agent enrollment requires recovery")
var ErrJournal = errors.New("agent journal requires recovery")
var ErrInventory = errors.New("agent runtime inventory requires recovery")
var ErrPlatform = errors.New("agent requires Linux boot identity")
var ErrExecution = errors.New("agent management loop stopped; recover saved state")

// Config holds the agent daemon settings from one private JSON file.
type Config struct {
	InstallationID      string   `json:"installation_id"`
	NodeID              string   `json:"node_id"`
	EnrollmentDir       string   `json:"enrollment_dir"`
	JournalDir          string   `json:"journal_dir"`
	RecoveryGeneration  string   `json:"recovery_generation"`
	ControllerEndpoints []string `json:"controller_endpoints"`
	DockerSocket        string   `json:"docker_socket"`
	// StoragePath is the one local pool that capacity is measured from. The
	// placement contract forbids summing unrelated filesystems, so exactly one
	// path is measured and its identity is declared beside it.
	StoragePath string `json:"storage_path"`
	// StorageID is the stable identity that operator policy approves. It is required,
	// because the agent answers no capacity probe without it and placement then has
	// no node to choose.
	StorageID string `json:"storage_id"`
	// MinFreeBytes is the floor this operator declares for the local pool. New
	// work that would take it to or below this is refused. Zero declares no floor,
	// so the pool is only refused when it has no free space at all.
	MinFreeBytes int64 `json:"min_free_bytes"`
	// Zero selects the journal component default for each limit.
	JournalMaxRetainedBytes      int64 `json:"journal_max_retained_bytes,omitzero"`
	JournalMaxStoredBytes        int64 `json:"journal_max_stored_bytes,omitzero"`
	JournalRecoveryWindowSeconds int64 `json:"journal_recovery_window_seconds,omitzero"`
}

// Load accepts one bounded, private JSON file. It does not create local state.
func Load(path string) (Config, error) {
	file, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if err != nil {
		return Config{}, fmt.Errorf("%w: open %s: %w", ErrConfiguration, path, err)
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return Config{}, fmt.Errorf("%w: stat %s: %w", ErrConfiguration, path, err)
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0o077 != 0 || info.Sys().(*syscall.Stat_t).Uid != uint32(os.Geteuid()) || info.Size() > 64<<10 {
		return Config{}, fmt.Errorf("%w: %s must be a regular file of at most 64 KiB, owned by this user, with no group or other permission", ErrConfiguration, path)
	}
	data, err := io.ReadAll(io.LimitReader(file, (64<<10)+1))
	if err != nil {
		return Config{}, fmt.Errorf("%w: read %s: %w", ErrConfiguration, path, err)
	}
	if len(data) > 64<<10 {
		return Config{}, fmt.Errorf("%w: %s exceeds 64 KiB", ErrConfiguration, path)
	}
	var config Config
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&config); err != nil {
		return Config{}, fmt.Errorf("%w: decode %s: %w", ErrConfiguration, path, err)
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return Config{}, fmt.Errorf("%w: %s carries content after the configuration document", ErrConfiguration, path)
	}
	if config.DockerSocket == "" {
		config.DockerSocket = "/var/run/docker.sock"
	}
	if err := config.validate(); err != nil {
		return Config{}, fmt.Errorf("%s: %w", path, err)
	}
	return config, nil
}

// pool is the local storage pool this agent measures.
//
// The journal already lives on the filesystem that holds this node's working space,
// so it names that pool when no path is configured. The default belongs here rather
// than in Load, because Run accepts a Config that was never loaded from a file and an
// agent with no pool refuses every storage-consuming command.
func (c Config) pool() string {
	if c.StoragePath != "" {
		return c.StoragePath
	}
	return c.JournalDir
}

func (c Config) validate() error {
	if c.JournalMaxRetainedBytes < 0 || c.JournalMaxStoredBytes < 0 || c.JournalRecoveryWindowSeconds < 0 || c.JournalRecoveryWindowSeconds > math.MaxInt64/int64(time.Second) {
		return fmt.Errorf("%w: journal budgets and recovery window must be nonnegative and within range", ErrConfiguration)
	}
	for _, field := range []struct{ name, value string }{
		{"installation_id", c.InstallationID}, {"node_id", c.NodeID}, {"recovery_generation", c.RecoveryGeneration},
	} {
		parsed, err := uuid.Parse(field.value)
		if err != nil {
			return fmt.Errorf("%w: parse %s: %w", ErrConfiguration, field.name, err)
		}
		if parsed == uuid.Nil || parsed.String() != field.value {
			return fmt.Errorf("%w: %s is not a canonical non-nil UUID", ErrConfiguration, field.name)
		}
	}
	for _, path := range []string{c.EnrollmentDir, c.JournalDir, c.DockerSocket} {
		if !filepath.IsAbs(path) || filepath.Clean(path) != path {
			return fmt.Errorf("%w: %q is not a clean absolute path", ErrConfiguration, path)
		}
	}
	if c.StoragePath != "" && (!filepath.IsAbs(c.StoragePath) || filepath.Clean(c.StoragePath) != c.StoragePath) {
		return fmt.Errorf("%w: storage_path %q is not a clean absolute path", ErrConfiguration, c.StoragePath)
	}
	if len(c.StorageID) < 1 || len(c.StorageID) > 128 || strings.ContainsFunc(c.StorageID, func(r rune) bool { return r < '!' || r > '~' }) {
		return fmt.Errorf("%w: storage_id must be 1 to 128 visible ASCII characters and match the --storage-id of synod node approve", ErrConfiguration)
	}
	if c.EnrollmentDir == c.JournalDir || strings.HasPrefix(c.EnrollmentDir, c.JournalDir+string(os.PathSeparator)) || strings.HasPrefix(c.JournalDir, c.EnrollmentDir+string(os.PathSeparator)) {
		return fmt.Errorf("%w: enrollment_dir %q and journal_dir %q must not contain one another", ErrConfiguration, c.EnrollmentDir, c.JournalDir)
	}
	if len(c.ControllerEndpoints) < 1 || len(c.ControllerEndpoints) > 8 {
		return fmt.Errorf("%w: controller_endpoints holds %d entries; use 1 to 8", ErrConfiguration, len(c.ControllerEndpoints))
	}
	for _, endpoint := range c.ControllerEndpoints {
		host, port, err := net.SplitHostPort(endpoint)
		if err != nil {
			return fmt.Errorf("%w: split controller endpoint %q: %w", ErrConfiguration, endpoint, err)
		}
		number, numberErr := strconv.Atoi(port)
		if host == "" || strings.ContainsAny(host, "/\\%?#@ \t\r\n") || numberErr != nil || number < 1 || number > 65535 {
			return fmt.Errorf("%w: controller endpoint %q needs a plain host and a port of 1 to 65535", ErrConfiguration, endpoint)
		}
	}
	return nil
}

func privateDirectory(path string) error {
	info, err := os.Lstat(path)
	if err != nil {
		return fmt.Errorf("%w: stat %s: %w", ErrJournal, path, err)
	}
	if !info.IsDir() || info.Mode().Perm()&0o077 != 0 || info.Sys().(*syscall.Stat_t).Uid != uint32(os.Geteuid()) {
		return fmt.Errorf("%w: %s must be a directory owned by this user with no group or other permission", ErrJournal, path)
	}
	return nil
}
