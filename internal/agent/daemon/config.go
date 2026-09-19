// Package daemon owns enrolled agent startup and borrowed component lifetimes.
package daemon

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"

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
	// StorageID is the stable identity that operator policy approves.
	StorageID string `json:"storage_id"`
	// MinFreeBytes is the floor this operator declares for the local pool. New
	// work that would take it to or below this is refused. Zero declares no floor,
	// so the pool is only refused when it has no free space at all.
	MinFreeBytes int64 `json:"min_free_bytes"`
}

// Load accepts one bounded, private JSON file. It does not create local state.
func Load(path string) (Config, error) {
	file, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if err != nil {
		return Config{}, ErrConfiguration
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0o077 != 0 || info.Sys().(*syscall.Stat_t).Uid != uint32(os.Geteuid()) || info.Size() > 64<<10 {
		return Config{}, ErrConfiguration
	}
	data, err := io.ReadAll(io.LimitReader(file, (64<<10)+1))
	if err != nil || len(data) > 64<<10 {
		return Config{}, ErrConfiguration
	}
	var config Config
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&config) != nil || decoder.Decode(new(any)) != io.EOF {
		return Config{}, ErrConfiguration
	}
	if config.DockerSocket == "" {
		config.DockerSocket = "/var/run/docker.sock"
	}
	if config.validate() != nil {
		return Config{}, ErrConfiguration
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
	for _, id := range []string{c.InstallationID, c.NodeID, c.RecoveryGeneration} {
		parsed, err := uuid.Parse(id)
		if err != nil || parsed == uuid.Nil || parsed.String() != id {
			return ErrConfiguration
		}
	}
	for _, path := range []string{c.EnrollmentDir, c.JournalDir, c.DockerSocket} {
		if !filepath.IsAbs(path) || filepath.Clean(path) != path {
			return ErrConfiguration
		}
	}
	if c.EnrollmentDir == c.JournalDir || strings.HasPrefix(c.EnrollmentDir, c.JournalDir+string(os.PathSeparator)) || strings.HasPrefix(c.JournalDir, c.EnrollmentDir+string(os.PathSeparator)) {
		return ErrConfiguration
	}
	if len(c.ControllerEndpoints) < 1 || len(c.ControllerEndpoints) > 8 {
		return ErrConfiguration
	}
	for _, endpoint := range c.ControllerEndpoints {
		host, port, err := net.SplitHostPort(endpoint)
		number, numberErr := strconv.Atoi(port)
		if err != nil || host == "" || strings.ContainsAny(host, "/\\%?#@ \t\r\n") || numberErr != nil || number < 1 || number > 65535 {
			return ErrConfiguration
		}
	}
	return nil
}

func privateDirectory(path string) error {
	info, err := os.Lstat(path)
	if err != nil || !info.IsDir() || info.Mode().Perm()&0o077 != 0 || info.Sys().(*syscall.Stat_t).Uid != uint32(os.Geteuid()) {
		return ErrJournal
	}
	return nil
}
