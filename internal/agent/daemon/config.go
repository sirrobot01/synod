// Package daemon runs an enrolled agent.
package daemon

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/destination"
)

var ErrConfiguration = errors.New("invalid protected agent configuration")
var ErrEnrollment = errors.New("completed agent enrollment requires recovery")
var ErrRuntime = errors.New("container runtime unavailable")
var ErrExecution = errors.New("agent management loop stopped")

// Config holds the agent daemon settings from one private JSON file.
type Config struct {
	InstallationID      string   `json:"installation_id"`
	NodeID              string   `json:"node_id"`
	EnrollmentDir       string   `json:"enrollment_dir"`
	ControllerEndpoints []string `json:"controller_endpoints"`
	DockerSocket        string   `json:"docker_socket"`
	DestinationListen   string   `json:"destination_listen,omitempty"`
	DestinationAddress  string   `json:"destination_address,omitempty"`
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

func (c Config) validate() error {
	if c.DestinationListen != "" && !destination.ValidAddress(c.DestinationListen, true) {
		return fmt.Errorf("%w: destination_listen requires a host and port", ErrConfiguration)
	}
	if c.DestinationAddress != "" && !destination.ValidAddress(c.DestinationAddress, false) {
		return fmt.Errorf("%w: destination_address requires a reachable host and port", ErrConfiguration)
	}
	for _, field := range []struct{ name, value string }{{"installation_id", c.InstallationID}, {"node_id", c.NodeID}} {
		parsed, err := uuid.Parse(field.value)
		if err != nil {
			return fmt.Errorf("%w: parse %s: %w", ErrConfiguration, field.name, err)
		}
		if parsed == uuid.Nil || parsed.String() != field.value {
			return fmt.Errorf("%w: %s is not a canonical non-nil UUID", ErrConfiguration, field.name)
		}
	}
	for _, path := range []string{c.EnrollmentDir, c.DockerSocket} {
		if !filepath.IsAbs(path) || filepath.Clean(path) != path {
			return fmt.Errorf("%w: %q is not a clean absolute path", ErrConfiguration, path)
		}
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
