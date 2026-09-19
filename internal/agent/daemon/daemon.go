package daemon

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/moby/moby/client"
	"github.com/sirrobot01/synod/internal/agent/capacity"
	"github.com/sirrobot01/synod/internal/agent/enrollment"
	"github.com/sirrobot01/synod/internal/agent/executor"
	"github.com/sirrobot01/synod/internal/agent/journal"
	"github.com/sirrobot01/synod/internal/agent/transport"
	noderuntime "github.com/sirrobot01/synod/internal/runtime"
	docker "github.com/sirrobot01/synod/internal/runtime/docker"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
)

// Initialize creates a first journal only after empty native inventory is proved.
func Initialize(ctx context.Context, config Config, diagnostics io.Writer) (journal.Identity, error) {
	if config.validate() != nil || diagnostics == nil {
		return journal.Identity{}, ErrConfiguration
	}
	state, err := enrollment.Open(config.EnrollmentDir, enrollment.Options{Identity: enrollment.Identity{InstallationID: uuid.MustParse(config.InstallationID), NodeID: uuid.MustParse(config.NodeID)}})
	if err != nil {
		return journal.Identity{}, ErrEnrollment
	}
	defer state.Close()
	if _, err := state.Credentials(); err != nil {
		return journal.Identity{}, ErrEnrollment
	}
	if _, err := os.Lstat(config.JournalDir); !errors.Is(err, os.ErrNotExist) || privateDirectory(filepath.Dir(config.JournalDir)) != nil {
		return journal.Identity{}, ErrJournal
	}
	api, _, _, inventory, err := connectDocker(ctx, config)
	if err != nil {
		return journal.Identity{}, err
	}
	defer api.Close()
	if len(inventory) != 0 {
		return journal.Identity{}, ErrInventory
	}
	if err := os.Mkdir(config.JournalDir, 0o700); err != nil {
		return journal.Identity{}, ErrJournal
	}
	j, err := journal.Open(config.JournalDir, journal.Options{Identity: journal.Identity{InstallationID: config.InstallationID, NodeID: config.NodeID, RecoveryGeneration: config.RecoveryGeneration}, AllowCreate: true, EmptyInventory: true, Logger: journalLogger{log: slog.New(slog.NewJSONHandler(diagnostics, nil))}})
	if err != nil {
		return journal.Identity{}, ErrJournal
	}
	identity := j.Identity()
	if err := j.Close(); err != nil {
		return journal.Identity{}, ErrJournal
	}
	parent, err := os.Open(filepath.Dir(config.JournalDir))
	if err != nil {
		return journal.Identity{}, ErrJournal
	}
	syncErr := parent.Sync()
	closeErr := parent.Close()
	if syncErr != nil || closeErr != nil {
		return journal.Identity{}, ErrJournal
	}
	if err := state.Close(); err != nil {
		return journal.Identity{}, ErrEnrollment
	}
	return identity, nil
}

// Run retains both state locks until all management loops and workers stop.
// testBootID is allowed only for explicit local macOS fixture execution.
func Run(ctx context.Context, config Config, testBootID string, diagnostics io.Writer) error {
	if config.validate() != nil || diagnostics == nil {
		return ErrConfiguration
	}
	logger := slog.New(slog.NewJSONHandler(diagnostics, nil))
	bootID, err := bootIdentity(testBootID)
	if err != nil {
		return err
	}
	state, err := enrollment.Open(config.EnrollmentDir, enrollment.Options{Identity: enrollment.Identity{InstallationID: uuid.MustParse(config.InstallationID), NodeID: uuid.MustParse(config.NodeID)}})
	if err != nil {
		return ErrEnrollment
	}
	defer state.Close()
	identity, err := state.Credentials()
	if err != nil {
		return ErrEnrollment
	}
	if privateDirectory(config.JournalDir) != nil {
		return ErrJournal
	}
	j, err := journal.Open(config.JournalDir, journal.Options{Identity: journal.Identity{InstallationID: config.InstallationID, NodeID: config.NodeID, RecoveryGeneration: config.RecoveryGeneration}, Logger: journalLogger{log: logger}})
	if err != nil {
		return ErrJournal
	}
	defer j.Close()
	api, native, version, _, err := connectDocker(ctx, config)
	if err != nil {
		return err
	}
	defer api.Close()
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM([]byte(identity.CACertificatePEM)) {
		return ErrEnrollment
	}
	// The agent measures its own host. Nothing here declares a figure: an absent
	// storage identity leaves the executor unable to answer a capacity challenge,
	// which is the honest outcome when policy has approved no pool.
	execution, err := executor.New(j, native, executor.Options{
		Host:           capacity.Host{ProcRoot: "/proc", StoragePath: config.pool()},
		BootID:         bootID,
		StorageID:      config.StorageID,
		RuntimeVersion: version,
		MinFreeBytes:   config.MinFreeBytes,
	})
	if err != nil {
		return ErrExecution
	}
	stream, err := transport.New(transport.Config{Endpoints: config.ControllerEndpoints, TLS: &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots, Certificates: []tls.Certificate{identity.Certificate}}, Hello: &agentv1.Hello{InstallationId: config.InstallationID, NodeId: config.NodeID, BootId: bootID, JournalGeneration: j.Identity().JournalGeneration, Protocol: &agentv1.VersionRange{Minimum: 1, Maximum: 1}, Capabilities: []string{"runtime.docker"}, RuntimeVersion: version, ActivePayloadVersions: []uint32{1}}, Handle: execution.Handle})
	if err != nil {
		return ErrConfiguration
	}
	logger.Info("agent_started", "identity", j.Identity())
	process, cancel := context.WithCancel(ctx)
	defer cancel()
	stopped := make(chan error, 2)
	go func() { stopped <- execution.Run(process, stream) }()
	go func() { stopped <- stream.Run(process) }()
	first := <-stopped
	cancel()
	second := <-stopped
	// Caller cancellation is normal shutdown. A worker failure must remain visible.
	if (first != nil && !errors.Is(first, context.Canceled)) || (second != nil && !errors.Is(second, context.Canceled)) {
		return ErrExecution
	}
	if ctx.Err() == nil {
		return ErrExecution
	}
	if err := j.Close(); err != nil {
		return ErrJournal
	}
	if err := state.Close(); err != nil {
		return ErrEnrollment
	}
	return nil
}

func connectDocker(ctx context.Context, config Config) (*client.Client, noderuntime.Runtime, string, []noderuntime.Resource, error) {
	api, err := client.New(client.WithHost("unix://"+config.DockerSocket), client.WithAPIVersionNegotiation())
	if err != nil {
		return nil, nil, "", nil, ErrConfiguration
	}
	startup, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	version, err := api.ServerVersion(startup, client.ServerVersionOptions{})
	if err != nil || len(version.Version) > 256 || version.Version == "" {
		api.Close()
		return nil, nil, "", nil, ErrInventory
	}
	native, err := docker.New(api, docker.Options{InstallationID: uuid.MustParse(config.InstallationID), NodeID: uuid.MustParse(config.NodeID)})
	if err != nil {
		api.Close()
		return nil, nil, "", nil, ErrConfiguration
	}
	inventory, err := native.Inventory(startup)
	if err != nil {
		api.Close()
		return nil, nil, "", nil, ErrInventory
	}
	return api, native, version.Version, inventory, nil
}

func bootIdentity(testID string) (string, error) {
	var id string
	if runtime.GOOS == "darwin" && testID != "" {
		id = testID
	} else {
		if runtime.GOOS != "linux" || testID != "" {
			return "", ErrPlatform
		}
		file, err := os.OpenFile("/proc/sys/kernel/random/boot_id", os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
		if err != nil {
			return "", ErrPlatform
		}
		defer file.Close()
		info, err := file.Stat()
		if err != nil || !info.Mode().IsRegular() {
			return "", ErrPlatform
		}
		data, err := io.ReadAll(io.LimitReader(file, 38))
		if err != nil || len(data) > 37 {
			return "", ErrPlatform
		}
		id = strings.TrimSuffix(string(data), "\n")
	}
	parsed, err := uuid.Parse(id)
	if err != nil || parsed == uuid.Nil || parsed.String() != id {
		return "", ErrPlatform
	}
	return id, nil
}
