package daemon

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
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
	"github.com/sirrobot01/synod/internal/agent/replication"
	"github.com/sirrobot01/synod/internal/agent/transport"
	"github.com/sirrobot01/synod/internal/controlstream"
	"github.com/sirrobot01/synod/internal/router"
	noderuntime "github.com/sirrobot01/synod/internal/runtime"
	docker "github.com/sirrobot01/synod/internal/runtime/docker"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
)

// Initialize creates a first journal only after empty native inventory is proved.
func Initialize(ctx context.Context, config Config, diagnostics io.Writer) (journal.Identity, error) {
	if diagnostics == nil {
		return journal.Identity{}, fmt.Errorf("%w: a diagnostics writer is required", ErrConfiguration)
	}
	if err := config.validate(); err != nil {
		return journal.Identity{}, err
	}
	state, err := enrollment.Open(config.EnrollmentDir, enrollment.Options{Identity: enrollment.Identity{InstallationID: uuid.MustParse(config.InstallationID), NodeID: uuid.MustParse(config.NodeID)}})
	if err != nil {
		return journal.Identity{}, fmt.Errorf("%w: open enrollment state %s: %w", ErrEnrollment, config.EnrollmentDir, err)
	}
	defer state.Close()
	if _, err := state.Credentials(); err != nil {
		return journal.Identity{}, fmt.Errorf("%w: read enrollment credentials: %w", ErrEnrollment, err)
	}
	if _, err := os.Lstat(config.JournalDir); err == nil {
		return journal.Identity{}, fmt.Errorf("%w: journal directory %s already exists", ErrJournal, config.JournalDir)
	} else if !errors.Is(err, os.ErrNotExist) {
		return journal.Identity{}, fmt.Errorf("%w: check journal directory %s: %w", ErrJournal, config.JournalDir, err)
	}
	if err := privateDirectory(filepath.Dir(config.JournalDir)); err != nil {
		return journal.Identity{}, fmt.Errorf("%w: journal parent %s: %w", ErrJournal, filepath.Dir(config.JournalDir), err)
	}
	api, _, _, inventory, err := connectDocker(ctx, config)
	if err != nil {
		return journal.Identity{}, err
	}
	defer api.Close()
	if len(inventory) != 0 {
		return journal.Identity{}, fmt.Errorf("%w: the runtime already holds %d Synod resources", ErrInventory, len(inventory))
	}
	if err := os.Mkdir(config.JournalDir, 0o700); err != nil {
		return journal.Identity{}, fmt.Errorf("%w: create journal directory %s: %w", ErrJournal, config.JournalDir, err)
	}
	j, err := journal.Open(config.JournalDir, journal.Options{Identity: journal.Identity{InstallationID: config.InstallationID, NodeID: config.NodeID, RecoveryGeneration: config.RecoveryGeneration}, MaxRetainedBytes: config.JournalMaxRetainedBytes, MaxStoredBytes: config.JournalMaxStoredBytes, RecoveryWindow: time.Duration(config.JournalRecoveryWindowSeconds) * time.Second, AllowCreate: true, EmptyInventory: true, Logger: journalLogger{log: slog.New(slog.NewJSONHandler(diagnostics, nil))}})
	if err != nil {
		return journal.Identity{}, fmt.Errorf("%w: create journal in %s: %w", ErrJournal, config.JournalDir, err)
	}
	identity := j.Identity()
	if err := j.Close(); err != nil {
		return journal.Identity{}, fmt.Errorf("%w: close the new journal: %w", ErrJournal, err)
	}
	parent, err := os.Open(filepath.Dir(config.JournalDir))
	if err != nil {
		return journal.Identity{}, fmt.Errorf("%w: open journal parent %s: %w", ErrJournal, filepath.Dir(config.JournalDir), err)
	}
	syncErr := parent.Sync()
	closeErr := parent.Close()
	if syncErr != nil || closeErr != nil {
		return journal.Identity{}, fmt.Errorf("%w: sync journal parent %s: %w", ErrJournal, filepath.Dir(config.JournalDir), errors.Join(syncErr, closeErr))
	}
	if err := state.Close(); err != nil {
		return journal.Identity{}, fmt.Errorf("%w: close enrollment state: %w", ErrEnrollment, err)
	}
	return identity, nil
}

// Run retains both state locks until all management loops and workers stop.
// testBootID is allowed only for explicit local macOS fixture execution.
func Run(ctx context.Context, config Config, testBootID string, diagnostics io.Writer) error {
	if diagnostics == nil {
		return fmt.Errorf("%w: a diagnostics writer is required", ErrConfiguration)
	}
	if err := config.validate(); err != nil {
		return err
	}
	logger := slog.New(slog.NewJSONHandler(diagnostics, nil))
	bootID, err := bootIdentity(testBootID)
	if err != nil {
		return err
	}
	state, err := enrollment.Open(config.EnrollmentDir, enrollment.Options{Identity: enrollment.Identity{InstallationID: uuid.MustParse(config.InstallationID), NodeID: uuid.MustParse(config.NodeID)}})
	if err != nil {
		return fmt.Errorf("%w: open enrollment state %s: %w", ErrEnrollment, config.EnrollmentDir, err)
	}
	defer state.Close()
	identity, err := state.Credentials()
	if err != nil {
		return fmt.Errorf("%w: read enrollment credentials: %w", ErrEnrollment, err)
	}
	if err := privateDirectory(config.JournalDir); err != nil {
		return fmt.Errorf("%w: journal directory %s: %w", ErrJournal, config.JournalDir, err)
	}
	j, err := journal.Open(config.JournalDir, journal.Options{Identity: journal.Identity{InstallationID: config.InstallationID, NodeID: config.NodeID, RecoveryGeneration: config.RecoveryGeneration}, MaxRetainedBytes: config.JournalMaxRetainedBytes, MaxStoredBytes: config.JournalMaxStoredBytes, RecoveryWindow: time.Duration(config.JournalRecoveryWindowSeconds) * time.Second, Logger: journalLogger{log: logger}})
	if err != nil {
		return fmt.Errorf("%w: open journal %s: %w", ErrJournal, config.JournalDir, err)
	}
	defer j.Close()
	api, native, version, _, err := connectDocker(ctx, config)
	if err != nil {
		return err
	}
	defer api.Close()
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM([]byte(identity.CACertificatePEM)) {
		return fmt.Errorf("%w: the enrolled CA bundle contains no usable certificate", ErrEnrollment)
	}
	// The agent measures its own host. Nothing here declares a figure: an absent
	// storage identity leaves the executor unable to answer a capacity challenge,
	// which is the honest outcome when policy has approved no pool.
	routes, err := router.New(ctx, router.Options{})
	if err != nil {
		return fmt.Errorf("%w: build the router: %w", ErrExecution, err)
	}
	defer routes.Close()
	capabilities := []string{"runtime.docker", controlstream.RouterCapability}
	var replicas *replication.Server
	var replicaListener net.Listener
	if config.AppsDir != "" {
		if err := os.MkdirAll(config.AppsDir, 0o700); err != nil {
			return fmt.Errorf("%w: create apps directory %s: %w", ErrConfiguration, config.AppsDir, err)
		}
		if err := privateDirectory(config.AppsDir); err != nil {
			return err
		}
		installation, err := uuid.Parse(config.InstallationID)
		if err != nil {
			return fmt.Errorf("%w: parse installation_id: %w", ErrConfiguration, err)
		}
		replicas = &replication.Server{InstallationID: installation, Root: config.AppsDir, Table: &replication.Table{}, Status: &replication.Status{}, Logger: logger}
		if replicaListener, err = net.Listen("tcp", config.replicationListen()); err != nil {
			return fmt.Errorf("%w: listen for replication on %s: %w", ErrConfiguration, config.replicationListen(), err)
		}
		defer replicaListener.Close()
		capabilities = append(capabilities, replication.Capability)
	}
	options := executor.Options{
		Router:         routes,
		Host:           capacity.Host{ProcRoot: "/proc", StoragePath: config.pool()},
		BootID:         bootID,
		StorageID:      config.StorageID,
		RuntimeVersion: version,
		MinFreeBytes:   config.MinFreeBytes,
		EngineTLS:      &identity.Certificate,
		EngineCA:       []byte(identity.CACertificatePEM),
	}
	transportConfig := transport.Config{Endpoints: config.ControllerEndpoints, TLS: &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots, Certificates: []tls.Certificate{identity.Certificate}}, Logger: logger, Hello: &agentv1.Hello{InstallationId: config.InstallationID, NodeId: config.NodeID, BootId: bootID, JournalGeneration: j.Identity().JournalGeneration, Protocol: &agentv1.VersionRange{Minimum: 1, Maximum: 1}, Capabilities: capabilities, RuntimeVersion: version, ActivePayloadVersions: []uint32{1}}}
	if replicas != nil {
		options.Replication, transportConfig.Replicas = replicas.Table, replicas.Status.List
		options.AppsDir, options.ReplicaStatus = config.AppsDir, replicas.Status
		options.PushSnapshot = (replication.Client{InstallationID: replicas.InstallationID, Certificate: identity.Certificate, Roots: roots}).Push
	}
	execution, err := executor.New(j, native, options)
	if err != nil {
		return fmt.Errorf("%w: build the executor: %w", ErrExecution, err)
	}
	transportConfig.Handle, transportConfig.Observations = execution.Handle, execution.Observations
	transportConfig.WriterClock = execution.WriterClock
	stream, err := transport.New(transportConfig)
	if err != nil {
		return fmt.Errorf("%w: build the agent transport: %w", ErrConfiguration, err)
	}
	logger.Info("agent_started", "identity", j.Identity())
	process, cancel := context.WithCancel(ctx)
	defer cancel()
	stopped := make(chan error, 2)
	go func() { stopped <- execution.Run(process, stream) }()
	go func() { stopped <- stream.Run(process) }()
	serving := make(chan struct{})
	go func() {
		defer close(serving)
		if replicas == nil {
			return
		}
		// A replication failure is logged, not fatal: the node still runs its apps without it.
		if err := replicas.Serve(process, replicaListener, identity.Certificate, roots); err != nil {
			logger.Warn("replication_listener_stopped", "error", err)
		}
	}()
	first := <-stopped
	cancel()
	second := <-stopped
	<-serving
	// Caller cancellation is normal shutdown. A worker failure must remain visible.
	if (first != nil && !errors.Is(first, context.Canceled)) || (second != nil && !errors.Is(second, context.Canceled)) {
		return fmt.Errorf("%w: %w", ErrExecution, errors.Join(first, second))
	}
	if ctx.Err() == nil {
		return fmt.Errorf("%w: both management loops stopped while the agent was not asked to stop", ErrExecution)
	}
	if err := j.Close(); err != nil {
		return fmt.Errorf("%w: close the journal: %w", ErrJournal, err)
	}
	if err := state.Close(); err != nil {
		return fmt.Errorf("%w: close enrollment state: %w", ErrEnrollment, err)
	}
	return nil
}

func connectDocker(ctx context.Context, config Config) (*client.Client, noderuntime.Runtime, string, []noderuntime.Resource, error) {
	api, err := client.New(client.WithHost("unix://"+config.DockerSocket), client.WithAPIVersionNegotiation())
	if err != nil {
		return nil, nil, "", nil, fmt.Errorf("%w: build a runtime client for %s: %w", ErrConfiguration, config.DockerSocket, err)
	}
	startup, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	version, err := api.ServerVersion(startup, client.ServerVersionOptions{})
	if err == nil && (len(version.Version) > 256 || version.Version == "") {
		err = fmt.Errorf("the runtime reported a version of %d bytes", len(version.Version))
	}
	if err != nil {
		return nil, nil, "", nil, errors.Join(fmt.Errorf("%w: read the runtime version: %w", ErrInventory, err), api.Close())
	}
	native, err := docker.New(api, docker.Options{AppsDir: config.AppsDir, InstallationID: uuid.MustParse(config.InstallationID), NodeID: uuid.MustParse(config.NodeID)})
	if err != nil {
		return nil, nil, "", nil, errors.Join(fmt.Errorf("%w: build the native runtime: %w", ErrConfiguration, err), api.Close())
	}
	inventory, err := native.Inventory(startup)
	if err != nil {
		return nil, nil, "", nil, errors.Join(fmt.Errorf("%w: read the runtime inventory: %w", ErrInventory, err), api.Close())
	}
	return api, native, version.Version, inventory, nil
}

func bootIdentity(testID string) (string, error) {
	var id string
	if runtime.GOOS == "darwin" && testID != "" {
		id = testID
	} else {
		if runtime.GOOS != "linux" || testID != "" {
			return "", fmt.Errorf("%w: boot identity is readable on Linux only, and a fixture value is refused there", ErrPlatform)
		}
		file, err := os.OpenFile("/proc/sys/kernel/random/boot_id", os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
		if err != nil {
			return "", fmt.Errorf("%w: open the kernel boot identity: %w", ErrPlatform, err)
		}
		defer file.Close()
		info, err := file.Stat()
		if err != nil {
			return "", fmt.Errorf("%w: stat the kernel boot identity: %w", ErrPlatform, err)
		}
		if !info.Mode().IsRegular() {
			return "", fmt.Errorf("%w: the kernel boot identity is not a regular file", ErrPlatform)
		}
		data, err := io.ReadAll(io.LimitReader(file, 38))
		if err != nil {
			return "", fmt.Errorf("%w: read the kernel boot identity: %w", ErrPlatform, err)
		}
		if len(data) > 37 {
			return "", fmt.Errorf("%w: the kernel boot identity is %d bytes", ErrPlatform, len(data))
		}
		id = strings.TrimSuffix(string(data), "\n")
	}
	parsed, err := uuid.Parse(id)
	if err != nil {
		return "", fmt.Errorf("%w: parse the boot identity: %w", ErrPlatform, err)
	}
	if parsed == uuid.Nil || parsed.String() != id {
		return "", fmt.Errorf("%w: the boot identity is not a canonical non-nil UUID", ErrPlatform)
	}
	return id, nil
}
