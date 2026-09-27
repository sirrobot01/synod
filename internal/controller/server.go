package controller

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"syscall"
	"time"

	"encoding/base64"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sirrobot01/synod/internal/api"
	"github.com/sirrobot01/synod/internal/auth"
	"github.com/sirrobot01/synod/internal/cluster"
	"github.com/sirrobot01/synod/internal/config"
	"github.com/sirrobot01/synod/internal/controller/agentstream"
	"github.com/sirrobot01/synod/internal/controller/capacity"
	"github.com/sirrobot01/synod/internal/controller/provision"
	"github.com/sirrobot01/synod/internal/controller/runner"
	"github.com/sirrobot01/synod/internal/controlstream"
	"github.com/sirrobot01/synod/internal/coordinator"
	"github.com/sirrobot01/synod/internal/engine"
	postgresengine "github.com/sirrobot01/synod/internal/engine/postgres"
	redisengine "github.com/sirrobot01/synod/internal/engine/redis"
	"github.com/sirrobot01/synod/internal/pki"
	"github.com/sirrobot01/synod/internal/placement"
	"github.com/sirrobot01/synod/internal/secret"
	"github.com/sirrobot01/synod/internal/store/postgres"
	"github.com/sirrobot01/synod/internal/web"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"github.com/spf13/cobra"
)

// ownerID identifies this controller process in claims and elections.
func ownerID() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "controller"
	}
	return host + "/" + strconv.Itoa(os.Getpid())
}

// Command returns the server command. It runs the controller, and its init
// subcommand writes the installation material.
func Command() *cobra.Command {
	command := &cobra.Command{Use: "server", Short: "Run the Synod controller", SilenceErrors: true, SilenceUsage: true,
		// Cobra's own refusal repeats the argument, which can be a misplaced secret.
		Args: func(_ *cobra.Command, args []string) error {
			if len(args) != 0 {
				return errors.New("unexpected controller arguments")
			}
			return nil
		}}
	load := config.BindController(command.Flags())
	command.RunE = func(cmd *cobra.Command, _ []string) error {
		settings, err := load()
		if err != nil {
			return err
		}
		return serve(cmd.Context(), settings, cmd.ErrOrStderr())
	}
	command.AddCommand(initCommand())
	return command
}

// Report writes err to stderr and returns the exit status.
func Report(err error, stderr io.Writer) int {
	if err == nil {
		return 0
	}
	fmt.Fprintln(stderr, err)
	return 1
}

func serve(ctx context.Context, settings config.Controller, output io.Writer) error {
	var level slog.Level
	if err := level.UnmarshalText([]byte(settings.LogLevel)); err != nil {
		return fmt.Errorf("invalid log level %q: %w", settings.LogLevel, err)
	}
	logger := slog.New(slog.NewJSONHandler(output, &slog.HandlerOptions{Level: level}))
	// Read through an open descriptor so checks apply to the bytes we use.
	token, err := readStartupFile(settings.AdminTokenFile, 258, true)
	if err != nil {
		return fmt.Errorf("read the protected administrator token file: %w", err)
	}
	// The token itself is never named here; only that its shape was refused.
	admin, err := auth.NewAdminToken(strings.TrimSuffix(strings.TrimSuffix(string(token), "\n"), "\r"), settings.ActorID)
	if err != nil {
		return fmt.Errorf("the administrator token file is invalid: %w", err)
	}
	key, err := readStartupFile(settings.TLSKeyFile, 1<<20, true)
	if err != nil {
		return fmt.Errorf("read the protected TLS key file: %w", err)
	}
	cert, err := readStartupFile(settings.TLSCertFile, 1<<20, false)
	if err != nil {
		return fmt.Errorf("read the TLS certificate file: %w", err)
	}
	certificate, err := tls.X509KeyPair(cert, key)
	if err != nil {
		return fmt.Errorf("load the API TLS certificate and key: %w", err)
	}
	var authority *pki.CA
	if settings.CACertFile != "" {
		caCertificate, err := readStartupFile(settings.CACertFile, 64<<10, false)
		if err != nil {
			return fmt.Errorf("read the enrollment CA certificate: %w", err)
		}
		caKey, err := readStartupFile(settings.CAKeyFile, 64<<10, true)
		if err != nil {
			return fmt.Errorf("read the protected enrollment CA key: %w", err)
		}
		authority, err = pki.LoadCA(uuid.MustParse(settings.InstallationID), caCertificate, caKey)
		clear(caKey)
		if err != nil {
			return fmt.Errorf("load the enrollment CA: %w", err)
		}
	}
	var agentTLS *tls.Config
	if settings.AgentListen != "" {
		agentCert, err := readStartupFile(settings.AgentTLSCertFile, 64<<10, false)
		if err != nil {
			return fmt.Errorf("read the agent-transport controller certificate: %w", err)
		}
		agentKey, err := readStartupFile(settings.AgentTLSKeyFile, 64<<10, true)
		if err != nil {
			return fmt.Errorf("read the protected agent-transport controller key: %w", err)
		}
		agentTrust, err := readStartupFile(settings.AgentCAFile, 1<<20, false)
		if err != nil {
			return fmt.Errorf("read the agent-transport installation trust: %w", err)
		}
		agentTLS, err = agentstream.TLSConfig(uuid.MustParse(settings.InstallationID), agentCert, agentKey, agentTrust)
		clear(agentKey)
		if err != nil {
			return err
		}
	}
	var ring *secret.Keyring
	if settings.SecretKeyFile != "" {
		ring, err = loadKeyring(settings.SecretKeyFile, settings.SecretKeyID)
		if err != nil {
			return err
		}
	}
	startup, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	// pgxpool.New repeats the DSN in its error and the DSN carries the database
	// password, so this one reports the failure without its cause.
	pool, err := pgxpool.New(startup, settings.DatabaseURL)
	if err != nil {
		return errors.New("configure metadata connection failed")
	}
	defer pool.Close()
	if err := pool.Ping(startup); err != nil {
		return fmt.Errorf("connect to metadata PostgreSQL: %w", err)
	}
	store := postgres.New(pool)
	if err := store.Migrate(startup); err != nil {
		return fmt.Errorf("migrate metadata PostgreSQL: %w", err)
	}
	// What this installation serves. Each engine advertises its own combinations, so
	// a version list is no longer enough to say yes to a cluster.
	catalog, err := engine.NewCatalog(postgresengine.New(), redisengine.New())
	if err != nil {
		return err
	}
	handler, err := api.NewServer(store, admin, uuid.MustParse(settings.InstallationID), catalog, logger)
	if err != nil {
		return err
	}
	handler.EnrollmentAuthority = authority
	handler.PublicOrigin = settings.PublicOrigin
	handler.AgentListen = settings.AgentListen
	handler.SecretKeyring = ring
	assets, err := web.Assets()
	if err != nil {
		return fmt.Errorf("load the web application: %w", err)
	}
	handler.SetAssets(assets)
	listener, err := (&net.ListenConfig{}).Listen(ctx, "tcp", settings.Listen)
	if err != nil {
		return fmt.Errorf("listen for API requests on %s: %w", settings.Listen, err)
	}
	// ServeTLS closes the listener when it returns, so this only closes one that
	// never began serving. net.ErrClosed is the ordinary path and says nothing.
	defer func() {
		if err := listener.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			logger.Error("close the API listener", "error", err)
		}
	}()
	server := &http.Server{
		Handler: handler, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second,
		WriteTimeout: 30 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 16 << 10,
		TLSConfig: &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{certificate}},
		ErrorLog:  slog.NewLogLogger(logger.Handler(), slog.LevelWarn),
	}
	var agentServer *controlstream.Server
	var agentListener net.Listener
	var operations *runner.Runner
	var capacityDriver *capacity.Driver
	if agentTLS != nil {
		// The runner needs the transport and the transport needs the runner's routing,
		// so the handler reads the pointer that the runner build fills in below.
		handle := (agentstream.Receiver{Store: store}).Handle
		agentServer, err = controlstream.New(controlstream.Config{InstallationID: uuid.MustParse(settings.InstallationID), TLS: agentTLS, Authorize: store.CheckAgentCertificate, Capabilities: []string{"runtime.docker", controlstream.CommandInspectionCapability, controlstream.RouterCapability}, Logger: logger,
			Handle: func(ctx context.Context, peer controlstream.Peer, message *agentv1.AgentMessage) error {
				if operations != nil {
					return operations.Handle(ctx, peer, message)
				}
				return handle(ctx, peer, message)
			}})
		if err != nil {
			return err
		}
		defer agentServer.Stop()
		agentListener, err = (&net.ListenConfig{}).Listen(ctx, "tcp", settings.AgentListen)
		if err != nil {
			return fmt.Errorf("listen for agent transport on %s: %w", settings.AgentListen, err)
		}
		defer func() {
			if err := agentListener.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
				logger.Error("close the agent transport listener", "error", err)
			}
		}()
		elector, err := coordinator.New(pool)
		if err != nil {
			return err
		}
		// App plans need no database credential key. Database plans still require one.
		runnerConfig := runner.Config{
			Store: store, Transport: agentServer, Coordinator: elector,
			InstallationID: uuid.MustParse(settings.InstallationID), OwnerID: ownerID(), Logger: logger,
		}
		{
			capacityDriver, err = capacity.New(capacity.Config{Store: store, Transport: agentServer, Logger: logger})
			if err != nil {
				return err
			}
			var architectures []string
			if settings.PlacementArchitecture != "" {
				architectures = strings.Split(settings.PlacementArchitecture, ",")
			}
			planner, err := provision.New(provision.Config{
				Store: store, Decider: capacityDriver, Catalog: catalog, Keyring: ring,
				Profile: provision.Profile{
					Architectures:  architectures,
					RuntimeVersion: settings.PlacementRuntimeVersion,
					StorageID:      settings.PlacementStorageID,
				},
				Images: pinnedImages,
				Budget: provisionBudget,
				EndpointPorts: placement.PortRange{
					First: uint16(settings.EngineEndpointFirstPort),
					Last:  uint16(settings.EngineEndpointLastPort),
				},
			})
			if err != nil {
				return err
			}
			runnerConfig.Keyring, runnerConfig.Planner = ring, planner
		}
		operations, err = runner.New(runnerConfig)
		if err != nil {
			return err
		}
	}
	stopped := make(chan error, 2)
	serving := 1
	go func() { stopped <- server.ServeTLS(listener, "", "") }()
	logger.Info("controller API listening", "address", listener.Addr().String(), "installation_id", settings.InstallationID)
	if agentServer != nil {
		serving++
		go func() { stopped <- agentServer.Serve(agentListener) }()
		logger.Info("controller agent transport listening", "agent_address", agentListener.Addr().String(), "installation_id", settings.InstallationID)
	}
	// A failed listener must also stop the runner, so it runs on its own context.
	runnerCtx, stopRunner := context.WithCancel(ctx)
	defer stopRunner()
	routesStopped := make(chan struct{})
	if agentServer != nil {
		go func() {
			defer close(routesStopped)
			runAppRoutes(runnerCtx, store, agentServer, uuid.MustParse(settings.InstallationID), logger)
		}()
	} else {
		close(routesStopped)
	}
	probesStopped := make(chan struct{})
	if capacityDriver != nil {
		go func() {
			defer close(probesStopped)
			ticker := time.NewTicker(2 * time.Second)
			defer ticker.Stop()
			for {
				work, cancel := context.WithTimeout(runnerCtx, placement.ProbeWindow)
				snapshot, err := store.ReadPlacementSnapshot(work, uuid.MustParse(settings.InstallationID))
				if err == nil {
					_, err = capacityDriver.Refresh(work, snapshot.InstallationID, snapshot)
				}
				if err != nil && runnerCtx.Err() == nil {
					logger.Warn("refresh node facts", "error", err)
				}
				cancel()
				select {
				case <-runnerCtx.Done():
					return
				case <-ticker.C:
				}
			}
		}()
	} else {
		close(probesStopped)
	}
	running := make(chan struct{})
	if operations != nil {
		go func() {
			defer close(running)
			if err := operations.Run(runnerCtx); err != nil {
				logger.Error("controller runner stopped", "error", err.Error())
			}
		}()
		logger.Info("controller runner electing cluster owners", "installation_id", settings.InstallationID)
	} else {
		close(running)
	}
	failed := false
	select {
	case <-stopped:
		serving--
		failed = true
	case <-ctx.Done():
	}
	// Wait for workers before the transport closes, so a late receipt still commits.
	stopRunner()
	<-running
	<-routesStopped
	<-probesStopped
	if agentServer != nil {
		agentServer.Stop()
	}
	shutdown, stopShutdown := context.WithTimeout(context.Background(), 10*time.Second)
	defer stopShutdown()
	shutdownErr := server.Shutdown(shutdown)
	if shutdownErr != nil {
		// Close is the forced stop after a graceful one ran out of time. Its own
		// failure is separate evidence and belongs beside the timeout.
		shutdownErr = errors.Join(shutdownErr, server.Close())
	}
	for range serving {
		<-stopped
	}
	if shutdownErr != nil {
		return fmt.Errorf("API shutdown exceeded its deadline: %w", shutdownErr)
	}
	if failed {
		return errors.New("controller management listener stopped unexpectedly")
	}
	return nil

}

func readStartupFile(path string, maximum int64, private bool) ([]byte, error) {
	file, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NONBLOCK, 0)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || (private && info.Mode().Perm()&0o077 != 0) || (!private && info.Mode().Perm()&0o022 != 0) {
		return nil, errors.New("file must be regular and private to its owner")
	}
	data, err := io.ReadAll(io.LimitReader(file, maximum+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > maximum {
		return nil, errors.New("protected file exceeds its size limit")
	}
	return data, nil
}

// Pin engine images. Refuse combinations without a digest.
// The Redis index contains both listed architectures. Runtime qualification is
// tracked separately in the support matrix.
var pinnedImages = []provision.Image{
	{Engine: cluster.PostgreSQL, Version: "17.11", Architecture: "arm64",
		Digest: "postgres@sha256:051f7b7b3abdd564d5d1bd1e8c4b9c1b6e77087d1dd22020ede611c096a272e0"},
	{Engine: cluster.PostgreSQL, Version: "17.11", Architecture: "amd64",
		Digest: "postgres@sha256:f4c66b820c6f974249089d3d16d86a3698eae11e8746eb6644b2271031e91232"},
	{Engine: cluster.Redis, Version: "8.2.7", Architecture: "arm64",
		Digest: "redis@sha256:d30960f73a599496d8b2802c97758bab6b1cd421fd06337f837779c47a57e1f3"},
	{Engine: cluster.Redis, Version: "8.2.7", Architecture: "amd64",
		Digest: "redis@sha256:d30960f73a599496d8b2802c97758bab6b1cd421fd06337f837779c47a57e1f3"},
}

// What one instance reserves. The host port it publishes is configuration, because a
// fleet cannot be described by a constant.
var provisionBudget = placement.Vector{CPUMillis: 1000, MemoryBytes: 1 << 30, DiskBytes: 8 << 30}

// loadKeyring reads the active credential wrapping key.
//
// The file holds base64 so it stays text-editable and unambiguous about length. A key
// of the wrong size is refused rather than padded, because a short key is a weak one
// that looks like a strong one.
func loadKeyring(path, id string) (*secret.Keyring, error) {
	active, err := uuid.Parse(id)
	if err != nil {
		return nil, fmt.Errorf("parse secret_key_id: %w", err)
	}
	if active == uuid.Nil {
		return nil, errors.New("secret_key_id must be a nonzero UUID")
	}
	encoded, err := readStartupFile(path, 4096, true)
	if err != nil {
		return nil, fmt.Errorf("read the credential wrapping key: %w", err)
	}
	defer clear(encoded)
	key, err := base64.StdEncoding.DecodeString(strings.TrimSpace(string(encoded)))
	defer clear(key)
	// A base64 fault names an offset, never the bytes, so the cause is safe to carry.
	if err != nil {
		return nil, fmt.Errorf("the credential wrapping key must be the base64 of 32 bytes: %w", err)
	}
	if len(key) != 32 {
		return nil, errors.New("the credential wrapping key must be the base64 of 32 bytes")
	}
	ring, err := secret.NewKeyring(active, map[uuid.UUID][]byte{active: key})
	if err != nil {
		return nil, fmt.Errorf("build the credential keyring: %w", err)
	}
	return ring, nil
}
