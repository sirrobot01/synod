package controller

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"syscall"
	"time"

	"encoding/base64"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/api"
	"github.com/sirrobot01/synod/internal/auth"
	"github.com/sirrobot01/synod/internal/config"
	"github.com/sirrobot01/synod/internal/controller/agentstream"
	"github.com/sirrobot01/synod/internal/controlstream"
	"github.com/sirrobot01/synod/internal/pki"
	"github.com/sirrobot01/synod/internal/secret"
	"github.com/sirrobot01/synod/internal/store"
	"github.com/sirrobot01/synod/internal/web"
	"github.com/spf13/cobra"
)

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
	var keyring *secret.Keyring
	if settings.SecretKeyFile != "" {
		keyring, err = loadKeyring(settings.SecretKeyFile, settings.SecretKeyID)
		if err != nil {
			return err
		}
	}
	startup, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	metadata, err := store.Open(startup, settings.DatabasePath)
	if err != nil {
		return err
	}
	defer metadata.Close()
	handler, err := api.NewServer(metadata, admin, uuid.MustParse(settings.InstallationID), logger)
	if err != nil {
		return err
	}
	handler.CredentialKeyring = keyring
	handler.EnrollmentAuthority = authority
	handler.PublicOrigin = settings.PublicOrigin
	handler.AgentListen = settings.AgentListen
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
	stopped := make(chan error, 2)
	go func() { stopped <- server.ServeTLS(listener, "", "") }()
	logger.Info("controller API listening", "address", listener.Addr().String(), "installation_id", settings.InstallationID)
	if agentTLS != nil {
		agentServer, err := controlstream.New(controlstream.Config{
			InstallationID: uuid.MustParse(settings.InstallationID), TLS: agentTLS, Logger: logger,
			Authorize: func(ctx context.Context, _, nodeID uuid.UUID, certificate *x509.Certificate) error {
				return metadata.CheckAgentCertificate(ctx, nodeID, certificate)
			},
			Capabilities: []string{"runtime.docker", "apps.discovery", "destinations"},
			Handle:       (agentstream.Receiver{Store: metadata}).Handle,
		})
		if err != nil {
			return err
		}
		defer agentServer.Stop()
		deliveryCtx, stopDelivery := context.WithCancel(ctx)
		var delivery sync.WaitGroup
		delivery.Go(func() {
			deliverDestinations(deliveryCtx, uuid.MustParse(settings.InstallationID), metadata, keyring, agentServer, logger)
		})
		defer func() { stopDelivery(); delivery.Wait() }()
		agentListener, err := (&net.ListenConfig{}).Listen(ctx, "tcp", settings.AgentListen)
		if err != nil {
			return fmt.Errorf("listen for agent transport on %s: %w", settings.AgentListen, err)
		}
		go func() { stopped <- agentServer.Serve(agentListener) }()
		logger.Info("controller agent transport listening", "agent_address", agentListener.Addr().String(), "installation_id", settings.InstallationID)
	}
	failed := false
	select {
	case <-stopped:
		failed = true
	case <-ctx.Done():
	}
	shutdown, stopShutdown := context.WithTimeout(context.Background(), 10*time.Second)
	defer stopShutdown()
	shutdownErr := server.Shutdown(shutdown)
	if shutdownErr != nil {
		// Close is the forced stop after a graceful one ran out of time. Its own
		// failure is separate evidence and belongs beside the timeout.
		shutdownErr = errors.Join(shutdownErr, server.Close())
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
