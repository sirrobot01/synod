package controller

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
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
	"github.com/sirrobot01/synod/internal/jointoken"
	"github.com/sirrobot01/synod/internal/logging"
	"github.com/sirrobot01/synod/internal/pki"
	"github.com/sirrobot01/synod/internal/restore"
	"github.com/sirrobot01/synod/internal/secret"
	"github.com/sirrobot01/synod/internal/store"
	"github.com/sirrobot01/synod/internal/web"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
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
		// Without settings, the controller uses the default directory and creates the
		// installation on its first start. The container image runs it this way.
		if cmd.Flags().NFlag() == 0 && os.Getenv("SYNOD_CONFIG") == "" {
			path, err := firstStart(cmd.Context(), config.DefaultControllerDir, cmd.OutOrStdout())
			if err != nil {
				return err
			}
			if err := cmd.Flags().Set("config", path); err != nil {
				return err
			}
		}
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
	logger, err := logging.New(output, settings.LogLevel, settings.LogFormat)
	if err != nil {
		return err
	}
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
	var apiCertificate liveCertificate
	apiCertificate.current.Store(&certificate)
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
	var agentCertificate liveCertificate
	var renewals []func(time.Time) error
	if agentTLS != nil {
		agentCertificate.current.Store(&agentTLS.Certificates[0])
		agentTLS.Certificates, agentTLS.GetCertificate = nil, agentCertificate.get
		if authority != nil {
			renewals = append(renewals, func(now time.Time) error {
				if !agentCertificate.due(now) {
					return nil
				}
				return renewAgentTransport(authority, settings.AgentTLSCertFile, settings.AgentTLSKeyFile, &agentCertificate, now)
			})
		}
	}
	if settings.APICACertFile != "" {
		renewals = append(renewals, func(now time.Time) error {
			if !apiCertificate.due(now) {
				return nil
			}
			return renewAPI(settings.APICACertFile, settings.APICAKeyFile, settings.TLSCertFile, settings.TLSKeyFile, &apiCertificate, now)
		})
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
	handler.JoinAddress, handler.APICAFingerprint, err = joinSettings(settings)
	if err != nil {
		return err
	}
	installation := uuid.MustParse(settings.InstallationID)
	handler.RecoveryKit = func(ctx context.Context) (string, error) {
		password, err := metadata.RepositoryPassword(ctx, installation, keyring)
		if err != nil || password == "" {
			return "", err
		}
		return recoveryKit(installation, password, time.Now().UTC()), nil
	}
	handler.EnrollmentAuthority = authority
	handler.PublicOrigin = settings.PublicOrigin
	handler.AgentListen = settings.AgentListen
	assets, err := web.Assets()
	if err != nil {
		return fmt.Errorf("load the web application: %w", err)
	}
	handler.SetAssets(assets)
	// The first unknown-user sign-in must not pay for the decoy hash, so compute it now.
	auth.SpendUnknownUser("")
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
		TLSConfig: &tls.Config{MinVersion: tls.VersionTLS12, GetCertificate: apiCertificate.get},
		ErrorLog:  slog.NewLogLogger(logger.Handler(), slog.LevelWarn),
	}
	notificationCtx, stopNotifications := context.WithCancel(ctx)
	var notifications sync.WaitGroup
	notifications.Go(func() {
		deliverNotifications(notificationCtx, uuid.MustParse(settings.InstallationID), metadata, keyring, logger)
	})
	notifications.Go(func() { keepCertificates(notificationCtx, renewals, logger) })
	defer func() { stopNotifications(); notifications.Wait() }()
	stopped := make(chan error, 2)
	logger.Info("controller API listening", "address", listener.Addr().String(), "installation_id", settings.InstallationID)
	if agentTLS != nil {
		receiver := agentstream.Receiver{Store: metadata, Authority: authority, Logger: logger}
		agentServer, err := controlstream.New(controlstream.Config{
			InstallationID: uuid.MustParse(settings.InstallationID), TLS: agentTLS, Logger: logger,
			Authorize: func(ctx context.Context, _, nodeID uuid.UUID, certificate *x509.Certificate) error {
				return metadata.CheckAgentCertificate(ctx, nodeID, certificate)
			},
			Capabilities: []string{"runtime.docker", "apps.discovery", "destinations", "destination.serve", "destination.health", "backup.exec", "restore.exec", "proof.exec", "files.read", "certificate.renew"},
			Handle: func(ctx context.Context, peer controlstream.Peer, message *agentv1.AgentMessage) error {
				return receiver.Handle(ctx, peer, message)
			},
		})
		if err != nil {
			return err
		}
		receiver.Send = agentServer.Send
		handler.ReadFile = func(ctx context.Context, request restore.FileRequest) (restore.FileHeader, io.ReadCloser, error) {
			return readSnapshotFile(ctx, metadata, agentServer, request)
		}
		defer agentServer.Stop()
		deliveryCtx, stopDelivery := context.WithCancel(ctx)
		var delivery sync.WaitGroup
		delivery.Go(func() {
			deliverDestinations(deliveryCtx, uuid.MustParse(settings.InstallationID), metadata, keyring, agentServer, logger)
		})
		delivery.Go(func() {
			deliverJobs(deliveryCtx, metadata, agentServer, logger)
		})
		defer func() { stopDelivery(); delivery.Wait() }()
		agentListener, err := (&net.ListenConfig{}).Listen(ctx, "tcp", settings.AgentListen)
		if err != nil {
			return fmt.Errorf("listen for agent transport on %s: %w", settings.AgentListen, err)
		}
		go func() { stopped <- agentServer.Serve(agentListener) }()
		logger.Info("controller agent transport listening", "agent_address", agentListener.Addr().String(), "installation_id", settings.InstallationID)
	}
	go func() { stopped <- server.ServeTLS(listener, "", "") }()
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

// joinSettings returns the address and the API CA pin that join tokens name.
// Without an address, a specified listen host is the address. A wildcard listener
// leaves it empty, so the API uses the host of each request.
func joinSettings(settings config.Controller) (string, string, error) {
	host, port, err := net.SplitHostPort(settings.Listen)
	if err != nil {
		return "", "", err
	}
	address := ""
	if settings.Address != "" {
		address = net.JoinHostPort(settings.Address, port)
	} else if ip := net.ParseIP(host); host != "" && (ip == nil || !ip.IsUnspecified()) {
		address = settings.Listen
	}
	if settings.APICACertFile == "" {
		return address, "", nil
	}
	data, err := readStartupFile(settings.APICACertFile, 64<<10, false)
	if err != nil {
		return "", "", fmt.Errorf("read the API CA certificate: %w", err)
	}
	block, _ := pem.Decode(data)
	if block == nil {
		return "", "", errors.New("the API CA certificate file holds no certificate")
	}
	return address, jointoken.Fingerprint(block.Bytes), nil
}
