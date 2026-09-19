package main

import (
	"context"
	"crypto/tls"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sirrobot01/synod/internal/api"
	"github.com/sirrobot01/synod/internal/auth"
	"github.com/sirrobot01/synod/internal/cluster"
	"github.com/sirrobot01/synod/internal/config"
	"github.com/sirrobot01/synod/internal/controller/agentstream"
	"github.com/sirrobot01/synod/internal/controller/runner"
	"github.com/sirrobot01/synod/internal/controlstream"
	"github.com/sirrobot01/synod/internal/coordinator"
	"github.com/sirrobot01/synod/internal/pki"
	"github.com/sirrobot01/synod/internal/store/postgres"
	"github.com/sirrobot01/synod/internal/web"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

// ownerID identifies this controller process in claims and elections.
func ownerID() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "controller"
	}
	return host + "/" + strconv.Itoa(os.Getpid())
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if err := run(ctx, os.Args[1:], os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, output io.Writer) error {
	settings, err := config.LoadController(args, output)
	if errors.Is(err, flag.ErrHelp) {
		return nil
	}
	if err != nil {
		return err
	}
	var level slog.Level
	if err := level.UnmarshalText([]byte(settings.LogLevel)); err != nil {
		return errors.New("invalid log level")
	}
	logger := slog.New(slog.NewJSONHandler(output, &slog.HandlerOptions{Level: level}))
	// Read through an open descriptor so checks apply to the bytes we use.
	token, err := readStartupFile(settings.AdminTokenFile, 258, true)
	if err != nil {
		return errors.New("read protected administrator token file failed")
	}
	admin, err := auth.NewAdminToken(strings.TrimSuffix(strings.TrimSuffix(string(token), "\n"), "\r"), settings.ActorID)
	if err != nil {
		return errors.New("administrator token file is invalid")
	}
	key, err := readStartupFile(settings.TLSKeyFile, 1<<20, true)
	if err != nil {
		return errors.New("read protected TLS key file failed")
	}
	cert, err := readStartupFile(settings.TLSCertFile, 1<<20, false)
	if err != nil {
		return errors.New("read TLS certificate file failed")
	}
	certificate, err := tls.X509KeyPair(cert, key)
	if err != nil {
		return errors.New("load API TLS certificate and key failed")
	}
	var authority *pki.CA
	if settings.CACertFile != "" {
		caCertificate, err := readStartupFile(settings.CACertFile, 64<<10, false)
		if err != nil {
			return errors.New("read enrollment CA certificate failed")
		}
		caKey, err := readStartupFile(settings.CAKeyFile, 64<<10, true)
		if err != nil {
			return errors.New("read protected enrollment CA key failed")
		}
		authority, err = pki.LoadCA(uuid.MustParse(settings.InstallationID), caCertificate, caKey)
		clear(caKey)
		if err != nil {
			return errors.New("load enrollment CA failed")
		}
	}
	var agentTLS *tls.Config
	if settings.AgentListen != "" {
		agentCert, err := readStartupFile(settings.AgentTLSCertFile, 64<<10, false)
		if err != nil {
			return errors.New("read agent-transport controller certificate failed")
		}
		agentKey, err := readStartupFile(settings.AgentTLSKeyFile, 64<<10, true)
		if err != nil {
			return errors.New("read protected agent-transport controller key failed")
		}
		agentTrust, err := readStartupFile(settings.AgentCAFile, 1<<20, false)
		if err != nil {
			return errors.New("read agent-transport installation trust failed")
		}
		agentTLS, err = agentstream.TLSConfig(uuid.MustParse(settings.InstallationID), agentCert, agentKey, agentTrust)
		clear(agentKey)
		if err != nil {
			return err
		}
	}
	startup, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	pool, err := pgxpool.New(startup, settings.DatabaseURL)
	if err != nil {
		return errors.New("configure metadata connection failed")
	}
	defer pool.Close()
	if err := pool.Ping(startup); err != nil {
		return errors.New("connect to metadata PostgreSQL failed")
	}
	store := postgres.New(pool)
	if err := store.Migrate(startup); err != nil {
		return errors.New("migrate metadata PostgreSQL failed")
	}
	handler, err := api.NewServer(store, admin, uuid.MustParse(settings.InstallationID), map[cluster.Engine][]string{
		cluster.PostgreSQL: {"17.11", "18.6"}, cluster.Redis: {"8.2.7"},
	}, logger)
	if err != nil {
		return err
	}
	handler.EnrollmentAuthority = authority
	handler.PublicOrigin = settings.PublicOrigin
	assets, err := web.Assets()
	if err != nil {
		return errors.New("load the web application failed")
	}
	handler.SetAssets(assets)
	listener, err := (&net.ListenConfig{}).Listen(ctx, "tcp", settings.Listen)
	if err != nil {
		return errors.New("listen for API requests failed")
	}
	defer listener.Close()
	server := &http.Server{
		Handler: handler, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second,
		WriteTimeout: 30 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 16 << 10,
		TLSConfig: &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{certificate}},
		ErrorLog:  slog.NewLogLogger(logger.Handler(), slog.LevelWarn),
	}
	var agentServer *controlstream.Server
	var agentListener net.Listener
	var operations *runner.Runner
	if agentTLS != nil {
		// The runner needs the transport and the transport needs the runner's routing,
		// so the handler reads the pointer that the runner build fills in below.
		handle := (agentstream.Receiver{Store: store}).Handle
		agentServer, err = controlstream.New(controlstream.Config{InstallationID: uuid.MustParse(settings.InstallationID), TLS: agentTLS, Authorize: store.CheckAgentCertificate, Capabilities: []string{"runtime.docker", controlstream.CommandInspectionCapability},
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
			return errors.New("listen for agent transport failed")
		}
		defer agentListener.Close()
		if settings.EtcdEndpoints != "" {
			endpoints := strings.Split(settings.EtcdEndpoints, ",")
			for index, endpoint := range endpoints {
				endpoints[index] = strings.TrimSpace(endpoint)
			}
			etcd, err := clientv3.New(clientv3.Config{Endpoints: endpoints, DialTimeout: 5 * time.Second, Context: ctx, Logger: zap.NewNop()})
			if err != nil {
				return errors.New("connect to etcd failed")
			}
			defer etcd.Close()
			elector, err := coordinator.New(etcd, settings.EtcdPrefix)
			if err != nil {
				return err
			}
			operations, err = runner.New(runner.Config{
				Store: store, Transport: agentServer, Coordinator: elector,
				InstallationID: uuid.MustParse(settings.InstallationID), OwnerID: ownerID(), Logger: logger,
			})
			if err != nil {
				return err
			}
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
	running := make(chan struct{})
	if operations != nil {
		go func() {
			defer close(running)
			if err := operations.Run(runnerCtx); err != nil {
				logger.Error("controller runner stopped", "error", err.Error())
			}
		}()
		logger.Info("controller runner electing cluster owners", "etcd_prefix", settings.EtcdPrefix, "installation_id", settings.InstallationID)
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
	if agentServer != nil {
		agentServer.Stop()
	}
	shutdown, stopShutdown := context.WithTimeout(context.Background(), 10*time.Second)
	defer stopShutdown()
	shutdownErr := server.Shutdown(shutdown)
	if shutdownErr != nil {
		_ = server.Close()
	}
	for range serving {
		<-stopped
	}
	if shutdownErr != nil {
		return errors.New("API shutdown exceeded its deadline")
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
