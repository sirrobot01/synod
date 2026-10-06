package daemon

import (
	"cmp"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/moby/moby/client"
	agentbackup "github.com/sirrobot01/synod/internal/agent/backup"
	"github.com/sirrobot01/synod/internal/agent/blobserver"
	"github.com/sirrobot01/synod/internal/agent/enrollment"
	"github.com/sirrobot01/synod/internal/agent/journal"
	"github.com/sirrobot01/synod/internal/agent/transport"
	"github.com/sirrobot01/synod/internal/backup"
	"github.com/sirrobot01/synod/internal/controlstream"
	"github.com/sirrobot01/synod/internal/destination"
	"github.com/sirrobot01/synod/internal/repository"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
)

// Version is the agent build version that Hello reports.
var Version = "dev"

// Run connects an enrolled agent to its controller and keeps the session until ctx ends.
func Run(ctx context.Context, config Config, diagnostics io.Writer) error {
	if diagnostics == nil {
		return fmt.Errorf("%w: a diagnostics writer is required", ErrConfiguration)
	}
	if err := config.validate(); err != nil {
		return err
	}
	logger := slog.New(slog.NewJSONHandler(diagnostics, nil))
	state, err := enrollment.Open(config.EnrollmentDir, enrollment.Options{Identity: enrollment.Identity{InstallationID: uuid.MustParse(config.InstallationID), NodeID: uuid.MustParse(config.NodeID)}})
	if err != nil {
		return fmt.Errorf("%w: open enrollment state %s: %w", ErrEnrollment, config.EnrollmentDir, err)
	}
	defer state.Close()
	identity, err := state.Credentials()
	if err != nil {
		return fmt.Errorf("%w: read enrollment credentials: %w", ErrEnrollment, err)
	}
	cache, err := journal.Open(ctx, config.EnrollmentDir+".db", identity.Identity.InstallationID, identity.Identity.NodeID)
	if err != nil {
		return fmt.Errorf("%w: open agent journal: %w", ErrConfiguration, err)
	}
	defer cache.Close()
	ring, err := journal.DestinationKeyring(identity.Identity.InstallationID, identity.Identity.NodeID, identity.Certificate.PrivateKey)
	if err != nil {
		return fmt.Errorf("%w: build destination cache key: %w", ErrConfiguration, err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM([]byte(identity.CACertificatePEM)) {
		return fmt.Errorf("%w: the enrolled CA bundle contains no usable certificate", ErrEnrollment)
	}
	facts, err := runtimeFacts(ctx, config.DockerSocket)
	if err != nil {
		return err
	}
	hostname, err := os.Hostname()
	if err != nil {
		return fmt.Errorf("%w: read the hostname: %w", ErrConfiguration, err)
	}
	blobHandler, err := blobserver.New(identity.Identity.InstallationID, identity.Identity.NodeID, identity.Certificate.Leaf, func(ctx context.Context) (destination.Plan, error) { return cache.DestinationPlan(ctx, ring) })
	if err != nil {
		return fmt.Errorf("%w: build destination service: %w", ErrConfiguration, err)
	}
	blobListener, err := (&net.ListenConfig{}).Listen(ctx, "tcp", cmp.Or(config.DestinationListen, ":8445"))
	if err != nil {
		return fmt.Errorf("%w: listen for destination requests: %w", ErrConfiguration, err)
	}
	defer blobListener.Close()
	destinationAddress := cmp.Or(config.DestinationAddress, blobListener.Addr().String())
	hello := &agentv1.Hello{
		InstallationId: config.InstallationID, NodeId: config.NodeID, DestinationAddress: destinationAddress,
		Protocol:     &agentv1.VersionRange{Minimum: controlstream.ProtocolVersion, Maximum: controlstream.ProtocolVersion},
		Capabilities: []string{"runtime.docker", "apps.discovery", "destinations", "destination.serve", "backup.exec"}, RuntimeVersion: facts.version, Architecture: facts.architecture,
		Hostname: hostname, AgentVersion: Version,
	}
	stream, err := transport.New(transport.Config{
		Endpoints: config.ControllerEndpoints, Logger: logger, Hello: hello,
		TLS: &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots, Certificates: []tls.Certificate{identity.Certificate}},
		Handle: func(ctx context.Context, message *agentv1.ControllerMessage) (*agentv1.AgentMessage, error) {
			if job := message.GetBackupJob(); job != nil {
				var request backup.Request
				if len(job.RequestJson) > 8192 || json.Unmarshal(job.RequestJson, &request) != nil || request.Validate() != nil || request.NodeID != identity.Identity.NodeID || request.InstallationID != identity.Identity.InstallationID {
					return nil, backup.ErrInvalid
				}
				raw, _ := json.Marshal(request)
				if _, err := cache.Admit(ctx, request.ID, raw); err != nil {
					return nil, err
				}
				return nil, nil
			}
			if ack := message.GetBackupAck(); ack != nil {
				id, err := uuid.Parse(ack.OperationId)
				if err != nil {
					return nil, backup.ErrInvalid
				}
				return nil, cache.Acknowledge(ctx, id)
			}
			if update := message.GetDestinationPlan(); update != nil {
				var plan destination.Plan
				if json.Unmarshal(update.SettingsJson, &plan) != nil || cache.SaveDestinationPlan(ctx, ring, plan) != nil {
					logger.Warn("destination_plan_failed", "code", "destination_cache_failed")
					return nil, destination.ErrInvalid
				}
				return &agentv1.AgentMessage{Message: &agentv1.AgentMessage_DestinationAck{DestinationAck: &agentv1.DestinationAck{Revision: plan.Revision}}}, nil
			}
			if failure := message.GetError(); failure != nil {
				logger.Warn("controller_error", "code", failure.Code, "message", failure.Message)
			}
			return nil, nil
		},
	})
	if err != nil {
		return fmt.Errorf("%w: build the agent transport: %w", ErrConfiguration, err)
	}
	api, err := client.New(client.WithHost("unix://"+config.DockerSocket), client.WithAPIVersionNegotiation())
	if err != nil {
		return fmt.Errorf("%w: open Docker for app discovery: %w", ErrRuntime, err)
	}
	defer api.Close()
	workerCtx, stopWorker := context.WithCancel(ctx)
	defer stopWorker()
	var worker sync.WaitGroup
	keyDER, err := x509.MarshalPKCS8PrivateKey(identity.Certificate.PrivateKey)
	if err != nil {
		return fmt.Errorf("%w: prepare repository identity", ErrEnrollment)
	}
	deviceIdentity := &repository.DeviceIdentity{InstallationID: identity.Identity.InstallationID, CAPEM: identity.CACertificatePEM, PrivateKeyPEM: string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}))}
	clear(keyDER)
	for _, certificate := range identity.Certificate.Certificate {
		deviceIdentity.CertificatePEM += string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certificate}))
	}
	enrollmentPath, err := filepath.Abs(config.EnrollmentDir)
	if err != nil {
		return fmt.Errorf("%w: resolve enrollment directory", ErrConfiguration)
	}
	backupWorker := &agentbackup.Worker{Docker: api, Journal: cache, Keyring: ring, InstallationID: identity.Identity.InstallationID, NodeID: identity.Identity.NodeID, NodeName: hostname, Architecture: facts.architecture, Identity: deviceIdentity, PrivatePaths: []string{enrollmentPath, enrollmentPath + ".db", enrollmentPath + ".db-wal", enrollmentPath + ".db-shm"}}
	var waiting atomic.Pointer[uuid.UUID]
	worker.Go(func() { executeBackups(workerCtx, backupWorker, &waiting, logger) })
	worker.Go(func() { reportBackups(workerCtx, cache, stream, &waiting, logger) })
	worker.Go(func() { discover(workerCtx, api, stream, logger) })
	defer func() { stopWorker(); worker.Wait() }()
	logger.Info("agent_started", "node_id", config.NodeID, "architecture", facts.architecture, "runtime_version", facts.version)
	blobHTTP := &http.Server{Handler: blobHandler, TLSConfig: &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{identity.Certificate}, ClientCAs: roots, ClientAuth: tls.RequireAndVerifyClientCert}, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 2 * time.Minute, WriteTimeout: 2 * time.Minute, IdleTimeout: time.Minute, MaxHeaderBytes: 16 << 10, ErrorLog: slog.NewLogLogger(logger.Handler(), slog.LevelWarn), BaseContext: func(net.Listener) context.Context { return workerCtx }}
	stopped := make(chan error, 2)
	worker.Go(func() { stopped <- stream.Run(workerCtx) })
	worker.Go(func() { stopped <- blobHTTP.ServeTLS(blobListener, "", "") })
	select {
	case err = <-stopped:
	case <-ctx.Done():
		err = ctx.Err()
	}
	stopWorker()
	shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if stopErr := blobHTTP.Shutdown(shutdown); stopErr != nil {
		err = errors.Join(err, stopErr, blobHTTP.Close())
	}
	if ctx.Err() == nil {
		return fmt.Errorf("%w: %w", ErrExecution, err)
	}
	// Keep the enrollment lock until restart recovery has finished.
	worker.Wait()
	if err := state.Close(); err != nil {
		return fmt.Errorf("%w: close enrollment state: %w", ErrEnrollment, err)
	}
	return nil
}

type hostFacts struct {
	version      string
	architecture string
}

// runtimeFacts reads the Docker version and the host architecture that Docker reports.
func runtimeFacts(ctx context.Context, socket string) (hostFacts, error) {
	api, err := client.New(client.WithHost("unix://"+socket), client.WithAPIVersionNegotiation())
	if err != nil {
		return hostFacts{}, fmt.Errorf("%w: build a runtime client for %s: %w", ErrRuntime, socket, err)
	}
	defer api.Close()
	startup, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	version, err := api.ServerVersion(startup, client.ServerVersionOptions{})
	if err != nil {
		return hostFacts{}, fmt.Errorf("%w: read the runtime version from %s: %w", ErrRuntime, socket, err)
	}
	if version.Version == "" || len(version.Version) > 256 {
		return hostFacts{}, fmt.Errorf("%w: the runtime reported a version of %d bytes", ErrRuntime, len(version.Version))
	}
	switch version.Arch {
	case "amd64", "arm64":
	default:
		return hostFacts{}, fmt.Errorf("%w: the runtime reports architecture %q; Synod supports amd64 and arm64", ErrRuntime, version.Arch)
	}
	return hostFacts{version: version.Version, architecture: version.Arch}, nil
}
