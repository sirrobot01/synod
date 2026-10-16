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
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/moby/moby/client"
	agentbackup "github.com/sirrobot01/synod/internal/agent/backup"
	"github.com/sirrobot01/synod/internal/agent/blobserver"
	"github.com/sirrobot01/synod/internal/agent/enrollment"
	"github.com/sirrobot01/synod/internal/agent/journal"
	agentrestore "github.com/sirrobot01/synod/internal/agent/restore"
	"github.com/sirrobot01/synod/internal/agent/transport"
	"github.com/sirrobot01/synod/internal/backup"
	"github.com/sirrobot01/synod/internal/buildinfo"
	"github.com/sirrobot01/synod/internal/controlstream"
	"github.com/sirrobot01/synod/internal/destination"
	"github.com/sirrobot01/synod/internal/logging"
	"github.com/sirrobot01/synod/internal/pki"
	"github.com/sirrobot01/synod/internal/proof"
	"github.com/sirrobot01/synod/internal/repository"
	"github.com/sirrobot01/synod/internal/restore"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
)

// errRenewed asks Run to start again with the renewed certificate.
var errRenewed = errors.New("agent certificate renewed")

// renewalSettle gives peers time to receive the new certificate fingerprint in a plan
// before the agent presents the new certificate.
var renewalSettle = 2 * time.Minute

// Run connects an enrolled agent to its controller and keeps the session until ctx ends.
// After a certificate renewal, it starts again with the new certificate.
func Run(ctx context.Context, config Config, diagnostics io.Writer) error {
	for {
		if err := run(ctx, config, diagnostics); !errors.Is(err, errRenewed) {
			return err
		}
	}
}

func run(ctx context.Context, config Config, diagnostics io.Writer) error {
	if diagnostics == nil {
		return fmt.Errorf("%w: a diagnostics writer is required", ErrConfiguration)
	}
	if err := config.validate(); err != nil {
		return err
	}
	logger, err := logging.New(diagnostics, config.LogLevel, config.LogFormat)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrConfiguration, err)
	}
	state, err := enrollment.Open(config.EnrollmentDir, enrollment.Options{Identity: enrollment.Identity{InstallationID: uuid.MustParse(config.InstallationID), NodeID: uuid.MustParse(config.NodeID)}})
	if err != nil {
		return fmt.Errorf("%w: open enrollment state %s: %w", ErrEnrollment, config.EnrollmentDir, err)
	}
	defer state.Close()
	identity, err := state.Credentials()
	if notAfter, expiryErr := state.NotAfter(); err != nil && expiryErr == nil && !time.Now().Before(notAfter) {
		return fmt.Errorf("%w: the device certificate expired at %s. Stop this agent, run synod node join-token --device %s on the controller, then run synod agent renew TOKEN on this device", ErrEnrollment, notAfter.UTC().Format(time.RFC3339), config.NodeID)
	}
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
		Capabilities: []string{"runtime.docker", "apps.discovery", "destinations", "destination.serve", "destination.health", "backup.exec", "restore.exec", "proof.exec", "files.read", "certificate.renew"}, RuntimeVersion: facts.version, Architecture: facts.architecture,
		Hostname: hostname, AgentVersion: buildinfo.Version,
	}
	// renewed holds the time of a saved renewal. The agent starts again when it is idle.
	var renewed atomic.Pointer[time.Time]
	// File requests wait in a bounded queue. A full queue refuses the request, and the controller times out.
	fileRequests := make(chan restore.FileRequest, 8)
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
				logger.Debug("backup request received", "app", agentbackup.AppName(request.SourceKey), "operation", request.ID)
				return nil, nil
			}
			if file := message.GetFileRequest(); file != nil {
				var request restore.FileRequest
				if len(file.RequestJson) > 8192 || json.Unmarshal(file.RequestJson, &request) != nil || request.Validate() != nil || request.NodeID != identity.Identity.NodeID || request.InstallationID != identity.Identity.InstallationID {
					return nil, restore.ErrInvalid
				}
				select {
				case fileRequests <- request:
				default:
					logger.Warn("file request refused: too many file requests are waiting", "app", request.AppID, "request", request.ID)
				}
				return nil, nil
			}
			if job := message.GetProofJob(); job != nil {
				var request proof.Request
				if len(job.RequestJson) > 8192 || json.Unmarshal(job.RequestJson, &request) != nil || request.Validate() != nil || request.NodeID != identity.Identity.NodeID || request.InstallationID != identity.Identity.InstallationID {
					return nil, proof.ErrInvalid
				}
				raw, _ := json.Marshal(request)
				if _, err := cache.Admit(ctx, request.ID, raw); err != nil {
					return nil, err
				}
				return nil, nil
			}
			if job := message.GetRestoreJob(); job != nil {
				var request restore.Request
				if len(job.RequestJson) > 8192 || json.Unmarshal(job.RequestJson, &request) != nil || request.Validate() != nil || request.NodeID != identity.Identity.NodeID || request.InstallationID != identity.Identity.InstallationID {
					return nil, restore.ErrInvalid
				}
				raw, _ := json.Marshal(request)
				if _, err := cache.Admit(ctx, request.ID, raw); err != nil {
					return nil, err
				}
				return nil, nil
			}
			if ack := message.GetOperationAck(); ack != nil {
				id, err := uuid.Parse(ack.OperationId)
				if err != nil {
					return nil, backup.ErrInvalid
				}
				return nil, cache.Acknowledge(ctx, id)
			}
			if update := message.GetDestinationPlan(); update != nil {
				var plan destination.Plan
				err := json.Unmarshal(update.SettingsJson, &plan)
				if err == nil {
					err = cache.SaveDestinationPlan(ctx, ring, plan)
				}
				if err != nil {
					logger.Warn("destination settings not saved", "revision", plan.Revision, "error", err)
					return nil, destination.ErrInvalid
				}
				return &agentv1.AgentMessage{Message: &agentv1.AgentMessage_DestinationAck{DestinationAck: &agentv1.DestinationAck{Revision: plan.Revision}}}, nil
			}
			if issued := message.GetCertificateIssued(); issued != nil {
				id, err := uuid.Parse(issued.CertificateId)
				if err == nil {
					err = state.Renew(id, issued.CertificatePem, issued.CaCertificatePem)
				}
				if err != nil {
					logger.Warn("certificate renewal failed: the controller sent an unusable certificate", "error", err)
					return nil, nil
				}
				renewed.CompareAndSwap(nil, new(time.Now()))
				logger.Info("certificate renewed", "certificate", id)
				return nil, nil
			}
			if failure := message.GetError(); failure != nil {
				logger.Warn("controller refused a message from this agent", "code", failure.Code, "error", failure.Message)
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
	// No backup runs yet, so every Synod snapshot is left from an interrupted backup.
	if err := agentbackup.RemoveStaleSnapshots(ctx); err != nil {
		logger.Warn("leftover filesystem snapshots not removed", "error", err)
	}
	backupWorker := &agentbackup.Worker{Docker: api, Journal: cache, Keyring: ring, InstallationID: identity.Identity.InstallationID, NodeID: identity.Identity.NodeID, NodeName: hostname, Architecture: facts.architecture, Identity: deviceIdentity, PrivatePaths: []string{enrollmentPath, enrollmentPath + ".db", enrollmentPath + ".db-wal", enrollmentPath + ".db-shm"}, Logger: logger, CacheDirectory: filepath.Join(filepath.Dir(enrollmentPath), "cache")}
	// Restored apps live beside the agent directory, in apps, so a new agent directory keeps them.
	restoreWorker := &agentrestore.Worker{Backup: backupWorker, Directory: filepath.Join(filepath.Dir(enrollmentPath), "restores"), Apps: filepath.Join(filepath.Dir(filepath.Dir(enrollmentPath)), "apps")}
	var waiting atomic.Pointer[uuid.UUID]
	reload := make(chan struct{})
	worker.Go(func() {
		executeJobs(workerCtx, backupWorker, restoreWorker, &waiting, logger, func() bool {
			// Peers receive the new certificate fingerprint in a plan before the agent uses it.
			at := renewed.Load()
			if at == nil || time.Since(*at) < renewalSettle {
				return false
			}
			close(reload)
			return true
		})
	})
	worker.Go(func() { requestRenewal(workerCtx, state, stream, &renewed, logger) })
	worker.Go(func() { reportJobs(workerCtx, cache, stream, &waiting, logger) })
	worker.Go(func() { discover(workerCtx, api, stream, logger) })
	worker.Go(func() { reportDestinationHealth(workerCtx, cache, ring, identity.Identity.NodeID, stream, logger) })
	for range 4 {
		worker.Go(func() { serveFiles(workerCtx, fileRequests, restoreWorker, stream, logger) })
	}
	defer func() { stopWorker(); worker.Wait() }()
	logger.Info("agent started", "device", hostname, "device_id", config.NodeID, "version", buildinfo.Version, "architecture", facts.architecture, "docker", facts.version, "controller", strings.Join(config.ControllerEndpoints, ","))
	// A blob write waits for the disk. A slow disk can need minutes to make packs durable.
	blobHTTP := &http.Server{Handler: blobHandler, TLSConfig: &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{identity.Certificate}, ClientCAs: roots, ClientAuth: tls.RequireAndVerifyClientCert}, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Minute, WriteTimeout: 10 * time.Minute, IdleTimeout: time.Minute, MaxHeaderBytes: 16 << 10, ErrorLog: slog.NewLogLogger(logger.Handler(), slog.LevelWarn), BaseContext: func(net.Listener) context.Context { return workerCtx }}
	stopped := make(chan error, 2)
	worker.Go(func() { stopped <- stream.Run(workerCtx) })
	worker.Go(func() { stopped <- blobHTTP.ServeTLS(blobListener, "", "") })
	select {
	case err = <-stopped:
	case <-reload:
		err = errRenewed
	case <-ctx.Done():
		err = ctx.Err()
	}
	stopWorker()
	shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if stopErr := blobHTTP.Shutdown(shutdown); stopErr != nil {
		err = errors.Join(err, stopErr, blobHTTP.Close())
	}
	if ctx.Err() == nil && !errors.Is(err, errRenewed) {
		return fmt.Errorf("%w: %w", ErrExecution, err)
	}
	// Keep the enrollment lock until restart recovery has finished.
	worker.Wait()
	if err := state.Close(); err != nil {
		return fmt.Errorf("%w: close enrollment state: %w", ErrEnrollment, err)
	}
	if errors.Is(err, errRenewed) {
		logger.Info("agent restarting to use its renewed certificate")
		return errRenewed
	}
	return nil
}

// requestRenewal asks for a new certificate once the current one is within the renewal window.
// An unsent request repeats after 15 seconds, and a lost reply after an hour.
// The controller returns a recent certificate again instead of issuing another.
func requestRenewal(ctx context.Context, state *enrollment.State, stream *transport.Client, renewed *atomic.Pointer[time.Time], logger *slog.Logger) {
	for {
		wait := time.Hour
		notAfter, err := state.NotAfter()
		if err == nil && renewed.Load() == nil && time.Until(notAfter) < pki.RenewalWindow {
			csr, err := state.RenewalRequest()
			if err == nil {
				err = stream.Send(&agentv1.AgentMessage{Message: &agentv1.AgentMessage_CertificateRequest{CertificateRequest: &agentv1.CertificateRequest{CsrDer: csr}}})
			}
			if err != nil {
				wait = 15 * time.Second
				if !errors.Is(err, controlstream.ErrDisconnected) {
					logger.Warn("certificate renewal request not sent; retrying in 15 seconds", "error", err)
				}
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(wait):
		}
	}
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
