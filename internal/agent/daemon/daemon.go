package daemon

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"log/slog"
	"os"
	"time"

	"github.com/google/uuid"
	"github.com/moby/moby/client"
	"github.com/sirrobot01/synod/internal/agent/enrollment"
	"github.com/sirrobot01/synod/internal/agent/transport"
	"github.com/sirrobot01/synod/internal/controlstream"
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
	hello := &agentv1.Hello{
		InstallationId: config.InstallationID, NodeId: config.NodeID,
		Protocol:     &agentv1.VersionRange{Minimum: controlstream.ProtocolVersion, Maximum: controlstream.ProtocolVersion},
		Capabilities: []string{"runtime.docker"}, RuntimeVersion: facts.version, Architecture: facts.architecture,
		Hostname: hostname, AgentVersion: Version,
	}
	stream, err := transport.New(transport.Config{
		Endpoints: config.ControllerEndpoints, Logger: logger, Hello: hello,
		TLS: &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots, Certificates: []tls.Certificate{identity.Certificate}},
		Handle: func(_ context.Context, message *agentv1.ControllerMessage) (*agentv1.AgentMessage, error) {
			if failure := message.GetError(); failure != nil {
				logger.Warn("controller_error", "code", failure.Code, "message", failure.Message)
			}
			return nil, nil
		},
	})
	if err != nil {
		return fmt.Errorf("%w: build the agent transport: %w", ErrConfiguration, err)
	}
	logger.Info("agent_started", "node_id", config.NodeID, "architecture", facts.architecture, "runtime_version", facts.version)
	err = stream.Run(ctx)
	if ctx.Err() == nil {
		return fmt.Errorf("%w: %w", ErrExecution, err)
	}
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
