package cli

import (
	"cmp"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/spf13/cobra"

	"github.com/sirrobot01/synod/internal/agent/daemon"
	"github.com/sirrobot01/synod/internal/agent/enrollment"
	"github.com/sirrobot01/synod/internal/jointoken"
	"github.com/sirrobot01/synod/internal/systemd"
)

const agentServiceTemplate = `[Unit]
Description=Synod Agent
After=network.target docker.service
Wants=docker.service

[Service]
Type=simple
ExecStart=%s agent run --config %s
Restart=always
RestartSec=5s

[Install]
WantedBy=multi-user.target
`

// joinOptions holds the flags of synod agent join.
type joinOptions struct {
	dir          string
	dockerSocket string
	systemdDir   string
	// controllerEndpoint replaces the agent endpoint that enrollment returns.
	controllerEndpoint string
	destinationListen  string
	destinationAddress string
}

func joinCommand() *cobra.Command {
	var options joinOptions
	command := &cobra.Command{
		Use:   "join TOKEN",
		Short: "Enroll this device with a join token and prepare the agent to run",
		Long: `Join enrolls this device and writes the agent configuration and service unit.
Run it again with the same token and directory to resume after a failure.`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return join(cmd.Context(), args[0], options, cmd.OutOrStdout())
		},
	}
	flags := command.Flags()
	flags.StringVar(&options.destinationListen, "destination-listen", ":8445", "Agent destination listener")
	flags.StringVar(&options.destinationAddress, "destination-address", "", "Reachable host:port for other agents (default: controller-observed address)")
	flags.StringVar(&options.dir, "dir", "/var/lib/synod/agent", "Private directory for the agent state and configuration")
	flags.StringVar(&options.dockerSocket, "docker-socket", "/var/run/docker.sock", "Docker Engine socket")
	flags.StringVar(&options.controllerEndpoint, "controller-endpoint", "", "Controller agent transport host:port (default: the address enrollment returns)")
	flags.StringVar(&options.systemdDir, "systemd-dir", "/etc/systemd/system", "Systemd units directory to install the service into")
	return command
}

func join(ctx context.Context, encoded string, options joinOptions, stdout io.Writer) error {
	token, err := jointoken.Decode(encoded)
	if err != nil {
		return fmt.Errorf("%w: %w", enrollment.ErrConfiguration, err)
	}
	dir, err := filepath.Abs(options.dir)
	if err != nil {
		return fmt.Errorf("%w: resolve --dir: %w", enrollment.ErrConfiguration, err)
	}
	configPath := filepath.Join(dir, "agent.json")
	if _, err := os.Lstat(configPath); err == nil {
		return fmt.Errorf("%w: %s exists, so this node already joined", enrollment.ErrState, configPath)
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("%w: create %s: %w", enrollment.ErrState, dir, err)
	}

	// The node ID is kept before enrollment, so that a rerun repeats the same request.
	nodeID := uuid.New()
	if token.NodeID != uuid.Nil {
		nodeID = token.NodeID
	}
	nodePath := filepath.Join(dir, "node-id")
	if saved, err := os.ReadFile(nodePath); err == nil {
		if nodeID, err = uuid.Parse(strings.TrimSpace(string(saved))); err != nil || nodeID == uuid.Nil {
			return fmt.Errorf("%w: %s does not hold a node UUID", enrollment.ErrState, nodePath)
		}
		if token.NodeID != uuid.Nil && token.NodeID != nodeID {
			return fmt.Errorf("%w: the join token names a different device; use a new --dir", enrollment.ErrState)
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("%w: read %s: %w", enrollment.ErrState, nodePath, err)
	} else if err := os.WriteFile(nodePath, []byte(nodeID.String()+"\n"), 0o600); err != nil {
		return fmt.Errorf("%w: write %s: %w", enrollment.ErrState, nodePath, err)
	}

	credentials, err := enroll(ctx, token, nodeID, dir)
	if err != nil {
		return err
	}
	endpoint := cmp.Or(options.controllerEndpoint, credentials.AgentEndpoint)
	if endpoint == "" {
		return fmt.Errorf("%w: the controller returned no agent endpoint; run this command again with --controller-endpoint", enrollment.ErrConfiguration)
	}

	config := daemon.Config{
		InstallationID:      token.InstallationID.String(),
		NodeID:              nodeID.String(),
		EnrollmentDir:       filepath.Join(dir, "enrollment"),
		ControllerEndpoints: []string{endpoint},
		DockerSocket:        options.dockerSocket,
		DestinationListen:   options.destinationListen,
		DestinationAddress:  options.destinationAddress,
	}

	executable, err := os.Executable()
	if err != nil {
		executable = "synod"
	}
	service := fmt.Sprintf(agentServiceTemplate, executable, configPath)
	servicePath := filepath.Join(dir, "synod-agent.service")
	if err := os.WriteFile(servicePath, []byte(service), 0o644); err != nil {
		return fmt.Errorf("write %s: %w", servicePath, err)
	}
	installed := false
	if info, err := os.Stat(options.systemdDir); err == nil && info.IsDir() {
		unit := filepath.Join(options.systemdDir, "synod-agent.service")
		if err := os.WriteFile(unit, []byte(service), 0o644); err != nil {
			return fmt.Errorf("install %s: %w", unit, err)
		}
		servicePath, installed = unit, true
	}

	// The configuration is written last. Its presence marks a completed join.
	encodedConfig, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		return fmt.Errorf("encode the agent configuration: %w", err)
	}
	if err := os.WriteFile(configPath, append(encodedConfig, '\n'), 0o600); err != nil {
		return fmt.Errorf("write %s: %w", configPath, err)
	}
	start := fmt.Sprintf("Start the agent on this device:\n  synod agent run --config %s", configPath)
	if installed {
		start = "Start the agent on this device:\n  systemctl daemon-reload && systemctl enable --now synod-agent"
		// The configuration exists now, so the service can start.
		if options.systemdDir == systemd.UnitDir && systemd.Running() {
			if err := systemd.Start("synod-agent"); err != nil {
				start = fmt.Sprintf("The synod-agent service did not start: %v\n%s", err, start)
			} else {
				start = "Started the synod-agent service. It starts again after a reboot."
			}
		}
	}
	_, err = fmt.Fprintf(stdout, `Device %s joined installation %s.

Agent configuration: %s
Service unit: %s

%s

Then approve the device on the controller:
  synod node approve %s
`, nodeID, token.InstallationID, configPath, servicePath, start, nodeID)
	return err
}

// enroll completes enrollment in dir/enrollment, or reads a completed one.
func enroll(ctx context.Context, token jointoken.Token, nodeID uuid.UUID, dir string) (enrollment.Credentials, error) {
	state, err := enrollment.Open(filepath.Join(dir, "enrollment"), enrollment.Options{Identity: enrollment.Identity{InstallationID: token.InstallationID, NodeID: nodeID}, AllowCreate: true})
	if err != nil {
		return enrollment.Credentials{}, err
	}
	defer state.Close()
	if credentials, err := state.Credentials(); err == nil {
		return credentials, nil
	}
	roots, err := pinnedAPICA(ctx, token)
	if err != nil {
		return enrollment.Credentials{}, err
	}
	// The enrollment client reads the secret from a private file.
	tokenPath := filepath.Join(dir, "join.token")
	if err := os.WriteFile(tokenPath, []byte(token.Secret+"\n"), 0o600); err != nil {
		return enrollment.Credentials{}, fmt.Errorf("%w: write %s: %w", enrollment.ErrState, tokenPath, err)
	}
	defer os.Remove(tokenPath)
	credentials, err := state.Submit(ctx, enrollment.Config{Endpoint: "https://" + token.Address, TokenFile: tokenPath, RootCAs: roots})
	if err != nil {
		return enrollment.Credentials{}, err
	}
	return credentials, state.Close()
}

// pinnedAPICA reads the chain the API serves and trusts only the CA the token pins.
// The enrollment request then verifies the API certificate against that CA.
func pinnedAPICA(ctx context.Context, token jointoken.Token) (*x509.CertPool, error) {
	dialer := &tls.Dialer{NetDialer: &net.Dialer{Timeout: 10 * time.Second}, Config: &tls.Config{MinVersion: tls.VersionTLS12, InsecureSkipVerify: true}}
	conn, err := dialer.DialContext(ctx, "tcp", token.Address)
	if err != nil {
		return nil, fmt.Errorf("%w: connect to %s: %w", enrollment.ErrTransport, token.Address, err)
	}
	defer conn.Close()
	for _, certificate := range conn.(*tls.Conn).ConnectionState().PeerCertificates {
		if certificate.IsCA && jointoken.Fingerprint(certificate.Raw) == token.Fingerprint {
			roots := x509.NewCertPool()
			roots.AddCert(certificate)
			return roots, nil
		}
	}
	return nil, fmt.Errorf("%w: %s serves no CA certificate that matches the join token fingerprint", enrollment.ErrCertificate, token.Address)
}

func renewCommand() *cobra.Command {
	var dir string
	command := &cobra.Command{
		Use:   "renew TOKEN",
		Short: "Get a new certificate for this device with a device-bound join token",
		Long: `Renew recovers a device whose certificate expired while it was offline.
Create the token with synod node join-token --device DEVICE. The device keeps its key,
identity, approval, and encrypted cache. Stop the agent, renew, and start it again.`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return renew(cmd.Context(), args[0], dir, cmd.OutOrStdout())
		},
	}
	command.Flags().StringVar(&dir, "dir", "/var/lib/synod/agent", "Private directory with the agent state")
	return command
}

func renew(ctx context.Context, encoded, dir string, stdout io.Writer) error {
	token, err := jointoken.Decode(encoded)
	if err != nil {
		return fmt.Errorf("%w: %w", enrollment.ErrConfiguration, err)
	}
	dir, err = filepath.Abs(dir)
	if err != nil {
		return fmt.Errorf("%w: resolve --dir: %w", enrollment.ErrConfiguration, err)
	}
	saved, err := os.ReadFile(filepath.Join(dir, "node-id"))
	if err != nil {
		return fmt.Errorf("%w: read the device identity in %s: %w", enrollment.ErrState, dir, err)
	}
	nodeID, err := uuid.Parse(strings.TrimSpace(string(saved)))
	if err != nil || token.NodeID == uuid.Nil || token.NodeID != nodeID {
		return fmt.Errorf("%w: renewal needs a token from synod node join-token --device %s", enrollment.ErrConfiguration, strings.TrimSpace(string(saved)))
	}
	// The agent holds its enrollment state open. Stop its service, and start it again at the end.
	running := systemd.Serves("synod-agent", filepath.Join(dir, "agent.json"))
	if running {
		if err := systemd.Run("stop", "synod-agent"); err != nil {
			return err
		}
		defer func() {
			if err := systemd.Run("start", "synod-agent"); err != nil {
				fmt.Fprintf(stdout, "The synod-agent service did not start: %v\n", err)
			}
		}()
	}
	state, err := enrollment.Open(filepath.Join(dir, "enrollment"), enrollment.Options{Identity: enrollment.Identity{InstallationID: token.InstallationID, NodeID: nodeID}})
	if err != nil {
		return err
	}
	defer state.Close()
	roots, err := pinnedAPICA(ctx, token)
	if err != nil {
		return err
	}
	tokenPath := filepath.Join(dir, "renew.token")
	if err := os.WriteFile(tokenPath, []byte(token.Secret+"\n"), 0o600); err != nil {
		return fmt.Errorf("%w: write %s: %w", enrollment.ErrState, tokenPath, err)
	}
	defer os.Remove(tokenPath)
	if _, err := state.RenewWithToken(ctx, enrollment.Config{Endpoint: "https://" + token.Address, TokenFile: tokenPath, RootCAs: roots}); err != nil {
		return err
	}
	notAfter, err := state.NotAfter()
	if err != nil {
		return err
	}
	next := "Start the agent again:\n  systemctl restart synod-agent"
	if running {
		next = "The synod-agent service starts again with the new certificate."
	}
	_, err = fmt.Fprintf(stdout, "Device %s has a new certificate until %s.\n%s\n", nodeID, notAfter.UTC().Format(time.RFC3339), next)
	return err
}
