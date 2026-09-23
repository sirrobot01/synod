// Package cli exposes protected agent enrollment commands.
package cli

import (
	"bytes"
	"context"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"os"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/agent/daemon"
	"github.com/sirrobot01/synod/internal/agent/enrollment"
	"github.com/spf13/cobra"
)

// Identity is the public enrollment result that the CLI prints.
type Identity struct {
	InstallationID uuid.UUID `json:"installation_id"`
	NodeID         uuid.UUID `json:"node_id"`
	RequestID      uuid.UUID `json:"request_id"`
	CertificateID  uuid.UUID `json:"certificate_id"`
	AcceptedAt     time.Time `json:"accepted_at"`
	ExpiresAt      time.Time `json:"expires_at"`
}

// Failure is the bounded error that the CLI prints for a failed command.
type Failure struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// Run returns an exit status. Enrollment output contains public identity only.
func Run(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	var installationID, nodeID, statePath, caPath string
	var config enrollment.Config
	root := &cobra.Command{Use: "synod-agent", Short: "Enroll and run a protected local agent", SilenceErrors: true, SilenceUsage: true, CompletionOptions: cobra.CompletionOptions{DisableDefaultCmd: true}}
	root.SetArgs(args)
	root.SetOut(stdout)
	root.SetErr(stderr)
	group := &cobra.Command{Use: "enroll", Short: "Create or recover agent enrollment"}
	flags := group.PersistentFlags()
	flags.StringVar(&installationID, "installation-id", "", "Assigned installation UUID (required)")
	flags.StringVar(&nodeID, "node-id", "", "Assigned node UUID (required)")
	flags.StringVar(&statePath, "state-dir", "", "Protected enrollment state directory (required)")
	flags.StringVar(&config.Endpoint, "api-url", "", "Verified HTTPS API origin for pending enrollment")
	flags.StringVar(&config.TokenFile, "token-file", "", "Private enrollment token file for pending enrollment")
	flags.StringVar(&caPath, "ca-file", "", "API CA PEM file; omit to use system trust")
	flags.DurationVar(&config.RequestTimeout, "request-timeout", 10*time.Second, "Timeout per HTTPS attempt (at most 1m)")
	flags.IntVar(&config.Attempts, "attempts", 3, "Maximum HTTPS attempts (1 to 5)")
	for _, mode := range []string{"bootstrap", "retry"} {
		command := &cobra.Command{Use: mode, Short: map[string]string{"bootstrap": "Create first enrollment state in an absent directory", "retry": "Retry saved enrollment or read its completed identity"}[mode], Args: cobra.NoArgs,
			RunE: func(cmd *cobra.Command, _ []string) error {
				installation, err := uuid.Parse(installationID)
				if err != nil {
					return fmt.Errorf("%w: parse --installation-id: %w", enrollment.ErrConfiguration, err)
				}
				if installation == uuid.Nil {
					return fmt.Errorf("%w: --installation-id must be a nonzero UUID", enrollment.ErrConfiguration)
				}
				node, err := uuid.Parse(nodeID)
				if err != nil {
					return fmt.Errorf("%w: parse --node-id: %w", enrollment.ErrConfiguration, err)
				}
				if node == uuid.Nil || statePath == "" {
					return fmt.Errorf("%w: --node-id must be a nonzero UUID and --state-dir must be set", enrollment.ErrConfiguration)
				}
				if config.RequestTimeout <= 0 || config.RequestTimeout > time.Minute || config.Attempts < 1 || config.Attempts > 5 {
					return fmt.Errorf("%w: use a request timeout of up to a minute and 1 to 5 attempts", enrollment.ErrConfiguration)
				}
				if mode == "bootstrap" {
					if _, err := os.Lstat(statePath); !errors.Is(err, os.ErrNotExist) {
						return enrollment.ErrState
					}
				}
				state, err := enrollment.Open(statePath, enrollment.Options{Identity: enrollment.Identity{InstallationID: installation, NodeID: node}, AllowCreate: mode == "bootstrap"})
				if err != nil {
					return err
				}
				defer state.Close()
				credentials, err := state.Credentials()
				if err != nil {
					if caPath != "" {
						config.RootCAs, err = readTrust(caPath)
						if err != nil {
							return err
						}
					}
					credentials, err = state.Submit(cmd.Context(), config)
					if err != nil {
						return err
					}
				}
				if err := state.Close(); err != nil {
					return err
				}
				return json.NewEncoder(stdout).Encode(Identity{InstallationID: credentials.Identity.InstallationID, NodeID: credentials.Identity.NodeID, RequestID: credentials.RequestID, CertificateID: credentials.CertificateID, AcceptedAt: credentials.AcceptedAt, ExpiresAt: credentials.Certificate.Leaf.NotAfter})
			}}
		group.AddCommand(command)
	}
	root.AddCommand(group)
	var configPath, testBootID string
	run := &cobra.Command{Use: "run", Short: "Run an enrolled agent with existing journal state", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, _ []string) error {
		config, err := daemon.Load(configPath)
		if err != nil {
			return err
		}
		if err := daemon.Run(cmd.Context(), config, testBootID, stderr); err != nil {
			return err
		}
		return json.NewEncoder(stderr).Encode(struct {
			Event string `json:"event"`
		}{Event: "agent_stopped"})
	}}
	run.Flags().StringVar(&configPath, "config", "", "Private agent JSON configuration file (required)")
	run.Flags().StringVar(&testBootID, "test-boot-id", "", "Explicit macOS fixture boot UUID; forbidden on Linux")
	journalCommand := &cobra.Command{Use: "journal", Short: "Initialize the first journal after empty native inventory"}
	initialize := &cobra.Command{Use: "init", Short: "Create a first journal in an absent directory", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, _ []string) error {
		config, err := daemon.Load(configPath)
		if err != nil {
			return err
		}
		identity, err := daemon.Initialize(cmd.Context(), config, stderr)
		if err != nil {
			return err
		}
		return json.NewEncoder(stdout).Encode(identity)
	}}
	initialize.Flags().StringVar(&configPath, "config", "", "Private agent JSON configuration file (required)")
	journalCommand.AddCommand(initialize)
	root.AddCommand(run, journalCommand)
	err := root.ExecuteContext(ctx)
	if err == nil {
		return 0
	}
	failure := Failure{Code: "invalid_configuration", Message: "Check command arguments and protected enrollment inputs. Use command help."}
	for _, known := range []struct {
		err  error
		code string
	}{
		{enrollment.ErrState, "state_recovery_required"}, {enrollment.ErrLocked, "state_locked"},
		{enrollment.ErrCertificate, "invalid_certificate"}, {enrollment.ErrUnauthorized, "unauthorized"},
		{enrollment.ErrConflict, "enrollment_conflict"}, {enrollment.ErrUnavailable, "outcome_unavailable"},
		{enrollment.ErrTransport, "transport_failed"},
		{daemon.ErrConfiguration, "invalid_configuration"}, {daemon.ErrEnrollment, "enrollment_recovery_required"},
		{daemon.ErrJournal, "journal_recovery_required"}, {daemon.ErrInventory, "inventory_recovery_required"},
		{daemon.ErrPlatform, "unsupported_platform"}, {daemon.ErrExecution, "agent_failed"},
		{context.Canceled, "cancelled"}, {context.DeadlineExceeded, "deadline_exceeded"},
	} {
		if errors.Is(err, known.err) {
			failure = Failure{Code: known.code, Message: known.err.Error()}
			break
		}
	}
	// Cobra parse errors can contain raw argument values. Emit only fixed text.
	_ = json.NewEncoder(stderr).Encode(failure)
	return 1
}

func readTrust(path string) (*x509.CertPool, error) {
	const limit = 1 << 20
	file, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NONBLOCK|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return nil, fmt.Errorf("%w: open the trust file: %w", enrollment.ErrConfiguration, err)
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("%w: stat the trust file: %w", enrollment.ErrConfiguration, err)
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0o022 != 0 || info.Size() > limit {
		return nil, fmt.Errorf("%w: the trust file must be a regular file of at most 1 MiB that nobody else can write", enrollment.ErrConfiguration)
	}
	data, err := io.ReadAll(io.LimitReader(file, limit+1))
	if err != nil {
		return nil, fmt.Errorf("%w: read the trust file: %w", enrollment.ErrConfiguration, err)
	}
	if len(data) > limit {
		return nil, fmt.Errorf("%w: the trust file exceeds 1 MiB", enrollment.ErrConfiguration)
	}
	roots := x509.NewCertPool()
	count := 0
	for len(bytes.TrimSpace(data)) > 0 {
		if !bytes.HasPrefix(bytes.TrimSpace(data), []byte("-----BEGIN CERTIFICATE-----")) {
			return nil, fmt.Errorf("%w: the trust file carries content that is not a certificate block after %d certificates", enrollment.ErrConfiguration, count)
		}
		block, rest := pem.Decode(data)
		if block == nil || block.Type != "CERTIFICATE" || len(block.Headers) != 0 {
			return nil, fmt.Errorf("%w: trust file entry %d is not a bare CERTIFICATE block", enrollment.ErrConfiguration, count+1)
		}
		cert, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			return nil, fmt.Errorf("%w: parse trust file entry %d: %w", enrollment.ErrConfiguration, count+1, err)
		}
		roots.AddCert(cert)
		count++
		data = rest
	}
	if count == 0 {
		return nil, enrollment.ErrConfiguration
	}
	return roots, nil
}
