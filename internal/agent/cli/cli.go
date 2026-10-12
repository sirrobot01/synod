// Package cli exposes the agent commands: join and run.
package cli

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"runtime"

	"github.com/sirrobot01/synod/internal/agent/daemon"
	"github.com/sirrobot01/synod/internal/agent/enrollment"
	"github.com/spf13/cobra"
)

var errUnsupportedPlatform = errors.New("Synod agents require Linux. Run agent commands on a Linux Docker device. This device can run the controller with synod server init.")

// Failure is the bounded error that the CLI prints for a failed command.
type Failure struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	// Detail names the cause of a runtime refusal. Those errors carry paths, never
	// arguments or secrets. Other failures leave it empty.
	Detail string `json:"detail,omitempty"`
}

// Command returns the agent command tree. Report writes its failures.
func Command() *cobra.Command {
	root := &cobra.Command{Use: "agent", Short: "Join a Linux Docker device and run its agent", SilenceErrors: true, SilenceUsage: true,
		PersistentPreRunE: func(*cobra.Command, []string) error {
			if runtime.GOOS != "linux" {
				return errUnsupportedPlatform
			}
			return nil
		},
	}
	var configPath, joinToken, destinationAddress string
	run := &cobra.Command{Use: "run", Short: "Run the agent of a joined device", Long: `Run starts the agent of a joined device.
With --join-token, run first joins a device that has no configuration yet.
The agent container image uses this, so one Compose file joins and runs the agent.`, Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, _ []string) error {
		if err := joinIfNeeded(cmd.Context(), configPath, cmp.Or(joinToken, os.Getenv("SYNOD_JOIN_TOKEN")), cmp.Or(destinationAddress, os.Getenv("SYNOD_DESTINATION_ADDRESS")), cmd.ErrOrStderr()); err != nil {
			return err
		}
		config, err := daemon.Load(configPath)
		if err != nil {
			return err
		}
		if err := daemon.Run(cmd.Context(), config, cmd.ErrOrStderr()); err != nil {
			return err
		}
		return json.NewEncoder(cmd.ErrOrStderr()).Encode(struct {
			Event string `json:"event"`
		}{Event: "agent_stopped"})
	}}
	run.Flags().StringVar(&configPath, "config", "", "Private agent JSON configuration file (required)")
	run.Flags().StringVar(&joinToken, "join-token", "", "Join token to use when the device has not joined (default: $SYNOD_JOIN_TOKEN)")
	run.Flags().StringVar(&destinationAddress, "destination-address", "", "Reachable host:port for other agents when run joins the device (default: $SYNOD_DESTINATION_ADDRESS)")
	root.AddCommand(joinCommand(), renewCommand(), run)
	return root
}

// joinIfNeeded joins the device when configPath does not exist and a token is given.
// The token is used once. Later starts find the configuration and ignore it.
func joinIfNeeded(ctx context.Context, configPath, token, destinationAddress string, output io.Writer) error {
	if _, err := os.Lstat(configPath); token == "" || configPath == "" || !errors.Is(err, os.ErrNotExist) {
		return nil
	}
	options := joinOptions{dir: filepath.Dir(configPath), dockerSocket: "/var/run/docker.sock", destinationListen: ":8445", destinationAddress: destinationAddress}
	return join(ctx, token, options, output)
}

// Report writes the failure for err to stderr and returns the exit status.
func Report(err error, stderr io.Writer) int {
	if err == nil {
		return 0
	}
	// Cobra parse errors can contain raw argument values. Emit only fixed text.
	_ = json.NewEncoder(stderr).Encode(failureOf(err))
	return 1
}

// failureOf maps an error to its stable code and fixed message. Only a runtime
// refusal adds its cause as detail.
func failureOf(err error) Failure {
	failure := Failure{Code: "invalid_configuration", Message: "Check command arguments and protected enrollment inputs. Use command help."}
	for _, known := range []struct {
		err  error
		code string
	}{
		{errUnsupportedPlatform, "unsupported_platform"},
		{enrollment.ErrState, "state_recovery_required"}, {enrollment.ErrLocked, "state_locked"},
		{enrollment.ErrCertificate, "invalid_certificate"}, {enrollment.ErrUnauthorized, "unauthorized"},
		{enrollment.ErrConflict, "enrollment_conflict"}, {enrollment.ErrUnavailable, "outcome_unavailable"},
		{enrollment.ErrTransport, "transport_failed"},
		{daemon.ErrConfiguration, "invalid_configuration"}, {daemon.ErrEnrollment, "enrollment_recovery_required"},
		{daemon.ErrRuntime, "runtime_unavailable"}, {daemon.ErrExecution, "agent_failed"},
		{context.Canceled, "cancelled"}, {context.DeadlineExceeded, "deadline_exceeded"},
	} {
		if errors.Is(err, known.err) {
			failure = Failure{Code: known.code, Message: known.err.Error()}
			if known.err == daemon.ErrRuntime {
				failure.Detail = err.Error()
			}
			break
		}
	}
	return failure
}
