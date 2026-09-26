package cli

import (
	"time"

	"github.com/sirrobot01/synod/internal/client"
	"github.com/spf13/cobra"
)

func newEnrollmentTokenCommand(app *app) *cobra.Command {
	var options client.EnrollmentTokenOptions
	var node string
	var ttl time.Duration
	group := &cobra.Command{Use: "enrollment-token", Short: "Register a token for one agent node"}
	command := &cobra.Command{Use: "register", Short: "Save and register a token; retry with the same request directory", Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			app.ran = true
			if cmd.Flags().Changed("node-id") {
				value, err := parseID(node, "Use a nonzero node UUID.")
				if err != nil {
					return err
				}
				options.NodeID = value
			}
			if cmd.Flags().Changed("ttl") {
				if ttl < time.Minute || ttl > time.Hour || ttl%time.Second != 0 {
					return &client.Error{Code: "invalid_request", Message: "Use a TTL of 60 to 3600 whole seconds."}
				}
				options.TTLSeconds = int(ttl / time.Second)
			}
			api, err := client.New(app.settings)
			if err != nil {
				return err
			}
			defer api.Close()
			value, err := api.RegisterEnrollmentToken(cmd.Context(), options)
			if err == nil {
				app.result.EnrollmentToken = &value
			}
			return err
		}}
	command.Flags().StringVar(&options.RequestDir, "request-dir", "", "Dedicated private request directory (required)")
	command.Flags().BoolVar(&options.Initialize, "initialize", false, "Explicitly create a new request in an absent or empty directory")
	command.Flags().StringVar(&node, "node-id", "", "Assigned node UUID (required for a new request)")
	command.Flags().DurationVar(&ttl, "ttl", 10*time.Minute, "Token lifetime; saved value on retry (60 to 3600 whole seconds)")
	group.AddCommand(command)
	return group
}
