package cli

import (
	"fmt"
	"slices"

	"github.com/sirrobot01/synod/internal/client"
	"github.com/sirrobot01/synod/internal/cluster"
	"github.com/spf13/cobra"
)

func newEndpointCommand(app *app) *cobra.Command {
	group := &cobra.Command{Use: "endpoint", Short: "Inspect database connection metadata", PersistentPreRunE: app.connect}
	group.AddCommand(&cobra.Command{Use: "get CLUSTER_ID", Short: "Read reserved direct addresses and stored lifecycle observations", Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			id, err := parseID(args[0], "Use a nonzero cluster UUID.")
			if err != nil {
				return err
			}
			value, err := app.api.GetEndpoints(cmd.Context(), id)
			if err != nil {
				return err
			}
			// Only an endpoint without verify-full TLS carries plaintext traffic.
			message := "This metadata is not a live health check."
			if slices.ContainsFunc(value.Endpoints, func(endpoint cluster.Endpoint) bool { return endpoint.TLSMode != "verify-full" }) {
				message = "Warning: this standalone profile uses plaintext database traffic. Database TLS is disabled. " + message
			}
			if _, err := fmt.Fprintln(app.stderr, message); err != nil {
				return &client.Error{Code: "local_error", Message: "Display the database traffic warning failed."}
			}
			app.result.EndpointInspection = &value
			return nil
		}})
	return group
}

func newCredentialCommand(app *app) *cobra.Command {
	group := &cobra.Command{Use: "credential", Short: "Inspect metadata or explicitly reveal a database credential", PersistentPreRunE: app.connect}
	group.AddCommand(&cobra.Command{Use: "list CLUSTER_ID", Short: "Read credential purposes, usernames, and versions without passwords", Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			id, err := parseID(args[0], "Use a nonzero cluster UUID.")
			if err != nil {
				return err
			}
			value, err := app.api.ListCredentials(cmd.Context(), id)
			if err == nil {
				app.result.CredentialInspection = &value
			}
			return err
		}})
	var purpose string
	reveal := &cobra.Command{Use: "reveal CLUSTER_ID", Short: "Reveal one database password through audited administrator access", Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			id, err := parseID(args[0], "Use a nonzero cluster UUID.")
			if err != nil {
				return err
			}
			if !cmd.Flags().Changed("purpose") || (purpose != "application" && purpose != "superuser" && purpose != "replication") {
				return &client.Error{Code: "invalid_request", Message: "Set --purpose to application, superuser, or replication."}
			}
			if _, err := fmt.Fprintln(app.stderr, "The JSON result will contain a database password. Protect stdout. Use a private terminal or a file readable only by its owner. Do not send this output to logs or shared files."); err != nil {
				return &client.Error{Code: "local_error", Message: "Display protected-output guidance failed. No credential reveal request was sent."}
			}
			value, err := app.api.RevealCredential(cmd.Context(), id, purpose)
			if err == nil {
				app.result.RevealedCredential = &value
			}
			return err
		}}
	reveal.Flags().StringVar(&purpose, "purpose", "", "Credential purpose: application, superuser, or replication (required)")
	group.AddCommand(reveal)
	return group
}
