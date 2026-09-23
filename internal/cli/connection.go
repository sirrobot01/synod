package cli

import (
	"fmt"
	"io"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/client"
	"github.com/spf13/cobra"
)

func newEndpointCommand(api **client.Client, result *Result, prepare func(*cobra.Command, []string) error, stderr io.Writer) *cobra.Command {
	group := &cobra.Command{Use: "endpoint", Short: "Inspect database connection metadata", PersistentPreRunE: prepare}
	group.AddCommand(&cobra.Command{Use: "get CLUSTER_ID", Short: "Read reserved direct addresses and stored lifecycle observations", Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			id, err := uuid.Parse(args[0])
			if err != nil || id == uuid.Nil {
				return &client.Error{Code: "invalid_request", Message: "Use a nonzero cluster UUID."}
			}
			value, err := (*api).GetEndpoints(cmd.Context(), id)
			if err != nil {
				return err
			}
			if _, err := fmt.Fprintln(stderr, "Warning: this standalone profile uses plaintext database traffic. Database TLS is disabled. This metadata is not a live health check."); err != nil {
				return &client.Error{Code: "local_error", Message: "Display the database traffic warning failed."}
			}
			result.EndpointInspection = &value
			return nil
		}})
	return group
}

func newCredentialCommand(api **client.Client, result *Result, prepare func(*cobra.Command, []string) error, stderr io.Writer) *cobra.Command {
	group := &cobra.Command{Use: "credential", Short: "Inspect metadata or explicitly reveal a database credential", PersistentPreRunE: prepare}
	group.AddCommand(&cobra.Command{Use: "list CLUSTER_ID", Short: "Read credential purposes, usernames, and versions without passwords", Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			id, err := uuid.Parse(args[0])
			if err != nil || id == uuid.Nil {
				return &client.Error{Code: "invalid_request", Message: "Use a nonzero cluster UUID."}
			}
			value, err := (*api).ListCredentials(cmd.Context(), id)
			if err == nil {
				result.CredentialInspection = &value
			}
			return err
		}})
	var purpose string
	reveal := &cobra.Command{Use: "reveal CLUSTER_ID", Short: "Reveal one database password through audited administrator access", Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			id, err := uuid.Parse(args[0])
			if err != nil || id == uuid.Nil {
				return &client.Error{Code: "invalid_request", Message: "Use a nonzero cluster UUID."}
			}
			if !cmd.Flags().Changed("purpose") || (purpose != "application" && purpose != "superuser" && purpose != "replication") {
				return &client.Error{Code: "invalid_request", Message: "Set --purpose to application, superuser, or replication."}
			}
			if _, err := fmt.Fprintln(stderr, "The JSON result will contain a database password. Protect stdout. Use a private terminal or a file readable only by its owner. Do not send this output to logs or shared files."); err != nil {
				return &client.Error{Code: "local_error", Message: "Display protected-output guidance failed. No credential reveal request was sent."}
			}
			value, err := (*api).RevealCredential(cmd.Context(), id, purpose)
			if err == nil {
				result.RevealedCredential = &value
			}
			return err
		}}
	reveal.Flags().StringVar(&purpose, "purpose", "", "Credential purpose: application, superuser, or replication (required)")
	group.AddCommand(reveal)
	return group
}
