package cli

import (
	"github.com/sirrobot01/synod/internal/client"
	"github.com/spf13/cobra"
)

func newInstallationCommand(api **client.Client, result *Result, prepare func(cmd *cobra.Command, args []string) error) *cobra.Command {
	group := &cobra.Command{Use: "installation", Short: "Inspect the installation", PersistentPreRunE: prepare}
	get := &cobra.Command{Use: "get", Short: "Read the installation ID and recovery generation for an agent configuration", Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			value, err := (*api).GetInstallation(cmd.Context())
			if err == nil {
				result.Installation = &value
			}
			return err
		}}
	group.AddCommand(get)
	return group
}
