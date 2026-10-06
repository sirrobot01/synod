package cli

import (
	"github.com/spf13/cobra"
)

func newInstallationCommand(app *app) *cobra.Command {
	group := &cobra.Command{Use: "installation", Short: "Inspect the installation", PersistentPreRunE: app.connect}
	get := &cobra.Command{Use: "get", Short: "Read the installation ID", Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			value, err := app.api.GetInstallation(cmd.Context())
			if err == nil {
				app.result.Installation = &value
			}
			return err
		}}
	group.AddCommand(get)
	return group
}
