package cli

import "github.com/spf13/cobra"

func newAppsCommand(app *app) *cobra.Command {
	group := &cobra.Command{Use: "app", Short: "Inspect discovered Docker apps"}
	group.AddCommand(&cobra.Command{Use: "list", Short: "List apps, paths, and the last scan state", Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if err := app.connect(cmd, nil); err != nil {
				return err
			}
			catalog, err := app.api.ListApps(cmd.Context())
			if err == nil {
				app.result.Apps, app.result.Discovery = catalog.Apps, catalog.Devices
			}
			return err
		}})
	return group
}
