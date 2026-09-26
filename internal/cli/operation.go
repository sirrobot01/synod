package cli

import (
	"github.com/google/uuid"
	"github.com/spf13/cobra"
)

func newOperationCommand(app *app) *cobra.Command {
	group := &cobra.Command{Use: "operation", Short: "Inspect or wait for durable work", PersistentPreRunE: app.connect}
	get := &cobra.Command{Use: "get ID", Aliases: []string{"inspect"}, Short: "Read an operation", Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			id, err := parseID(args[0], "Use a nonzero operation UUID.")
			if err != nil {
				return err
			}
			value, err := app.api.GetOperation(cmd.Context(), id)
			if value.ID != uuid.Nil {
				app.result.Operation = &value
			}
			return err
		}}
	wait := &cobra.Command{Use: "wait ID", Short: "Wait for terminal work; timeout never cancels the operation", Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			id, err := parseID(args[0], "Use a nonzero operation UUID.")
			if err != nil {
				return err
			}
			return app.wait(cmd.Context(), id)
		}}
	group.AddCommand(get, wait)
	return group
}
