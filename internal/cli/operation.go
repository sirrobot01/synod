package cli

import (
	"time"

	"github.com/sirrobot01/synod/internal/client"
	"github.com/sirrobot01/synod/internal/operation"
	"github.com/spf13/cobra"
)

func newOperationCommand(app *app) *cobra.Command {
	group := &cobra.Command{Use: "operation", Short: "Inspect durable work", PersistentPreRunE: app.connect}
	group.AddCommand(&cobra.Command{Use: "list", Short: "List up to 100 recent operations", Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			ops, err := app.api.ListOperations(cmd.Context())
			app.result.Operations = ops
			return err
		}})
	for _, name := range []string{"get", "wait"} {
		command := &cobra.Command{Use: name + " ID", Short: "Read an operation", Args: cobra.ExactArgs(1),
			RunE: func(cmd *cobra.Command, args []string) error {
				id, err := parseID(args[0], "Use a nonzero operation UUID.")
				if err != nil {
					return err
				}
				for {
					op, err := app.api.GetOperation(cmd.Context(), id)
					if err != nil {
						return err
					}
					app.result.Operation = &op
					if name == "wait" && (op.Status == operation.Failed || op.Status == operation.Cancelled) {
						return &client.Error{Code: "operation_" + string(op.Status), Message: "The operation did not succeed. Inspect its saved result."}
					}
					if name == "get" || op.Status.Terminal() {
						return nil
					}
					select {
					case <-cmd.Context().Done():
						return cmd.Context().Err()
					case <-time.After(time.Second):
					}
				}
			}}
		if name == "get" {
			command.Aliases = []string{"inspect"}
		} else {
			command.Short = "Wait for completion; interruption does not cancel the work"
		}
		group.AddCommand(command)
	}
	return group
}
