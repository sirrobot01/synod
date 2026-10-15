package cli

import (
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/sirrobot01/synod/internal/client"
	"github.com/sirrobot01/synod/internal/operation"
	"github.com/spf13/cobra"
)

func newOperationCommand(app *app) *cobra.Command {
	group := &cobra.Command{Use: "operation", Short: "Inspect durable work", PersistentPreRunE: app.connect}
	var limit int
	var before string
	list := &cobra.Command{Use: "list", Short: "List recent operations, newest first", Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			older := uuid.Nil
			if before != "" {
				id, err := parseID(before, "Use an operation UUID for --before.")
				if err != nil {
					return err
				}
				older = id
			}
			ops, err := app.api.ListOperations(cmd.Context(), limit, older)
			app.result.Operations = ops
			return err
		}}
	list.Flags().IntVar(&limit, "limit", 100, "Operations to list, from 1 to 200")
	list.Flags().StringVar(&before, "before", "", "List only operations older than this operation ID")
	remove := &cobra.Command{Use: "remove ID", Short: "Remove a finished operation from activity; status history keeps it", Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			id, err := parseID(args[0], "Use a nonzero operation UUID.")
			if err != nil {
				return err
			}
			if err := app.api.RemoveOperation(cmd.Context(), id); err != nil {
				return err
			}
			app.result.Message = "Removed operation " + id.String() + " from activity."
			return nil
		}}
	clear := &cobra.Command{Use: "clear", Short: "Remove every finished operation from activity; status history keeps them", Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			removed, err := app.api.ClearOperations(cmd.Context())
			if err != nil {
				return err
			}
			app.result.Message = fmt.Sprintf("Removed %d finished operations from activity.", removed)
			return nil
		}}
	group.AddCommand(list, remove, clear)
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
					case <-time.After(250 * time.Millisecond):
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
