package cli

import (
	"github.com/spf13/cobra"
)

// newNodeCommand lists, approves, and joins the devices of the installation.
func newNodeCommand(app *app) *cobra.Command {
	group := &cobra.Command{Use: "node", Short: "Manage the devices of this installation"}
	list := &cobra.Command{Use: "list", Short: "List enrolled devices", Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if err := app.connect(cmd, nil); err != nil {
				return err
			}
			nodes, err := app.api.ListNodes(cmd.Context())
			if err == nil {
				app.result.Nodes = nodes
			}
			return err
		}}
	approve := &cobra.Command{Use: "approve NODE", Short: "Approve an enrolled device by its ID or name", Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := app.connect(cmd, nil); err != nil {
				return err
			}
			node, err := app.api.ResolveNode(cmd.Context(), args[0])
			if err != nil {
				return err
			}
			node, err = app.api.ApproveNode(cmd.Context(), node.ID)
			if err == nil {
				app.result.Node = &node
			}
			return err
		}}
	group.AddCommand(list, approve, newJoinTokenCommand(app))
	return group
}
