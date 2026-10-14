package cli

import (
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/client"
	"github.com/sirrobot01/synod/internal/operation"
	"github.com/sirrobot01/synod/internal/proof"
	"github.com/spf13/cobra"
)

func newCheckCommand(app *app) *cobra.Command {
	var device string
	var wait bool
	command := &cobra.Command{Use: "check DESTINATION --on DEVICE", Short: "Read and verify a sample of encrypted repository content", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		if err := app.connect(cmd, nil); err != nil {
			return err
		}
		node, err := app.api.ResolveNode(cmd.Context(), device)
		if err != nil {
			return err
		}
		listing, err := app.api.ListDestinations(cmd.Context())
		if err != nil {
			return err
		}
		input := proof.Submission{ID: uuid.New(), Kind: proof.Integrity, NodeID: node.ID}
		for _, destination := range listing.Destinations {
			if destination.ID.String() == args[0] || destination.Name == args[0] {
				if input.DestinationID != uuid.Nil {
					return &client.Error{Code: "ambiguous_name", Message: "Use the destination ID to select one destination."}
				}
				input.DestinationID = destination.ID
			}
		}
		if input.DestinationID == uuid.Nil {
			return &client.Error{Code: "not_found", Message: "The destination was not found. Run synod destination list."}
		}
		saved, err := app.api.CreateProof(cmd.Context(), input)
		if err != nil {
			return err
		}
		app.result.Operation = &saved
		app.result.Message = "Integrity check request: " + input.ID.String()
		if !wait {
			return nil
		}
		fmt.Fprintf(app.stderr, "Integrity check %s. Interruption does not cancel it.\n", saved.ID)
		for !saved.Status.Terminal() {
			select {
			case <-cmd.Context().Done():
				return cmd.Context().Err()
			case <-time.After(250 * time.Millisecond):
			}
			saved, err = app.api.GetOperation(cmd.Context(), saved.ID)
			if err != nil {
				return err
			}
			app.result.Operation = &saved
		}
		if saved.Status != operation.Succeeded && saved.FailureCode == "repository_uninitialized" {
			return &client.Error{Code: "repository_uninitialized", Message: "No backup has used this destination yet. The first backup creates its repository."}
		}
		if saved.Status != operation.Succeeded {
			return &client.Error{Code: "integrity_check_failed", Message: "The repository check failed. Inspect the operation's failure code."}
		}
		return nil
	}}
	command.Flags().StringVar(&device, "on", "", "Device that can read the destination")
	command.MarkFlagRequired("on")
	command.Flags().BoolVar(&wait, "wait", true, "Wait for the saved result")
	return command
}
