package cli

import (
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/client"
	"github.com/sirrobot01/synod/internal/inventory"
	"github.com/sirrobot01/synod/internal/operation"
	"github.com/sirrobot01/synod/internal/proof"
	"github.com/sirrobot01/synod/internal/restore"
	"github.com/spf13/cobra"
)

func newRestoreCommand(app *app) *cobra.Command {
	var target, sourceDevice, selected, snapshot string
	var replace, wait, drill bool
	command := &cobra.Command{Use: "restore APP --to DEVICE", Short: "Restore an app from its saved definition and data", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		app.ran = true
		input := restore.Submission{ID: uuid.New(), Replace: replace}
		if snapshot != "" {
			id, err := uuid.Parse(snapshot)
			if err != nil || id == uuid.Nil {
				return &client.Error{Code: "invalid_request", Message: "Use a snapshot ID with --snapshot."}
			}
			input.SnapshotID = id
		}
		if err := app.connect(cmd, nil); err != nil {
			return err
		}
		node, err := app.api.ResolveNode(cmd.Context(), target)
		if err != nil {
			return err
		}
		input.NodeID = node.ID
		var sourceID uuid.UUID
		if sourceDevice != "" {
			source, err := app.api.ResolveNode(cmd.Context(), sourceDevice)
			if err != nil {
				return err
			}
			sourceID = source.ID
		}
		catalog, err := app.api.ListApps(cmd.Context())
		if err != nil {
			return err
		}
		var matches []inventory.App
		for _, candidate := range catalog.Apps {
			if candidate.Definition != nil && (sourceID == uuid.Nil || candidate.NodeID == sourceID) && (candidate.ID.String() == args[0] || candidate.Definition.Name == args[0]) {
				matches = append(matches, candidate)
			}
		}
		if len(matches) == 0 {
			return &client.Error{Code: "not_found", Message: "The app was not found. Run synod app list and use its name or ID."}
		}
		if len(matches) != 1 {
			return &client.Error{Code: "ambiguous_name", Message: "More than one device has this app. Use its app ID or --device."}
		}
		input.AppID = matches[0].ID
		if selected != "" {
			listing, err := app.api.ListDestinations(cmd.Context())
			if err != nil {
				return err
			}
			for _, destination := range listing.Destinations {
				if destination.ID.String() == selected || destination.Name == selected {
					if input.DestinationID != uuid.Nil {
						return &client.Error{Code: "ambiguous_name", Message: "Use the destination ID to select one destination."}
					}
					input.DestinationID = destination.ID
				}
			}
			if input.DestinationID == uuid.Nil {
				return &client.Error{Code: "not_found", Message: "The destination was not found. Run synod destination list."}
			}
		}
		app.result.Message = "Restore request: " + input.ID.String()
		var saved operation.Operation
		if drill {
			saved, err = app.api.CreateProof(cmd.Context(), proof.Submission{ID: input.ID, Kind: proof.Drill, NodeID: input.NodeID, AppID: input.AppID, DestinationID: input.DestinationID, SnapshotID: input.SnapshotID})
			app.result.Message = "Restore drill request: " + input.ID.String()
		} else {
			saved, err = app.api.CreateRestore(cmd.Context(), input)
		}
		if err != nil {
			return err
		}
		app.result.Operation = &saved
		if !wait {
			return nil
		}
		fmt.Fprintf(app.stderr, "Restore operation %s. Interruption does not cancel it.\n", saved.ID)
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
		if saved.Status != operation.Succeeded {
			return &client.Error{Code: "restore_failed", Message: "The restore did not succeed. Inspect the operation's failure code and resource."}
		}
		return nil
	}}
	command.Flags().StringVar(&target, "to", "", "Target device name or ID")
	command.MarkFlagRequired("to")
	command.Flags().StringVar(&sourceDevice, "device", "", "Select the source device when app names repeat")
	command.Flags().StringVar(&selected, "destination", "", "Read one destination; the default checks all available destinations")
	command.Flags().StringVar(&snapshot, "snapshot", "", "Snapshot ID; the default uses the latest available snapshot")
	command.Flags().BoolVar(&drill, "drill", false, "Test an isolated temporary restore, then remove it")
	command.Flags().BoolVar(&replace, "replace", false, "Save a safety snapshot, then replace the matching target app")
	command.Flags().BoolVar(&wait, "wait", true, "Wait for the saved result; interruption does not cancel the restore")
	command.MarkFlagsMutuallyExclusive("replace", "drill")
	return command
}
