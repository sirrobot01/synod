package cli

import (
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/backup"
	"github.com/sirrobot01/synod/internal/client"
	"github.com/sirrobot01/synod/internal/destination"
	"github.com/sirrobot01/synod/internal/inventory"
	"github.com/sirrobot01/synod/internal/operation"
	"github.com/spf13/cobra"
)

func newBackupCommand(app *app) *cobra.Command {
	var device, selected, method string
	var wait bool
	command := &cobra.Command{Use: "backup APP", Short: "Back up an app and keep its result after disconnect", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		app.ran = true
		if method != "stop" && method != "live" {
			return &client.Error{Code: "invalid_request", Message: "Use --method stop or live."}
		}
		if err := app.connect(cmd, nil); err != nil {
			return err
		}
		var nodeID uuid.UUID
		if device != "" {
			node, err := app.api.ResolveNode(cmd.Context(), device)
			if err != nil {
				return err
			}
			nodeID = node.ID
		}
		catalog, err := app.api.ListApps(cmd.Context())
		if err != nil {
			return err
		}
		var matches []inventory.App
		for _, candidate := range catalog.Apps {
			if candidate.Present && (nodeID == uuid.Nil || candidate.NodeID == nodeID) && (candidate.ID.String() == args[0] || candidate.Definition.Name == args[0]) {
				matches = append(matches, candidate)
			}
		}
		if len(matches) == 0 {
			return &client.Error{Code: "not_found", Message: "The app was not found. Run synod app list and use its name or ID."}
		}
		if len(matches) != 1 {
			return &client.Error{Code: "ambiguous_name", Message: "More than one device has this app. Use its app ID or --device."}
		}
		source := matches[0]
		listing, err := app.api.ListDestinations(cmd.Context())
		if err != nil {
			return err
		}
		var choices []destination.Destination
		for _, candidate := range listing.Destinations {
			if candidate.Location.Kind == "local" && candidate.NodeID != source.NodeID {
				continue
			}
			if selected == "" || candidate.ID.String() == selected || candidate.Name == selected {
				choices = append(choices, candidate)
			}
		}
		if len(choices) == 0 {
			return &client.Error{Code: "not_found", Message: "No matching destination is available to this app. Add a destination or select one with --destination."}
		}
		if len(choices) != 1 {
			return &client.Error{Code: "destination_required", Message: "Choose this app's destination with --destination NAME or ID."}
		}
		if destination.OnlyOnSource(source.NodeID, choices) {
			app.result.Warnings = append(app.result.Warnings, "This copy is on the app's own device. It does not protect against device loss.")
		}
		input := backup.Submission{ID: uuid.New(), AppID: source.ID, DestinationID: choices[0].ID, Method: backup.Method(method)}
		app.result.Message = "Backup request: " + input.ID.String()
		saved, err := app.api.CreateBackup(cmd.Context(), input)
		if err != nil {
			return err
		}
		app.result.Operation = &saved
		if !wait {
			return nil
		}
		fmt.Fprintf(app.stderr, "Backup operation %s. Interruption does not cancel it.\n", saved.ID)
		for !saved.Status.Terminal() {
			select {
			case <-cmd.Context().Done():
				return cmd.Context().Err()
			case <-time.After(time.Second):
			}
			saved, err = app.api.GetOperation(cmd.Context(), saved.ID)
			if err != nil {
				return err
			}
			app.result.Operation = &saved
		}
		if saved.Status != operation.Succeeded {
			return &client.Error{Code: "backup_failed", Message: "The backup did not succeed. Inspect the operation's failure code and restart status."}
		}
		return nil
	}}
	command.Flags().StringVar(&device, "device", "", "Select the source device when app names repeat")
	command.Flags().StringVar(&selected, "destination", "", "Destination name or ID; optional when only one is available")
	command.Flags().StringVar(&method, "method", "stop", "Copy method: stop or live")
	command.Flags().BoolVar(&wait, "wait", true, "Wait for the saved result; interruption does not cancel the backup")
	return command
}
