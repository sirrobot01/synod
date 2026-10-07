package cli

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/client"
	"github.com/sirrobot01/synod/internal/restore"
	"github.com/spf13/cobra"
)

func newSnapshotCommand(app *app) *cobra.Command {
	command := &cobra.Command{Use: "snapshot", Short: "Inspect saved app snapshots"}
	command.AddCommand(newFileAction(app, restore.FileSnapshots))
	return command
}

func newFileCommand(app *app) *cobra.Command {
	command := &cobra.Command{Use: "file", Short: "List and download saved files"}
	command.AddCommand(newFileAction(app, restore.FileList), newFileAction(app, restore.FileRead))
	return command
}

// newFileAction shares snapshot selection across all file commands.
func newFileAction(app *app, action string) *cobra.Command {
	var device, via, selected, snapshot, output string
	command := &cobra.Command{Use: "list APP [PATH]", Short: "List saved paths", Args: cobra.RangeArgs(1, 2)}
	if action == restore.FileSnapshots {
		command.Use, command.Short, command.Args = "list APP", "List available snapshots", cobra.ExactArgs(1)
	}
	if action == restore.FileRead {
		command.Use, command.Short, command.Args = "get APP PATH --output FILE", "Save a file, or a directory as a tar archive", cobra.ExactArgs(2)
	}
	command.RunE = func(cmd *cobra.Command, args []string) error {
		if err := app.connect(cmd, nil); err != nil {
			return err
		}
		input := restore.FileRequest{Action: action}
		var source uuid.UUID
		if device != "" {
			node, err := app.api.ResolveNode(cmd.Context(), device)
			if err != nil {
				return err
			}
			source = node.ID
		}
		if via != "" {
			node, err := app.api.ResolveNode(cmd.Context(), via)
			if err != nil {
				return err
			}
			input.NodeID = node.ID
		}
		catalog, err := app.api.ListApps(cmd.Context())
		if err != nil {
			return err
		}
		for _, candidate := range catalog.Apps {
			if (source == uuid.Nil || candidate.NodeID == source) && (candidate.ID.String() == args[0] || candidate.Definition.Name == args[0]) {
				if input.AppID != uuid.Nil {
					return &client.Error{Code: "ambiguous_name", Message: "Use the app ID or --device when app names repeat."}
				}
				input.AppID = candidate.ID
			}
		}
		if input.AppID == uuid.Nil {
			return &client.Error{Code: "not_found", Message: "The app was not found. Run synod app list."}
		}
		if snapshot != "" {
			input.SnapshotID, err = parseID(snapshot, "Use a snapshot ID with --snapshot.")
			if err != nil {
				return err
			}
		}
		if selected != "" {
			listing, err := app.api.ListDestinations(cmd.Context())
			if err != nil {
				return err
			}
			for _, d := range listing.Destinations {
				if d.ID.String() == selected || d.Name == selected {
					if input.DestinationID != uuid.Nil {
						return &client.Error{Code: "ambiguous_name", Message: "Use the destination ID when names repeat."}
					}
					input.DestinationID = d.ID
				}
			}
			if input.DestinationID == uuid.Nil {
				return &client.Error{Code: "not_found", Message: "The destination was not found. Run synod destination list."}
			}
		}
		if len(args) == 2 {
			input.Path = args[1]
		}
		if action != restore.FileRead {
			header, err := app.api.ListFiles(cmd.Context(), input)
			if err != nil {
				return err
			}
			app.result.Files = &header
			return nil
		}
		if _, err := os.Lstat(output); !os.IsNotExist(err) {
			return &client.Error{Code: "output_exists", Message: "Choose a new output file. Existing files are not replaced."}
		}
		file, err := os.CreateTemp(filepath.Dir(output), ".synod-download-*")
		if err != nil {
			return &client.Error{Code: "output_failed", Message: "Create the private download file failed. Check the output directory."}
		}
		defer os.Remove(file.Name())
		defer file.Close()
		header, err := app.api.DownloadFile(cmd.Context(), input, file)
		if err != nil {
			return err
		}
		if err := file.Sync(); err != nil {
			return &client.Error{Code: "output_failed", Message: "Save the download failed. Check free disk space."}
		}
		if err := file.Close(); err != nil {
			return &client.Error{Code: "output_failed", Message: "Close the download failed. Retry with a new output file."}
		}
		// Link publishes the complete file without replacing a file created during the download.
		if err := os.Link(file.Name(), output); err != nil {
			return &client.Error{Code: "output_failed", Message: "Publish the download failed. Check permissions and choose a new output file."}
		}
		app.result.Files = &header
		app.result.Message = fmt.Sprintf("Saved %d bytes to %s (%s).", header.Size, output, header.Format)
		return nil
	}
	command.Flags().StringVar(&device, "device", "", "Select the source device when app names repeat")
	command.Flags().StringVar(&via, "via", "", "Read through this online device")
	command.Flags().StringVar(&selected, "destination", "", "Read this destination name or ID")
	if action != restore.FileSnapshots {
		command.Flags().StringVar(&snapshot, "snapshot", "", "Snapshot ID; the default uses the latest available snapshot")
	}
	if action == restore.FileRead {
		command.Flags().StringVar(&output, "output", "", "New output file; directories are saved as tar archives")
		command.MarkFlagRequired("output")
	}
	return command
}
