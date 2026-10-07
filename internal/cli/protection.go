package cli

import (
	"fmt"
	"slices"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/backup"
	"github.com/sirrobot01/synod/internal/client"
	"github.com/sirrobot01/synod/internal/destination"
	"github.com/spf13/cobra"
)

func newProtectCommand(app *app) *cobra.Command {
	var all, off bool
	var device, method, at, zone string
	var selected []string
	var confirm int64
	var daily, weekly, monthly int
	command := &cobra.Command{Use: "protect [APP]", Short: "Review a protection plan, then confirm its revision", Args: cobra.MaximumNArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		app.ran = true
		if (all && len(args) != 0) || (!all && len(args) != 1) || (all && device != "") || confirm < 0 {
			return &client.Error{Code: "invalid_request", Message: "Select APP or --all. Use --device only with APP."}
		}
		if err := app.connect(cmd, nil); err != nil {
			return err
		}
		plan, err := app.api.ProtectionPlan(cmd.Context())
		if err != nil {
			return err
		}
		if confirm != 0 && confirm != plan.Revision {
			return &client.Error{Code: "protect_plan_changed", Message: "The plan changed. Run this command without --confirm to review it again."}
		}
		var nodeID uuid.UUID
		if device != "" {
			node, err := app.api.ResolveNode(cmd.Context(), device)
			if err != nil {
				return err
			}
			nodeID = node.ID
		}
		var choices []backup.AppPlan
		for _, entry := range plan.Apps {
			if all {
				if off || (entry.App.Present && entry.BlockedCode != "device_not_approved") {
					choices = append(choices, entry)
				}
				continue
			}
			if (nodeID == uuid.Nil || entry.App.NodeID == nodeID) && (entry.App.ID.String() == args[0] || (entry.App.Definition != nil && entry.App.Definition.Name == args[0])) {
				choices = append(choices, entry)
			}
		}
		if !all && len(choices) == 0 {
			return &client.Error{Code: "not_found", Message: "The app was not found. Run synod app list and use its name or ID."}
		}
		if !all && len(choices) > 1 {
			return &client.Error{Code: "ambiguous_name", Message: "More than one device has this app. Use its app ID or --device."}
		}
		settings := plan.Defaults
		if !all {
			settings = choices[0].Settings
		}
		if settings.Method == "" {
			settings = backup.DefaultSettings()
		}
		if cmd.Flags().Changed("method") {
			settings.Method = backup.Method(method)
		}
		if cmd.Flags().Changed("at") {
			settings.At = at
		}
		if cmd.Flags().Changed("time-zone") {
			settings.TimeZone = zone
		}
		if cmd.Flags().Changed("keep-daily") {
			settings.Retention.Daily = daily
		}
		if cmd.Flags().Changed("keep-weekly") {
			settings.Retention.Weekly = weekly
		}
		if cmd.Flags().Changed("keep-monthly") {
			settings.Retention.Monthly = monthly
		}
		var targets []destination.Destination
		if !off {
			listing, err := app.api.ListDestinations(cmd.Context())
			if err != nil {
				return err
			}
			available := listing.Destinations
			if !all {
				available = slices.DeleteFunc(slices.Clone(available), func(d destination.Destination) bool {
					return d.Location.Kind == "local" && d.NodeID != choices[0].App.NodeID
				})
			}
			if len(selected) > 0 {
				settings.Destinations = nil
				for _, name := range selected {
					var matches []destination.Destination
					for _, d := range available {
						if d.ID.String() == name || d.Name == name {
							matches = append(matches, d)
						}
					}
					if len(matches) != 1 {
						return &client.Error{Code: "destination_required", Message: "Each --destination must name one available destination. Use its ID when names repeat."}
					}
					settings.Destinations = append(settings.Destinations, matches[0].ID)
				}
			} else if len(settings.Destinations) == 0 {
				if len(available) != 1 {
					return &client.Error{Code: "destination_required", Message: "Select one or more destinations with --destination NAME or ID."}
				}
				settings.Destinations = []uuid.UUID{available[0].ID}
			}
			if settings.Validate() != nil {
				return &client.Error{Code: "invalid_request", Message: "Use distinct destinations, method auto/dump/stop/live, HH:MM, an IANA time zone, and valid retention counts."}
			}
			for _, id := range settings.Destinations {
				index := slices.IndexFunc(available, func(d destination.Destination) bool { return d.ID == id })
				if index < 0 {
					return &client.Error{Code: "destination_required", Message: "A saved destination is unavailable. Select another destination."}
				}
				d := available[index]
				for _, entry := range choices {
					if d.Location.Kind == "local" && d.NodeID != entry.App.NodeID {
						return &client.Error{Code: "invalid_request", Message: "A local destination can protect only apps on its own device. Use a device, SFTP, or S3 destination for all devices."}
					}
				}
				targets = append(targets, d)
			}
		}
		update := backup.ProtectionUpdate{ID: uuid.New(), ExpectedRevision: plan.Revision, All: all, Enabled: !off, Settings: settings}
		if !all {
			update.AppIDs = []uuid.UUID{choices[0].App.ID}
		}
		if confirm != 0 {
			result, err := app.api.UpdateProtection(cmd.Context(), update)
			if err != nil {
				return err
			}
			app.result.Message = fmt.Sprintf("Protection saved at revision %d. Agents apply it after delivery. Admitted backups still finish.", result.Revision)
			current, err := app.api.ProtectionPlan(cmd.Context())
			if err == nil {
				app.result.Protection = &current
			}
			return err
		}
		for i := range choices {
			choices[i].Enabled = !off
			if !off {
				choices[i].Settings = settings
				choices[i].OnlyOnSource = destination.OnlyOnSource(choices[i].App.NodeID, targets)
			}
		}
		plan.Apps = choices
		app.result.Protection = &plan
		app.result.ProtectionUpdate = &update
		app.result.Message = fmt.Sprintf("Preview only. Repeat this command with --confirm %d to save this plan.", plan.Revision)
		return nil
	}}
	command.Flags().BoolVar(&all, "all", false, "Select all apps and set the default for new apps")
	command.Flags().BoolVar(&off, "off", false, "Disable future backups; keep snapshots and admitted work")
	command.Flags().StringVar(&device, "device", "", "Select the source device when app names repeat")
	command.Flags().StringSliceVar(&selected, "destination", nil, "Destination names or IDs; repeat for more than one")
	command.Flags().StringVar(&method, "method", "auto", "Copy method: auto, dump, stop, or live; retain the saved choice by default")
	command.Flags().StringVar(&at, "at", "03:00", "Daily time in HH:MM; retain the saved choice by default")
	command.Flags().StringVar(&zone, "time-zone", "UTC", "IANA time zone, such as Africa/Lagos")
	command.Flags().IntVar(&daily, "keep-daily", 7, "Daily snapshots to keep, from 0 to 366")
	command.Flags().IntVar(&weekly, "keep-weekly", 4, "Weekly snapshots to keep, from 0 to 104")
	command.Flags().IntVar(&monthly, "keep-monthly", 6, "Monthly snapshots to keep, from 0 to 120")
	command.Flags().Int64Var(&confirm, "confirm", 0, "Save the reviewed plan at this revision")
	return command
}
