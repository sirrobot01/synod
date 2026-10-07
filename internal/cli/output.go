package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/backup"
	"github.com/sirrobot01/synod/internal/client"
	"github.com/sirrobot01/synod/internal/destination"
	"github.com/sirrobot01/synod/internal/inventory"
	"github.com/sirrobot01/synod/internal/operation"
	"github.com/sirrobot01/synod/internal/proof"
	"github.com/sirrobot01/synod/internal/restore"
	"github.com/spf13/cobra"
)

// Result is the single JSON document that the CLI writes to stdout.
type Result struct {
	Files            *restore.FileHeader      `json:"files,omitzero"`
	Protection       *backup.ProtectionPlan   `json:"protection,omitzero"`
	ProtectionUpdate *backup.ProtectionUpdate `json:"protection_update,omitzero"`
	Destination      *destination.Destination `json:"destination,omitzero"`
	Destinations     *destination.Listing     `json:"destinations,omitzero"`
	Message          string                   `json:"message,omitempty"`
	Warnings         []string                 `json:"warnings,omitempty"`
	Apps             []inventory.App          `json:"apps,omitempty"`
	Discovery        []inventory.Device       `json:"discovery,omitempty"`
	Operation        *operation.Operation     `json:"operation,omitzero"`
	Operations       []operation.Operation    `json:"operations,omitempty"`
	EnrollmentToken  *client.EnrollmentToken  `json:"enrollment_token,omitzero"`
	User             *client.User             `json:"user,omitzero"`
	Token            *client.APIToken         `json:"token,omitzero"`
	Tokens           []client.APIToken        `json:"tokens,omitempty"`
	Node             *client.Node             `json:"node,omitzero"`
	Nodes            []client.Node            `json:"nodes,omitempty"`
	Error            *client.Error            `json:"error,omitzero"`
	Installation     *client.Installation     `json:"installation,omitzero"`
	// JoinToken is the one secret an operator copies to the joining node.
	JoinToken string `json:"join_token,omitempty"`
}

// finish closes the connection and writes the result of a client command to stdout.
// Progress and diagnostics stay on stderr. It returns the exit status.
func (a *app) finish(selected *cobra.Command, err error) int {
	if a.api != nil {
		a.api.Close()
	}
	if err == nil && !a.ran {
		return 0
	}
	code := 0
	if err != nil {
		code = 1
		failure, ok := errors.AsType[*client.Error](err)
		if !ok {
			failure = &client.Error{Code: "invalid_command", Message: "Invalid command or flags. Run synod --help."}
			if errors.Is(err, context.DeadlineExceeded) {
				failure = &client.Error{Code: "request_timeout", Message: "The request timed out. Server work was not cancelled."}
			} else if errors.Is(err, context.Canceled) {
				failure = &client.Error{Code: "interrupted", Message: "The request was interrupted. Server work was not cancelled."}
			}
		}
		a.result.Error = failure
		if failure.Code == "interrupted" {
			code = 130
		}
		fmt.Fprintln(a.stderr, failure.Code+": "+failure.Message)
	}
	if a.json {
		if err := json.NewEncoder(a.stdout).Encode(a.result); err != nil {
			fmt.Fprintln(a.stderr, "Write the JSON result failed: "+err.Error())
			return 1
		}
		return code
	}
	var output strings.Builder
	if files := a.result.Files; files != nil {
		if files.SnapshotID != uuid.Nil {
			fmt.Fprintf(&output, "Snapshot: %s\nDestination: %s\n", files.SnapshotID, files.DestinationID)
		}
		for _, saved := range files.Snapshots {
			fmt.Fprintf(&output, "%s  %s  %s  %d bytes\n", saved.ID, saved.CompletedAt.Format(time.RFC3339), saved.Method, saved.FileBytes)
		}
		for _, entry := range files.Entries {
			fmt.Fprintf(&output, "%s  %d  %q\n", entry.Type, entry.Size, entry.Name)
		}
		for _, warning := range files.Warnings {
			fmt.Fprintf(&output, "Warning: %s\n", warning)
		}
		if files.Truncated {
			fmt.Fprintln(&output, "Warning: The listing is incomplete. Select a more specific path.")
		}
	}
	if a.result.Message != "" {
		fmt.Fprintln(&output, a.result.Message)
	}
	for _, warning := range a.result.Warnings {
		fmt.Fprintln(&output, "Warning: "+warning)
	}
	if d := a.result.Destination; d != nil {
		fmt.Fprintf(&output, "Destination: %s  %s  %s\n", d.ID, d.Name, d.Location.Kind)
	}
	if listing := a.result.Destinations; listing != nil {
		for _, d := range listing.Destinations {
			fmt.Fprintf(&output, "%s  %s  %s\n", d.ID, d.Name, d.Location.Kind)
			if d.Location.Kind == "local" || d.Location.Kind == "device" {
				fmt.Fprintf(&output, "  Device: %s  Path: %s\n  Warning: Copies on this device do not protect its apps from device loss.\n", d.NodeID, d.Location.Path)
			}
		}
		for _, check := range listing.Checks {
			fmt.Fprintf(&output, "Destination %s: integrity check %s  Read: %d of %d contents  Failure: %s\n", check.DestinationID, check.State, check.Result.Read, check.Result.Contents, check.Result.FailureCode)
		}
		for _, d := range listing.Delivery {
			state := "pending"
			if d.Current {
				state = "saved by agent"
			}
			fmt.Fprintf(&output, "Device %s: settings %s (revision %d of %d).\n", d.NodeName, state, d.Revision, listing.Revision)
		}
	}
	for _, device := range a.result.Discovery {
		if device.ReceivedAt == nil {
			fmt.Fprintf(&output, "Device %s: no app scan received.\n", device.NodeName)
		} else if device.FailureCode != "" {
			fmt.Fprintf(&output, "Device %s: %s. The last app list is kept.\n", device.NodeName, device.FailureCode)
		}
	}
	if plan := a.result.Protection; plan != nil {
		fmt.Fprintf(&output, "Protection revision: %d\nAuto-protect new apps: %t\n", plan.Revision, plan.AutoProtect)
		if update := a.result.ProtectionUpdate; update != nil && update.All {
			fmt.Fprintf(&output, "Proposed auto-protect: %t\n", update.Enabled)
		}
		for _, entry := range plan.Apps {
			name := entry.App.ID.String()
			if entry.App.Definition != nil {
				name = entry.App.Definition.Name
			}
			fmt.Fprintf(&output, "%s  %s  %s  Current status: %s\n", entry.App.ID, name, entry.App.NodeName, entry.State)
			if a.result.ProtectionUpdate != nil {
				fmt.Fprintf(&output, "  Proposed enabled: %t\n", entry.Enabled)
			} else {
				fmt.Fprintf(&output, "  Enabled: %t  Settings saved by agent: %t\n", entry.Enabled, entry.SettingsCurrent)
			}
			fmt.Fprintf(&output, "  Method: %s  Daily: %s %s  Keep: %d daily, %d weekly, %d monthly\n", entry.Settings.Method, entry.Settings.At, entry.Settings.TimeZone, entry.Settings.Retention.Daily, entry.Settings.Retention.Weekly, entry.Settings.Retention.Monthly)
			fmt.Fprintf(&output, "  Destinations: %v\n", entry.Settings.Destinations)
			if entry.DataLossWindowSeconds == nil {
				fmt.Fprintln(&output, "  Data-loss window: no good snapshot reported")
			} else {
				fmt.Fprintf(&output, "  Data-loss window: %s\n", (time.Duration(*entry.DataLossWindowSeconds) * time.Second).String())
			}
			if entry.RestoreTestedAt != nil {
				fmt.Fprintf(&output, "  Restore tested on %s\n", entry.RestoreTestedAt.Format(time.RFC3339))
			} else {
				fmt.Fprintln(&output, "  Restore not tested yet")
			}
			if entry.LastDrill != nil {
				fmt.Fprintf(&output, "  Last drill: %s  Failure: %s\n", entry.LastDrill.State, entry.LastDrill.Result.FailureCode)
			}
			if entry.OnlyOnSource {
				fmt.Fprintln(&output, "  Warning: The only copies are on the app's own device. They do not protect against device loss.")
			}
			if entry.BlockedCode != "" {
				fmt.Fprintf(&output, "  Blocked: %s\n", entry.BlockedCode)
			}
			for _, target := range entry.Targets {
				fmt.Fprintf(&output, "  Destination %s: %s  Failure: %s\n", target.DestinationID, target.State, target.FailureCode)
				if target.LastGood != nil {
					fmt.Fprintf(&output, "    Last good: %s  Snapshot: %s\n", target.LastGood.StartedAt.Format(time.RFC3339), target.LastGood.SnapshotID)
					for _, code := range target.LastGood.WarningCodes {
						fmt.Fprintf(&output, "    Warning: %s\n", code)
					}
				}
			}
			if a.result.ProtectionUpdate != nil && entry.App.Definition != nil {
				for _, path := range entry.App.Definition.Paths {
					fmt.Fprintf(&output, "  %-8s %s  %s\n", path.Status, path.Source, path.Reason)
				}
			}
		}
	}
	for _, app := range a.result.Apps {
		presence := "present"
		if !app.Present {
			presence = "not found in the last scan"
		}
		fmt.Fprintf(&output, "%s  %s  %s  %s  scanned %s\n", app.ID, app.Definition.Name, app.NodeName, presence, app.ObservedAt.Format(time.RFC3339))
		for _, path := range app.Definition.Paths {
			size := "unknown size"
			if path.SizeKnown {
				size = fmt.Sprintf("%d bytes", path.SizeBytes)
			} else if path.SizeBytes > 0 {
				size = fmt.Sprintf("at least %d bytes", path.SizeBytes)
			}
			fmt.Fprintf(&output, "  %-8s %s  %s  %s\n", path.Status, path.Source, size, path.Reason)
		}
		for _, issue := range app.Definition.Issues {
			fmt.Fprintf(&output, "  Issue: %s\n", issue)
		}
	}
	if op := a.result.Operation; op != nil {
		fmt.Fprintf(&output, "Operation: %s\nKind: %s\nStatus: %s\nFailure: %s\n", op.ID, op.Kind, op.Status, op.FailureCode)
		if (op.Kind == string(proof.Integrity) || op.Kind == string(proof.Drill)) && len(op.Result) > 0 {
			var result proof.Result
			if json.Unmarshal(op.Result, &result) == nil {
				fmt.Fprintf(&output, "Destination: %s\n", result.DestinationID)
				if op.Kind == string(proof.Integrity) {
					fmt.Fprintf(&output, "Read: %d of %d contents (%d bytes)\n", result.Read, result.Contents, result.BytesRead)
				} else {
					fmt.Fprintf(&output, "Snapshot: %s\nContainers tested: %d\n", result.SnapshotID, result.Containers)
				}
				if result.Resource != "" {
					fmt.Fprintf(&output, "Resource: %s\n", result.Resource)
				}
			}
		}
		if op.Kind == "backup" && len(op.Result) > 0 {
			var result backup.Result
			if json.Unmarshal(op.Result, &result) == nil {
				if result.SnapshotID != uuid.Nil {
					fmt.Fprintf(&output, "Snapshot: %s\nFiles: %d bytes\nDowntime: %d ms\n", result.SnapshotID, result.FileBytes, result.DowntimeMillis)
				}
				if len(result.RestartPending) > 0 {
					fmt.Fprintf(&output, "Restart pending: %s\n", strings.Join(result.RestartPending, ", "))
				}
				for _, code := range result.WarningCodes {
					fmt.Fprintf(&output, "Warning: %s\n", code)
				}
			}
		}
	}
	for _, op := range a.result.Operations {
		fmt.Fprintf(&output, "%s  %-16s %s\n", op.ID, op.Kind, op.Status)
	}
	if a.result.JoinToken != "" && a.result.EnrollmentToken != nil {
		fmt.Fprintf(&output, "Join token for one device, valid until %s:\n  %s\n\nOn that device, run:\n  synod agent join %s\n", a.result.EnrollmentToken.ExpiresAt.Format(time.RFC3339), a.result.JoinToken, a.result.JoinToken)
	}
	if node := a.result.Node; node != nil {
		fmt.Fprintf(&output, "Device: %s\nName: %s\nArchitecture: %s\nApproved: %t\n", node.ID, node.Name, node.Architecture, node.ApprovedAt != nil)
	}
	for _, node := range a.result.Nodes {
		state := "pending approval"
		if node.ApprovedAt != nil {
			state = "approved"
		}
		seen := "never connected"
		if node.LastSeenAt != nil {
			seen = "seen " + node.LastSeenAt.Format(time.RFC3339)
		}
		fmt.Fprintf(&output, "%s  %-20s %-6s %-17s %s\n", node.ID, node.Name, node.Architecture, state, seen)
	}
	if a.result.Installation != nil {
		fmt.Fprintf(&output, "Installation: %s\n", a.result.Installation.InstallationID)
	}
	if a.result.User != nil {
		fmt.Fprintf(&output, "User: %s\nUsername: %s\nRole: %s\n", a.result.User.ID, a.result.User.Username, a.result.User.Role)
	}
	if a.result.Token != nil {
		// The secret appears once. A caller that cannot see it here cannot recover it.
		fmt.Fprintf(&output, "Token: %s\nName: %s\nActor: %s\nSecret: %s\nStore the secret now. No later command repeats it.\n",
			a.result.Token.ID, a.result.Token.Name, a.result.Token.ActorID, a.result.Token.Plaintext)
	}
	for _, token := range a.result.Tokens {
		state := "live"
		if token.RevokedAt != nil {
			state = "revoked"
		}
		fmt.Fprintf(&output, "%s  %-24s %-16s %s\n", token.ID, token.Name, token.ActorID, state)
	}
	if _, err := io.WriteString(a.stdout, output.String()); err != nil {
		fmt.Fprintln(a.stderr, "Write the result failed: "+err.Error())
		return 1
	}
	return code
}
