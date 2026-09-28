package cli

import (
	"fmt"

	"github.com/google/uuid"
	containerapp "github.com/sirrobot01/synod/internal/app"
	"github.com/sirrobot01/synod/internal/client"
	"github.com/spf13/cobra"
)

func newAppInspectCommand(a *app) *cobra.Command {
	return &cobra.Command{Use: "inspect ID", Short: "Show app copies, health, and endpoints", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		id, err := parseID(args[0], "Use a nonzero app UUID.")
		if err != nil {
			return err
		}
		item, err := a.api.GetApp(cmd.Context(), id)
		if err == nil {
			a.result.App = &item
		}
		return err
	}}
}

func newAppActionCommand(a *app, action string) *cobra.Command {
	var expected int64
	var key, to string
	var wait, yes bool
	cmd := &cobra.Command{Use: action + " ID", Short: action + " an app", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		var id uuid.UUID
		var err error
		if action == "switchover" {
			var item containerapp.App
			if parsed, e := uuid.Parse(args[0]); e == nil && parsed != uuid.Nil {
				item, err = a.api.GetApp(cmd.Context(), parsed)
			} else {
				item, err = a.api.GetAppByName(cmd.Context(), args[0])
			}
			if err != nil {
				return err
			}
			if item.Tier != containerapp.TierDatabase {
				return &client.Error{Code: "invalid_request", Message: "Switchover requires a database app."}
			}
			id = item.ID
			a.result.App = &item
		} else {
			id, err = parseID(args[0], "Use a nonzero app UUID.")
			if err != nil {
				return err
			}
		}
		if (cmd.Flags().Changed("expected-revision") && expected < 1) || (key != "" && expected < 1) {
			return &client.Error{Code: "invalid_request", Message: "Use the original positive --expected-revision with a retry key."}
		}
		if expected == 0 || (action == "delete" && !yes) {
			item := containerapp.App{}
			if action == "switchover" {
				item = *a.result.App
			} else {
				item, err = a.api.GetApp(cmd.Context(), id)
			}
			if err != nil {
				return err
			}
			a.result.App = &item
			if expected == 0 {
				expected = item.DesiredRevision
			}
		}
		key, err = idempotencyKey(key)
		if err != nil {
			return err
		}
		a.result.IdempotencyKey = key
		a.result.ExpectedRevision = expected
		if _, err := fmt.Fprintf(a.stderr, "Action: %s\nApp: %s\nExpected revision: %d\nIdempotency key: %s\n", action, id, expected, key); err != nil {
			return &client.Error{Code: "local_error", Message: "Display the retry details failed. No action request was sent."}
		}
		if action == "delete" {
			if _, err := fmt.Fprintln(a.stderr, "Delete stops the app and removes its containers. External bind paths stay on their hosts."); err != nil {
				return &client.Error{Code: "local_error", Message: "Display the delete details failed. No action request was sent."}
			}
			if !yes {
				return &client.Error{Code: "confirmation_required", Message: fmt.Sprintf("Confirm with --yes --expected-revision %d --idempotency-key %s.", expected, key)}
			}
		}
		var result containerapp.Admission
		if action == "move" || action == "switchover" {
			target, parseErr := uuid.Parse(to)
			if action == "switchover" && parseErr != nil {
				target, parseErr = a.api.ResolveNodeName(cmd.Context(), to)
				if parseErr != nil {
					return parseErr
				}
			}
			if parseErr != nil || target == uuid.Nil {
				return &client.Error{Code: "invalid_request", Message: "Use a nonzero device UUID with --to."}
			}
			result, err = a.api.MoveApp(cmd.Context(), id, target, expected, key)
		} else {
			result, err = a.api.ActApp(cmd.Context(), id, action, expected, key)
		}
		if err != nil {
			return err
		}
		a.result.AppAdmission = &result
		if wait {
			return a.wait(cmd.Context(), result.OperationID)
		}
		return nil
	}}
	if action == "move" || action == "switchover" {
		cmd.Flags().StringVar(&to, "to", "", "Target device UUID")
		if action == "switchover" {
			cmd.Use = "switchover NAME"
			cmd.Short = "Move the database primary after catch-up"
			cmd.Flags().Lookup("to").Usage = "Target device name or UUID"
		}
		_ = cmd.MarkFlagRequired("to")
	}
	cmd.Flags().Int64Var(&expected, "expected-revision", 0, "Revision to replace (default: read current app)")
	cmd.Flags().StringVar(&key, "idempotency-key", "", "Original retry key; requires --expected-revision")
	cmd.Flags().BoolVar(&wait, "wait", false, "Wait for the operation to finish")
	if action == "delete" {
		cmd.Flags().BoolVar(&yes, "yes", false, "Confirm container removal")
	}
	return cmd
}
