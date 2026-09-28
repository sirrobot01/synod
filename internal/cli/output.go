package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	appmodel "github.com/sirrobot01/synod/internal/app"
	"github.com/sirrobot01/synod/internal/client"
	"github.com/sirrobot01/synod/internal/operation"
	"github.com/spf13/cobra"
)

// Result is the single JSON document that the CLI writes to stdout.
type Result struct {
	App                *appmodel.App            `json:"app,omitzero"`
	AppAdmission       *appmodel.Admission      `json:"app_admission,omitzero"`
	EnrollmentToken    *client.EnrollmentToken  `json:"enrollment_token,omitzero"`
	Operation          *operation.Operation     `json:"operation,omitzero"`
	User               *client.User             `json:"user,omitzero"`
	Token              *client.APIToken         `json:"token,omitzero"`
	Tokens             []client.APIToken        `json:"tokens,omitempty"`
	NodePolicy         *client.NodePolicyResult `json:"node_policy,omitzero"`
	Error              *client.Error            `json:"error,omitzero"`
	IdempotencyKey     string                   `json:"idempotency_key,omitempty"`
	ExpectedRevision   int64                    `json:"expected_revision,omitzero"`
	DatabaseCredential *DatabaseCredential      `json:"database_credential,omitzero"`
	Installation       *client.Installation     `json:"installation,omitzero"`
	// JoinToken is the one secret an operator copies to the joining node.
	JoinToken string `json:"join_token,omitempty"`
}

// finish closes the connection and writes the result of a client command to stdout.
// Progress and diagnostics stay on stderr. It returns the exit status.
func (a *app) finish(selected *cobra.Command, err error) int {
	// Credential output is always JSON.
	if selected != nil && selected.Name() == "credentials" {
		a.json = true
	}
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
		switch failure.Code {
		case "wait_timeout":
			code = 2
		case "operation_failed":
			code = 3
		case "operation_cancelled":
			code = 4
		case "interrupted":
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
	if a.result.EnrollmentToken != nil && a.result.JoinToken == "" {
		value := a.result.EnrollmentToken
		fmt.Fprintf(&output, "Enrollment registration: %s\nNode: %s\nExpires: %s\nReplayed: %t\nToken file: %s\n", value.ID, value.NodeID, value.ExpiresAt.Format(time.RFC3339), value.Replayed, value.TokenFile)
	}
	if a.result.JoinToken != "" && a.result.EnrollmentToken != nil {
		fmt.Fprintf(&output, "Join token for one agent node, valid until %s:\n  %s\n\nOn that node, run:\n  synod agent join %s\n", a.result.EnrollmentToken.ExpiresAt.Format(time.RFC3339), a.result.JoinToken, a.result.JoinToken)
	}
	if a.result.AppAdmission != nil {
		fmt.Fprintf(&output, "Accepted app: %s\nOperation: %s\nDesired revision: %d\nIdempotency key: %s\n", a.result.AppAdmission.AppID, a.result.AppAdmission.OperationID, a.result.AppAdmission.DesiredRevision, a.result.IdempotencyKey)
	}
	if item := a.result.App; item != nil {
		fmt.Fprintf(&output, "App: %s\nName: %s\nDesired: %s (revision %d)\nObserved: %s (revision %d)\n", item.ID, item.Name, item.DesiredLifecycle, item.DesiredRevision, item.ObservedLifecycle, item.ObservedRevision)
		if item.Status != nil {
			fmt.Fprintf(&output, "State: %s\nOperation: %s (%s)\n", item.Status.State, item.Status.OperationID, item.Status.OperationState)
			if item.Status.FailureCode != "" {
				fmt.Fprintf(&output, "Failure: %s\n", item.Status.FailureCode)
			}
			if item.Tier != appmodel.TierParallel {
				fmt.Fprintf(&output, "Active device: %s\nWriter epoch: %d\nPlacement: %s\n", item.Status.ActiveNodeID, item.Status.WriterEpoch, item.Status.Placement)
				if item.Status.LossWindowSeconds != nil {
					fmt.Fprintf(&output, "Loss window: %.1f seconds\n", *item.Status.LossWindowSeconds)
				} else {
					fmt.Fprintln(&output, "Loss window: no usable replica")
				}
				fmt.Fprintf(&output, "Snapshot pause: %.3f seconds (maximum %.3f)\nSkipped snapshots: %d\n", item.Status.LastPauseSeconds, item.Status.MaxPauseSeconds, item.Status.SkippedSnapshots)
				for _, standby := range item.Status.Standbys {
					fmt.Fprintf(&output, "Standby: %s (epoch %d, sequence %d, usable: %t)\n", standby.NodeID, standby.Epoch, standby.Sequence, standby.Usable)
					if standby.State != "" {
						fmt.Fprintf(&output, "  State: %s\n", standby.State)
					}
					if standby.CompletedAt != nil {
						fmt.Fprintf(&output, "  Completed: %s\n", standby.CompletedAt.Format(time.RFC3339))
					}
					if standby.LagSeconds != nil {
						fmt.Fprintf(&output, "  Lag: %.1f seconds\n", *standby.LagSeconds)
					}
					if standby.FailureCode != "" {
						fmt.Fprintf(&output, "  Failure: %s\n", standby.FailureCode)
					}
				}
			}
			for _, copy := range item.Status.Copies {
				fmt.Fprintf(&output, "Copy %d: %s on %s (%s, %s, %s)\n", copy.CopyIndex, copy.ID, copy.NodeID, copy.Role, copy.ObservedState, copy.Health)
			}
			for _, endpoint := range item.Status.Endpoints {
				fmt.Fprintf(&output, "Endpoint: %s (ready: %t)\n", endpoint.Address, endpoint.Ready)
			}
		}
	}
	if a.result.Installation != nil {
		fmt.Fprintf(&output, "Installation: %s\nRecovery generation: %s\n", a.result.Installation.InstallationID, a.result.Installation.RecoveryGeneration)
	}
	if a.result.Operation != nil {
		fmt.Fprintf(&output, "Operation: %s\nStatus: %s\nPlan version: %d\n", a.result.Operation.ID, a.result.Operation.Status, a.result.Operation.PlanVersion)
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
