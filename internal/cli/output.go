package cli

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/client"
	"github.com/sirrobot01/synod/internal/cluster"
	"github.com/sirrobot01/synod/internal/operation"
	"github.com/spf13/cobra"
)

// Result is the single JSON document that the CLI writes to stdout.
type Result struct {
	EnrollmentToken      *client.EnrollmentToken       `json:"enrollment_token,omitzero"`
	Admission            *client.Admission             `json:"admission,omitzero"`
	Cluster              *cluster.Cluster              `json:"cluster,omitzero"`
	ClusterPage          *cluster.Page                 `json:"cluster_page,omitzero"`
	Operation            *operation.Operation          `json:"operation,omitzero"`
	User                 *client.User                  `json:"user,omitzero"`
	Token                *client.APIToken              `json:"token,omitzero"`
	Tokens               []client.APIToken             `json:"tokens,omitempty"`
	NodePolicy           *client.NodePolicyResult      `json:"node_policy,omitzero"`
	Error                *client.Error                 `json:"error,omitzero"`
	IdempotencyKey       string                        `json:"idempotency_key,omitempty"`
	ExpectedRevision     int64                         `json:"expected_revision,omitzero"`
	ActionPlan           *cluster.ActionPlan           `json:"action_plan,omitzero"`
	EndpointInspection   *cluster.EndpointInspection   `json:"endpoint_inspection,omitzero"`
	CredentialInspection *cluster.CredentialInspection `json:"credential_inspection,omitzero"`
	RevealedCredential   *cluster.RevealedCredential   `json:"revealed_credential,omitzero"`
	Members              []client.ClusterMember        `json:"members,omitempty"`
	Installation         *client.Installation          `json:"installation,omitzero"`
	// JoinToken is the one secret an operator copies to the joining node.
	JoinToken string `json:"join_token,omitempty"`
}

// finish closes the connection and writes the result of a client command to stdout.
// Progress and diagnostics stay on stderr. It returns the exit status.
func (a *app) finish(selected *cobra.Command, err error) int {
	// Endpoint and credential output is always JSON.
	for command := selected; command != nil; command = command.Parent() {
		if command.Name() == "endpoint" || command.Name() == "credential" {
			a.json = true
			break
		}
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
	if a.result.Admission != nil {
		fmt.Fprintf(&output, "Accepted cluster: %s\nOperation: %s\nIdempotency key: %s\n", a.result.Admission.ClusterID, a.result.Admission.OperationID, a.result.IdempotencyKey)
	}
	if a.result.Cluster != nil {
		fmt.Fprintf(&output, "Cluster: %s\nName: %s\nDesired: %s (revision %d)\nObserved: %s (revision %d)\n", a.result.Cluster.ID, a.result.Cluster.Spec.Name, a.result.Cluster.DesiredLifecycle, a.result.Cluster.DesiredRevision, a.result.Cluster.ObservedLifecycle, a.result.Cluster.ObservedRevision)
	}
	if a.result.ClusterPage != nil {
		if len(a.result.ClusterPage.Items) == 0 {
			fmt.Fprintln(&output, "No clusters on this page.")
		} else {
			fmt.Fprintln(&output, "ID  NAME  ENGINE  DESIRED (REVISION)  OBSERVED (REVISION)")
			for _, item := range a.result.ClusterPage.Items {
				fmt.Fprintf(&output, "%s  %s  %s  %s (%d)  %s (%d)\n", item.ID, item.Spec.Name, item.Spec.Engine, item.DesiredLifecycle, item.DesiredRevision, item.ObservedLifecycle, item.ObservedRevision)
			}
		}
		if a.result.ClusterPage.NextCursor != "" {
			fmt.Fprintf(&output, "Next cursor: %q\n", a.result.ClusterPage.NextCursor)
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
	for _, member := range a.result.Members {
		node := "-"
		if member.NodeID != uuid.Nil {
			node = member.NodeID.String()
		}
		address := cmp.Or(member.Address, "-")
		fmt.Fprintf(&output, "%s  %-8s %-9s %-36s %s\n", member.ID, member.Role, member.State, node, address)
	}
	if _, err := io.WriteString(a.stdout, output.String()); err != nil {
		fmt.Fprintln(a.stderr, "Write the result failed: "+err.Error())
		return 1
	}
	return code
}
