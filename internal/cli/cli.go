package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
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
}

// Run writes one result to stdout and keeps progress and diagnostics on stderr.
func Run(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	settings := client.Config{Endpoint: os.Getenv("SYNOD_API_URL"), TokenFile: os.Getenv("SYNOD_ADMIN_TOKEN_FILE"), CAFile: os.Getenv("SYNOD_CA_FILE")}
	if settings.Endpoint == "" {
		settings.Endpoint = "https://localhost:8443"
	}
	var jsonOutput, wait bool
	var name, version, key, persistence string
	var redisMaxmemory int64
	var redisEviction, postgresReplication string
	var timeout, interval time.Duration
	var result Result
	var api *client.Client
	executed := false
	root := &cobra.Command{Use: "synod", Short: "Manage Synod cluster admission and inspect operations", SilenceErrors: true, SilenceUsage: true,
		CompletionOptions: cobra.CompletionOptions{DisableDefaultCmd: true}}
	root.SetOut(stdout)
	root.SetErr(stderr)
	root.SetArgs(args)
	root.PersistentFlags().StringVar(&settings.Endpoint, "api-url", settings.Endpoint, "HTTPS API origin (SYNOD_API_URL)")
	root.PersistentFlags().StringVar(&settings.TokenFile, "token-file", settings.TokenFile, "Private administrator token file (SYNOD_ADMIN_TOKEN_FILE)")
	root.PersistentFlags().StringVar(&settings.CAFile, "ca-file", settings.CAFile, "Additional trusted API CA PEM file (SYNOD_CA_FILE)")
	// Environment values may contain invalid credentials. Never echo them in help.
	for _, name := range []string{"api-url", "token-file", "ca-file"} {
		root.PersistentFlags().Lookup(name).DefValue = ""
	}
	root.PersistentFlags().DurationVar(&settings.RequestTimeout, "request-timeout", 10*time.Second, "Timeout for each HTTPS attempt")
	root.PersistentFlags().IntVar(&settings.Attempts, "attempts", 3, "Maximum attempts per request (1 to 5)")
	root.PersistentFlags().BoolVar(&jsonOutput, "json", false, "Write one JSON result to stdout")
	root.PersistentFlags().DurationVar(&timeout, "timeout", time.Minute, "Maximum operation wait time; does not cancel server work")
	root.PersistentFlags().DurationVar(&interval, "poll-interval", time.Second, "Interval between operation reads while waiting")
	prepare := func(cmd *cobra.Command, _ []string) error {
		executed = true
		if timeout <= 0 || interval <= 0 {
			return &client.Error{Code: "invalid_request", Message: "Use positive wait timeout and poll interval values."}
		}
		var err error
		api, err = client.New(settings)
		return err
	}
	db := &cobra.Command{Use: "db", Aliases: []string{"cluster"}, Short: "Create, inspect, and change a dedicated cluster", PersistentPreRunE: prepare}
	create := &cobra.Command{Use: "create", Short: "Create a standalone cluster"}
	create.PersistentFlags().StringVar(&name, "name", "", "Cluster name (required)")
	create.PersistentFlags().StringVar(&version, "version", "", "Exact engine version (required; the server checks its catalog)")
	create.PersistentFlags().StringVar(&key, "idempotency-key", "", "Reuse a prior key with the same request for recovery")
	create.PersistentFlags().BoolVar(&wait, "wait", false, "Wait for terminal work within --timeout")
	for _, engine := range []cluster.Engine{cluster.PostgreSQL, cluster.Redis} {
		command := &cobra.Command{Use: string(engine), Short: "Admit a standalone " + string(engine) + " cluster", Args: cobra.NoArgs,
			RunE: func(cmd *cobra.Command, _ []string) error {
				spec := cluster.CreateSpec{SchemaVersion: 1, Name: name, Engine: engine}
				if engine == cluster.PostgreSQL {
					spec.PostgreSQL = &cluster.PostgreSQLSpec{Version: version, Topology: "standalone", ReplicationPolicy: postgresReplication}
				} else {
					spec.Redis = &cluster.RedisSpec{Version: version, Topology: "standalone", Persistence: persistence, MaxmemoryBytes: redisMaxmemory, EvictionPolicy: redisEviction}
				}
				if err := spec.Validate(); err != nil {
					return &client.Error{Code: "invalid_request", Message: "Set a valid --name, exact --version, and supported persistence mode. See command help."}
				}
				if key == "" {
					id, err := uuid.NewRandom()
					if err != nil {
						return &client.Error{Code: "local_error", Message: "Create an idempotency key failed."}
					}
					key = id.String()
				}
				if len(key) > 128 || strings.ContainsFunc(key, func(r rune) bool { return r < '!' || r > '~' }) {
					return &client.Error{Code: "invalid_request", Message: "Use an idempotency key with 1 to 128 visible ASCII bytes."}
				}
				result.IdempotencyKey = key
				if _, err := fmt.Fprintln(stderr, "Idempotency key:", key); err != nil {
					return &client.Error{Code: "local_error", Message: "Disclose the retry key failed; no create request was sent."}
				}
				if engine == cluster.PostgreSQL {
					message := "Asynchronous replication: commits do not wait for replicas. Recent writes can be lost if the primary is lost."
					if postgresReplication == "synchronous" {
						message = "Synchronous replication: commits wait for one replica. This cluster starts without replicas; add one before writing."
					}
					if _, err := fmt.Fprintln(stderr, message); err != nil {
						return &client.Error{Code: "local_error", Message: "Display replication policy failed. No create request was sent."}
					}
				}
				admission, err := api.Create(cmd.Context(), spec, key)
				if err != nil {
					return err
				}
				result.Admission = &admission
				if wait {
					waiting, cancel := context.WithTimeout(cmd.Context(), timeout)
					defer cancel()
					value, err := api.Wait(waiting, admission.OperationID, interval)
					if value.ID != uuid.Nil {
						result.Operation = &value
					}
					return err
				}
				return nil
			}}
		if engine == cluster.PostgreSQL {
			command.Flags().StringVar(&postgresReplication, "replication-policy", "asynchronous", "Replication policy: asynchronous or synchronous (commits wait for one replica)")
		}
		if engine == cluster.Redis {
			command.Flags().StringVar(&persistence, "persistence", "rdb+aof", "Persistence: none, rdb, aof, or rdb+aof")
			command.Flags().Int64Var(&redisMaxmemory, "maxmemory-bytes", 0, "Redis memory limit (default: 75 percent of container memory)")
			command.Flags().StringVar(&redisEviction, "eviction-policy", "noeviction", "Redis maxmemory policy")
		}
		create.AddCommand(command)
	}
	var listOptions cluster.ListOptions
	listClusters := &cobra.Command{Use: "list", Short: "Read one page of clusters", Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if cmd.Flags().Changed("cursor") && listOptions.Cursor == "" {
				return &client.Error{Code: "invalid_request", Message: "Use a nonempty continuation cursor, or omit --cursor for the first page."}
			}
			page, err := api.ListClusters(cmd.Context(), listOptions)
			if err == nil {
				result.ClusterPage = &page
			}
			return err
		}}
	listClusters.Flags().IntVar(&listOptions.Limit, "limit", 50, "Maximum clusters on this page (1 to 200)")
	listClusters.Flags().StringVar(&listOptions.Cursor, "cursor", "", "Opaque continuation cursor from the previous page")
	listClusters.Flags().BoolVar(&listOptions.IncludeDeleted, "include-deleted", false, "Include completed cluster deletions")
	getCluster := &cobra.Command{Use: "get ID", Aliases: []string{"inspect"}, Short: "Read a cluster and its revisions", Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			id, err := uuid.Parse(args[0])
			if err != nil || id == uuid.Nil {
				return &client.Error{Code: "invalid_request", Message: "Use a nonzero cluster UUID."}
			}
			value, err := api.GetCluster(cmd.Context(), id)
			if err == nil {
				result.Cluster = &value
			}
			return err
		}}
	// Confirm actions that change retained data availability before admission.
	actions := make([]*cobra.Command, 0, 5)
	for _, action := range []string{"stop", "start", "delete", "recover-setup", "rebuild-replica"} {
		reviewed := action == "delete" || action == "recover-setup" || action == "rebuild-replica"
		var expected int64
		var planToken string
		var yes bool
		command := &cobra.Command{Use: action + " ID", Short: strings.ToUpper(action[:1]) + action[1:] + " a cluster", Args: cobra.ExactArgs(1),
			RunE: func(cmd *cobra.Command, args []string) error {
				id, err := uuid.Parse(args[0])
				if err != nil || id == uuid.Nil {
					return &client.Error{Code: "invalid_request", Message: "Use a nonzero cluster UUID."}
				}
				if cmd.Flags().Changed("expected-revision") && expected < 1 {
					return &client.Error{Code: "invalid_request", Message: "Use a positive expected revision."}
				}
				var confirmation *cluster.ActionConfirmation
				if reviewed && cmd.Flags().Changed("plan-token") && planToken == "" {
					return &client.Error{Code: "invalid_request", Message: "Use a nonzero action plan token."}
				}
				if planToken != "" {
					parsed, err := uuid.Parse(planToken)
					if err != nil || parsed == uuid.Nil || expected < 1 || key == "" {
						return &client.Error{Code: "invalid_request", Message: "Use --plan-token with its positive --expected-revision and original --idempotency-key."}
					}
					confirmation = &cluster.ActionConfirmation{PlanToken: parsed, Confirm: true, DataDisposition: "retain"}
				}
				if key == "" {
					generated, err := uuid.NewRandom()
					if err != nil {
						return &client.Error{Code: "local_error", Message: "Create an idempotency key failed."}
					}
					key = generated.String()
				}
				if len(key) > 128 || strings.ContainsFunc(key, func(r rune) bool { return r < '!' || r > '~' }) {
					return &client.Error{Code: "invalid_request", Message: "Use an idempotency key with 1 to 128 visible ASCII bytes."}
				}
				result.IdempotencyKey = key
				result.ExpectedRevision = expected
				if reviewed {
					if confirmation == nil {
						preview := api.PlanDelete
						if action == "recover-setup" {
							preview = api.PlanSetupRecovery
						}
						if action == "rebuild-replica" {
							preview = api.PlanReplicaRebuild
						}
						plan, err := preview(cmd.Context(), id)
						if err != nil {
							return err
						}
						result.ActionPlan = &plan
						if expected > 0 && expected != plan.ExpectedRevision {
							return &client.Error{Code: "revision_conflict", Message: "The action plan has a different revision. Review the current plan."}
						}
						expected = plan.ExpectedRevision
						confirmation = &cluster.ActionConfirmation{PlanToken: plan.PlanToken, Confirm: true, DataDisposition: plan.DataDisposition}
						if _, err := fmt.Fprintf(stderr, "Action: %s\nCluster: %s (%s)\nData disposition: %s\nRetained volumes: %v\nDowntime: %s\nPlan token: %s\nPlan expires: %s\n", action, plan.ClusterName, plan.ClusterID, plan.DataDisposition, plan.RetainedVolumes, plan.Downtime, plan.PlanToken, plan.ExpiresAt.Format(time.RFC3339)); err != nil {
							return &client.Error{Code: "local_error", Message: "Display the action plan failed. No action request was sent."}
						}
						if action == "recover-setup" || action == "rebuild-replica" {
							if _, err := fmt.Fprintf(stderr, "Member: %s\nResource: %s\n", plan.MemberID, plan.ResourceID); err != nil {
								return &client.Error{Code: "local_error", Message: "Display the recovery target failed. No action request was sent."}
							}
						}
					} else if _, err := fmt.Fprintf(stderr, "Retry %s for cluster: %s\nData disposition: retain\nReviewed plan token: %s\n", action, id, confirmation.PlanToken); err != nil {
						return &client.Error{Code: "local_error", Message: "Display the action retry failed. No action request was sent."}
					}
				} else if !cmd.Flags().Changed("expected-revision") {
					current, err := api.GetCluster(cmd.Context(), id)
					if err != nil {
						return err
					}
					expected = current.DesiredRevision
					if expected < 1 {
						return &client.Error{Code: "invalid_response", Message: "The cluster has no positive desired revision."}
					}
				}
				result.ExpectedRevision = expected
				if _, err := fmt.Fprintf(stderr, "Idempotency key: %s\nExpected revision: %d\n", key, expected); err != nil {
					return &client.Error{Code: "local_error", Message: "Disclose the retry key and revision failed. No mutation request was sent."}
				}
				if reviewed && !yes {
					return &client.Error{Code: "confirmation_required", Message: fmt.Sprintf("Review the plan. Confirm with --yes --plan-token %s --expected-revision %d --idempotency-key %s.", confirmation.PlanToken, expected, key)}
				}
				admission, err := api.Act(cmd.Context(), id, action, expected, key, confirmation)
				if err != nil {
					return err
				}
				result.Admission = &admission
				if wait {
					waiting, cancel := context.WithTimeout(cmd.Context(), timeout)
					defer cancel()
					value, err := api.Wait(waiting, admission.OperationID, interval)
					if value.ID != uuid.Nil {
						result.Operation = &value
					}
					return err
				}
				return nil
			}}
		if action == "recover-setup" {
			command.Short = "Recover confirmed PostgreSQL setup failure"
		}
		if action == "rebuild-replica" {
			command.Short = "Rebuild a failed PostgreSQL replica and retain its old volume"
		}
		command.Flags().StringVar(&key, "idempotency-key", "", "Reuse a prior key with the same request for recovery")
		command.Flags().BoolVar(&wait, "wait", false, "Wait for terminal work within --timeout")
		command.Flags().Int64Var(&expected, "expected-revision", 0, "Positive desired revision this action replaces; read the current revision when omitted")
		if reviewed {
			command.Flags().BoolVar(&yes, "yes", false, "Confirm the displayed action plan")
			command.Flags().StringVar(&planToken, "plan-token", "", "Retry the original plan with its expected revision and idempotency key")
		}
		actions = append(actions, command)
	}
	// A scale carries a replica count, so it does not share the action loop above.
	var scaleExpected int64
	var replicas int
	var scalePlanToken string
	var scaleYes, failedReplica bool
	scale := &cobra.Command{Use: "scale ID", Short: "Set how many replicas a cluster runs", Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			id, err := uuid.Parse(args[0])
			if err != nil || id == uuid.Nil {
				return &client.Error{Code: "invalid_request", Message: "Use a nonzero cluster UUID."}
			}
			if !cmd.Flags().Changed("replicas") {
				return &client.Error{Code: "invalid_request", Message: "Name the number of replicas with --replicas."}
			}
			if failedReplica && scalePlanToken != "" {
				return &client.Error{Code: "invalid_request", Message: "Use --failed-replica to request a new plan. Use --plan-token alone to confirm its stored mode."}
			}
			if failedReplica && replicas != 0 {
				return &client.Error{Code: "invalid_request", Message: "Use --failed-replica only with --replicas 0."}
			}
			if cmd.Flags().Changed("expected-revision") && scaleExpected < 1 {
				return &client.Error{Code: "invalid_request", Message: "Use a positive expected revision."}
			}
			if key == "" {
				generated, err := uuid.NewRandom()
				if err != nil {
					return &client.Error{Code: "local_error", Message: "Create an idempotency key failed."}
				}
				key = generated.String()
			}
			if len(key) > 128 || strings.ContainsFunc(key, func(r rune) bool { return r < '!' || r > '~' }) {
				return &client.Error{Code: "invalid_request", Message: "Use an idempotency key with 1 to 128 visible ASCII bytes."}
			}
			if !cmd.Flags().Changed("expected-revision") {
				current, err := api.GetCluster(cmd.Context(), id)
				if err != nil {
					return err
				}
				scaleExpected = current.DesiredRevision
				if scaleExpected < 1 {
					return &client.Error{Code: "invalid_response", Message: "The cluster has no positive desired revision."}
				}
			}
			// Removing a member is destructive, so it is shown and confirmed first.
			var confirmation *cluster.ActionConfirmation
			if replicas == 0 {
				if scalePlanToken != "" {
					parsed, err := uuid.Parse(scalePlanToken)
					if err != nil || parsed == uuid.Nil {
						return &client.Error{Code: "invalid_request", Message: "Use a nonzero scale-down plan token."}
					}
					confirmation = &cluster.ActionConfirmation{PlanToken: parsed, Confirm: true, DataDisposition: "retain"}
				} else {
					preview := api.PlanScaleDown
					if failedReplica {
						preview = api.PlanFailedReplicaRemoval
					}
					plan, err := preview(cmd.Context(), id)
					if err != nil {
						return err
					}
					result.ActionPlan = &plan
					if cmd.Flags().Changed("expected-revision") && scaleExpected != plan.ExpectedRevision {
						return &client.Error{Code: "revision_conflict", Message: "The scale-down plan has a different revision. Review the current plan."}
					}
					scaleExpected = plan.ExpectedRevision
					confirmation = &cluster.ActionConfirmation{PlanToken: plan.PlanToken, Confirm: true, DataDisposition: plan.DataDisposition}
					if _, err := fmt.Fprintf(stderr, "Remove member: %s\nCluster: %s (%s)\nData disposition: %s\nRetained volumes: %v\n%s\nPlan token: %s\nPlan expires: %s\n",
						plan.MemberID, plan.ClusterName, plan.ClusterID, plan.DataDisposition, plan.RetainedVolumes, plan.Downtime, plan.PlanToken, plan.ExpiresAt.Format(time.RFC3339)); err != nil {
						return &client.Error{Code: "local_error", Message: "Display the scale-down plan failed. No scale request was sent."}
					}
				}
			}
			result.IdempotencyKey, result.ExpectedRevision = key, scaleExpected
			if _, err := fmt.Fprintf(stderr, "Idempotency key: %s\nExpected revision: %d\nReplicas: %d\n", key, scaleExpected, replicas); err != nil {
				return &client.Error{Code: "local_error", Message: "Disclose the retry key and revision failed. No mutation request was sent."}
			}
			if replicas == 0 && !scaleYes {
				return &client.Error{Code: "confirmation_required", Message: fmt.Sprintf("Review the plan. Confirm with --yes --plan-token %s --expected-revision %d --idempotency-key %s.", confirmation.PlanToken, scaleExpected, key)}
			}
			admission, err := api.Scale(cmd.Context(), id, replicas, scaleExpected, key, confirmation)
			if err != nil {
				return err
			}
			result.Admission = &admission
			if wait {
				waiting, cancel := context.WithTimeout(cmd.Context(), timeout)
				defer cancel()
				value, err := api.Wait(waiting, admission.OperationID, interval)
				if value.ID != uuid.Nil {
					result.Operation = &value
				}
				return err
			}
			return nil
		}}
	scale.Flags().IntVar(&replicas, "replicas", 0, "How many replicas to run beside the primary")
	scale.Flags().StringVar(&key, "idempotency-key", "", "Reuse a prior key with the same request for recovery")
	scale.Flags().BoolVar(&wait, "wait", false, "Wait for terminal work within --timeout")
	scale.Flags().Int64Var(&scaleExpected, "expected-revision", 0, "Positive desired revision this scale replaces; read the current revision when omitted")
	scale.Flags().BoolVar(&failedReplica, "failed-replica", false, "Review removal that requires native PostgreSQL failure proof and retains data")
	scale.Flags().BoolVar(&scaleYes, "yes", false, "Confirm the displayed replica removal")
	scale.Flags().StringVar(&scalePlanToken, "plan-token", "", "Retry the original removal plan with its expected revision and idempotency key")

	members := &cobra.Command{Use: "members ID", Short: "List the instances a cluster runs", Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			id, err := uuid.Parse(args[0])
			if err != nil || id == uuid.Nil {
				return &client.Error{Code: "invalid_request", Message: "Use a nonzero cluster UUID."}
			}
			found, err := api.ClusterMembers(cmd.Context(), id)
			if err != nil {
				return err
			}
			result.Members = found
			return nil
		}}
	db.AddCommand(append([]*cobra.Command{create, listClusters, getCluster, scale, members}, actions...)...)
	operations := &cobra.Command{Use: "operation", Short: "Inspect or wait for durable work", PersistentPreRunE: prepare}
	for _, action := range []string{"get", "wait"} {
		command := &cobra.Command{Use: action + " ID", Short: "Read an operation", Args: cobra.ExactArgs(1),
			RunE: func(cmd *cobra.Command, args []string) error {
				id, err := uuid.Parse(args[0])
				if err != nil || id == uuid.Nil {
					return &client.Error{Code: "invalid_request", Message: "Use a nonzero operation UUID."}
				}
				var value operation.Operation
				if action == "wait" {
					waiting, cancel := context.WithTimeout(cmd.Context(), timeout)
					defer cancel()
					value, err = api.Wait(waiting, id, interval)
				} else {
					value, err = api.GetOperation(cmd.Context(), id)
				}
				if value.ID != uuid.Nil {
					result.Operation = &value
				}
				return err
			}}
		if action == "get" {
			command.Aliases = []string{"inspect"}
		} else {
			command.Short = "Wait for terminal work; timeout never cancels the operation"
		}
		operations.AddCommand(command)
	}
	root.AddCommand(db, operations, newEnrollmentTokenCommand(&settings, &result, &executed),
		newUserCommand(&api, &result, prepare, stderr), newTokenCommand(&api, &result, prepare),
		newNodeCommand(&api, &result, prepare), newEndpointCommand(&api, &result, prepare, stderr),
		newCredentialCommand(&api, &result, prepare, stderr))
	selected, err := root.ExecuteContextC(ctx)
	for command := selected; command != nil; command = command.Parent() {
		if command.Name() == "endpoint" || command.Name() == "credential" {
			jsonOutput = true
			break
		}
	}
	if api != nil {
		api.Close()
	}
	if err == nil && !executed {
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
		result.Error = failure
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
		fmt.Fprintln(stderr, failure.Code+": "+failure.Message)
	}
	if jsonOutput {
		if err := json.NewEncoder(stdout).Encode(result); err != nil {
			fmt.Fprintln(stderr, "Write the JSON result failed: "+err.Error())
			return 1
		}
		return code
	}
	var output strings.Builder
	if result.EnrollmentToken != nil {
		value := result.EnrollmentToken
		fmt.Fprintf(&output, "Enrollment registration: %s\nNode: %s\nExpires: %s\nReplayed: %t\nToken file: %s\n", value.ID, value.NodeID, value.ExpiresAt.Format(time.RFC3339), value.Replayed, value.TokenFile)
	}
	if result.Admission != nil {
		fmt.Fprintf(&output, "Accepted cluster: %s\nOperation: %s\nIdempotency key: %s\n", result.Admission.ClusterID, result.Admission.OperationID, result.IdempotencyKey)
	}
	if result.Cluster != nil {
		fmt.Fprintf(&output, "Cluster: %s\nName: %s\nDesired: %s (revision %d)\nObserved: %s (revision %d)\n", result.Cluster.ID, result.Cluster.Spec.Name, result.Cluster.DesiredLifecycle, result.Cluster.DesiredRevision, result.Cluster.ObservedLifecycle, result.Cluster.ObservedRevision)
	}
	if result.ClusterPage != nil {
		if len(result.ClusterPage.Items) == 0 {
			fmt.Fprintln(&output, "No clusters on this page.")
		} else {
			fmt.Fprintln(&output, "ID  NAME  ENGINE  DESIRED (REVISION)  OBSERVED (REVISION)")
			for _, item := range result.ClusterPage.Items {
				fmt.Fprintf(&output, "%s  %s  %s  %s (%d)  %s (%d)\n", item.ID, item.Spec.Name, item.Spec.Engine, item.DesiredLifecycle, item.DesiredRevision, item.ObservedLifecycle, item.ObservedRevision)
			}
		}
		if result.ClusterPage.NextCursor != "" {
			fmt.Fprintf(&output, "Next cursor: %q\n", result.ClusterPage.NextCursor)
		}
	}
	if result.Operation != nil {
		fmt.Fprintf(&output, "Operation: %s\nStatus: %s\nPlan version: %d\n", result.Operation.ID, result.Operation.Status, result.Operation.PlanVersion)
	}
	if result.User != nil {
		fmt.Fprintf(&output, "User: %s\nUsername: %s\nRole: %s\n", result.User.ID, result.User.Username, result.User.Role)
	}
	if result.Token != nil {
		// The secret appears once. A caller that cannot see it here cannot recover it.
		fmt.Fprintf(&output, "Token: %s\nName: %s\nActor: %s\nSecret: %s\nStore the secret now. No later command repeats it.\n",
			result.Token.ID, result.Token.Name, result.Token.ActorID, result.Token.Plaintext)
	}
	for _, token := range result.Tokens {
		state := "live"
		if token.RevokedAt != nil {
			state = "revoked"
		}
		fmt.Fprintf(&output, "%s  %-24s %-16s %s\n", token.ID, token.Name, token.ActorID, state)
	}
	for _, member := range result.Members {
		fmt.Fprintf(&output, "%s  %-8s %s\n", member.ID, member.Role, member.State)
	}
	if _, err := io.WriteString(stdout, output.String()); err != nil {
		fmt.Fprintln(stderr, "Write the result failed: "+err.Error())
		return 1
	}
	return code
}
