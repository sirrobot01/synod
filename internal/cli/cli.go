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
	EnrollmentToken *client.EnrollmentToken `json:"enrollment_token,omitzero"`
	Admission       *client.Admission       `json:"admission,omitzero"`
	Cluster         *cluster.Cluster        `json:"cluster,omitzero"`
	Operation       *operation.Operation    `json:"operation,omitzero"`
	User            *client.User            `json:"user,omitzero"`
	Token           *client.APIToken        `json:"token,omitzero"`
	Tokens          []client.APIToken       `json:"tokens,omitempty"`
	Error           *client.Error           `json:"error,omitzero"`
	IdempotencyKey  string                  `json:"idempotency_key,omitempty"`
}

// Run writes one result to stdout and keeps progress and diagnostics on stderr.
func Run(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	settings := client.Config{Endpoint: os.Getenv("SYNOD_API_URL"), TokenFile: os.Getenv("SYNOD_ADMIN_TOKEN_FILE"), CAFile: os.Getenv("SYNOD_CA_FILE")}
	if settings.Endpoint == "" {
		settings.Endpoint = "https://localhost:8443"
	}
	var jsonOutput, wait bool
	var name, version, key, persistence string
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
	db := &cobra.Command{Use: "db", Aliases: []string{"cluster"}, Short: "Create or inspect a dedicated cluster", PersistentPreRunE: prepare}
	create := &cobra.Command{Use: "create", Short: "Admit a standalone cluster; provisioning is not connected yet"}
	create.PersistentFlags().StringVar(&name, "name", "", "Cluster name (required)")
	create.PersistentFlags().StringVar(&version, "version", "", "Exact engine version (required; the server checks its catalog)")
	create.PersistentFlags().StringVar(&key, "idempotency-key", "", "Reuse a prior key with the same request for recovery")
	create.PersistentFlags().BoolVar(&wait, "wait", false, "Wait for terminal work within --timeout")
	for _, engine := range []cluster.Engine{cluster.PostgreSQL, cluster.Redis} {
		command := &cobra.Command{Use: string(engine), Short: "Admit a standalone " + string(engine) + " cluster", Args: cobra.NoArgs,
			RunE: func(cmd *cobra.Command, _ []string) error {
				spec := cluster.CreateSpec{SchemaVersion: 1, Name: name, Engine: engine}
				if engine == cluster.PostgreSQL {
					spec.PostgreSQL = &cluster.PostgreSQLSpec{Version: version, Topology: "standalone"}
				} else {
					spec.Redis = &cluster.RedisSpec{Version: version, Topology: "standalone", Persistence: persistence}
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
		if engine == cluster.Redis {
			command.Flags().StringVar(&persistence, "persistence", "rdb+aof", "Persistence: none, rdb, aof, or rdb+aof")
		}
		create.AddCommand(command)
	}
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
	db.AddCommand(create, getCluster)
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
		newUserCommand(&api, &result, prepare, stderr), newTokenCommand(&api, &result, prepare))
	err := root.ExecuteContext(ctx)
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
			fmt.Fprintln(stderr, "Write the JSON result failed.")
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
	if _, err := io.WriteString(stdout, output.String()); err != nil {
		fmt.Fprintln(stderr, "Write the result failed.")
		return 1
	}
	return code
}
