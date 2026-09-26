package cli

import (
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/client"
	"github.com/sirrobot01/synod/internal/cluster"
	"github.com/spf13/cobra"
)

func newDBCommand(app *app) *cobra.Command {
	group := &cobra.Command{Use: "db", Aliases: []string{"cluster"}, Short: "Create, inspect, and change a dedicated cluster", PersistentPreRunE: app.connect}
	group.AddCommand(
		newCreateCommand(app),
		newListCommand(app),
		newGetCommand(app),
		newMembersCommand(app),
		newScaleCommand(app),
	)
	for _, action := range clusterActions {
		group.AddCommand(newActionCommand(app, action))
	}
	return group
}

// createFlags are the flags that every engine's create command shares.
type createFlags struct {
	name, version, key string
	wait               bool
}

func newCreateCommand(app *app) *cobra.Command {
	var shared createFlags
	group := &cobra.Command{Use: "create", Short: "Create a cluster"}
	flags := group.PersistentFlags()
	flags.StringVar(&shared.name, "name", "", "Cluster name (required)")
	flags.StringVar(&shared.version, "version", "", "Exact engine version (required; the server checks its catalog)")
	flags.StringVar(&shared.key, "idempotency-key", "", "Reuse a prior key with the same request for recovery")
	flags.BoolVar(&shared.wait, "wait", false, "Wait for terminal work within --timeout")
	group.AddCommand(newCreatePostgresCommand(app, &shared), newCreateRedisCommand(app, &shared))
	return group
}

func newCreatePostgresCommand(app *app, shared *createFlags) *cobra.Command {
	var spec cluster.PostgreSQLSpec
	var endpoint cluster.PostgresEndpoint
	command := &cobra.Command{Use: string(cluster.PostgreSQL), Short: "Admit a " + string(cluster.PostgreSQL) + " cluster", Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			spec.Version = shared.version
			changed := 0
			for _, flag := range []string{"endpoint-address", "endpoint-writer-port", "endpoint-reader-port"} {
				if cmd.Flags().Changed(flag) {
					changed++
				}
			}
			if changed != 0 && changed != 3 {
				return &client.Error{Code: "invalid_request", Message: "Set --endpoint-address, --endpoint-writer-port, and --endpoint-reader-port together."}
			}
			if changed == 3 {
				spec.Endpoint = &endpoint
			}
			// Only Patroni replicates, so only a Patroni create discloses its replication policy.
			notice := ""
			if spec.Topology == "patroni" {
				notice = "Asynchronous replication: commits do not wait for replicas. Recent writes can be lost if the primary is lost."
				if spec.ReplicationPolicy == "synchronous" {
					notice = "Synchronous replication: commits wait for one replica."
				}
			}
			return app.create(cmd, shared, cluster.CreateSpec{Engine: cluster.PostgreSQL, PostgreSQL: &spec}, notice)
		}}
	flags := command.Flags()
	flags.StringVar(&spec.ReplicationPolicy, "replication-policy", "", "Patroni replication policy: asynchronous (default) or synchronous (commits wait for one replica)")
	flags.StringVar(&spec.Topology, "topology", "standalone", "Topology: standalone, or patroni for three members with native failover")
	flags.StringVar(&spec.FailureDomain, "failure-domain", "", "Host failure a patroni cluster survives: host (required for patroni)")
	flags.StringVar(&endpoint.Address, "endpoint-address", "", "Load balancer IP in front of the patroni routers (optional)")
	flags.Uint16Var(&endpoint.WriterPort, "endpoint-writer-port", 0, "Load balancer port that forwards to the router writer ports")
	flags.Uint16Var(&endpoint.ReaderPort, "endpoint-reader-port", 0, "Load balancer port that forwards to the router reader ports")
	return command
}

func newCreateRedisCommand(app *app, shared *createFlags) *cobra.Command {
	spec := cluster.RedisSpec{Topology: "standalone"}
	command := &cobra.Command{Use: string(cluster.Redis), Short: "Admit a " + string(cluster.Redis) + " cluster", Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			spec.Version = shared.version
			return app.create(cmd, shared, cluster.CreateSpec{Engine: cluster.Redis, Redis: &spec}, "")
		}}
	flags := command.Flags()
	flags.StringVar(&spec.Persistence, "persistence", "rdb+aof", "Persistence: none, rdb, aof, or rdb+aof")
	flags.Int64Var(&spec.MaxmemoryBytes, "maxmemory-bytes", 0, "Redis memory limit (default: 75 percent of container memory)")
	flags.StringVar(&spec.EvictionPolicy, "eviction-policy", "noeviction", "Redis maxmemory policy")
	return command
}

// create validates spec, discloses the retry key and notice, and admits the cluster.
func (a *app) create(cmd *cobra.Command, shared *createFlags, spec cluster.CreateSpec, notice string) error {
	spec.SchemaVersion, spec.Name = 1, shared.name
	if err := spec.Validate(); err != nil {
		return &client.Error{Code: "invalid_request", Message: "Set a valid --name, exact --version, supported persistence mode, and a patroni topology with --failure-domain host. An endpoint or a synchronous policy needs topology patroni. An endpoint needs a routable IP and two different ports. See command help."}
	}
	key, err := idempotencyKey(shared.key)
	if err != nil {
		return err
	}
	a.result.IdempotencyKey = key
	if _, err := fmt.Fprintln(a.stderr, "Idempotency key:", key); err != nil {
		return &client.Error{Code: "local_error", Message: "Disclose the retry key failed; no create request was sent."}
	}
	if notice != "" {
		if _, err := fmt.Fprintln(a.stderr, notice); err != nil {
			return &client.Error{Code: "local_error", Message: "Display replication policy failed. No create request was sent."}
		}
	}
	admission, err := a.api.Create(cmd.Context(), spec, key)
	if err != nil {
		return err
	}
	return a.admitted(cmd.Context(), admission, shared.wait)
}

func newListCommand(app *app) *cobra.Command {
	var options cluster.ListOptions
	command := &cobra.Command{Use: "list", Short: "Read one page of clusters", Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if cmd.Flags().Changed("cursor") && options.Cursor == "" {
				return &client.Error{Code: "invalid_request", Message: "Use a nonempty continuation cursor, or omit --cursor for the first page."}
			}
			page, err := app.api.ListClusters(cmd.Context(), options)
			if err == nil {
				app.result.ClusterPage = &page
			}
			return err
		}}
	command.Flags().IntVar(&options.Limit, "limit", 50, "Maximum clusters on this page (1 to 200)")
	command.Flags().StringVar(&options.Cursor, "cursor", "", "Opaque continuation cursor from the previous page")
	command.Flags().BoolVar(&options.IncludeDeleted, "include-deleted", false, "Include completed cluster deletions")
	return command
}

func newGetCommand(app *app) *cobra.Command {
	return &cobra.Command{Use: "get ID", Aliases: []string{"inspect"}, Short: "Read a cluster and its revisions", Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			id, err := parseID(args[0], "Use a nonzero cluster UUID.")
			if err != nil {
				return err
			}
			value, err := app.api.GetCluster(cmd.Context(), id)
			if err == nil {
				app.result.Cluster = &value
			}
			return err
		}}
}

func newMembersCommand(app *app) *cobra.Command {
	return &cobra.Command{Use: "members ID", Short: "List the instances a cluster runs", Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			id, err := parseID(args[0], "Use a nonzero cluster UUID.")
			if err != nil {
				return err
			}
			found, err := app.api.ClusterMembers(cmd.Context(), id)
			if err != nil {
				return err
			}
			app.result.Members = found
			return nil
		}}
}

// clusterAction is one db action. A reviewed action changes retained data
// availability, so it shows its plan and needs confirmation before admission.
type clusterAction struct {
	name, short string
	reviewed    bool
	// member is the help text of --member. An action without it takes no member.
	member string
}

var clusterActions = []clusterAction{
	{name: "stop", short: "Stop a cluster"},
	{name: "start", short: "Start a cluster"},
	{name: "renew-certificates", short: "Renew the native certificates of a Patroni cluster one member at a time"},
	{name: "switchover", short: "Make a named Patroni member the leader", member: "Member ID of the Patroni member that becomes the leader"},
	{name: "delete", short: "Delete a cluster", reviewed: true},
	{name: "recover-setup", short: "Recover confirmed PostgreSQL setup failure", reviewed: true},
	{name: "rebuild-member", short: "Rebuild a failed Patroni member on its node and retain its old volume", reviewed: true, member: "Member ID of the failed Patroni member"},
}

func newActionCommand(app *app, action clusterAction) *cobra.Command {
	var expected int64
	var key, planToken, memberFlag string
	var wait, yes bool
	command := &cobra.Command{Use: action.name + " ID", Short: action.short, Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			id, err := parseID(args[0], "Use a nonzero cluster UUID.")
			if err != nil {
				return err
			}
			if cmd.Flags().Changed("expected-revision") && expected < 1 {
				return &client.Error{Code: "invalid_request", Message: "Use a positive expected revision."}
			}
			var member uuid.UUID
			// A confirmed member rebuild names its member in the stored plan.
			if action.name == "switchover" || (action.name == "rebuild-member" && planToken == "") {
				if member, err = parseID(memberFlag, "Name the member with --member."); err != nil {
					return err
				}
			}
			var confirmation *cluster.ActionConfirmation
			if action.reviewed && cmd.Flags().Changed("plan-token") && planToken == "" {
				return &client.Error{Code: "invalid_request", Message: "Use a nonzero action plan token."}
			}
			if planToken != "" {
				parsed, err := uuid.Parse(planToken)
				if err != nil || parsed == uuid.Nil || expected < 1 || key == "" {
					return &client.Error{Code: "invalid_request", Message: "Use --plan-token with its positive --expected-revision and original --idempotency-key."}
				}
				confirmation = &cluster.ActionConfirmation{PlanToken: parsed, Confirm: true, DataDisposition: "retain"}
			}
			if key, err = idempotencyKey(key); err != nil {
				return err
			}
			app.result.IdempotencyKey = key
			app.result.ExpectedRevision = expected
			switch {
			case action.reviewed && confirmation == nil:
				plan, err := app.previewAction(cmd, action.name, id, member)
				if err != nil {
					return err
				}
				if expected > 0 && expected != plan.ExpectedRevision {
					return &client.Error{Code: "revision_conflict", Message: "The action plan has a different revision. Review the current plan."}
				}
				expected = plan.ExpectedRevision
				confirmation = &cluster.ActionConfirmation{PlanToken: plan.PlanToken, Confirm: true, DataDisposition: plan.DataDisposition}
				if err := app.showPlan(action.name, plan); err != nil {
					return err
				}
			case action.reviewed:
				if _, err := fmt.Fprintf(app.stderr, "Retry %s for cluster: %s\nData disposition: retain\nReviewed plan token: %s\n", action.name, id, confirmation.PlanToken); err != nil {
					return &client.Error{Code: "local_error", Message: "Display the action retry failed. No action request was sent."}
				}
			case !cmd.Flags().Changed("expected-revision"):
				if expected, err = app.desiredRevision(cmd, id); err != nil {
					return err
				}
			}
			app.result.ExpectedRevision = expected
			if _, err := fmt.Fprintf(app.stderr, "Idempotency key: %s\nExpected revision: %d\n", key, expected); err != nil {
				return &client.Error{Code: "local_error", Message: "Disclose the retry key and revision failed. No mutation request was sent."}
			}
			if action.reviewed && !yes {
				return &client.Error{Code: "confirmation_required", Message: fmt.Sprintf("Review the plan. Confirm with --yes --plan-token %s --expected-revision %d --idempotency-key %s.", confirmation.PlanToken, expected, key)}
			}
			var admission client.Admission
			if action.name == "switchover" {
				admission, err = app.api.Switchover(cmd.Context(), id, member, expected, key)
			} else {
				admission, err = app.api.Act(cmd.Context(), id, action.name, expected, key, confirmation)
			}
			if err != nil {
				return err
			}
			return app.admitted(cmd.Context(), admission, wait)
		}}
	flags := command.Flags()
	if action.member != "" {
		flags.StringVar(&memberFlag, "member", "", action.member)
	}
	flags.StringVar(&key, "idempotency-key", "", "Reuse a prior key with the same request for recovery")
	flags.BoolVar(&wait, "wait", false, "Wait for terminal work within --timeout")
	flags.Int64Var(&expected, "expected-revision", 0, "Positive desired revision this action replaces; read the current revision when omitted")
	if action.reviewed {
		flags.BoolVar(&yes, "yes", false, "Confirm the displayed action plan")
		flags.StringVar(&planToken, "plan-token", "", "Retry the original plan with its expected revision and idempotency key")
	}
	return command
}

// previewAction reads the plan of a reviewed action. No action request is sent.
func (a *app) previewAction(cmd *cobra.Command, action string, id, member uuid.UUID) (cluster.ActionPlan, error) {
	var plan cluster.ActionPlan
	var err error
	switch action {
	case "recover-setup":
		plan, err = a.api.PlanSetupRecovery(cmd.Context(), id)
	case "rebuild-member":
		plan, err = a.api.PlanMemberRebuild(cmd.Context(), id, member)
	default:
		plan, err = a.api.PlanDelete(cmd.Context(), id)
	}
	if err == nil {
		a.result.ActionPlan = &plan
	}
	return plan, err
}

// showPlan writes a reviewed action plan to stderr before the operator confirms it.
func (a *app) showPlan(action string, plan cluster.ActionPlan) error {
	if _, err := fmt.Fprintf(a.stderr, "Action: %s\nCluster: %s (%s)\nData disposition: %s\nRetained volumes: %v\nDowntime: %s\nPlan token: %s\nPlan expires: %s\n", action, plan.ClusterName, plan.ClusterID, plan.DataDisposition, plan.RetainedVolumes, plan.Downtime, plan.PlanToken, plan.ExpiresAt.Format(time.RFC3339)); err != nil {
		return &client.Error{Code: "local_error", Message: "Display the action plan failed. No action request was sent."}
	}
	// Delete acts on the whole cluster. The other reviewed actions name one member.
	if action != "delete" {
		if _, err := fmt.Fprintf(a.stderr, "Member: %s\nResource: %s\n", plan.MemberID, plan.ResourceID); err != nil {
			return &client.Error{Code: "local_error", Message: "Display the recovery target failed. No action request was sent."}
		}
	}
	return nil
}

// desiredRevision reads the revision that a mutation replaces when the operator names none.
func (a *app) desiredRevision(cmd *cobra.Command, id uuid.UUID) (int64, error) {
	current, err := a.api.GetCluster(cmd.Context(), id)
	if err != nil {
		return 0, err
	}
	if current.DesiredRevision < 1 {
		return 0, &client.Error{Code: "invalid_response", Message: "The cluster has no positive desired revision."}
	}
	return current.DesiredRevision, nil
}

func newScaleCommand(app *app) *cobra.Command {
	var expected int64
	var replicas int
	var key, planToken, removed string
	var wait, yes bool
	command := &cobra.Command{Use: "scale ID", Short: "Set how many replicas a cluster runs beside its primary", Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			id, err := parseID(args[0], "Use a nonzero cluster UUID.")
			if err != nil {
				return err
			}
			if !cmd.Flags().Changed("replicas") {
				return &client.Error{Code: "invalid_request", Message: "Name the number of replicas with --replicas."}
			}
			if cmd.Flags().Changed("expected-revision") && expected < 1 {
				return &client.Error{Code: "invalid_request", Message: "Use a positive expected revision."}
			}
			if key, err = idempotencyKey(key); err != nil {
				return err
			}
			if !cmd.Flags().Changed("expected-revision") {
				if expected, err = app.desiredRevision(cmd, id); err != nil {
					return err
				}
			}
			// Removing a member is destructive, so it is shown and confirmed first. A Patroni
			// removal names its member. A Redis removal scales to zero.
			var member uuid.UUID
			if removed != "" {
				if member, err = parseID(removed, "Use a nonzero member UUID."); err != nil {
					return err
				}
			}
			removal := replicas == 0 || member != uuid.Nil
			var confirmation *cluster.ActionConfirmation
			if removal && planToken != "" {
				parsed, err := parseID(planToken, "Use a nonzero scale-down plan token.")
				if err != nil {
					return err
				}
				confirmation = &cluster.ActionConfirmation{PlanToken: parsed, Confirm: true, DataDisposition: "retain"}
			} else if removal {
				plan, err := app.api.PlanScaleDown(cmd.Context(), id, member)
				if err != nil {
					return err
				}
				app.result.ActionPlan = &plan
				if cmd.Flags().Changed("expected-revision") && expected != plan.ExpectedRevision {
					return &client.Error{Code: "revision_conflict", Message: "The scale-down plan has a different revision. Review the current plan."}
				}
				expected = plan.ExpectedRevision
				confirmation = &cluster.ActionConfirmation{PlanToken: plan.PlanToken, Confirm: true, DataDisposition: plan.DataDisposition}
				if _, err := fmt.Fprintf(app.stderr, "Remove member: %s\nCluster: %s (%s)\nData disposition: %s\nRetained volumes: %v\n%s\nPlan token: %s\nPlan expires: %s\n",
					plan.MemberID, plan.ClusterName, plan.ClusterID, plan.DataDisposition, plan.RetainedVolumes, plan.Downtime, plan.PlanToken, plan.ExpiresAt.Format(time.RFC3339)); err != nil {
					return &client.Error{Code: "local_error", Message: "Display the scale-down plan failed. No scale request was sent."}
				}
			}
			app.result.IdempotencyKey, app.result.ExpectedRevision = key, expected
			if _, err := fmt.Fprintf(app.stderr, "Idempotency key: %s\nExpected revision: %d\nReplicas: %d\n", key, expected, replicas); err != nil {
				return &client.Error{Code: "local_error", Message: "Disclose the retry key and revision failed. No mutation request was sent."}
			}
			if removal && !yes {
				return &client.Error{Code: "confirmation_required", Message: fmt.Sprintf("Review the plan. Confirm with --yes --plan-token %s --expected-revision %d --idempotency-key %s.", confirmation.PlanToken, expected, key)}
			}
			admission, err := app.api.Scale(cmd.Context(), id, replicas, expected, key, confirmation)
			if err != nil {
				return err
			}
			return app.admitted(cmd.Context(), admission, wait)
		}}
	flags := command.Flags()
	flags.IntVar(&replicas, "replicas", 0, "How many replicas to run beside the primary: 0 or 1 for Redis, 2 to 4 for Patroni (one more member per scale)")
	flags.StringVar(&key, "idempotency-key", "", "Reuse a prior key with the same request for recovery")
	flags.BoolVar(&wait, "wait", false, "Wait for terminal work within --timeout")
	flags.Int64Var(&expected, "expected-revision", 0, "Positive desired revision this scale replaces; read the current revision when omitted")
	flags.BoolVar(&yes, "yes", false, "Confirm the displayed replica removal")
	flags.StringVar(&planToken, "plan-token", "", "Retry the original removal plan with its expected revision and idempotency key")
	flags.StringVar(&removed, "member", "", "Patroni member UUID to remove when --replicas is one less than now")
	return command
}
