package cli

import (
	"fmt"

	appmodel "github.com/sirrobot01/synod/internal/app"
	"github.com/sirrobot01/synod/internal/client"
	"github.com/sirrobot01/synod/internal/compose"
	"github.com/spf13/cobra"
)

func newAppCommand(a *app) *cobra.Command {
	var options compose.Options
	var filename, key string
	var tier, port int
	group := &cobra.Command{Use: "app", Short: "Manage container apps", PersistentPreRunE: a.connect}
	command := &cobra.Command{Use: "import [SERVICE]", Short: "Import one Compose service as a tier 2 or tier 3 app", Args: cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) == 1 {
				options.Service = args[0]
			}
			if tier != 2 && tier != 3 {
				return &client.Error{Code: "invalid_request", Message: "Compose import supports tier 2 and tier 3."}
			}
			if port < 0 || port > 65535 || (cmd.Flags().Changed("port") && port == 0) {
				return &client.Error{Code: "invalid_request", Message: "Use an ingress port from 1 to 65535."}
			}
			if cmd.Flags().Changed("standbys") && (options.Standbys < 1 || options.Standbys > 3) {
				return &client.Error{Code: "invalid_request", Message: "Use 1 to 3 standbys."}
			}
			if cmd.Flags().Changed("copies") && options.Copies == "" {
				return &client.Error{Code: "invalid_request", Message: "Set copies to a positive integer or all_capable."}
			}
			options.Tier, options.Port = appmodel.Tier(tier), uint16(port)
			spec, err := compose.Import(cmd.Context(), filename, options)
			if err != nil {
				return &client.Error{Code: "invalid_request", Message: err.Error()}
			}
			key, err = idempotencyKey(key)
			if err != nil {
				return err
			}
			a.result.IdempotencyKey = key
			if _, err := fmt.Fprintln(a.stderr, "Idempotency key:", key); err != nil {
				return &client.Error{Code: "local_error", Message: "Disclose the retry key failed; no request was sent."}
			}
			result, err := a.api.CreateApp(cmd.Context(), spec, key)
			if err == nil {
				a.result.AppAdmission = &result
			}
			return err
		},
	}
	flags := command.Flags()
	flags.BoolVar(&options.Worker, "worker", false, "Run a tier 3 worker without published ports or ingress")
	flags.StringVarP(&filename, "file", "f", "docker-compose.yml", "Compose file")
	flags.StringVar(&options.Name, "name", "", "App name (default: service name)")
	flags.IntVar(&tier, "tier", 3, "Workload tier: 2 (one active copy with replicated data) or 3 (parallel copies)")
	flags.StringVar(&options.Copies, "copies", "", "Copy count or all_capable (default: deploy.replicas or 1)")
	flags.IntVar(&port, "port", 0, "Ingress port on each device (default: published Compose port)")
	flags.StringSliceVar(&options.Preferred, "prefer", nil, "Preferred device IDs or names, most preferred first")
	flags.StringSliceVar(&options.Allowed, "allow", nil, "Devices the app may also use")
	flags.StringSliceVar(&options.Never, "never", nil, "Devices the app must not use")
	flags.IntVar(&options.Standbys, "standbys", 0, "Tier 2: standby devices that hold data copies (default 1)")
	flags.DurationVar(&options.Interval, "interval", 0, "Tier 2: time between data copies (default 60s)")
	flags.StringVar(&options.Failback, "failback", "", "Tier 2: auto or manual return to the preferred device (default auto)")
	flags.StringVar(&key, "idempotency-key", "", "Reuse a prior key with the same request for recovery")
	group.AddCommand(command)
	group.AddCommand(newAppInspectCommand(a))
	for _, action := range []string{"stop", "start", "delete", "move"} {
		group.AddCommand(newAppActionCommand(a, action))
	}
	return group
}
