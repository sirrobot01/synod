package cli

import (
	"strings"

	"github.com/sirrobot01/synod/internal/client"
	"github.com/spf13/cobra"
)

// newNodeCommand approves an enrolled node for placement, which is the step between
// enrolling it and it being able to hold work.
func newNodeCommand(app *app) *cobra.Command {
	var policy client.NodePolicy
	var node string
	var expected int64
	var engines, domains []string
	group := &cobra.Command{Use: "node", Short: "Manage enrolled nodes"}
	approve := &cobra.Command{Use: "approve [NODE_ID]", Short: "Approve one enrolled node for placement", Args: cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) == 1 && node == "" {
				node = args[0]
			}
			id, err := parseID(node, "Use a nonzero node UUID.")
			if err != nil {
				return err
			}
			for _, declared := range engines {
				name, version, ok := strings.Cut(declared, ":")
				if !ok || name == "" || version == "" {
					return &client.Error{Code: "invalid_request", Message: "Declare each engine as engine:version, such as postgres:17.11."}
				}
				policy.Engines = append(policy.Engines, client.EngineVersion{Engine: name, Version: version})
			}
			for _, declared := range domains {
				key, value, ok := strings.Cut(declared, "=")
				if !ok || key == "" || value == "" {
					return &client.Error{Code: "invalid_request", Message: "Declare each failure domain as key=value, such as host=rack1-server2."}
				}
				if _, repeated := policy.FailureDomains[key]; repeated {
					return &client.Error{Code: "invalid_request", Message: "Declare each failure-domain key once."}
				}
				if policy.FailureDomains == nil {
					policy.FailureDomains = make(map[string]string)
				}
				policy.FailureDomains[key] = value
			}
			if err := app.connect(cmd, nil); err != nil {
				return err
			}
			value, err := app.api.ApproveNode(cmd.Context(), id, expected, policy)
			if err == nil {
				app.result.NodePolicy = &value
			}
			return err
		}}
	approve.Flags().StringVar(&node, "node-id", "", "Enrolled node UUID")
	approve.Flags().Int64Var(&expected, "expected-revision", 0, "The policy revision this replaces; 0 for a node with none")
	approve.Flags().StringVar(&policy.Architecture, "architecture", "", "Node CPU architecture, amd64 or arm64")
	approve.Flags().StringVar(&policy.RuntimeVersion, "runtime-version", "", "Container runtime version the node reports")
	approve.Flags().StringVar(&policy.StorageID, "storage-id", "", "Storage pool identifier")
	approve.Flags().Uint16Var(&policy.ReplicationPort, "replication-port", 0, "Agent replication port; 0 selects 8445")
	approve.Flags().StringVar(&policy.ClientAddress, "client-address", "", "Canonical node IPv4 or IPv6 address for database clients")
	approve.Flags().StringSliceVar(&engines, "engine", nil, "Engine and version the node may run, as engine:version (repeatable)")
	// HA placement spreads members across distinct values of one key, such as host.
	approve.Flags().StringArrayVar(&domains, "failure-domain", nil, "Failure domain as key=value, such as host=rack1-server2 (repeatable)")
	approve.Flags().Int64Var(&policy.Headroom.CPUMillis, "headroom-cpu-millis", 0, "CPU millis placement must leave unused")
	approve.Flags().Int64Var(&policy.Headroom.MemoryBytes, "headroom-memory-bytes", 0, "Memory bytes placement must leave unused")
	approve.Flags().Int64Var(&policy.Headroom.DiskBytes, "headroom-disk-bytes", 0, "Disk bytes placement must leave unused")
	approve.Flags().BoolVar(&policy.Cordoned, "cordoned", false, "Approve the node but stop new placement onto it")
	group.AddCommand(approve, newJoinTokenCommand(app))
	return group
}
