package cli

import (
	"strings"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/client"
	"github.com/spf13/cobra"
)

// newNodeCommand approves an enrolled node for placement, which is the step between
// enrolling it and it being able to hold work.
func newNodeCommand(api **client.Client, result *Result, prepare func(cmd *cobra.Command, args []string) error) *cobra.Command {
	var policy client.NodePolicy
	var node string
	var expected int64
	var engines []string
	group := &cobra.Command{Use: "node", Short: "Manage enrolled nodes"}
	approve := &cobra.Command{Use: "approve", Short: "Approve one enrolled node for placement", Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			id, err := uuid.Parse(node)
			if err != nil || id == uuid.Nil {
				return &client.Error{Code: "invalid_request", Message: "Use a nonzero node UUID."}
			}
			for _, declared := range engines {
				name, version, ok := strings.Cut(declared, ":")
				if !ok || name == "" || version == "" {
					return &client.Error{Code: "invalid_request", Message: "Declare each engine as engine:version, such as postgres:17.11."}
				}
				policy.Engines = append(policy.Engines, client.EngineVersion{Engine: name, Version: version})
			}
			if len(policy.Engines) == 0 {
				return &client.Error{Code: "invalid_request", Message: "Declare at least one engine the node may run."}
			}
			if err := prepare(cmd, nil); err != nil {
				return err
			}
			value, err := (*api).ApproveNode(cmd.Context(), id, expected, policy)
			if err == nil {
				result.NodePolicy = &value
			}
			return err
		}}
	approve.Flags().StringVar(&node, "node-id", "", "Enrolled node UUID (required)")
	approve.Flags().Int64Var(&expected, "expected-revision", 0, "The policy revision this replaces; 0 for a node with none")
	approve.Flags().StringVar(&policy.Architecture, "architecture", "", "Node CPU architecture, amd64 or arm64 (required)")
	approve.Flags().StringVar(&policy.RuntimeVersion, "runtime-version", "", "Container runtime version the node reports (required)")
	approve.Flags().StringVar(&policy.StorageID, "storage-id", "", "Storage pool identifier (required)")
	approve.Flags().StringVar(&policy.ClientAddress, "client-address", "", "Canonical node IPv4 or IPv6 address for database clients")
	approve.Flags().StringSliceVar(&engines, "engine", nil, "Engine and version the node may run, as engine:version (repeatable, required)")
	// No default headroom: a figure invented here reserves an amount nobody chose.
	approve.Flags().Int64Var(&policy.Headroom.CPUMillis, "headroom-cpu-millis", 0, "CPU millis placement must leave unused (required)")
	approve.Flags().Int64Var(&policy.Headroom.MemoryBytes, "headroom-memory-bytes", 0, "Memory bytes placement must leave unused (required)")
	approve.Flags().Int64Var(&policy.Headroom.DiskBytes, "headroom-disk-bytes", 0, "Disk bytes placement must leave unused (required)")
	approve.Flags().BoolVar(&policy.Cordoned, "cordoned", false, "Approve the node but stop new placement onto it")
	for _, required := range []string{"node-id", "architecture", "runtime-version", "storage-id", "engine"} {
		_ = approve.MarkFlagRequired(required)
	}
	group.AddCommand(approve)
	return group
}
