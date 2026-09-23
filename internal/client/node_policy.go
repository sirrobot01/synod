package client

import (
	"context"
	"net/http"
	"strconv"

	"github.com/google/uuid"
)

// NodePolicy is what an operator declares about one node. Nothing infers it from the
// host, and the planner refuses a node with no policy.
type NodePolicy struct {
	Architecture   string            `json:"architecture"`
	RuntimeVersion string            `json:"runtime_version"`
	StorageID      string            `json:"storage_id"`
	ClientAddress  string            `json:"client_address,omitempty"`
	Engines        []EngineVersion   `json:"engines"`
	Labels         map[string]string `json:"labels,omitempty"`
	FailureDomains map[string]string `json:"failure_domains,omitempty"`
	Headroom       Vector            `json:"headroom"`
	Cordoned       bool              `json:"cordoned,omitempty"`
	Maintenance    bool              `json:"maintenance,omitempty"`
}

// EngineVersion names one engine and version a node may run.
type EngineVersion struct {
	Engine  string `json:"engine"`
	Version string `json:"version"`
}

// Vector is a resource amount in CPU millis, memory bytes, and disk bytes.
type Vector struct {
	CPUMillis   int64 `json:"cpu_millis"`
	MemoryBytes int64 `json:"memory_bytes"`
	DiskBytes   int64 `json:"disk_bytes"`
}

// NodePolicyResult is what the API returns after a policy is approved.
type NodePolicyResult struct {
	NodeID   uuid.UUID `json:"node_id"`
	Revision int64     `json:"revision"`
}

// ApproveNode gives one enrolled node a placement policy. The expected revision is
// required rather than defaulted; a first write expects 0.
func (c *Client) ApproveNode(ctx context.Context, nodeID uuid.UUID, expected int64, policy NodePolicy) (NodePolicyResult, error) {
	var value NodePolicyResult
	if nodeID == uuid.Nil || expected < 0 {
		return value, &Error{Code: "invalid_request", Message: "A node ID and a non-negative expected revision are required."}
	}
	err := c.requestWith(ctx, http.MethodPut, "/v1/nodes/"+nodeID.String()+"/placement-policy",
		policy, "", strconv.Quote(strconv.FormatInt(expected, 10)), http.StatusOK, &value)
	return value, err
}
