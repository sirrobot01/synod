package client

import (
	"context"
	"net/http"
	"net/url"
	"time"

	"github.com/google/uuid"
)

// Node is one enrolled device and its last known presence.
type Node struct {
	ID                 uuid.UUID  `json:"id"`
	Name               string     `json:"name"`
	Architecture       string     `json:"architecture"`
	RuntimeVersion     string     `json:"runtime_version"`
	AgentVersion       string     `json:"agent_version"`
	DestinationAddress string     `json:"destination_address,omitempty"`
	EnrolledAt         time.Time  `json:"enrolled_at"`
	ApprovedAt         *time.Time `json:"approved_at"`
	ConnectedAt        *time.Time `json:"connected_at"`
	LastSeenAt         *time.Time `json:"last_seen_at"`
}

// ListNodes reads every enrolled node.
func (c *Client) ListNodes(ctx context.Context) ([]Node, error) {
	var nodes []Node
	err := c.request(ctx, http.MethodGet, "/v1/nodes", nil, "", http.StatusOK, &nodes)
	return nodes, err
}

// ResolveNode reads a node by its UUID or, failing that, by its name.
func (c *Client) ResolveNode(ctx context.Context, reference string) (Node, error) {
	var node Node
	path := "/v1/nodes/by-name/" + url.PathEscape(reference)
	if id, err := uuid.Parse(reference); err == nil && id != uuid.Nil {
		path = "/v1/nodes/" + id.String()
	}
	err := c.request(ctx, http.MethodGet, path, nil, "", http.StatusOK, &node)
	return node, err
}

// ApproveNode trusts one enrolled node. Repeat approval returns the node unchanged.
func (c *Client) ApproveNode(ctx context.Context, id uuid.UUID) (Node, error) {
	var node Node
	err := c.request(ctx, http.MethodPost, "/v1/nodes/"+id.String()+"/actions/approve", struct{}{}, "", http.StatusOK, &node)
	if err == nil && node.ID != id {
		err = &Error{Code: "invalid_response", Message: "The API returned an unexpected node identity."}
	}
	return node, err
}

// RenameNode gives a device a unique name.
func (c *Client) RenameNode(ctx context.Context, id uuid.UUID, name string) (Node, error) {
	var node Node
	err := c.request(ctx, http.MethodPatch, "/v1/nodes/"+id.String(), struct {
		Name string `json:"name"`
	}{name}, "", http.StatusOK, &node)
	return node, err
}
