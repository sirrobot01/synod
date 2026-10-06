package client

import (
	"context"
	"net/http"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/operation"
)

func (c *Client) GetOperation(ctx context.Context, id uuid.UUID) (operation.Operation, error) {
	var op operation.Operation
	err := c.request(ctx, http.MethodGet, "/v1/operations/"+id.String(), nil, "", http.StatusOK, &op)
	if err == nil && op.ID != id {
		err = &Error{Code: "invalid_response", Message: "The API returned an unexpected operation identity."}
	}
	return op, err
}

func (c *Client) ListOperations(ctx context.Context) ([]operation.Operation, error) {
	var ops []operation.Operation
	err := c.request(ctx, http.MethodGet, "/v1/operations", nil, "", http.StatusOK, &ops)
	return ops, err
}
