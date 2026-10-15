package client

import (
	"context"
	"net/http"
	"net/url"
	"strconv"

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

// ListOperations reads at most limit operations that activity shows, newest first.
// A nonzero before reads the operations older than that one.
func (c *Client) ListOperations(ctx context.Context, limit int, before uuid.UUID) ([]operation.Operation, error) {
	query := url.Values{"limit": {strconv.Itoa(limit)}}
	if before != uuid.Nil {
		query.Set("before", before.String())
	}
	var ops []operation.Operation
	err := c.request(ctx, http.MethodGet, "/v1/operations?"+query.Encode(), nil, "", http.StatusOK, &ops)
	return ops, err
}

// RemoveOperation hides one finished operation from activity.
func (c *Client) RemoveOperation(ctx context.Context, id uuid.UUID) error {
	return c.request(ctx, http.MethodDelete, "/v1/operations/"+id.String(), nil, "", http.StatusNoContent, nil)
}

// ClearOperations hides every finished operation from activity and returns how many it hid.
func (c *Client) ClearOperations(ctx context.Context) (int64, error) {
	var result struct {
		Removed int64 `json:"removed"`
	}
	err := c.request(ctx, http.MethodPost, "/v1/operations/actions/clear", struct{}{}, "", http.StatusOK, &result)
	return result.Removed, err
}
