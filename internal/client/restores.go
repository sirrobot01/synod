package client

import (
	"context"
	"github.com/sirrobot01/synod/internal/operation"
	"github.com/sirrobot01/synod/internal/restore"
	"net/http"
)

func (c *Client) CreateRestore(ctx context.Context, input restore.Submission) (operation.Operation, error) {
	var result operation.Operation
	err := c.request(ctx, http.MethodPost, "/v1/restores", input, input.ID.String(), http.StatusOK, &result)
	if err == nil && (result.ID != input.ID || result.AppID != input.AppID || result.NodeID != input.NodeID || result.Kind != "restore") {
		err = &Error{Code: "invalid_response", Message: "The API returned an unexpected restore operation."}
	}
	return result, err
}
