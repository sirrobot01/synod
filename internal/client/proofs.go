package client

import (
	"context"
	"github.com/sirrobot01/synod/internal/operation"
	"github.com/sirrobot01/synod/internal/proof"
	"net/http"
)

func (c *Client) CreateProof(ctx context.Context, input proof.Submission) (operation.Operation, error) {
	var result operation.Operation
	err := c.request(ctx, http.MethodPost, "/v1/proofs", input, input.ID.String(), http.StatusOK, &result)
	if err == nil && (result.ID != input.ID || result.AppID != input.AppID || result.NodeID != input.NodeID || result.Kind != string(input.Kind)) {
		err = &Error{Code: "invalid_response", Message: "The API returned an unexpected proof operation."}
	}
	return result, err
}
