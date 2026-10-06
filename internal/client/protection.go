package client

import (
	"context"
	"net/http"

	"github.com/sirrobot01/synod/internal/backup"
)

func (c *Client) ProtectionPlan(ctx context.Context) (backup.ProtectionPlan, error) {
	var plan backup.ProtectionPlan
	err := c.request(ctx, http.MethodGet, "/v1/protection", nil, "", http.StatusOK, &plan)
	return plan, err
}

func (c *Client) UpdateProtection(ctx context.Context, input backup.ProtectionUpdate) (backup.ProtectionConfirmation, error) {
	var result backup.ProtectionConfirmation
	err := c.request(ctx, http.MethodPut, "/v1/protection", input, input.ID.String(), http.StatusOK, &result)
	if err == nil && (result.ID != input.ID || result.Revision <= input.ExpectedRevision) {
		err = &Error{Code: "invalid_response", Message: "The API returned an unexpected protection confirmation."}
	}
	return result, err
}
