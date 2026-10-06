package client

import (
	"context"
	"net/http"

	"github.com/sirrobot01/synod/internal/backup"
	"github.com/sirrobot01/synod/internal/operation"
)

func (c *Client) CreateBackup(ctx context.Context, input backup.Submission) (operation.Operation, error) {
	var result operation.Operation
	err := c.request(ctx, http.MethodPost, "/v1/backups", input, input.ID.String(), http.StatusOK, &result)
	if err == nil && (result.ID != input.ID || result.AppID != input.AppID || result.Kind != "backup") {
		err = &Error{Code: "invalid_response", Message: "The API returned an unexpected backup operation."}
	}
	return result, err
}
