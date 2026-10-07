package client

import (
	"context"
	"github.com/sirrobot01/synod/internal/notification"
	"net/http"
)

func (c *Client) NotificationStatus(ctx context.Context) (notification.Status, error) {
	var result notification.Status
	err := c.request(ctx, http.MethodGet, "/v1/notifications", nil, "", http.StatusOK, &result)
	return result, err
}
func (c *Client) ConfigureNotifications(ctx context.Context, input notification.Update) (notification.Confirmation, error) {
	var result notification.Confirmation
	err := c.request(ctx, http.MethodPut, "/v1/notifications", input, input.ID.String(), http.StatusOK, &result)
	if err == nil && (result.ID != input.ID || result.Revision != input.ExpectedRevision+1) {
		err = &Error{Code: "invalid_response", Message: "The API returned an unexpected notification revision."}
	}
	return result, err
}
func (c *Client) TestNotification(ctx context.Context, input notification.TestRequest) (notification.Confirmation, error) {
	var result notification.Confirmation
	err := c.request(ctx, http.MethodPost, "/v1/notifications/test", input, input.ID.String(), http.StatusOK, &result)
	if err == nil && (result.ID != input.ID || result.Revision != input.ExpectedRevision) {
		err = &Error{Code: "invalid_response", Message: "The API returned an unexpected test notification."}
	}
	return result, err
}
