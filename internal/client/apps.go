package client

import (
	"context"
	"net/http"
	"strconv"
	"strings"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/app"
)

func (c *Client) CreateApp(ctx context.Context, spec app.AppSpec, key string) (app.Admission, error) {
	var result app.Admission
	if len(key) < 1 || len(key) > 128 || strings.ContainsFunc(key, func(r rune) bool { return r < '!' || r > '~' }) {
		return result, &Error{Code: "invalid_request", Message: "Use an idempotency key with 1 to 128 visible ASCII bytes."}
	}
	err := c.request(ctx, http.MethodPost, "/v1/apps", spec, key, http.StatusAccepted, &result)
	if err == nil && (result.AppID == uuid.Nil || result.OperationID == uuid.Nil || result.DesiredRevision < 1 || result.AcceptedAt.IsZero()) {
		err = &Error{Code: "invalid_response", Message: "The API returned an invalid app admission. Retry with the same key and body."}
	}
	return result, err
}

// ActApp submits a lifecycle action with a fixed revision and retry key.
func (c *Client) ActApp(ctx context.Context, id uuid.UUID, action string, expected int64, key string) (app.Admission, error) {
	var result app.Admission
	if id == uuid.Nil || expected < 1 || (action != "stop" && action != "start" && action != "delete") || len(key) < 1 || len(key) > 128 || strings.ContainsFunc(key, func(r rune) bool { return r < '!' || r > '~' }) {
		return result, &Error{Code: "invalid_request", Message: "Use an app ID, stop/start/delete, a positive revision, and a visible ASCII retry key."}
	}
	method, path := http.MethodPost, "/v1/apps/"+id.String()+"/actions/"+action
	if action == "delete" {
		method, path = http.MethodDelete, "/v1/apps/"+id.String()
	}
	err := c.requestWith(ctx, method, path, struct{}{}, key, strconv.Quote(strconv.FormatInt(expected, 10)), http.StatusAccepted, &result)
	if err == nil && (result.AppID != id || result.OperationID == uuid.Nil || result.DesiredRevision != expected+1 || result.AcceptedAt.IsZero()) {
		err = &Error{Code: "invalid_response", Message: "The API returned an invalid app admission. Retry with the same key and revision."}
	}
	return result, err
}

// GetApp reads current copy health and route acknowledgements.
func (c *Client) GetApp(ctx context.Context, id uuid.UUID) (app.App, error) {
	var result app.App
	if id == uuid.Nil {
		return result, &Error{Code: "invalid_request", Message: "Use a valid app ID."}
	}
	err := c.request(ctx, http.MethodGet, "/v1/apps/"+id.String(), nil, "", http.StatusOK, &result)
	if err == nil && (result.ID != id || result.Status == nil || result.Status.OperationID == uuid.Nil) {
		err = &Error{Code: "invalid_response", Message: "The API returned an invalid app status."}
	}
	return result, err
}
