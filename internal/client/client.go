package client

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"os"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/cluster"
	"github.com/sirrobot01/synod/internal/operation"
)

// Config holds the settings of one API client.
type Config struct {
	Endpoint       string
	TokenFile      string
	CAFile         string
	RequestTimeout time.Duration
	Attempts       int
}

// Client calls the administrator HTTP API over verified HTTPS.
type Client struct {
	endpoint string
	token    string
	http     *http.Client
	attempts int
}

// Error is a bounded API failure with a stable code.
type Error struct {
	Code        string    `json:"code"`
	Message     string    `json:"message"`
	RequestID   string    `json:"request_id,omitempty"`
	OperationID uuid.UUID `json:"operation_id,omitzero"`
	cause       error
}

func (e *Error) Error() string { return e.Message }

// Unwrap returns what actually failed.
//
// The message is what the operator is shown and stays fixed: it says what to do
// next and never repeats a server detail or a path back at them. The cause is not
// serialized, so it reaches a caller and a debugger without changing the output.
func (e *Error) Unwrap() error { return e.cause }

// Admission is the accepted result of one create request.
type Admission struct {
	ClusterID       uuid.UUID `json:"cluster_id"`
	OperationID     uuid.UUID `json:"operation_id"`
	DesiredRevision int64     `json:"desired_revision"`
	AcceptedAt      time.Time `json:"accepted_at"`
	Replayed        bool      `json:"replayed"`
}

// New checks the API origin and returns a client that does not follow redirects.
func New(config Config) (*Client, error) {
	endpoint, err := url.Parse(config.Endpoint)
	if err != nil || endpoint.Scheme != "https" || endpoint.Hostname() == "" || endpoint.User != nil ||
		endpoint.RawQuery != "" || endpoint.Fragment != "" || (endpoint.Path != "" && endpoint.Path != "/") {
		return nil, &Error{Code: "configuration_error", Message: "Use an HTTPS API origin without credentials, a path, query, or fragment.", cause: err}
	}
	if config.RequestTimeout <= 0 || config.Attempts < 1 || config.Attempts > 5 {
		return nil, &Error{Code: "configuration_error", Message: "Use a positive request timeout and 1 to 5 attempts."}
	}
	file, err := os.OpenFile(config.TokenFile, os.O_RDONLY|syscall.O_NONBLOCK, 0)
	if err != nil {
		return nil, &Error{Code: "configuration_error", Message: "Read the administrator token file failed.", cause: err}
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0o077 != 0 {
		return nil, &Error{Code: "configuration_error", Message: "The token file must be regular and private to its owner.", cause: err}
	}
	data, err := io.ReadAll(io.LimitReader(file, 259))
	if err != nil || len(data) > 258 {
		return nil, &Error{Code: "configuration_error", Message: "Read a bounded administrator token file failed.", cause: err}
	}
	token := strings.TrimSuffix(strings.TrimSuffix(string(data), "\n"), "\r")
	if len(token) < 32 || len(token) > 256 || strings.ContainsFunc(token, func(r rune) bool { return r < '!' || r > '~' }) {
		return nil, &Error{Code: "configuration_error", Message: "The token must contain 32 to 256 visible ASCII bytes."}
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.TLSClientConfig = &tls.Config{MinVersion: tls.VersionTLS12}
	if config.CAFile != "" {
		ca, err := os.OpenFile(config.CAFile, os.O_RDONLY|syscall.O_NONBLOCK, 0)
		if err != nil {
			return nil, &Error{Code: "configuration_error", Message: "Read the API CA file failed.", cause: err}
		}
		defer ca.Close()
		info, err := ca.Stat()
		if err != nil || !info.Mode().IsRegular() {
			return nil, &Error{Code: "configuration_error", Message: "The CA file must be regular.", cause: err}
		}
		pem, err := io.ReadAll(io.LimitReader(ca, (1<<20)+1))
		if err != nil || len(pem) > 1<<20 {
			return nil, &Error{Code: "configuration_error", Message: "The CA file must not exceed 1 MiB.", cause: err}
		}
		roots, err := x509.SystemCertPool()
		if err != nil {
			roots = x509.NewCertPool()
		}
		if !roots.AppendCertsFromPEM(pem) {
			return nil, &Error{Code: "configuration_error", Message: "The CA file contains no usable certificate."}
		}
		transport.TLSClientConfig.RootCAs = roots
	}
	return &Client{endpoint: strings.TrimSuffix(endpoint.String(), "/"), token: token, attempts: config.Attempts,
		http: &http.Client{Transport: transport, Timeout: config.RequestTimeout,
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}, nil
}

func (c *Client) Close() { c.http.CloseIdleConnections() }

// Create submits one cluster request under an idempotency key.
// Retry with the same key and body after an uncertain outcome.
func (c *Client) Create(ctx context.Context, spec cluster.CreateSpec, key string) (Admission, error) {
	var value Admission
	if len(key) < 1 || len(key) > 128 || strings.ContainsFunc(key, func(r rune) bool { return r < '!' || r > '~' }) {
		return value, &Error{Code: "invalid_request", Message: "Use an idempotency key with 1 to 128 visible ASCII bytes."}
	}
	err := c.request(ctx, http.MethodPost, "/v1/clusters", spec, key, http.StatusAccepted, &value)
	if err == nil && (value.ClusterID == uuid.Nil || value.OperationID == uuid.Nil) {
		err = &Error{Code: "invalid_response", Message: "The API returned an invalid admission response. Retry with the same key and body."}
	}
	return value, err
}

// PlanDelete requests a retained-data deletion plan without admitting an operation.
func (c *Client) PlanDelete(ctx context.Context, id uuid.UUID) (cluster.ActionPlan, error) {
	var value cluster.ActionPlan
	if id == uuid.Nil {
		return value, &Error{Code: "invalid_request", Message: "Use a nonzero cluster UUID."}
	}
	err := c.request(ctx, http.MethodPost, "/v1/clusters/"+id.String()+"/action-plans", struct {
		Action string `json:"action"`
	}{Action: "delete"}, "", http.StatusOK, &value)
	if err == nil && (value.ClusterID != id || value.PlanToken == uuid.Nil || value.ClusterName == "" || value.ExpectedRevision < 1 || value.Action != "delete" || value.DataDisposition != "retain" || value.RetainedVolumes == nil || slices.Contains(value.RetainedVolumes, uuid.Nil) || value.Downtime == "" || value.ExpiresAt.IsZero()) {
		err = &Error{Code: "invalid_response", Message: "The API returned an invalid deletion plan. No deletion request was sent."}
	}
	return value, err
}

// PlanScaleDown reviews the replica a removal would take away.
func (c *Client) PlanScaleDown(ctx context.Context, id uuid.UUID) (cluster.ActionPlan, error) {
	return c.planReplicaAction(ctx, id, "scale_down")
}

// PlanFailedReplicaRemoval reviews removal with native PostgreSQL failure proof.
func (c *Client) PlanFailedReplicaRemoval(ctx context.Context, id uuid.UUID) (cluster.ActionPlan, error) {
	return c.planReplicaAction(ctx, id, "scale_down_failed")
}

// PlanReplicaRebuild reviews replacement while retaining the failed replica's data.
func (c *Client) PlanReplicaRebuild(ctx context.Context, id uuid.UUID) (cluster.ActionPlan, error) {
	return c.planReplicaAction(ctx, id, "rebuild_replica")
}

func (c *Client) planReplicaAction(ctx context.Context, id uuid.UUID, action string) (cluster.ActionPlan, error) {
	var value cluster.ActionPlan
	if id == uuid.Nil {
		return value, &Error{Code: "invalid_request", Message: "Name a cluster."}
	}
	err := c.request(ctx, http.MethodPost, "/v1/clusters/"+id.String()+"/action-plans", struct {
		Action string `json:"action"`
	}{Action: action}, "", http.StatusOK, &value)
	if err == nil && (value.ClusterID != id || value.PlanToken == uuid.Nil || value.MemberID == uuid.Nil ||
		value.ExpectedRevision < 1 || value.Action != action || value.DataDisposition != "retain" ||
		value.Downtime == "" || value.ExpiresAt.IsZero() ||
		(action == "rebuild_replica" && (value.ClusterName == "" || len(value.RetainedVolumes) != 1)) ||
		(action != "scale_down" && (value.ResourceID == uuid.Nil || value.RetainedVolumes == nil || slices.Contains(value.RetainedVolumes, uuid.Nil)))) {
		err = &Error{Code: "invalid_response", Message: "The API returned an invalid replica action plan. No action request was sent."}
	}
	return value, err
}

// Act submits a lifecycle action with a fixed revision, retry key, and required confirmation.
func (c *Client) Act(ctx context.Context, id uuid.UUID, action string, expected int64, key string, confirmation *cluster.ActionConfirmation) (Admission, error) {
	var value Admission
	if id == uuid.Nil || (action != "stop" && action != "start" && action != "delete" && action != "recover-setup" && action != "rebuild-replica") || expected < 1 {
		return value, &Error{Code: "invalid_request", Message: "Name a cluster, a positive expected revision, and one of stop, start, delete, recover-setup, or rebuild-replica."}
	}
	if len(key) < 1 || len(key) > 128 || strings.ContainsFunc(key, func(r rune) bool { return r < '!' || r > '~' }) {
		return value, &Error{Code: "invalid_request", Message: "Use an idempotency key with 1 to 128 visible ASCII bytes."}
	}
	method, path := http.MethodPost, "/v1/clusters/"+id.String()+"/actions/"+action
	var body any = struct{}{}
	if action == "delete" || action == "recover-setup" || action == "rebuild-replica" {
		if confirmation == nil || confirmation.PlanToken == uuid.Nil || !confirmation.Confirm || confirmation.DataDisposition != "retain" {
			return value, &Error{Code: "invalid_request", Message: "Confirm a reviewed plan with data_disposition set to retain."}
		}
		body = confirmation
		if action == "delete" {
			method, path = http.MethodDelete, "/v1/clusters/"+id.String()
		}
	} else if confirmation != nil {
		return value, &Error{Code: "invalid_request", Message: "Use a plan confirmation only for delete, recover-setup, or rebuild-replica."}
	}
	match := strconv.Quote(strconv.FormatInt(expected, 10))
	err := c.requestWith(ctx, method, path, body, key, match, http.StatusAccepted, &value)
	if err == nil && (value.ClusterID != id || value.OperationID == uuid.Nil) {
		err = &Error{Code: "invalid_response", Message: "The API returned an invalid admission response. Retry with the same key."}
	}
	return value, err
}

// GetCluster reads one cluster and checks that the reply names it.
func (c *Client) GetCluster(ctx context.Context, id uuid.UUID) (cluster.Cluster, error) {
	var value cluster.Cluster
	err := c.request(ctx, http.MethodGet, "/v1/clusters/"+id.String(), nil, "", http.StatusOK, &value)
	if err == nil && (id == uuid.Nil || value.ID != id) {
		err = &Error{Code: "invalid_response", Message: "The API returned an unexpected cluster identity."}
	}
	return value, err
}

// GetOperation reads one operation and checks that the reply names it.
func (c *Client) GetOperation(ctx context.Context, id uuid.UUID) (operation.Operation, error) {
	var value operation.Operation
	err := c.request(ctx, http.MethodGet, "/v1/operations/"+id.String(), nil, "", http.StatusOK, &value)
	if err == nil && (id == uuid.Nil || value.ID != id) {
		err = &Error{Code: "invalid_response", Message: "The API returned an unexpected operation identity."}
	}
	return value, err
}

// Wait polls one operation until it ends or ctx ends.
func (c *Client) Wait(ctx context.Context, id uuid.UUID, interval time.Duration) (operation.Operation, error) {
	var last operation.Operation
	if interval <= 0 {
		return last, &Error{Code: "invalid_request", Message: "Use a positive poll interval."}
	}
	for {
		value, err := c.GetOperation(ctx, id)
		if ctx.Err() != nil {
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				return last, &Error{Code: "wait_timeout", Message: "The wait timed out. The server operation was not cancelled.", OperationID: id}
			}
			return last, &Error{Code: "interrupted", Message: "The wait was interrupted. The server operation was not cancelled.", OperationID: id}
		}
		if err != nil {
			return last, err
		}
		last = value
		switch value.Status {
		case operation.Succeeded:
			return value, nil
		case operation.Failed:
			return value, &Error{Code: "operation_failed", Message: "The server operation failed.", OperationID: id}
		case operation.Cancelled:
			return value, &Error{Code: "operation_cancelled", Message: "The server operation was cancelled.", OperationID: id}
		case operation.Pending, operation.Running, operation.Waiting, operation.Cancelling:
		default:
			return last, &Error{Code: "invalid_response", Message: "The API returned an unknown operation status.", OperationID: id}
		}
		select {
		case <-ctx.Done():
		case <-time.After(interval):
		}
	}
}

// request owns authentication, bounded replies, and replay of the same immutable body.
// request sends one call with no revision precondition.
func (c *Client) request(ctx context.Context, method, path string, body any, key string, status int, destination any) error {
	return c.requestWith(ctx, method, path, body, key, "", status, destination)
}

// requestWith sends one call, optionally carrying the revision it expects to replace.
func (c *Client) requestWith(ctx context.Context, method, path string, body any, key, ifMatch string, status int, destination any) error {
	var payload []byte
	if body != nil {
		var err error
		payload, err = json.Marshal(body)
		if err != nil {
			return &Error{Code: "invalid_request", Message: "The request could not be encoded."}
		}
	}
	// Retry only what is safe to repeat. A read is safe, and so is a write that
	// carries an idempotency key. Repeating an unkeyed write can mint a second
	// credential whose secret the caller never sees.
	attempts := c.attempts
	if method != http.MethodGet && method != http.MethodHead && key == "" && ifMatch == "" {
		attempts = 1
	}
	var failure error
	for attempt := range attempts {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(100 * time.Millisecond * time.Duration(1<<uint(attempt-1))):
			}
		}
		request, err := http.NewRequestWithContext(ctx, method, c.endpoint+path, bytes.NewReader(payload))
		if err != nil {
			return &Error{Code: "invalid_request", Message: "The API request could not be constructed."}
		}
		request.Header.Set("Authorization", "Bearer "+c.token)
		request.Header.Set("Accept", "application/json")
		// A revision precondition makes a write safe to repeat: the second attempt
		// carries the same expectation and is refused if the first one landed.
		if ifMatch != "" {
			request.Header.Set("If-Match", ifMatch)
		}
		if body != nil {
			request.Header.Set("Content-Type", "application/json")
		}
		// The retry key is independent of the request body.
		if key != "" {
			request.Header.Set("Idempotency-Key", key)
		}
		response, err := c.http.Do(request)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			failure = &Error{Code: "transport_error", Message: "The HTTPS request failed. Check the API address, connection, and trusted CA.", cause: err}
			continue
		}
		data, readErr := io.ReadAll(io.LimitReader(response.Body, (1<<20)+1))
		response.Body.Close()
		if readErr != nil {
			failure = &Error{Code: "transport_error", Message: "The API response was interrupted. Retry mutations with the same key, revision, and body.", cause: readErr}
			continue
		}
		if len(data) > 1<<20 {
			return &Error{Code: "invalid_response", Message: "The API response exceeds 1 MiB."}
		}
		if response.StatusCode == status {
			// A no-content reply carries no document, so there is nothing to decode.
			if destination == nil || len(data) == 0 {
				return nil
			}
			if err := json.Unmarshal(data, destination); err != nil {
				return &Error{Code: "invalid_response", Message: "The API returned invalid JSON. Retry mutations with the same key, revision, and body.", cause: err}
			}
			return nil
		}
		apiError := &Error{Code: "api_error", Message: "The API rejected the request."}
		var wire Error
		if json.Unmarshal(data, &wire) == nil {
			// Server messages and unrecognized codes may contain private input.
			switch wire.Code {
			case "unauthenticated", "invalid_request", "unsupported_spec", "idempotency_conflict", "name_conflict", "operation_conflict", "revision_conflict", "confirmation_required", "not_found", "outcome_unknown", "dependency_unavailable", "internal_error", "request_too_large", "unsupported_media_type", "method_not_allowed", "rate_limited":
				apiError.Code = wire.Code
			}
			if _, err := uuid.Parse(wire.RequestID); err == nil {
				apiError.RequestID = wire.RequestID
			}
			apiError.OperationID = wire.OperationID
		}
		if apiError.Code == "outcome_unknown" {
			apiError.Message = "The admission outcome is unknown. Retry with the same idempotency key, revision, and body."
		} else if apiError.Code == "revision_conflict" {
			apiError.Message = "The resource revision changed. Review its current state before a new mutation. Keep the original retry inputs for an uncertain mutation."
		}
		failure = apiError
		if response.StatusCode != 429 && response.StatusCode != 502 && response.StatusCode != 503 && response.StatusCode != 504 {
			return failure
		}
	}
	return failure
}

// Scale submits a replica count with a fixed revision and retry key.
//
// It is separate from Act because it carries a body the other actions do not, and
// because a count of zero is a request, not an absent field.
func (c *Client) Scale(ctx context.Context, id uuid.UUID, replicas int, expected int64, key string, confirmation *cluster.ActionConfirmation) (Admission, error) {
	var value Admission
	if id == uuid.Nil || expected < 1 || replicas < 0 || replicas > 1 {
		return value, &Error{Code: "invalid_request", Message: "Name a cluster, a positive expected revision, and zero or one replicas."}
	}
	// Removing a member is destructive, so it carries the plan the operator reviewed.
	if (replicas == 0) != (confirmation != nil) {
		return value, &Error{Code: "invalid_request", Message: "Confirm a reviewed scale-down plan to remove a replica, and use no confirmation to add one."}
	}
	if len(key) < 1 || len(key) > 128 || strings.ContainsFunc(key, func(r rune) bool { return r < '!' || r > '~' }) {
		return value, &Error{Code: "invalid_request", Message: "Use an idempotency key with 1 to 128 visible ASCII bytes."}
	}
	var body any = struct {
		Replicas int `json:"replicas"`
	}{replicas}
	if confirmation != nil {
		if confirmation.PlanToken == uuid.Nil || !confirmation.Confirm || confirmation.DataDisposition != "retain" {
			return value, &Error{Code: "invalid_request", Message: "Confirm a scale-down plan with data_disposition set to retain."}
		}
		body = struct {
			Replicas int `json:"replicas"`
			*cluster.ActionConfirmation
		}{replicas, confirmation}
	}
	match := strconv.Quote(strconv.FormatInt(expected, 10))
	err := c.requestWith(ctx, http.MethodPost, "/v1/clusters/"+id.String()+"/actions/scale", body, key, match, http.StatusAccepted, &value)
	if err == nil && (value.ClusterID != id || value.OperationID == uuid.Nil) {
		err = &Error{Code: "invalid_response", Message: "The API returned an invalid admission response. Retry with the same key."}
	}
	return value, err
}
