package client

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"syscall"
	"time"

	"github.com/google/uuid"
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
	Code      string `json:"code"`
	Message   string `json:"message"`
	RequestID string `json:"request_id,omitempty"`
	cause     error
}

func (e *Error) Error() string { return e.Message }

// Unwrap returns what actually failed.
//
// The message is what the operator is shown and stays fixed: it says what to do
// next and never repeats a server detail or a path back at them. The cause is not
// serialized, so it reaches a caller and a debugger without changing the output.
func (e *Error) Unwrap() error { return e.cause }

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
		limit := 1 << 20
		if path == "/v1/apps" {
			limit = 16 << 20
		}
		data, readErr := io.ReadAll(io.LimitReader(response.Body, int64(limit)+1))
		response.Body.Close()
		if readErr != nil {
			failure = &Error{Code: "transport_error", Message: "The API response was interrupted. Retry mutations with the same key, revision, and body.", cause: readErr}
			continue
		}
		if len(data) > limit {
			return &Error{Code: "invalid_response", Message: "The API response exceeds its size limit."}
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
			case "unauthenticated", "invalid_request", "unsupported_spec", "idempotency_conflict", "name_conflict", "operation_conflict", "revision_conflict", "confirmation_required", "not_found", "outcome_unknown", "dependency_unavailable", "internal_error", "request_too_large", "unsupported_media_type", "method_not_allowed", "rate_limited", "node_facts_unavailable", "ambiguous_name", "enrollment_conflict":
				apiError.Code = wire.Code
			}
			if _, err := uuid.Parse(wire.RequestID); err == nil {
				apiError.RequestID = wire.RequestID
			}
		}
		if apiError.Code == "outcome_unknown" {
			apiError.Message = "The admission outcome is unknown. Retry with the same idempotency key, revision, and body."
		} else if apiError.Code == "node_facts_unavailable" {
			apiError.Message = "The node has not reported measured facts. Start its agent and retry, or declare the architecture, runtime version, storage ID, and headroom."
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
