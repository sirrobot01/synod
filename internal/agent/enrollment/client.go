package enrollment

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"syscall"
	"time"
)

// Config holds the settings of one enrollment HTTPS client.
type Config struct {
	Endpoint  string
	TokenFile string
	// RootCAs is API trust supplied out of band. Nil uses system trust.
	RootCAs        *x509.CertPool
	RequestTimeout time.Duration
	Attempts       int
}

// Submit serializes enrollment and local completion. It never creates a new request.
// Reopen the same directory after an uncertain response or failed local write.
func (s *State) Submit(ctx context.Context, config Config) (Credentials, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failed || s.closed {
		return Credentials{}, fmt.Errorf("%w: this handle is closed or poisoned by an earlier failed write", ErrState)
	}
	if s.state.Response != nil {
		return s.credentials()
	}
	origin, err := url.Parse(config.Endpoint)
	if err != nil {
		return Credentials{}, fmt.Errorf("%w: parse the enrollment endpoint: %w", ErrConfiguration, err)
	}
	if origin.Scheme != "https" || origin.Hostname() == "" || origin.User != nil || origin.RawQuery != "" || origin.Fragment != "" || (origin.Path != "" && origin.Path != "/") {
		return Credentials{}, fmt.Errorf("%w: the enrollment endpoint must be an HTTPS origin without credentials, a path, a query, or a fragment", ErrConfiguration)
	}
	if config.RequestTimeout == 0 {
		config.RequestTimeout = 10 * time.Second
	}
	if config.Attempts == 0 {
		config.Attempts = 3
	}
	if config.RequestTimeout <= 0 || config.RequestTimeout > time.Minute || config.Attempts < 1 || config.Attempts > 5 {
		return Credentials{}, fmt.Errorf("%w: use a request timeout of up to a minute and 1 to 5 attempts", ErrConfiguration)
	}
	file, err := os.OpenFile(config.TokenFile, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if err != nil {
		return Credentials{}, fmt.Errorf("%w: open the enrollment token file: %w", ErrConfiguration, err)
	}
	data, readErr := readPrivate(file, 45)
	file.Close()
	if readErr != nil {
		return Credentials{}, fmt.Errorf("%w: read the enrollment token file: %w", ErrConfiguration, readErr)
	}
	token := strings.TrimSuffix(strings.TrimSuffix(string(data), "\n"), "\r")
	decoded, err := base64.RawURLEncoding.DecodeString(token)
	// The token itself never appears here. Its shape is the fault to report.
	if err != nil || len(decoded) != 32 || len(token) != 43 || base64.RawURLEncoding.EncodeToString(decoded) != token {
		return Credentials{}, fmt.Errorf("%w: the enrollment token must be 43 characters of raw URL base64 over 32 bytes", ErrConfiguration)
	}
	hash := sha256.Sum256([]byte(token))
	if len(s.state.TokenHash) == 0 {
		next := s.state
		next.TokenHash = hash[:]
		if err := s.persist(next); err != nil {
			return Credentials{}, err
		}
	} else if !bytes.Equal(s.state.TokenHash, hash[:]) {
		return Credentials{}, fmt.Errorf("%w: this state was started with a different enrollment token; keep the saved request", ErrConflict)
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.TLSClientConfig = &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: config.RootCAs}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: config.RequestTimeout, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	delay := 100 * time.Millisecond
	failure := ErrUnavailable
	for attempt := range config.Attempts {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return Credentials{}, ctx.Err()
			case <-time.After(delay):
			}
			delay = min(delay*2, time.Second)
		}
		request, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimSuffix(origin.String(), "/")+"/v1/enrollments", bytes.NewReader(s.state.Body))
		if err != nil {
			return Credentials{}, fmt.Errorf("%w: build the enrollment request: %w", ErrConfiguration, err)
		}
		request.Header.Set("Authorization", "Bearer "+token)
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("Accept", "application/json")
		response, err := client.Do(request)
		if err != nil {
			if ctx.Err() != nil {
				return Credentials{}, ctx.Err()
			}
			failure = ErrTransport
			continue
		}
		data, readErr := io.ReadAll(io.LimitReader(response.Body, (128<<10)+1))
		response.Body.Close()
		if readErr != nil {
			failure = ErrTransport
			continue
		}
		if len(data) > 128<<10 {
			return Credentials{}, fmt.Errorf("%w: the enrollment response exceeds 128 KiB", ErrCertificate)
		}
		if response.StatusCode == http.StatusCreated {
			var enrolled Response
			if err := json.Unmarshal(data, &enrolled); err != nil {
				return Credentials{}, fmt.Errorf("%w: decode the enrollment response: %w", ErrCertificate, err)
			}
			if err := s.validateCertificate(&enrolled); err != nil {
				return Credentials{}, err
			}
			next := s.state
			next.Response = &enrolled
			if err := s.persist(next); err != nil {
				return Credentials{}, err
			}
			return s.credentials()
		}
		switch response.StatusCode {
		case http.StatusUnauthorized, http.StatusForbidden:
			return Credentials{}, fmt.Errorf("%w: the controller answered %d", ErrUnauthorized, response.StatusCode)
		case http.StatusConflict:
			return Credentials{}, fmt.Errorf("%w: the controller answered %d", ErrConflict, response.StatusCode)
		case http.StatusTooManyRequests:
			failure = ErrUnavailable
			delay = time.Second
		case http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
			failure = ErrUnavailable
		default:
			return Credentials{}, fmt.Errorf("%w: the controller answered %d", ErrUnavailable, response.StatusCode)
		}
	}
	return Credentials{}, failure
}
