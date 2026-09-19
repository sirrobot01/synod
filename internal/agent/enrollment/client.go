package enrollment

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
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
		return Credentials{}, ErrState
	}
	if s.state.Response != nil {
		return s.credentials()
	}
	origin, err := url.Parse(config.Endpoint)
	if err != nil || origin.Scheme != "https" || origin.Hostname() == "" || origin.User != nil || origin.RawQuery != "" || origin.Fragment != "" || (origin.Path != "" && origin.Path != "/") {
		return Credentials{}, ErrConfiguration
	}
	if config.RequestTimeout == 0 {
		config.RequestTimeout = 10 * time.Second
	}
	if config.Attempts == 0 {
		config.Attempts = 3
	}
	if config.RequestTimeout <= 0 || config.RequestTimeout > time.Minute || config.Attempts < 1 || config.Attempts > 5 {
		return Credentials{}, ErrConfiguration
	}
	file, err := os.OpenFile(config.TokenFile, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if err != nil {
		return Credentials{}, ErrConfiguration
	}
	data, readErr := readPrivate(file, 45)
	file.Close()
	if readErr != nil {
		return Credentials{}, ErrConfiguration
	}
	token := strings.TrimSuffix(strings.TrimSuffix(string(data), "\n"), "\r")
	decoded, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil || len(decoded) != 32 || len(token) != 43 || base64.RawURLEncoding.EncodeToString(decoded) != token {
		return Credentials{}, ErrConfiguration
	}
	hash := sha256.Sum256([]byte(token))
	if len(s.state.TokenHash) == 0 {
		next := s.state
		next.TokenHash = hash[:]
		if err := s.persist(next); err != nil {
			return Credentials{}, err
		}
	} else if !bytes.Equal(s.state.TokenHash, hash[:]) {
		return Credentials{}, ErrConflict
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
			return Credentials{}, ErrConfiguration
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
			return Credentials{}, ErrCertificate
		}
		if response.StatusCode == http.StatusCreated {
			var enrolled Response
			if err := json.Unmarshal(data, &enrolled); err != nil {
				return Credentials{}, ErrCertificate
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
			return Credentials{}, ErrUnauthorized
		case http.StatusConflict:
			return Credentials{}, ErrConflict
		case http.StatusTooManyRequests:
			failure = ErrUnavailable
			delay = time.Second
		case http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
			failure = ErrUnavailable
		default:
			return Credentials{}, ErrUnavailable
		}
	}
	return Credentials{}, failure
}
