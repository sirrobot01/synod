package enrollment

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
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
	token, hash, err := readToken(config.TokenFile)
	if err != nil {
		return Credentials{}, err
	}
	if len(s.state.TokenHash) == 0 {
		next := s.state
		next.TokenHash = hash[:]
		if err := s.persist(next); err != nil {
			return Credentials{}, err
		}
	} else if !bytes.Equal(s.state.TokenHash, hash[:]) {
		return Credentials{}, fmt.Errorf("%w: this state was started with a different enrollment token; keep the saved request", ErrConflict)
	}
	enrolled, err := s.exchange(ctx, config, s.state.Body, token)
	if err != nil {
		return Credentials{}, err
	}
	next := s.state
	next.Response = &enrolled
	if err := s.persist(next); err != nil {
		return Credentials{}, err
	}
	return s.credentials()
}

// RenewWithToken gets a new certificate for the enrolled key with a device-bound join token.
// It recovers a device whose certificate expired while it was offline. Its approval stays.
// The request repeats exactly on a retry: Ed25519 signs deterministically, and the request
// ID comes from the node and the token.
func (s *State) RenewWithToken(ctx context.Context, config Config) (Credentials, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failed || s.closed || s.state.Response == nil {
		return Credentials{}, fmt.Errorf("%w: this handle is closed, poisoned, or holds no accepted enrollment", ErrState)
	}
	token, hash, err := readToken(config.TokenFile)
	if err != nil {
		return Credentials{}, err
	}
	key, err := x509.ParsePKCS8PrivateKey(s.state.KeyDER)
	if err != nil {
		return Credentials{}, fmt.Errorf("%w: parse the saved node key: %w", ErrState, err)
	}
	signer, ok := key.(ed25519.PrivateKey)
	if !ok {
		return Credentials{}, fmt.Errorf("%w: the saved node key is not Ed25519", ErrState)
	}
	csr, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{}, signer)
	if err != nil {
		return Credentials{}, fmt.Errorf("%w: create the renewal request: %w", ErrState, err)
	}
	body, err := json.Marshal(Request{NodeID: s.state.Identity.NodeID, RequestID: uuid.NewSHA1(s.state.Identity.NodeID, hash[:]), CSRDER: csr})
	if err != nil {
		return Credentials{}, fmt.Errorf("%w: encode the renewal request: %w", ErrState, err)
	}
	renewed, err := s.exchange(ctx, config, body, token)
	if err != nil {
		return Credentials{}, err
	}
	renewed.AgentEndpoint = s.state.Response.AgentEndpoint
	next := s.state
	next.Response = &renewed
	if err := s.persist(next); err != nil {
		return Credentials{}, err
	}
	return s.credentials()
}

func readToken(path string) (string, [32]byte, error) {
	file, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if err != nil {
		return "", [32]byte{}, fmt.Errorf("%w: open the enrollment token file: %w", ErrConfiguration, err)
	}
	data, readErr := readPrivate(file, 45)
	file.Close()
	if readErr != nil {
		return "", [32]byte{}, fmt.Errorf("%w: read the enrollment token file: %w", ErrConfiguration, readErr)
	}
	token := strings.TrimSuffix(strings.TrimSuffix(string(data), "\n"), "\r")
	decoded, err := base64.RawURLEncoding.DecodeString(token)
	// The token itself never appears here. Its shape is the fault to report.
	if err != nil || len(decoded) != 32 || len(token) != 43 || base64.RawURLEncoding.EncodeToString(decoded) != token {
		return "", [32]byte{}, fmt.Errorf("%w: the enrollment token must be 43 characters of raw URL base64 over 32 bytes", ErrConfiguration)
	}
	return token, sha256.Sum256([]byte(token)), nil
}

// exchange posts one enrollment body and checks the issued certificate.
func (s *State) exchange(ctx context.Context, config Config, body []byte, token string) (Response, error) {
	origin, err := url.Parse(config.Endpoint)
	if err != nil {
		return Response{}, fmt.Errorf("%w: parse the enrollment endpoint: %w", ErrConfiguration, err)
	}
	if origin.Scheme != "https" || origin.Hostname() == "" || origin.User != nil || origin.RawQuery != "" || origin.Fragment != "" || (origin.Path != "" && origin.Path != "/") {
		return Response{}, fmt.Errorf("%w: the enrollment endpoint must be an HTTPS origin without credentials, a path, a query, or a fragment", ErrConfiguration)
	}
	if config.RequestTimeout == 0 {
		config.RequestTimeout = 10 * time.Second
	}
	if config.Attempts == 0 {
		config.Attempts = 3
	}
	if config.RequestTimeout <= 0 || config.RequestTimeout > time.Minute || config.Attempts < 1 || config.Attempts > 5 {
		return Response{}, fmt.Errorf("%w: use a request timeout of up to a minute and 1 to 5 attempts", ErrConfiguration)
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
				return Response{}, ctx.Err()
			case <-time.After(delay):
			}
			delay = min(delay*2, time.Second)
		}
		request, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimSuffix(origin.String(), "/")+"/v1/enrollments", bytes.NewReader(body))
		if err != nil {
			return Response{}, fmt.Errorf("%w: build the enrollment request: %w", ErrConfiguration, err)
		}
		request.Header.Set("Authorization", "Bearer "+token)
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("Accept", "application/json")
		response, err := client.Do(request)
		if err != nil {
			if ctx.Err() != nil {
				return Response{}, ctx.Err()
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
			return Response{}, fmt.Errorf("%w: the enrollment response exceeds 128 KiB", ErrCertificate)
		}
		if response.StatusCode == http.StatusCreated {
			var enrolled Response
			if err := json.Unmarshal(data, &enrolled); err != nil {
				return Response{}, fmt.Errorf("%w: decode the enrollment response: %w", ErrCertificate, err)
			}
			if err := s.validateCertificate(&enrolled); err != nil {
				return Response{}, err
			}
			return enrolled, nil
		}
		switch response.StatusCode {
		case http.StatusUnauthorized, http.StatusForbidden:
			return Response{}, fmt.Errorf("%w: the controller answered %d", ErrUnauthorized, response.StatusCode)
		case http.StatusConflict:
			return Response{}, fmt.Errorf("%w: the controller answered %d", ErrConflict, response.StatusCode)
		case http.StatusTooManyRequests:
			failure = ErrUnavailable
			delay = time.Second
		case http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
			failure = ErrUnavailable
		default:
			return Response{}, fmt.Errorf("%w: the controller answered %d", ErrUnavailable, response.StatusCode)
		}
	}
	return Response{}, failure
}
