package repository

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"slices"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/blob/sharded"
	"github.com/sirrobot01/synod/internal/pki"
)

// DeviceLocation selects a directory served by another enrolled agent.
// Endpoint is resolved from the current device plan before opening the repository.
type DeviceLocation struct {
	NodeID            uuid.UUID `json:"node_id"`
	DestinationID     uuid.UUID `json:"destination_id"`
	Endpoint          string    `json:"-"`
	CertificateSHA256 []string  `json:"-"`
}

// DeviceIdentity stays on its owning agent. It is never part of destination delivery.
type DeviceIdentity struct {
	InstallationID uuid.UUID `json:"installation_id"`
	CertificatePEM string    `json:"certificate_pem"`
	PrivateKeyPEM  string    `json:"private_key_pem"`
	CAPEM          string    `json:"ca_pem"`
}

func (DeviceIdentity) String() string   { return "[private device identity]" }
func (DeviceIdentity) GoString() string { return "[private device identity]" }

// DeviceFile is the bounded metadata returned by a directory listing.
type DeviceFile struct {
	Filename   string    `json:"name"`
	SizeBytes  int64     `json:"size"`
	ModifiedAt time.Time `json:"modified_at"`
	Directory  bool      `json:"directory"`
}

func (f DeviceFile) Name() string       { return f.Filename }
func (f DeviceFile) Size() int64        { return f.SizeBytes }
func (f DeviceFile) ModTime() time.Time { return f.ModifiedAt }
func (f DeviceFile) IsDir() bool        { return f.Directory }
func (f DeviceFile) Sys() any           { return nil }
func (f DeviceFile) Mode() os.FileMode {
	if f.Directory {
		return os.ModeDir | 0700
	}
	return 0600
}

// deviceOptions is held only in Kopia's private temporary connection file.
type deviceOptions struct {
	NodeID            uuid.UUID      `json:"node_id"`
	DestinationID     uuid.UUID      `json:"destination_id"`
	Endpoint          string         `json:"endpoint"`
	Identity          DeviceIdentity `json:"identity"`
	CertificateSHA256 []string       `json:"certificate_sha256"`
}

type deviceStorage struct {
	sharded.Storage
	blob.DefaultProviderImplementation
	options deviceOptions
	client  *http.Client
}

func init() { blob.AddSupportedStorage("synod-device", deviceOptions{}, newDevice) }

func newDevice(ctx context.Context, options *deviceOptions, create bool) (blob.Storage, error) {
	endpoint, err := url.Parse(options.Endpoint)
	if err != nil || endpoint.Scheme != "https" || endpoint.Host == "" || endpoint.User != nil || endpoint.Path != "" || endpoint.RawQuery != "" || endpoint.Fragment != "" || options.NodeID == uuid.Nil || options.DestinationID == uuid.Nil || options.Identity.InstallationID == uuid.Nil || len(options.CertificateSHA256) == 0 || len(options.CertificateSHA256) > 256 {
		return nil, ErrLocation
	}
	pair, err := tls.X509KeyPair([]byte(options.Identity.CertificatePEM), []byte(options.Identity.PrivateKeyPEM))
	if err != nil {
		return nil, ErrCredentials
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM([]byte(options.Identity.CAPEM)) {
		return nil, ErrCredentials
	}
	// Agent certificates carry a URI identity. Verify the chain and that exact URI.
	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{pair}, InsecureSkipVerify: true, VerifyConnection: func(state tls.ConnectionState) error {
		if len(state.PeerCertificates) == 0 {
			return pki.ErrIdentity
		}
		intermediates := x509.NewCertPool()
		for _, cert := range state.PeerCertificates[1:] {
			intermediates.AddCert(cert)
		}
		chains, err := state.PeerCertificates[0].Verify(x509.VerifyOptions{Roots: roots, Intermediates: intermediates, KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}})
		if err != nil {
			return pki.ErrCertificate
		}
		state.VerifiedChains = chains
		state.HandshakeComplete = true
		hash := sha256.Sum256(state.PeerCertificates[0].Raw)
		if !slices.Contains(options.CertificateSHA256, hex.EncodeToString(hash[:])) {
			return pki.ErrIdentity
		}
		identity, err := pki.PeerIdentity(state, options.Identity.InstallationID, pki.Agent)
		if err != nil || identity.SubjectID != options.NodeID {
			return pki.ErrIdentity
		}
		return nil
	}}
	// An index write first makes every pack durable. On an SD card or a busy disk that can
	// take minutes, so the timeouts allow a slow disk and still end a dead connection.
	transport := &http.Transport{TLSClientConfig: tlsConfig, TLSHandshakeTimeout: 10 * time.Second, ResponseHeaderTimeout: 5 * time.Minute, MaxConnsPerHost: 8, MaxIdleConnsPerHost: 8, IdleConnTimeout: time.Minute}
	s := &deviceStorage{options: *options, client: &http.Client{Transport: transport, Timeout: 10 * time.Minute, CheckRedirect: func(*http.Request, []*http.Request) error { return errors.New("device storage redirect refused") }}}
	s.Storage = sharded.New(s, ".", sharded.Options{}, create)
	response, err := s.request(ctx, http.MethodGet, "", nil, nil)
	if err != nil {
		s.Close(ctx)
		return nil, err
	}
	response.Body.Close()
	return s, nil
}

func (s *deviceStorage) ConnectionInfo() blob.ConnectionInfo {
	return blob.ConnectionInfo{Type: "synod-device", Config: &s.options}
}
func (s *deviceStorage) DisplayName() string         { return "Synod device repository" }
func (s *deviceStorage) Close(context.Context) error { s.client.CloseIdleConnections(); return nil }

func (s *deviceStorage) request(ctx context.Context, method, file string, query url.Values, body blob.Bytes) (*http.Response, error) {
	if query == nil {
		query = url.Values{}
	}
	if file != "" {
		query.Set("path", path.Clean(file))
	}
	endpoint := s.options.Endpoint + "/v1/repositories/" + s.options.DestinationID.String() + "?" + query.Encode()
	var reader io.ReadSeekCloser
	if body != nil {
		reader = body.Reader()
		defer reader.Close()
	}
	request, err := http.NewRequestWithContext(ctx, method, endpoint, reader)
	if err != nil {
		return nil, err
	}
	if body != nil {
		request.ContentLength = int64(body.Length())
	}
	response, err := s.client.Do(request)
	if err != nil {
		return nil, err
	}
	if response.StatusCode >= 200 && response.StatusCode < 300 {
		return response, nil
	}
	response.Body.Close()
	switch response.StatusCode {
	case http.StatusNotFound:
		return nil, blob.ErrBlobNotFound
	case http.StatusRequestedRangeNotSatisfiable:
		return nil, blob.ErrInvalidRange
	case http.StatusForbidden, http.StatusUnauthorized:
		return nil, os.ErrPermission
	default:
		return nil, fmt.Errorf("device storage returned HTTP %d", response.StatusCode)
	}
}

func (s *deviceStorage) GetBlobFromPath(ctx context.Context, _, file string, offset, length int64, out blob.OutputBuffer) error {
	response, err := s.request(ctx, http.MethodGet, file, url.Values{"offset": {strconv.FormatInt(offset, 10)}, "length": {strconv.FormatInt(length, 10)}}, nil)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	out.Reset()
	if _, err := io.Copy(out, io.LimitReader(response.Body, (64<<20)+1)); err != nil {
		return err
	}
	if out.Length() > 64<<20 {
		return errors.New("device blob exceeds 64 MiB")
	}
	return blob.EnsureLengthExactly(out.Length(), length)
}

func (s *deviceStorage) GetMetadataFromPath(ctx context.Context, _, file string) (blob.Metadata, error) {
	response, err := s.request(ctx, http.MethodHead, file, nil, nil)
	if err != nil {
		return blob.Metadata{}, err
	}
	defer response.Body.Close()
	modified, err := time.Parse(time.RFC3339Nano, response.Header.Get("X-Synod-Modified"))
	if err != nil || response.ContentLength < 0 {
		return blob.Metadata{}, errors.New("invalid device blob metadata")
	}
	return blob.Metadata{Length: response.ContentLength, Timestamp: modified}, nil
}

func (s *deviceStorage) PutBlobInPath(ctx context.Context, _, file string, data blob.Bytes, options blob.PutOptions) error {
	if options.HasRetentionOptions() || options.DoNotRecreate {
		return blob.ErrUnsupportedPutBlobOption
	}
	if data.Length() > 64<<20 {
		return errors.New("device blob exceeds 64 MiB")
	}
	query := url.Values{}
	if !options.SetModTime.IsZero() {
		query.Set("modified", options.SetModTime.UTC().Format(time.RFC3339Nano))
	}
	response, err := s.request(ctx, http.MethodPut, file, query, data)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if options.GetModTime != nil {
		modified, err := time.Parse(time.RFC3339Nano, response.Header.Get("X-Synod-Modified"))
		if err != nil {
			return errors.New("invalid device blob timestamp")
		}
		*options.GetModTime = modified
	}
	return nil
}

func (s *deviceStorage) DeleteBlobInPath(ctx context.Context, _, file string) error {
	response, err := s.request(ctx, http.MethodDelete, file, nil, nil)
	if errors.Is(err, blob.ErrBlobNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	return response.Body.Close()
}

func (s *deviceStorage) ReadDir(ctx context.Context, path string) ([]os.FileInfo, error) {
	response, err := s.request(ctx, http.MethodGet, path, url.Values{"list": {"1"}}, nil)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	var files []DeviceFile
	decoder := json.NewDecoder(io.LimitReader(response.Body, (4<<20)+1))
	if err := decoder.Decode(&files); err != nil {
		return nil, errors.New("invalid device directory listing")
	}
	if len(files) > 20000 {
		return nil, errors.New("device directory listing exceeds its limit")
	}
	result := make([]os.FileInfo, len(files))
	for i, file := range files {
		result[i] = file
	}
	return result, nil
}
