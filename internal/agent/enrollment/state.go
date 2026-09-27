// Package enrollment persists local bootstrap identity before requesting a certificate.
package enrollment

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/pki"
)

var ErrState = errors.New("enrollment state requires recovery")
var ErrLocked = errors.New("enrollment state is already open")
var ErrCertificate = errors.New("invalid or expired enrollment certificate")
var ErrConfiguration = errors.New("invalid enrollment configuration")
var ErrUnauthorized = errors.New("enrollment authorization failed")
var ErrConflict = errors.New("enrollment request conflicts with existing state")
var ErrUnavailable = errors.New("enrollment outcome unavailable; retry the saved request")
var ErrTransport = errors.New("verified enrollment HTTPS request failed")

// Identity is the installation and node that an enrollment belongs to.
type Identity struct {
	InstallationID uuid.UUID `json:"installation_id"`
	NodeID         uuid.UUID `json:"node_id"`
}

// Options holds the enrollment identity and whether creation is permitted.
type Options struct {
	Identity    Identity
	AllowCreate bool
}

// Request is the saved certificate request that a retry must repeat exactly.
type Request struct {
	NodeID    uuid.UUID `json:"node_id"`
	RequestID uuid.UUID `json:"request_id"`
	CSRDER    []byte    `json:"csr_der"`
}

// Response is the issued certificate and its identity.
type Response struct {
	NodeID           uuid.UUID `json:"node_id"`
	CertificateID    uuid.UUID `json:"certificate_id"`
	CertificatePEM   string    `json:"certificate_pem"`
	CACertificatePEM string    `json:"ca_certificate_pem"`
	AcceptedAt       time.Time `json:"accepted_at"`
	Replayed         bool      `json:"replayed"`
	// RecoveryGeneration and AgentEndpoint are what a joining agent writes into its
	// configuration. An older controller leaves them empty.
	RecoveryGeneration uuid.UUID `json:"recovery_generation,omitzero"`
	AgentEndpoint      string    `json:"agent_endpoint,omitempty"`
}
type diskState struct {
	Version   int       `json:"version"`
	Identity  Identity  `json:"identity"`
	Request   Request   `json:"request"`
	Body      []byte    `json:"body"`
	KeyDER    []byte    `json:"key_der"`
	TokenHash []byte    `json:"token_hash,omitempty"`
	Response  *Response `json:"response,omitempty"`
}

// Credentials is a completed enrollment and its TLS key pair.
type Credentials struct {
	Identity         Identity
	RequestID        uuid.UUID
	CertificateID    uuid.UUID
	AcceptedAt       time.Time
	Certificate      tls.Certificate `json:"-"`
	CACertificatePEM string
	// RecoveryGeneration and AgentEndpoint come from the enrollment response.
	RecoveryGeneration uuid.UUID
	AgentEndpoint      string
}

// String and GoString redact the credentials, which hold the private key.
func (Credentials) String() string   { return "agent enrollment credentials (private key omitted)" }
func (Credentials) GoString() string { return "agent enrollment credentials (private key omitted)" }

// State is the locked on-disk enrollment state of one agent.
type State struct {
	mu     sync.Mutex
	root   *os.Root
	lock   *os.File
	state  diskState
	failed bool
	closed bool
}

// String and GoString redact the state, which holds the private key.
func (*State) String() string   { return "protected agent enrollment state" }
func (*State) GoString() string { return "protected agent enrollment state" }

// Open holds a process lock. Creation is explicit and requires an absent or empty directory.
// The caller must place that directory below a trusted parent.
func Open(path string, options Options) (*State, error) {
	if options.Identity.InstallationID == uuid.Nil || options.Identity.NodeID == uuid.Nil {
		return nil, fmt.Errorf("%w: the installation and the node must both be named", ErrConfiguration)
	}
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) && options.AllowCreate {
		if err := os.Mkdir(path, 0o700); err != nil {
			return nil, fmt.Errorf("%w: create the state directory: %w", ErrState, err)
		}
		parent, openErr := os.Open(filepath.Dir(path))
		if openErr != nil {
			return nil, fmt.Errorf("%w: open the state directory's parent: %w", ErrState, openErr)
		}
		syncErr := parent.Sync()
		closeErr := parent.Close()
		if syncErr != nil || closeErr != nil {
			return nil, fmt.Errorf("%w: sync the state directory's parent: %w", ErrState, errors.Join(syncErr, closeErr))
		}
		info, err = os.Lstat(path)
	}
	if err != nil {
		return nil, fmt.Errorf("%w: stat the state directory: %w", ErrState, err)
	}
	if !info.IsDir() || !private(info) {
		return nil, fmt.Errorf("%w: the state directory must be a directory owned by this user with no group or other permission", ErrState)
	}
	root, err := os.OpenRoot(path)
	if err != nil {
		return nil, fmt.Errorf("%w: open the state directory as a root: %w", ErrState, err)
	}
	opened := false
	defer func() {
		if !opened {
			root.Close()
		}
	}()
	directory, err := root.Open(".")
	if err != nil {
		return nil, fmt.Errorf("%w: open the state directory: %w", ErrState, err)
	}
	entries, err := directory.ReadDir(-1)
	directory.Close()
	if err != nil {
		return nil, fmt.Errorf("%w: read the state directory: %w", ErrState, err)
	}
	empty := len(entries) == 0
	for _, entry := range entries {
		name := entry.Name()
		if name != "state.json" && name != "lock" && !(strings.HasPrefix(name, ".state-") && strings.HasSuffix(name, ".tmp")) {
			return nil, fmt.Errorf("%w: the state directory holds %q, which enrollment did not write", ErrState, name)
		}
		info, err := root.Lstat(name)
		if err != nil {
			return nil, fmt.Errorf("%w: stat %q: %w", ErrState, name, err)
		}
		if !info.Mode().IsRegular() || !private(info) {
			return nil, fmt.Errorf("%w: %q must be a regular file owned by this user with no group or other permission", ErrState, name)
		}
	}
	if empty && !options.AllowCreate {
		return nil, fmt.Errorf("%w: the state directory is empty and creation was not requested", ErrState)
	}
	if !empty {
		if _, err := root.Lstat("lock"); err != nil {
			return nil, fmt.Errorf("%w: the state directory holds no lock: %w", ErrState, err)
		}
	}
	lock, err := root.OpenFile("lock", os.O_RDWR|os.O_CREATE|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0o600)
	if err != nil {
		return nil, fmt.Errorf("%w: open the lock: %w", ErrState, err)
	}
	defer func() {
		if !opened {
			lock.Close()
		}
	}()
	info, err = lock.Stat()
	if err != nil {
		return nil, fmt.Errorf("%w: stat the lock: %w", ErrState, err)
	}
	if !info.Mode().IsRegular() || !private(info) {
		return nil, fmt.Errorf("%w: the lock must be a regular file owned by this user with no group or other permission", ErrState)
	}
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrLocked, err)
	}
	s := &State{root: root, lock: lock}
	if _, err := root.Lstat("state.json"); err == nil {
		empty = false
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("%w: stat state.json: %w", ErrState, err)
	}
	if empty {
		_, key, err := ed25519.GenerateKey(rand.Reader)
		if err != nil {
			return nil, fmt.Errorf("%w: generate the node key: %w", ErrState, err)
		}
		csr, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{}, key)
		if err != nil {
			return nil, fmt.Errorf("%w: create the certificate request: %w", ErrState, err)
		}
		keyDER, err := x509.MarshalPKCS8PrivateKey(key)
		if err != nil {
			return nil, fmt.Errorf("%w: encode the node key: %w", ErrState, err)
		}
		requestID, err := uuid.NewRandom()
		if err != nil {
			return nil, fmt.Errorf("%w: generate the request identity: %w", ErrState, err)
		}
		request := Request{NodeID: options.Identity.NodeID, RequestID: requestID, CSRDER: csr}
		body, err := json.Marshal(request)
		if err != nil {
			return nil, fmt.Errorf("%w: encode the certificate request body: %w", ErrState, err)
		}
		initial := diskState{Version: 1, Identity: options.Identity, Request: request, Body: body, KeyDER: keyDER}
		if err := s.persist(initial); err != nil {
			return nil, err
		}
	} else {
		file, err := root.OpenFile("state.json", os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
		if err != nil {
			return nil, fmt.Errorf("%w: open state.json: %w", ErrState, err)
		}
		data, readErr := readPrivate(file, 128<<10)
		file.Close()
		if readErr != nil {
			return nil, fmt.Errorf("read state.json: %w", readErr)
		}
		decoder := json.NewDecoder(bytes.NewReader(data))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&s.state); err != nil {
			return nil, fmt.Errorf("%w: decode state.json: %w", ErrState, err)
		}
		if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
			return nil, fmt.Errorf("%w: state.json carries content after the state document", ErrState)
		}
		if err := s.validate(options.Identity); err != nil {
			return nil, err
		}
	}
	opened = true
	return s, nil
}

func private(info os.FileInfo) bool {
	stat, ok := info.Sys().(*syscall.Stat_t)
	return ok && stat.Uid == uint32(os.Geteuid()) && info.Mode().Perm()&0o077 == 0
}

func readPrivate(file *os.File, limit int64) ([]byte, error) {
	info, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("%w: stat %s: %w", ErrState, file.Name(), err)
	}
	if !info.Mode().IsRegular() || !private(info) {
		return nil, fmt.Errorf("%w: %s must be a regular file owned by this user with no group or other permission", ErrState, file.Name())
	}
	data, err := io.ReadAll(io.LimitReader(file, limit+1))
	if err != nil {
		return nil, fmt.Errorf("%w: read %s: %w", ErrState, file.Name(), err)
	}
	if int64(len(data)) > limit {
		return nil, fmt.Errorf("%w: %s exceeds %d bytes", ErrState, file.Name(), limit)
	}
	return data, nil
}

func (s *State) validate(identity Identity) error {
	d := s.state
	if d.Version != 1 || d.Identity != identity || d.Request.NodeID != identity.NodeID || d.Request.RequestID == uuid.Nil || len(d.Request.CSRDER) == 0 || len(d.Request.CSRDER) > 64<<10 || len(d.Body) > 64<<10 || (len(d.TokenHash) != 0 && len(d.TokenHash) != 32) || (d.Response != nil && len(d.TokenHash) != 32) {
		return fmt.Errorf("%w: the saved state does not describe version 1 of the enrollment of node %s", ErrState, identity.NodeID)
	}
	parsed, err := x509.ParsePKCS8PrivateKey(d.KeyDER)
	if err != nil {
		return fmt.Errorf("%w: parse the saved node key: %w", ErrState, err)
	}
	key, ok := parsed.(ed25519.PrivateKey)
	if !ok {
		return fmt.Errorf("%w: the saved node key is a %T and an Ed25519 key is required", ErrState, parsed)
	}
	csr, err := x509.ParseCertificateRequest(d.Request.CSRDER)
	if err != nil {
		return fmt.Errorf("%w: parse the saved certificate request: %w", ErrState, err)
	}
	if err := csr.CheckSignature(); err != nil {
		return fmt.Errorf("%w: check the saved certificate request signature: %w", ErrState, err)
	}
	if !key.Public().(ed25519.PublicKey).Equal(csr.PublicKey) {
		return fmt.Errorf("%w: the saved certificate request does not carry the saved node key", ErrState)
	}
	body, err := json.Marshal(d.Request)
	if err != nil {
		return fmt.Errorf("%w: re-encode the saved certificate request: %w", ErrState, err)
	}
	if !bytes.Equal(body, d.Body) {
		return fmt.Errorf("%w: the saved request body is not the encoding of the saved request, so a retry would not repeat it exactly", ErrState)
	}
	if d.Response != nil {
		return s.validateCertificate(d.Response)
	}
	return nil
}

func (s *State) validateCertificate(response *Response) error {
	if response == nil || response.NodeID != s.state.Identity.NodeID || response.CertificateID == uuid.Nil || response.AcceptedAt.IsZero() || response.AcceptedAt.After(time.Now().Add(time.Minute)) {
		return fmt.Errorf("%w: the response does not name an acceptance of node %s", ErrCertificate, s.state.Identity.NodeID)
	}
	block, rest := pem.Decode([]byte(response.CertificatePEM))
	if block == nil || block.Type != "CERTIFICATE" || len(bytes.TrimSpace(rest)) != 0 {
		return fmt.Errorf("%w: the issued material is not one bare CERTIFICATE block", ErrCertificate)
	}
	leaf, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return fmt.Errorf("%w: parse the issued certificate: %w", ErrCertificate, err)
	}
	caBlock, rest := pem.Decode([]byte(response.CACertificatePEM))
	if caBlock == nil || caBlock.Type != "CERTIFICATE" || len(bytes.TrimSpace(rest)) != 0 {
		return fmt.Errorf("%w: the delivered trust material is not one bare CERTIFICATE block", ErrCertificate)
	}
	ca, err := x509.ParseCertificate(caBlock.Bytes)
	if err != nil {
		return fmt.Errorf("%w: parse the delivered CA: %w", ErrCertificate, err)
	}
	if !ca.IsCA || !ca.BasicConstraintsValid || ca.KeyUsage&x509.KeyUsageCertSign == 0 {
		return fmt.Errorf("%w: the delivered CA cannot sign", ErrCertificate)
	}
	if err := ca.CheckSignatureFrom(ca); err != nil {
		return fmt.Errorf("%w: the delivered CA is not self-signed: %w", ErrCertificate, err)
	}
	if _, ok := ca.PublicKey.(ed25519.PublicKey); !ok {
		return fmt.Errorf("%w: the delivered CA carries a %T public key and an Ed25519 key is required", ErrCertificate, ca.PublicKey)
	}
	expectedCA := "spiffe://" + s.state.Identity.InstallationID.String() + ".synod/ca"
	if len(ca.URIs) != 1 || ca.URIs[0].String() != expectedCA {
		return fmt.Errorf("%w: the delivered CA names %v and this node expects %q", ErrCertificate, ca.URIs, expectedCA)
	}
	key, err := x509.ParsePKCS8PrivateKey(s.state.KeyDER)
	if err != nil {
		return fmt.Errorf("%w: parse the saved node key: %w", ErrState, err)
	}
	privateKey, ok := key.(ed25519.PrivateKey)
	if !ok || !privateKey.Public().(ed25519.PublicKey).Equal(leaf.PublicKey) {
		return fmt.Errorf("%w: the issued certificate does not carry this node's key", ErrCertificate)
	}
	expected, err := (pki.Identity{InstallationID: s.state.Identity.InstallationID, Role: pki.Agent, SubjectID: s.state.Identity.NodeID}).URI()
	if err != nil {
		return fmt.Errorf("%w: build the expected agent URI: %w", ErrCertificate, err)
	}
	if leaf.IsCA || !leaf.BasicConstraintsValid || leaf.SerialNumber == nil || leaf.SerialNumber.Sign() <= 0 || len(leaf.URIs) != 1 || leaf.URIs[0].String() != expected.String() || len(leaf.DNSNames) != 0 || len(leaf.IPAddresses) != 0 || len(leaf.EmailAddresses) != 0 || leaf.KeyUsage != x509.KeyUsageDigitalSignature || !slices.Equal(leaf.ExtKeyUsage, []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth}) || len(leaf.UnknownExtKeyUsage) != 0 {
		return fmt.Errorf("%w: the issued certificate is not a client-auth agent leaf named %q", ErrCertificate, expected)
	}
	roots := x509.NewCertPool()
	roots.AddCert(ca)
	if _, err := leaf.Verify(x509.VerifyOptions{Roots: roots, CurrentTime: time.Now(), KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}}); err != nil {
		return fmt.Errorf("%w: verify the issued certificate against the delivered CA: %w", ErrCertificate, err)
	}
	if leaf.NotBefore.Before(ca.NotBefore) || leaf.NotAfter.After(ca.NotAfter) || leaf.NotAfter.Sub(leaf.NotBefore) > 30*24*time.Hour+5*time.Minute {
		return fmt.Errorf("%w: the issued certificate lives from %s to %s, outside its CA or longer than 30 days", ErrCertificate, leaf.NotBefore.UTC().Format(time.RFC3339), leaf.NotAfter.UTC().Format(time.RFC3339))
	}
	return nil
}

// persist writes and syncs a new snapshot, renames it, then syncs its directory.
// Any failed boundary poisons this handle. Reopen decides which snapshot survived.
func (s *State) persist(next diskState) error {
	data, err := json.Marshal(next)
	if err != nil {
		s.failed = true
		return fmt.Errorf("%w: encode the next state: %w", ErrState, err)
	}
	if len(data) > 128<<10 {
		s.failed = true
		return fmt.Errorf("%w: the next state is %d bytes and the limit is %d", ErrState, len(data), 128<<10)
	}
	name := ".state-" + uuid.NewString() + ".tmp"
	file, err := s.root.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL|syscall.O_NOFOLLOW, 0o600)
	if err != nil {
		s.failed = true
		return fmt.Errorf("%w: create the state snapshot: %w", ErrState, err)
	}
	defer s.root.Remove(name)
	_, writeErr := file.Write(data)
	syncErr := file.Sync()
	closeErr := file.Close()
	if writeErr != nil || syncErr != nil || closeErr != nil {
		s.failed = true
		return fmt.Errorf("%w: write the state snapshot: %w", ErrState, errors.Join(writeErr, syncErr, closeErr))
	}
	if err := s.root.Rename(name, "state.json"); err != nil {
		s.failed = true
		return fmt.Errorf("%w: rename the state snapshot over state.json: %w", ErrState, err)
	}
	directory, err := s.root.Open(".")
	if err != nil {
		s.failed = true
		return fmt.Errorf("%w: open the state directory to sync the rename: %w", ErrState, err)
	}
	syncErr = directory.Sync()
	closeErr = directory.Close()
	if syncErr != nil || closeErr != nil {
		s.failed = true
		return fmt.Errorf("%w: sync the state directory after the rename: %w", ErrState, errors.Join(syncErr, closeErr))
	}
	s.state = next
	return nil
}

// Request returns a copy of the saved certificate request.
func (s *State) Request() (Request, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failed || s.closed {
		return Request{}, fmt.Errorf("%w: this handle is closed or poisoned by an earlier failed write", ErrState)
	}
	request := s.state.Request
	request.CSRDER = bytes.Clone(request.CSRDER)
	return request, nil
}

// Credentials returns the completed enrollment and its checked certificate.
func (s *State) Credentials() (Credentials, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.credentials()
}
func (s *State) credentials() (Credentials, error) {
	if s.failed || s.closed || s.state.Response == nil {
		return Credentials{}, fmt.Errorf("%w: this handle is closed, poisoned by an earlier failed write, or holds no accepted enrollment", ErrState)
	}
	if err := s.validateCertificate(s.state.Response); err != nil {
		return Credentials{}, err
	}
	response := s.state.Response
	pair, err := tls.X509KeyPair([]byte(response.CertificatePEM), pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: s.state.KeyDER}))
	if err != nil {
		return Credentials{}, fmt.Errorf("%w: load the issued certificate and the saved node key as a pair: %w", ErrCertificate, err)
	}
	return Credentials{Identity: s.state.Identity, RequestID: s.state.Request.RequestID, CertificateID: response.CertificateID, AcceptedAt: response.AcceptedAt, Certificate: pair, CACertificatePEM: response.CACertificatePEM, RecoveryGeneration: response.RecoveryGeneration, AgentEndpoint: response.AgentEndpoint}, nil
}

// Close releases the process lock. It is safe to call more than once.
func (s *State) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	lockErr := s.lock.Close()
	rootErr := s.root.Close()
	if lockErr != nil || rootErr != nil {
		return fmt.Errorf("%w: release the state lock and root: %w", ErrState, errors.Join(lockErr, rootErr))
	}
	return nil
}
