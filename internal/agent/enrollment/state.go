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
	"io"
	"os"
	"path/filepath"
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
		return nil, ErrConfiguration
	}
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) && options.AllowCreate {
		if err := os.Mkdir(path, 0o700); err != nil {
			return nil, ErrState
		}
		parent, openErr := os.Open(filepath.Dir(path))
		if openErr != nil {
			return nil, ErrState
		}
		syncErr := parent.Sync()
		closeErr := parent.Close()
		if syncErr != nil || closeErr != nil {
			return nil, ErrState
		}
		info, err = os.Lstat(path)
	}
	if err != nil || !info.IsDir() || !private(info) {
		return nil, ErrState
	}
	root, err := os.OpenRoot(path)
	if err != nil {
		return nil, ErrState
	}
	opened := false
	defer func() {
		if !opened {
			root.Close()
		}
	}()
	directory, err := root.Open(".")
	if err != nil {
		return nil, ErrState
	}
	entries, err := directory.ReadDir(-1)
	directory.Close()
	if err != nil {
		return nil, ErrState
	}
	empty := len(entries) == 0
	for _, entry := range entries {
		name := entry.Name()
		if name != "state.json" && name != "lock" && !(strings.HasPrefix(name, ".state-") && strings.HasSuffix(name, ".tmp")) {
			return nil, ErrState
		}
		info, err := root.Lstat(name)
		if err != nil || !info.Mode().IsRegular() || !private(info) {
			return nil, ErrState
		}
	}
	if empty && !options.AllowCreate {
		return nil, ErrState
	}
	if !empty {
		if _, err := root.Lstat("lock"); err != nil {
			return nil, ErrState
		}
	}
	lock, err := root.OpenFile("lock", os.O_RDWR|os.O_CREATE|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0o600)
	if err != nil {
		return nil, ErrState
	}
	defer func() {
		if !opened {
			lock.Close()
		}
	}()
	info, err = lock.Stat()
	if err != nil || !info.Mode().IsRegular() || !private(info) {
		return nil, ErrState
	}
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return nil, ErrLocked
	}
	s := &State{root: root, lock: lock}
	if _, err := root.Lstat("state.json"); err == nil {
		empty = false
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, ErrState
	}
	if empty {
		_, key, err := ed25519.GenerateKey(rand.Reader)
		if err != nil {
			return nil, ErrState
		}
		csr, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{}, key)
		if err != nil {
			return nil, ErrState
		}
		keyDER, err := x509.MarshalPKCS8PrivateKey(key)
		if err != nil {
			return nil, ErrState
		}
		requestID, err := uuid.NewRandom()
		if err != nil {
			return nil, ErrState
		}
		request := Request{NodeID: options.Identity.NodeID, RequestID: requestID, CSRDER: csr}
		body, err := json.Marshal(request)
		if err != nil {
			return nil, ErrState
		}
		initial := diskState{Version: 1, Identity: options.Identity, Request: request, Body: body, KeyDER: keyDER}
		if err := s.persist(initial); err != nil {
			return nil, err
		}
	} else {
		file, err := root.OpenFile("state.json", os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
		if err != nil {
			return nil, ErrState
		}
		data, readErr := readPrivate(file, 128<<10)
		file.Close()
		if readErr != nil {
			return nil, ErrState
		}
		decoder := json.NewDecoder(bytes.NewReader(data))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&s.state); err != nil {
			return nil, ErrState
		}
		if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
			return nil, ErrState
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
	if err != nil || !info.Mode().IsRegular() || !private(info) {
		return nil, ErrState
	}
	data, err := io.ReadAll(io.LimitReader(file, limit+1))
	if err != nil || int64(len(data)) > limit {
		return nil, ErrState
	}
	return data, nil
}

func (s *State) validate(identity Identity) error {
	d := s.state
	if d.Version != 1 || d.Identity != identity || d.Request.NodeID != identity.NodeID || d.Request.RequestID == uuid.Nil || len(d.Request.CSRDER) == 0 || len(d.Request.CSRDER) > 64<<10 || len(d.Body) > 64<<10 || (len(d.TokenHash) != 0 && len(d.TokenHash) != 32) || (d.Response != nil && len(d.TokenHash) != 32) {
		return ErrState
	}
	parsed, err := x509.ParsePKCS8PrivateKey(d.KeyDER)
	if err != nil {
		return ErrState
	}
	key, ok := parsed.(ed25519.PrivateKey)
	if !ok {
		return ErrState
	}
	csr, err := x509.ParseCertificateRequest(d.Request.CSRDER)
	if err != nil || csr.CheckSignature() != nil || !key.Public().(ed25519.PublicKey).Equal(csr.PublicKey) {
		return ErrState
	}
	body, err := json.Marshal(d.Request)
	if err != nil || !bytes.Equal(body, d.Body) {
		return ErrState
	}
	if d.Response != nil {
		return s.validateCertificate(d.Response)
	}
	return nil
}

func (s *State) validateCertificate(response *Response) error {
	if response == nil || response.NodeID != s.state.Identity.NodeID || response.CertificateID == uuid.Nil || response.AcceptedAt.IsZero() || response.AcceptedAt.After(time.Now().Add(time.Minute)) {
		return ErrCertificate
	}
	block, rest := pem.Decode([]byte(response.CertificatePEM))
	if block == nil || block.Type != "CERTIFICATE" || len(bytes.TrimSpace(rest)) != 0 {
		return ErrCertificate
	}
	leaf, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return ErrCertificate
	}
	caBlock, rest := pem.Decode([]byte(response.CACertificatePEM))
	if caBlock == nil || caBlock.Type != "CERTIFICATE" || len(bytes.TrimSpace(rest)) != 0 {
		return ErrCertificate
	}
	ca, err := x509.ParseCertificate(caBlock.Bytes)
	if err != nil || !ca.IsCA || !ca.BasicConstraintsValid || ca.KeyUsage&x509.KeyUsageCertSign == 0 || ca.CheckSignatureFrom(ca) != nil {
		return ErrCertificate
	}
	if _, ok := ca.PublicKey.(ed25519.PublicKey); !ok {
		return ErrCertificate
	}
	expectedCA := "spiffe://" + s.state.Identity.InstallationID.String() + ".synod/ca"
	if len(ca.URIs) != 1 || ca.URIs[0].String() != expectedCA {
		return ErrCertificate
	}
	key, err := x509.ParsePKCS8PrivateKey(s.state.KeyDER)
	if err != nil {
		return ErrState
	}
	privateKey, ok := key.(ed25519.PrivateKey)
	if !ok || !privateKey.Public().(ed25519.PublicKey).Equal(leaf.PublicKey) {
		return ErrCertificate
	}
	expected, err := (pki.Identity{InstallationID: s.state.Identity.InstallationID, Role: pki.Agent, SubjectID: s.state.Identity.NodeID}).URI()
	if err != nil || leaf.IsCA || !leaf.BasicConstraintsValid || leaf.SerialNumber == nil || leaf.SerialNumber.Sign() <= 0 || len(leaf.URIs) != 1 || leaf.URIs[0].String() != expected.String() || len(leaf.DNSNames) != 0 || len(leaf.IPAddresses) != 0 || len(leaf.EmailAddresses) != 0 || leaf.KeyUsage != x509.KeyUsageDigitalSignature || len(leaf.ExtKeyUsage) != 1 || leaf.ExtKeyUsage[0] != x509.ExtKeyUsageClientAuth || len(leaf.UnknownExtKeyUsage) != 0 {
		return ErrCertificate
	}
	roots := x509.NewCertPool()
	roots.AddCert(ca)
	if _, err := leaf.Verify(x509.VerifyOptions{Roots: roots, CurrentTime: time.Now(), KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}}); err != nil {
		return ErrCertificate
	}
	if leaf.NotBefore.Before(ca.NotBefore) || leaf.NotAfter.After(ca.NotAfter) || leaf.NotAfter.Sub(leaf.NotBefore) > 30*24*time.Hour+5*time.Minute {
		return ErrCertificate
	}
	return nil
}

// persist writes and syncs a new snapshot, renames it, then syncs its directory.
// Any failed boundary poisons this handle. Reopen decides which snapshot survived.
func (s *State) persist(next diskState) error {
	data, err := json.Marshal(next)
	if err != nil || len(data) > 128<<10 {
		s.failed = true
		return ErrState
	}
	name := ".state-" + uuid.NewString() + ".tmp"
	file, err := s.root.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL|syscall.O_NOFOLLOW, 0o600)
	if err != nil {
		s.failed = true
		return ErrState
	}
	defer s.root.Remove(name)
	_, writeErr := file.Write(data)
	syncErr := file.Sync()
	closeErr := file.Close()
	if writeErr != nil || syncErr != nil || closeErr != nil {
		s.failed = true
		return ErrState
	}
	if err := s.root.Rename(name, "state.json"); err != nil {
		s.failed = true
		return ErrState
	}
	directory, err := s.root.Open(".")
	if err != nil {
		s.failed = true
		return ErrState
	}
	syncErr = directory.Sync()
	closeErr = directory.Close()
	if syncErr != nil || closeErr != nil {
		s.failed = true
		return ErrState
	}
	s.state = next
	return nil
}

// Request returns a copy of the saved certificate request.
func (s *State) Request() (Request, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failed || s.closed {
		return Request{}, ErrState
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
		return Credentials{}, ErrState
	}
	if err := s.validateCertificate(s.state.Response); err != nil {
		return Credentials{}, err
	}
	response := s.state.Response
	pair, err := tls.X509KeyPair([]byte(response.CertificatePEM), pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: s.state.KeyDER}))
	if err != nil {
		return Credentials{}, ErrCertificate
	}
	return Credentials{Identity: s.state.Identity, RequestID: s.state.Request.RequestID, CertificateID: response.CertificateID, AcceptedAt: response.AcceptedAt, Certificate: pair, CACertificatePEM: response.CACertificatePEM}, nil
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
		return ErrState
	}
	return nil
}
