package client

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/google/uuid"
)

// EnrollmentTokenOptions selects a protected request directory. Zero NodeID and
// TTLSeconds reuse saved values. Creation requires Initialize and a node UUID.
// Place RequestDir below a trusted parent on a local Unix filesystem.
type EnrollmentTokenOptions struct {
	RequestDir string
	Initialize bool
	NodeID     uuid.UUID
	TTLSeconds int
}

// EnrollmentToken contains public registration metadata, never the secret token.
type EnrollmentToken struct {
	ID        uuid.UUID `json:"id"`
	NodeID    uuid.UUID `json:"node_id"`
	ExpiresAt time.Time `json:"expires_at"`
	Replayed  bool      `json:"replayed"`
	TokenFile string    `json:"token_file,omitempty"`
}

type enrollmentTokenRequest struct {
	NodeID     uuid.UUID `json:"node_id"`
	Token      string    `json:"token"`
	TTLSeconds int       `json:"ttl_seconds"`
}

type enrollmentTokenState struct {
	Version               int                    `json:"version"`
	Origin                string                 `json:"origin"`
	AdministratorVerifier []byte                 `json:"administrator_verifier"`
	Request               enrollmentTokenRequest `json:"request"`
	IdempotencyKey        uuid.UUID              `json:"idempotency_key"`
	Body                  []byte                 `json:"body"`
	Response              *EnrollmentToken       `json:"response,omitempty"`
}

// String and GoString redact these values, which hold the token body and verifier.
func (enrollmentTokenState) String() string     { return "protected enrollment registration" }
func (enrollmentTokenState) GoString() string   { return "protected enrollment registration" }
func (enrollmentTokenRequest) String() string   { return "protected enrollment request" }
func (enrollmentTokenRequest) GoString() string { return "protected enrollment request" }

var errEnrollmentState = &Error{Code: "registration_state", Message: "The protected registration state requires recovery. Keep the saved request."}

// RegisterEnrollmentToken saves one request before POST and holds its process
// lock until completion is synced. Retry with the same directory and credentials.
func (c *Client) RegisterEnrollmentToken(ctx context.Context, options EnrollmentTokenOptions) (EnrollmentToken, error) {
	var result EnrollmentToken
	if err := ctx.Err(); err != nil {
		return result, err
	}
	if options.RequestDir == "" || (options.TTLSeconds != 0 && (options.TTLSeconds < 60 || options.TTLSeconds > 3600)) {
		return result, &Error{Code: "invalid_request", Message: "Set a protected request directory and a TTL from 60 to 3600 seconds."}
	}
	path, err := filepath.Abs(options.RequestDir)
	if err != nil {
		return result, errEnrollmentState
	}
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) && options.Initialize && options.NodeID != uuid.Nil {
		if err := os.Mkdir(path, 0o700); err != nil {
			return result, errEnrollmentState
		}
		parent, openErr := os.Open(filepath.Dir(path))
		if openErr != nil {
			return result, errEnrollmentState
		}
		syncErr := parent.Sync()
		closeErr := parent.Close()
		if syncErr != nil || closeErr != nil {
			return result, errEnrollmentState
		}
		info, err = os.Lstat(path)
	}
	if err != nil || !info.IsDir() || !enrollmentPrivate(info, 0o700) {
		return result, errEnrollmentState
	}
	root, err := os.OpenRoot(path)
	if err != nil {
		return result, errEnrollmentState
	}
	defer root.Close()
	directory, err := root.Open(".")
	if err != nil {
		return result, errEnrollmentState
	}
	defer directory.Close()
	openedInfo, err := directory.Stat()
	if err != nil || !os.SameFile(info, openedInfo) || !enrollmentPrivate(openedInfo, 0o700) {
		return result, errEnrollmentState
	}
	empty := true
	for {
		entries, err := directory.ReadDir(32)
		if err != nil && !errors.Is(err, io.EOF) {
			return result, errEnrollmentState
		}
		for _, entry := range entries {
			empty = false
			name := entry.Name()
			if name != "request.json" && name != "enrollment-token" && name != "lock" && !(strings.HasPrefix(name, ".registration-") && strings.HasSuffix(name, ".tmp")) {
				return result, errEnrollmentState
			}
			entryInfo, err := root.Lstat(name)
			if err != nil || !entryInfo.Mode().IsRegular() || !enrollmentPrivate(entryInfo, 0o600) {
				return result, errEnrollmentState
			}
		}
		if errors.Is(err, io.EOF) {
			break
		}
	}
	if empty && (!options.Initialize || options.NodeID == uuid.Nil) {
		return result, errEnrollmentState
	}
	flags := os.O_RDWR | syscall.O_NOFOLLOW | syscall.O_NONBLOCK
	if empty {
		flags |= os.O_CREATE
	}
	lock, err := root.OpenFile("lock", flags, 0o600)
	if err != nil {
		return result, errEnrollmentState
	}
	defer lock.Close()
	lockInfo, err := lock.Stat()
	if err != nil || !lockInfo.Mode().IsRegular() || !enrollmentPrivate(lockInfo, 0o600) {
		return result, errEnrollmentState
	}
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return result, &Error{Code: "registration_locked", Message: "Another process holds the protected registration request."}
	}
	// Recheck after locking: a concurrent initializer can have completed while we opened the directory.
	if _, err := root.Lstat("request.json"); err == nil {
		empty = false
	} else if !errors.Is(err, os.ErrNotExist) {
		return result, errEnrollmentState
	}
	origin, _ := url.Parse(c.endpoint)
	origin.Host = strings.ToLower(origin.Host)
	if origin.Port() == "443" {
		origin.Host = origin.Hostname()
		if strings.Contains(origin.Host, ":") {
			origin.Host = "[" + origin.Host + "]"
		}
	}
	administrator := sha256.Sum256([]byte(c.token))
	var state enrollmentTokenState
	if empty {
		token := make([]byte, 32)
		if _, err := rand.Read(token); err != nil {
			return result, errEnrollmentState
		}
		key, err := uuid.NewRandom()
		if err != nil {
			return result, errEnrollmentState
		}
		ttl := options.TTLSeconds
		if ttl == 0 {
			ttl = 600
		}
		state = enrollmentTokenState{Version: 1, Origin: origin.String(), AdministratorVerifier: administrator[:],
			Request: enrollmentTokenRequest{NodeID: options.NodeID, Token: base64.RawURLEncoding.EncodeToString(token), TTLSeconds: ttl}, IdempotencyKey: key}
		state.Body, err = json.Marshal(state.Request)
		if err != nil {
			return result, errEnrollmentState
		}
		data, err := json.Marshal(state)
		if err != nil {
			return result, errEnrollmentState
		}
		if err := writeEnrollmentFile(root, directory, "request.json", data, false); err != nil {
			return result, err
		}
	} else {
		data, err := readEnrollmentFile(root, "request.json", 16<<10)
		if err != nil {
			return result, errEnrollmentState
		}
		decoder := json.NewDecoder(bytes.NewReader(data))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&state); err != nil {
			return result, errEnrollmentState
		}
		if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
			return result, errEnrollmentState
		}
	}
	body, err := json.Marshal(state.Request)
	tokenBytes, tokenErr := base64.RawURLEncoding.Strict().DecodeString(state.Request.Token)
	if err != nil || tokenErr != nil || len(tokenBytes) != 32 || len(state.Request.Token) != 43 ||
		state.Version != 1 || state.Origin != origin.String() || !bytes.Equal(state.AdministratorVerifier, administrator[:]) ||
		state.Request.NodeID == uuid.Nil || state.Request.TTLSeconds < 60 || state.Request.TTLSeconds > 3600 || state.IdempotencyKey == uuid.Nil || !bytes.Equal(state.Body, body) ||
		(options.NodeID != uuid.Nil && options.NodeID != state.Request.NodeID) || (options.TTLSeconds != 0 && options.TTLSeconds != state.Request.TTLSeconds) ||
		(state.Response != nil && (!validEnrollmentToken(*state.Response, state.Request.NodeID) || state.Response.TokenFile != "")) {
		return result, errEnrollmentState
	}
	export := []byte(state.Request.Token + "\n")
	if _, err := root.Lstat("enrollment-token"); errors.Is(err, os.ErrNotExist) {
		if err := writeEnrollmentFile(root, directory, "enrollment-token", export, false); err != nil {
			return result, err
		}
	} else {
		data, err := readEnrollmentFile(root, "enrollment-token", 44)
		if err != nil || !bytes.Equal(data, export) {
			return result, errEnrollmentState
		}
	}
	// Sync the stable lock and directory even when this call recovered a prior checkpoint.
	if err := lock.Sync(); err != nil {
		return result, errEnrollmentState
	}
	if err := directory.Sync(); err != nil {
		return result, errEnrollmentState
	}
	if state.Response != nil {
		result = *state.Response
		result.Replayed = true
	} else {
		if err := c.request(ctx, http.MethodPost, "/v1/enrollment-tokens", json.RawMessage(state.Body), state.IdempotencyKey.String(), http.StatusCreated, &result); err != nil {
			return EnrollmentToken{}, err
		}
		if !validEnrollmentToken(result, state.Request.NodeID) {
			return EnrollmentToken{}, &Error{Code: "invalid_response", Message: "The API returned invalid registration metadata. Retry the saved request."}
		}
		result.TokenFile = ""
		state.Response = &result
		data, err := json.Marshal(state)
		if err != nil {
			return EnrollmentToken{}, errEnrollmentState
		}
		if err := writeEnrollmentFile(root, directory, "request.json", data, true); err != nil {
			return EnrollmentToken{}, err
		}
	}
	result.TokenFile = filepath.Join(path, "enrollment-token")
	return result, nil
}

func validEnrollmentToken(value EnrollmentToken, node uuid.UUID) bool {
	return value.ID != uuid.Nil && value.NodeID == node && !value.ExpiresAt.IsZero() && value.ExpiresAt.Year() >= 1970 && value.ExpiresAt.Year() <= 9999
}

func enrollmentPrivate(info os.FileInfo, mode os.FileMode) bool {
	stat, ok := info.Sys().(*syscall.Stat_t)
	return ok && stat.Uid == uint32(os.Geteuid()) && info.Mode().Perm() == mode && info.Mode()&(os.ModeSetuid|os.ModeSetgid|os.ModeSticky) == 0
}

func readEnrollmentFile(root *os.Root, name string, limit int64) ([]byte, error) {
	file, err := root.OpenFile(name, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if err != nil {
		return nil, errEnrollmentState
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || !enrollmentPrivate(info, 0o600) {
		return nil, errEnrollmentState
	}
	data, err := io.ReadAll(io.LimitReader(file, limit+1))
	if err != nil || int64(len(data)) > limit {
		return nil, errEnrollmentState
	}
	return data, nil
}

// A hard link publishes initial files without replacing existing evidence.
// A rename replaces the request checkpoint. Both follow a complete file sync.
func writeEnrollmentFile(root *os.Root, directory *os.File, name string, data []byte, replace bool) error {
	id, err := uuid.NewRandom()
	if err != nil {
		return errEnrollmentState
	}
	temporary := ".registration-" + id.String() + ".tmp"
	file, err := root.OpenFile(temporary, os.O_WRONLY|os.O_CREATE|os.O_EXCL|syscall.O_NOFOLLOW, 0o600)
	if err != nil {
		return errEnrollmentState
	}
	defer root.Remove(temporary)
	_, writeErr := file.Write(data)
	syncErr := file.Sync()
	closeErr := file.Close()
	if writeErr != nil || syncErr != nil || closeErr != nil {
		return errEnrollmentState
	}
	if replace {
		err = root.Rename(temporary, name)
	} else {
		err = root.Link(temporary, name)
	}
	if err != nil {
		return errEnrollmentState
	}
	if !replace {
		if err := root.Remove(temporary); err != nil {
			return errEnrollmentState
		}
	}
	if err := directory.Sync(); err != nil {
		return errEnrollmentState
	}
	return nil
}
