package repository

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/content"
	"github.com/kopia/kopia/repo/format"
)

// Error keeps provider details out of logs while preserving errors.Is and errors.As.
type Error struct {
	Action string
	Cause  error
}

func (e *Error) Error() string {
	return "repository: " + e.Action + " failed; check the destination, credentials, and connection"
}
func (e *Error) Unwrap() error { return e.Cause }

// Repository owns an open Kopia repository. Close it after its callers finish.
type Repository struct {
	repo  repo.Repository
	dir   string
	fatal atomic.Pointer[error]
	write sync.Mutex
}

// Create initializes a new encrypted repository. It refuses an existing repository.
func Create(ctx context.Context, location Location, credentials Credentials) (*Repository, error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	st, err := storage(ctx, location, credentials, true)
	if err != nil {
		return nil, &Error{Action: "create storage", Cause: errors.Join(err, ctx.Err())}
	}
	defer st.Close(context.WithoutCancel(ctx))
	if err := repo.Initialize(ctx, st, newRepositoryOptions(), credentials.Password); err != nil {
		return nil, &Error{Action: "initialize", Cause: errors.Join(err, ctx.Err())}
	}
	return open(ctx, st, credentials, "synod", nil)
}

// Open requires only the location and credentials. It uses no saved controller state.
func Open(ctx context.Context, location Location, credentials Credentials) (*Repository, error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	st, err := storage(ctx, location, credentials, false)
	if err != nil {
		return nil, &Error{Action: "open storage", Cause: errors.Join(err, ctx.Err())}
	}
	defer st.Close(context.WithoutCancel(ctx))
	return open(ctx, st, credentials, "synod", nil)
}

// open confines Kopia's connection file to a private temporary directory.
// Kopia retains the open connection in memory. No credential file or cache remains.
// The empty directory stays until Close because Kopia puts its maintenance lock there.
func open(ctx context.Context, st blob.Storage, credentials Credentials, host string, now func() time.Time) (*Repository, error) {
	dir, err := os.MkdirTemp("", "synod-repository-")
	if err != nil {
		return nil, &Error{Action: "prepare connection", Cause: errors.Join(err, ctx.Err())}
	}
	info := st.ConnectionInfo()
	config := repo.LocalConfig{Storage: &info, ClientOptions: repo.ClientOptions{Hostname: host, Username: "synod", Description: "Synod repository", EnableActions: false}, Caching: &content.CachingOptions{}}
	if credentials.CacheDirectory != "" {
		if err := os.MkdirAll(credentials.CacheDirectory, 0o700); err != nil {
			os.RemoveAll(dir)
			return nil, &Error{Action: "prepare cache", Cause: err}
		}
		// Kopia caches encrypted index and metadata blobs. File contents stay remote.
		config.Caching = &content.CachingOptions{CacheDirectory: credentials.CacheDirectory, MetadataCacheSizeBytes: 512 << 20, ContentCacheSizeBytes: 64 << 20}
	}
	encoded, err := json.Marshal(config)
	if err != nil {
		return nil, &Error{Action: "prepare connection", Cause: errors.Join(err, ctx.Err())}
	}
	file := filepath.Join(dir, "connection.json")
	if err := os.WriteFile(file, encoded, 0600); err != nil {
		os.RemoveAll(dir)
		return nil, &Error{Action: "prepare connection", Cause: errors.Join(err, ctx.Err())}
	}
	r := &Repository{dir: dir}
	r.repo, err = repo.Open(ctx, file, credentials.Password, &repo.Options{DisableRepositoryLog: true, DoNotWaitForUpgrade: true, TimeNowFunc: now, OnFatalError: func(err error) { r.fatal.Store(&err) }})
	if removeErr := os.Remove(file); err == nil && removeErr != nil {
		r.repo.Close(ctx)
		err = removeErr
	}
	if err != nil {
		os.RemoveAll(dir)
		return nil, &Error{Action: "open", Cause: errors.Join(err, ctx.Err())}
	}
	return r, nil
}

func (r *Repository) Close(ctx context.Context) error {
	err := errors.Join(r.repo.Close(ctx), os.RemoveAll(r.dir))
	if fatal := r.fatal.Load(); fatal != nil {
		err = errors.Join(err, *fatal)
	}
	if err != nil {
		return &Error{Action: "close", Cause: errors.Join(err, ctx.Err())}
	}
	return nil
}

var ErrUninitialized = errors.New("repository is not initialized")

// newRepositoryOptions selects ChaCha20-Poly1305. Arm devices without AES instructions encrypt
// about seven times faster with it than with AES-GCM, and amd64 devices lose no speed.
func newRepositoryOptions() *repo.NewRepositoryOptions {
	return &repo.NewRepositoryOptions{BlockFormat: format.ContentFormat{Hash: "BLAKE2B-256-128", Encryption: "CHACHA20-POLY1305-HMAC-SHA256"}}
}

// Connect may create a repository only for its one designated initialization job.
// The controller must not assign another initializer until that job is terminal.
func Connect(ctx context.Context, location Location, credentials Credentials, initialize bool) (*Repository, error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	st, err := storage(ctx, location, credentials, initialize)
	if err != nil {
		return nil, &Error{Action: "connect storage", Cause: errors.Join(err, ctx.Err())}
	}
	defer st.Close(context.WithoutCancel(ctx))
	_, err = st.GetMetadata(ctx, format.KopiaRepositoryBlobID)
	if err != nil {
		if !errors.Is(err, blob.ErrBlobNotFound) {
			return nil, &Error{Action: "check repository", Cause: errors.Join(err, ctx.Err())}
		}
		if !initialize {
			return nil, ErrUninitialized
		}
		if err := repo.Initialize(ctx, st, newRepositoryOptions(), credentials.Password); err != nil {
			return nil, &Error{Action: "initialize", Cause: errors.Join(err, ctx.Err())}
		}
	}
	return open(ctx, st, credentials, "synod", nil)
}

// CheckInstallation refuses to mix the recovery state of two Synod installations.
func (r *Repository) CheckInstallation(ctx context.Context, installationID uuid.UUID) error {
	if installationID == uuid.Nil {
		return ErrCatalogInvalid
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if err := r.repo.Refresh(ctx); err != nil {
		return &Error{Action: "refresh installation state", Cause: err}
	}
	for _, kind := range []string{"synod.catalog", "synod.backup"} {
		entries, err := r.repo.FindManifests(ctx, map[string]string{"type": kind})
		if err != nil {
			return &Error{Action: "check installation state", Cause: err}
		}
		for _, entry := range entries {
			if entry.Labels["installation"] != installationID.String() {
				return ErrCatalogConflict
			}
		}
	}
	return nil
}
