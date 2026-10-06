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
	if err := repo.Initialize(ctx, st, nil, credentials.Password); err != nil {
		return nil, &Error{Action: "initialize", Cause: errors.Join(err, ctx.Err())}
	}
	return open(ctx, st, credentials.Password)
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
	return open(ctx, st, credentials.Password)
}

// open confines Kopia's connection file to a private temporary directory.
// Kopia retains the open connection in memory. No credential file or cache remains.
func open(ctx context.Context, st blob.Storage, password string) (*Repository, error) {
	dir, err := os.MkdirTemp("", "synod-repository-")
	if err != nil {
		return nil, &Error{Action: "prepare connection", Cause: errors.Join(err, ctx.Err())}
	}
	defer os.RemoveAll(dir)
	info := st.ConnectionInfo()
	config := repo.LocalConfig{Storage: &info, ClientOptions: repo.ClientOptions{Hostname: "synod", Username: "synod", Description: "Synod repository", EnableActions: false}, Caching: &content.CachingOptions{}}
	encoded, err := json.Marshal(config)
	if err != nil {
		return nil, &Error{Action: "prepare connection", Cause: errors.Join(err, ctx.Err())}
	}
	file := filepath.Join(dir, "connection.json")
	if err := os.WriteFile(file, encoded, 0600); err != nil {
		return nil, &Error{Action: "prepare connection", Cause: errors.Join(err, ctx.Err())}
	}
	r := new(Repository)
	r.repo, err = repo.Open(ctx, file, password, &repo.Options{DisableRepositoryLog: true, DoNotWaitForUpgrade: true, OnFatalError: func(err error) { r.fatal.Store(&err) }})
	if err != nil {
		return nil, &Error{Action: "open", Cause: errors.Join(err, ctx.Err())}
	}
	return r, nil
}

func (r *Repository) Close(ctx context.Context) error {
	err := r.repo.Close(ctx)
	if fatal := r.fatal.Load(); fatal != nil {
		err = errors.Join(err, *fatal)
	}
	if err != nil {
		return &Error{Action: "close", Cause: errors.Join(err, ctx.Err())}
	}
	return nil
}

var ErrUninitialized = errors.New("repository is not initialized")

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
		if err := repo.Initialize(ctx, st, nil, credentials.Password); err != nil {
			return nil, &Error{Action: "initialize", Cause: errors.Join(err, ctx.Err())}
		}
	}
	return open(ctx, st, credentials.Password)
}
