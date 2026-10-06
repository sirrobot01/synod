// Package repository stores encrypted backups and the recovery catalog through Kopia.
package repository

import (
	"context"
	"errors"
	"net/url"
	"path/filepath"
	"strings"

	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/blob/filesystem"
	"github.com/kopia/kopia/repo/blob/s3"
)

// Location contains connection settings. Credentials are supplied separately.
type Location struct {
	Kind string        `json:"kind"`
	Path string        `json:"path,omitempty"`
	SFTP *SFTPLocation `json:"sftp,omitzero"`
	S3   *S3Location   `json:"s3,omitzero"`
}

type SFTPLocation struct {
	Host       string `json:"host"`
	Port       int    `json:"port"`
	User       string `json:"user"`
	KnownHosts string `json:"known_hosts"`
}

type S3Location struct {
	Endpoint string `json:"endpoint"`
	Bucket   string `json:"bucket"`
	Prefix   string `json:"prefix,omitempty"`
	Region   string `json:"region,omitempty"`
}

// Credentials must be wrapped before persistent storage outside the repository.
type Credentials struct {
	Password        string `json:"password"`
	SSHKey          string `json:"ssh_key,omitempty"`
	AccessKeyID     string `json:"access_key_id,omitempty"`
	SecretAccessKey string `json:"secret_access_key,omitempty"`
	SessionToken    string `json:"session_token,omitempty"`
}

func (Credentials) String() string   { return "[repository credentials]" }
func (Credentials) GoString() string { return "[repository credentials]" }

var ErrLocation = errors.New("invalid repository location")
var ErrCredentials = errors.New("repository credentials are missing")

// Validate checks settings without contacting the destination.
func Validate(location Location, credentials Credentials) error {
	if credentials.Password == "" {
		return ErrCredentials
	}
	switch location.Kind {
	case "local":
		if !filepath.IsAbs(location.Path) || filepath.Clean(location.Path) != location.Path || location.SFTP != nil || location.S3 != nil {
			return ErrLocation
		}
		if credentials.SSHKey != "" || credentials.AccessKeyID != "" || credentials.SecretAccessKey != "" || credentials.SessionToken != "" {
			return ErrCredentials
		}
	case "sftp":
		if location.SFTP == nil || location.S3 != nil || !strings.HasPrefix(location.Path, "/") {
			return ErrLocation
		}
		remote := location.SFTP
		if remote.Host == "" || strings.ContainsAny(remote.Host, "/@ \t\r\n") || remote.User == "" || remote.Port < 1 || remote.Port > 65535 || remote.KnownHosts == "" {
			return ErrLocation
		}
		if credentials.SSHKey == "" || credentials.AccessKeyID != "" || credentials.SecretAccessKey != "" || credentials.SessionToken != "" {
			return ErrCredentials
		}
	case "s3":
		if location.S3 == nil || location.SFTP != nil || location.Path != "" {
			return ErrLocation
		}
		remote := location.S3
		endpoint, err := url.Parse(remote.Endpoint)
		if err != nil || (endpoint.Scheme != "https" && endpoint.Scheme != "http") || endpoint.Host == "" || endpoint.User != nil || endpoint.Path != "" || endpoint.RawQuery != "" || endpoint.Fragment != "" || remote.Bucket == "" {
			return ErrLocation
		}
		if credentials.AccessKeyID == "" || credentials.SecretAccessKey == "" || credentials.SSHKey != "" {
			return ErrCredentials
		}
	default:
		return ErrLocation
	}
	return nil
}

func storage(ctx context.Context, location Location, credentials Credentials, create bool) (blob.Storage, error) {
	if err := Validate(location, credentials); err != nil {
		return nil, err
	}
	switch location.Kind {
	case "local":
		return filesystem.New(ctx, &filesystem.Options{Path: location.Path, FileMode: 0600, DirectoryMode: 0700}, create)
	case "sftp":
		remote := location.SFTP
		return newSFTP(ctx, &sftpOptions{Path: location.Path, Host: remote.Host, Port: remote.Port, User: remote.User, KnownHosts: remote.KnownHosts, Key: credentials.SSHKey}, create)
	case "s3":
		remote := location.S3
		endpoint, _ := url.Parse(remote.Endpoint)
		return s3.New(ctx, &s3.Options{Endpoint: endpoint.Host, DoNotUseTLS: endpoint.Scheme == "http", BucketName: remote.Bucket, Prefix: remote.Prefix, Region: remote.Region, AccessKeyID: credentials.AccessKeyID, SecretAccessKey: credentials.SecretAccessKey, SessionToken: credentials.SessionToken}, create)
	default:
		return nil, ErrLocation
	}
}
