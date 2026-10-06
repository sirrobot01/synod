package repository

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/blob/sharded"
	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/knownhosts"
)

// sftpOptions is saved only in a private, temporary Kopia connection file.
type sftpOptions struct {
	Path       string `json:"path"`
	Host       string `json:"host"`
	Port       int    `json:"port"`
	User       string `json:"user"`
	KnownHosts string `json:"known_hosts"`
	Key        string `json:"key"`
}

type sftpStorage struct {
	sharded.Storage
	blob.DefaultProviderImplementation
	options    sftpOptions
	client     *sftp.Client
	connection *ssh.Client
}

func init() { blob.AddSupportedStorage("synod-sftp", sftpOptions{}, newSFTP) }

// newSFTP verifies the host key and bounds the TCP and SSH handshake.
func newSFTP(ctx context.Context, options *sftpOptions, create bool) (blob.Storage, error) {
	key, err := ssh.ParsePrivateKey([]byte(options.Key))
	if err != nil {
		return nil, err
	}
	known, err := os.CreateTemp("", "synod-known-hosts-")
	if err != nil {
		return nil, err
	}
	defer os.Remove(known.Name())
	_, writeErr := known.WriteString(options.KnownHosts)
	if err := errors.Join(writeErr, known.Close()); err != nil {
		return nil, err
	}
	verify, err := knownhosts.New(known.Name())
	if err != nil {
		return nil, err
	}
	address := net.JoinHostPort(options.Host, strconv.Itoa(options.Port))
	raw, err := (&net.Dialer{Timeout: 15 * time.Second}).DialContext(ctx, "tcp", address)
	if err != nil {
		return nil, err
	}
	stop := context.AfterFunc(ctx, func() { raw.Close() })
	defer stop()
	if err := raw.SetDeadline(time.Now().Add(15 * time.Second)); err != nil {
		raw.Close()
		return nil, err
	}
	conn, channels, requests, err := ssh.NewClientConn(raw, address, &ssh.ClientConfig{User: options.User, Auth: []ssh.AuthMethod{ssh.PublicKeys(key)}, HostKeyCallback: verify})
	if err != nil {
		raw.Close()
		return nil, err
	}
	sshClient := ssh.NewClient(conn, channels, requests)
	client, err := sftp.NewClient(sshClient)
	if err != nil {
		sshClient.Close()
		return nil, err
	}
	if _, ok := client.HasExtension("posix-rename@openssh.com"); !ok {
		sshClient.Close()
		return nil, errors.New("SFTP server needs atomic POSIX rename")
	}
	if err := raw.SetDeadline(time.Time{}); err != nil {
		sshClient.Close()
		return nil, err
	}
	if create {
		err = client.MkdirAll(options.Path)
	} else {
		_, err = client.Stat(options.Path)
	}
	if err != nil || ctx.Err() != nil {
		sshClient.Close()
		return nil, errors.Join(err, ctx.Err())
	}
	s := &sftpStorage{options: *options, client: client, connection: sshClient}
	s.Storage = sharded.New(s, options.Path, sharded.Options{}, create)
	return s, nil
}

func (s *sftpStorage) ConnectionInfo() blob.ConnectionInfo {
	return blob.ConnectionInfo{Type: "synod-sftp", Config: &s.options}
}
func (s *sftpStorage) DisplayName() string         { return "Synod SFTP repository" }
func (s *sftpStorage) Close(context.Context) error { return s.connection.Close() }

// Cancelling any request closes this connection. The caller must reopen the repository.
func (s *sftpStorage) GetBlobFromPath(ctx context.Context, _, file string, offset, length int64, out blob.OutputBuffer) error {
	stop := context.AfterFunc(ctx, func() { s.connection.Close() })
	defer stop()
	if err := ctx.Err(); err != nil {
		return err
	}
	f, err := s.client.Open(file)
	if os.IsNotExist(err) {
		return blob.ErrBlobNotFound
	}
	if err != nil {
		return err
	}
	defer f.Close()
	out.Reset()
	if length < 0 {
		_, err = io.Copy(out, f)
		return err
	}
	if offset < 0 {
		return blob.ErrInvalidRange
	}
	if _, err := f.Seek(offset, io.SeekStart); err != nil {
		return err
	}
	if _, err := io.Copy(out, io.LimitReader(f, length)); err != nil {
		return err
	}
	return blob.EnsureLengthExactly(out.Length(), length)
}

func (s *sftpStorage) GetMetadataFromPath(ctx context.Context, _, file string) (blob.Metadata, error) {
	stop := context.AfterFunc(ctx, func() { s.connection.Close() })
	defer stop()
	if err := ctx.Err(); err != nil {
		return blob.Metadata{}, err
	}
	info, err := s.client.Stat(file)
	if os.IsNotExist(err) {
		return blob.Metadata{}, blob.ErrBlobNotFound
	}
	if err != nil {
		return blob.Metadata{}, err
	}
	return blob.Metadata{Length: info.Size(), Timestamp: info.ModTime()}, nil
}

func (s *sftpStorage) PutBlobInPath(ctx context.Context, dir, file string, data blob.Bytes, options blob.PutOptions) error {
	if options.HasRetentionOptions() || options.DoNotRecreate {
		return blob.ErrUnsupportedPutBlobOption
	}
	stop := context.AfterFunc(ctx, func() { s.connection.Close() })
	defer stop()
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := s.client.MkdirAll(dir); err != nil {
		return err
	}
	temporary := file + ".tmp." + uuid.NewString()
	f, err := s.client.OpenFile(temporary, os.O_WRONLY|os.O_CREATE|os.O_EXCL)
	if err != nil {
		return err
	}
	defer s.client.Remove(temporary)
	_, writeErr := data.WriteTo(f)
	if err := errors.Join(writeErr, f.Close()); err != nil {
		return err
	}
	if !options.SetModTime.IsZero() {
		if err := s.client.Chtimes(temporary, options.SetModTime, options.SetModTime); err != nil {
			return err
		}
	}
	if err := s.client.PosixRename(temporary, file); err != nil {
		return err
	}
	if options.GetModTime != nil {
		info, err := s.client.Stat(file)
		if err != nil {
			return err
		}
		*options.GetModTime = info.ModTime()
	}
	return nil
}

func (s *sftpStorage) DeleteBlobInPath(ctx context.Context, _, file string) error {
	stop := context.AfterFunc(ctx, func() { s.connection.Close() })
	defer stop()
	if err := ctx.Err(); err != nil {
		return err
	}
	err := s.client.Remove(file)
	if os.IsNotExist(err) {
		return nil
	}
	return err
}

func (s *sftpStorage) ReadDir(ctx context.Context, dir string) ([]os.FileInfo, error) {
	stop := context.AfterFunc(ctx, func() { s.connection.Close() })
	defer stop()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	entries, err := s.client.ReadDir(dir)
	if os.IsNotExist(err) {
		return nil, nil
	}
	return entries, err
}
